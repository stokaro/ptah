//go:build integration

package integration_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A MySQL or MariaDB view declared without SQL SECURITY keeps the mode the
// database holds it with, and the plan records the mode before and after each
// statement on the view (stokaro/ptah#6022). CREATE OR REPLACE VIEW without the
// clause selects DEFINER and makes the account that applies it the definer, so
// a view held as INVOKER and replaced for a body change would run with that
// account's privileges.

// sqlSecurityEngine is one server of the MySQL family, reached with an account
// that may create databases.
type sqlSecurityEngine struct {
	name   string
	engine dbtarget.Engine
}

var sqlSecurityEngines = []sqlSecurityEngine{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

// sqlSecurityTarget is a database holding the table docs and the view my_docs,
// created with SQL SECURITY INVOKER.
type sqlSecurityTarget struct {
	db       *sql.DB
	database string
	url      string
}

func newSQLSecurityTarget(c *qt.C, engine dbtarget.Engine) sqlSecurityTarget {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	name, _ := scratch.database(c, "view_sql_security")
	for _, statement := range []string{
		"CREATE TABLE `" + name + "`.docs (id int PRIMARY KEY, owner_name varchar(64) NOT NULL)",
		"CREATE SQL SECURITY INVOKER VIEW `" + name + "`.my_docs AS SELECT id, owner_name FROM `" + name + "`.docs",
	} {
		_, err := scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return sqlSecurityTarget{
		db:       scratch.admin,
		database: name,
		url:      replaceMySQLDatabaseName(c, scratch.adminURL, name),
	}
}

// held reads back the mode information_schema.VIEWS reports for my_docs.
func (t sqlSecurityTarget) held(c *qt.C) string {
	c.Helper()
	var mode string
	c.Assert(t.db.QueryRowContext(c.Context(),
		"SELECT SECURITY_TYPE FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'my_docs'",
		t.database).Scan(&mode), qt.IsNil)
	return mode
}

// schemaFile writes a SQL schema declaring docs and my_docs, with the view's
// header clause and body given, and returns its path.
func (t sqlSecurityTarget) schemaFile(c *qt.C, clause, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	schema := "CREATE TABLE docs (id int PRIMARY KEY, owner_name varchar(64) NOT NULL);\n" +
		"CREATE " + clause + "VIEW my_docs AS " + body + ";\n"
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	return path
}

// sqlSecurityEffect is what a document says one statement does to the SQL
// SECURITY mode of my_docs, in the document's own words.
type sqlSecurityEffect struct {
	Version int
	Action  string
	Before  string
	After   string
	Changed bool
}

// sqlSecurityUnit is a statement of a saved plan, or a unit of an apply
// document, read the way a consumer reads it.
type sqlSecurityUnit struct {
	Effects []struct {
		Version int `json:"version"`
		Object  struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"object"`
		Action     string `json:"action"`
		Properties []struct {
			Name    string `json:"name"`
			Before  string `json:"before"`
			After   string `json:"after"`
			Changed bool   `json:"changed"`
		} `json:"properties"`
	} `json:"effects"`
}

// sqlSecurityDocument is the part of a saved plan or an apply document that
// carries effects.
type sqlSecurityDocument struct {
	Outcome    string            `json:"outcome"`
	Statements []sqlSecurityUnit `json:"statements"`
	Units      []sqlSecurityUnit `json:"units"`
}

// sqlSecurityEffects reads what each unit says it does to the mode of my_docs,
// in unit order. A unit that says nothing about it is left out.
func sqlSecurityEffects(units []sqlSecurityUnit) []sqlSecurityEffect {
	var found []sqlSecurityEffect
	for _, unit := range units {
		for _, effect := range unit.Effects {
			if effect.Object.Kind != "view" || effect.Object.Name != "my_docs" {
				continue
			}
			for _, property := range effect.Properties {
				if property.Name == "sql_security" {
					found = append(found, sqlSecurityEffect{
						Version: effect.Version, Action: effect.Action,
						Before: property.Before, After: property.After, Changed: property.Changed,
					})
				}
			}
		}
	}
	return found
}

// TestMySQLViewSQLSecurityE2E changes my_docs, which the database holds as
// INVOKER, through a saved plan, and reads back the mode the server holds and
// what the plan said each statement does to it. A declaration that names no
// mode keeps INVOKER through the replacement a body change plans, and the
// effect says the mode is unchanged. A declaration that names DEFINER changes
// it, and the effect says so.
func TestMySQLViewSQLSecurityE2E(t *testing.T) {
	tests := []struct {
		name   string
		clause string
		body   string
		want   string
	}{
		{name: "a body change that names no mode", clause: "", body: "SELECT id, owner_name FROM docs WHERE id > 0", want: "INVOKER"},
		{name: "a declared DEFINER", clause: "SQL SECURITY DEFINER ", body: "SELECT id, owner_name FROM docs", want: "DEFINER"},
	}
	for _, engine := range sqlSecurityEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newSQLSecurityTarget(c, engine.engine)
				schema := target.schemaFile(c, test.clause, test.body)
				planPath := filepath.Join(c.TempDir(), "my_docs.plan.json")
				before := target.held(c)

				planned := runPtahIn(c, "schema", "plan", "--db-url", target.url, "--schema-file", schema, "--output", planPath)
				saved, err := os.ReadFile(planPath)
				c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
				applied := runPtahIn(c, "schema", "apply", "--db-url", target.url, "--plan", planPath, "--auto-approve", "--json")
				again := runPtahIn(c, "schema", "plan", "--db-url", target.url, "--schema-file", schema, "--dry-run", "--json")

				assertRunExited(c, planned, 0)
				assertRunExited(c, applied, 0)
				assertRunExited(c, again, 0)
				var plan, apply, replan sqlSecurityDocument
				c.Assert(json.Unmarshal(saved, &plan), qt.IsNil, qt.Commentf("%s", saved))
				c.Assert(json.Unmarshal([]byte(applied.Stdout), &apply), qt.IsNil, qt.Commentf("%s", applied.Stdout))
				c.Assert(json.Unmarshal([]byte(again.Stdout), &replan), qt.IsNil, qt.Commentf("%s", again.Stdout))
				c.Assert(apply.Outcome, qt.Equals, "applied")
				c.Assert(replan.Outcome, qt.Equals, "no-changes", qt.Commentf("%s", again.Stdout))
				after := target.held(c)
				want := []sqlSecurityEffect{{Version: 1, Action: "replace", Before: before, After: after, Changed: before != after}}
				c.Assert(before, qt.Equals, "INVOKER")
				c.Assert(after, qt.Equals, test.want)
				c.Assert(sqlSecurityEffects(plan.Statements), qt.DeepEquals, want, qt.Commentf("%s", saved))
				c.Assert(sqlSecurityEffects(apply.Units), qt.DeepEquals, want, qt.Commentf("%s", applied.Stdout))
			})
		}
	}
}

// TestMySQLViewSQLSecurityUndeclaredIsSyncedE2E plans a declaration of my_docs
// that names no mode against the database that holds it as INVOKER, with the
// same body: the plan has nothing to change, so the view keeps INVOKER.
func TestMySQLViewSQLSecurityUndeclaredIsSyncedE2E(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newSQLSecurityTarget(c, engine.engine)
			schema := target.schemaFile(c, "", "SELECT id, owner_name FROM docs")

			planned := runPtahIn(c, "schema", "plan", "--db-url", target.url, "--schema-file", schema, "--dry-run", "--json")

			assertRunExited(c, planned, 0)
			var plan sqlSecurityDocument
			c.Assert(json.Unmarshal([]byte(planned.Stdout), &plan), qt.IsNil, qt.Commentf("%s", planned.Stdout))
			c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%s", planned.Stdout))
			c.Assert(target.held(c), qt.Equals, "INVOKER")
		})
	}
}
