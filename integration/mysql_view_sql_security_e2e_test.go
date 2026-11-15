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

// A MySQL or MariaDB view held as SQL SECURITY INVOKER and declared without a
// mode is refused before the server changes, as a PostgreSQL view that would
// lose security_invoker is (stokaro/ptah#6022). CREATE OR REPLACE VIEW without
// the clause selects DEFINER, so the view would run with its definer's
// privileges, not its caller's. A declared mode plans as declared, and the plan
// records the mode before and after each statement on the view.

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

// The body my_docs is created with, and a changed one.
const (
	sqlSecurityBody     = "SELECT id, owner_name FROM docs"
	sqlSecurityPositive = "SELECT id, owner_name FROM docs WHERE id > 0"
)

// sqlSecurityTarget is a database holding the table docs and the view my_docs,
// created with the SQL SECURITY mode given.
type sqlSecurityTarget struct {
	db       *sql.DB
	database string
	url      string
}

func newSQLSecurityTarget(c *qt.C, engine dbtarget.Engine, mode string) sqlSecurityTarget {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	name, _ := scratch.database(c, "view_sql_security")
	for _, statement := range []string{
		"CREATE TABLE `" + name + "`.docs (id int PRIMARY KEY, owner_name varchar(64) NOT NULL)",
		"CREATE SQL SECURITY " + mode + " VIEW `" + name + "`.my_docs AS SELECT id, owner_name FROM `" + name + "`.docs",
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

// definition reads back the body information_schema.VIEWS reports for my_docs.
func (t sqlSecurityTarget) definition(c *qt.C) string {
	c.Helper()
	var body string
	c.Assert(t.db.QueryRowContext(c.Context(),
		"SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'my_docs'",
		t.database).Scan(&body), qt.IsNil)
	return body
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

// TestMySQLViewSQLSecurityE2E changes my_docs through a saved plan, and reads
// back the mode the server holds and what the plan said each statement does to
// it. A declared mode is written as declared, and a view held as DEFINER and
// declared without a mode keeps the server default.
func TestMySQLViewSQLSecurityE2E(t *testing.T) {
	tests := []struct {
		name   string
		held   string
		clause string
		body   string
		want   string
	}{
		{name: "a body change that declares INVOKER", held: "INVOKER", clause: "SQL SECURITY INVOKER ", body: sqlSecurityPositive, want: "INVOKER"},
		{name: "a declared DEFINER", held: "INVOKER", clause: "SQL SECURITY DEFINER ", body: sqlSecurityBody, want: "DEFINER"},
		{name: "a body change to a DEFINER view that names no mode", held: "DEFINER", clause: "", body: sqlSecurityPositive, want: "DEFINER"},
	}
	for _, engine := range sqlSecurityEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newSQLSecurityTarget(c, engine.engine, test.held)
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
				c.Assert(before, qt.Equals, test.held)
				c.Assert(after, qt.Equals, test.want)
				c.Assert(sqlSecurityEffects(plan.Statements), qt.DeepEquals, want, qt.Commentf("%s", saved))
				c.Assert(sqlSecurityEffects(apply.Units), qt.DeepEquals, want, qt.Commentf("%s", applied.Stdout))
			})
		}
	}
}

// sqlSecurityRefusal is the part of a refused apply document that names the
// refusal.
type sqlSecurityRefusal struct {
	Outcome string `json:"outcome"`
	Refusal struct {
		Code        string `json:"code"`
		ViewOptions []struct {
			View   string `json:"view"`
			Option string `json:"option"`
		} `json:"view_options"`
	} `json:"refusal"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

// TestMySQLViewSQLSecurityRefusedE2E applies a schema that declares my_docs,
// which the database holds as INVOKER, without a mode: with a changed body, and
// with the body it has. Either plan would hand the view to DEFINER and fail no
// statement, so the proof the refusal came first is the server: the mode and
// the body are what they were, under --auto-approve.
func TestMySQLViewSQLSecurityRefusedE2E(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "a body change", body: sqlSecurityPositive},
		{name: "the mode alone", body: sqlSecurityBody},
	}
	for _, engine := range sqlSecurityEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newSQLSecurityTarget(c, engine.engine, "INVOKER")
				definition := target.definition(c)

				got := runPtahIn(c, "schema", "apply", "--db-url", target.url,
					"--schema-file", target.schemaFile(c, "", test.body), "--auto-approve", "--json")

				assertRunExited(c, got, 2)
				var document sqlSecurityRefusal
				c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("%s", got.Stdout))
				c.Assert(document.Outcome, qt.Equals, "refused")
				c.Assert(document.Refusal.Code, qt.Equals, "view-security-option")
				c.Assert(document.Refusal.ViewOptions, qt.HasLen, 1)
				c.Assert(document.Refusal.ViewOptions[0].View, qt.Equals, "my_docs")
				c.Assert(document.Refusal.ViewOptions[0].Option, qt.Equals, "sql_security")
				for _, part := range []string{
					"view my_docs holds SQL SECURITY INVOKER and the schema does not state it",
					"the view runs with its definer's privileges, not its caller's",
					"Declare SQL SECURITY INVOKER to keep it, or SQL SECURITY DEFINER to change it",
				} {
					c.Assert(document.Error.Message, qt.Contains, part)
				}
				c.Assert(target.held(c), qt.Equals, "INVOKER")
				c.Assert(target.definition(c), qt.Equals, definition)
			})
		}
	}
}

// TestMySQLViewSQLSecurityDriftE2E compares the same declaration with `schema
// drift`, which plans nothing: it reports the mode under views_modified and
// does not refuse.
func TestMySQLViewSQLSecurityDriftE2E(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newSQLSecurityTarget(c, engine.engine, "INVOKER")

			got := runPtahIn(c, "schema", "drift", "--db-url", target.url, "--schema-file", target.schemaFile(c, "", sqlSecurityBody))

			assertRunExited(c, got, 1)
			c.Assert(got.Stdout, qt.Contains, "views_modified")
			c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), "does not state it")
			c.Assert(target.held(c), qt.Equals, "INVOKER")
		})
	}
}
