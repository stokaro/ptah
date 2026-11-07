//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// checkOptionEngine is one server that stores a view's check option apart
// from its body (information_schema.views.check_option): how to get an empty
// database there, a database built from SQL with nothing of Ptah's in
// between, and the expression naming the schema its URL selects.
type checkOptionEngine struct {
	name          string
	emptyDB       func(c *qt.C) string
	builtDB       func(c *qt.C, sql string) string
	currentSchema string
}

var checkOptionEngines = []checkOptionEngine{
	{
		name: "PostgreSQL",
		emptyDB: func(c *qt.C) string {
			url, _ := scratchReplayDatabase(c)
			return url
		},
		builtDB:       databaseBuiltFrom,
		currentSchema: "current_schema()",
	},
	{
		name:          "MySQL",
		emptyDB:       func(c *qt.C) string { return mysqlFamilyEmptyDatabase(c, dbtarget.MySQLAdmin) },
		builtDB:       func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MySQLAdmin, sql) },
		currentSchema: "DATABASE()",
	},
	{
		name:          "MariaDB",
		emptyDB:       func(c *qt.C) string { return mysqlFamilyEmptyDatabase(c, dbtarget.MariaDBAdmin) },
		builtDB:       func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MariaDBAdmin, sql) },
		currentSchema: "DATABASE()",
	},
}

const checkOptionTable = "CREATE TABLE t (id int PRIMARY KEY, n int NOT NULL);\n"

// checkOptionView is the view over checkOptionTable, ending in clause.
func checkOptionView(clause string) string {
	return checkOptionTable + "CREATE VIEW v AS SELECT id, n FROM t WHERE n > 0 " + clause + ";\n"
}

// checkOptionSpellings are the spellings of the CASCADED check option a schema
// file may end a view with.
var checkOptionSpellings = []struct {
	name   string
	clause string
}{
	{name: "without a level", clause: "WITH CHECK OPTION"},
	{name: "the cascaded level in lower case, with a comment", clause: "with /* level */ cascaded check option"},
}

// writeCheckOptionSchema writes content as a schema file and returns its path.
func writeCheckOptionSchema(c *qt.C, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	return path
}

// storedCheckOption reads back the check option the server holds for view v
// of the schema url selects, and the number of tables there.
func storedCheckOption(c *qt.C, engine checkOptionEngine, url string) (checkOption string, tables int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	err = conn.QueryRowContext(c.Context(), `SELECT
		COALESCE((SELECT check_option FROM information_schema.views WHERE table_name = 'v' AND table_schema = `+engine.currentSchema+`), ''),
		(SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 't' AND table_schema = `+engine.currentSchema+`)`,
	).Scan(&checkOption, &tables)
	c.Assert(err, qt.IsNil)
	return checkOption, tables
}

// TestSchemaApplyFindsAViewCheckOptionSyncedE2E plans a schema file whose view
// ends in WITH CHECK OPTION against the database the server built from the
// same file: nothing is planned. The server stores the clause apart from the
// body, so a declaration that kept it in the body replaced the view on every
// plan.
func TestSchemaApplyFindsAViewCheckOptionSyncedE2E(t *testing.T) {
	for _, engine := range checkOptionEngines {
		for _, spelling := range checkOptionSpellings {
			t.Run(engine.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				schema := checkOptionView(spelling.clause)
				target := engine.builtDB(c, schema)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", writeCheckOptionSchema(c, schema), "--dry-run")
				stored, _ := storedCheckOption(c, engine, target)

				c.Assert(plan, qt.Contains, "Schema is synced")
				c.Assert(stored, qt.Equals, "CASCADED")
			})
		}
	}
}

// checkOptionChanges add and remove a view's check option. They are the
// control for the synced rows above: the comparison sees the check option, and
// the plan gives the server the one declared.
var checkOptionChanges = []struct {
	name       string
	live       string
	declared   string
	wantPlan   string
	wantStored string
}{
	{name: "added", live: checkOptionView(""), declared: checkOptionView("WITH CHECK OPTION"),
		wantPlan: "SELECT id, n FROM t WHERE n > 0\nWITH CHECK OPTION;", wantStored: "CASCADED"},
	{name: "removed", live: checkOptionView("WITH CASCADED CHECK OPTION"), declared: checkOptionView(""),
		wantPlan: "SELECT id, n FROM t WHERE n > 0;", wantStored: "NONE"},
}

// TestSchemaApplyChangesAViewCheckOptionE2E applies each change and plans the
// file again: the server holds the declared check option, and nothing is left
// to plan.
func TestSchemaApplyChangesAViewCheckOptionE2E(t *testing.T) {
	for _, engine := range checkOptionEngines {
		for _, change := range checkOptionChanges {
			t.Run(engine.name+"/"+change.name, func(t *testing.T) {
				c := qt.New(t)
				target := engine.builtDB(c, change.live)
				schema := writeCheckOptionSchema(c, change.declared)

				applied := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
				stored, _ := storedCheckOption(c, engine, target)
				replan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				c.Assert(applied, qt.Contains, change.wantPlan)
				c.Assert(stored, qt.Equals, change.wantStored)
				c.Assert(replan, qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestSchemaApplyRefusesALocalViewCheckOptionE2E refuses WITH LOCAL CHECK
// OPTION by name before a statement is sent. A declaration holds the check
// option without a level and is created WITH CHECK OPTION, which the server
// stores as CASCADED: the view would refuse rows the declared one takes.
func TestSchemaApplyRefusesALocalViewCheckOptionE2E(t *testing.T) {
	for _, engine := range checkOptionEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.emptyDB(c)
			schema := writeCheckOptionSchema(c, checkOptionView("WITH LOCAL CHECK OPTION"))

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			stored, tables := storedCheckOption(c, engine, target)

			c.Assert(err, qt.ErrorMatches, `(?s).*view v: WITH LOCAL CHECK OPTION: a declared view holds its check option without a level.*`)
			c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE")
			c.Assert(stored, qt.Equals, "")
			c.Assert(tables, qt.Equals, 0)
		})
	}
}
