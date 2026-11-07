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
// from its body (information_schema.views.check_option): how to get a
// database built from SQL with nothing of Ptah's in between there, and the
// expression naming the schema its URL selects.
type checkOptionEngine struct {
	name          string
	builtDB       func(c *qt.C, sql string) string
	currentSchema string
}

var checkOptionEngines = []checkOptionEngine{
	{
		name:          "PostgreSQL",
		builtDB:       databaseBuiltFrom,
		currentSchema: "current_schema()",
	},
	{
		name:          "MySQL",
		builtDB:       func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MySQLAdmin, sql) },
		currentSchema: "DATABASE()",
	},
	{
		name:          "MariaDB",
		builtDB:       func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MariaDBAdmin, sql) },
		currentSchema: "DATABASE()",
	},
}

const checkOptionTable = "CREATE TABLE t (id int PRIMARY KEY, n int NOT NULL);\n"

// checkOptionView is the view over checkOptionTable, ending in clause.
func checkOptionView(clause string) string {
	return checkOptionTable + "CREATE VIEW v AS SELECT id, n FROM t WHERE n > 0 " + clause + ";\n"
}

// checkOptionSpellings are the spellings of a check option a schema file may
// end a view with, and the level the server stores for each.
var checkOptionSpellings = []struct {
	name       string
	clause     string
	wantStored string
}{
	{name: "without a level", clause: "WITH CHECK OPTION", wantStored: "CASCADED"},
	{name: "the cascaded level in lower case, with a comment", clause: "with /* level */ cascaded check option", wantStored: "CASCADED"},
	{name: "the local level", clause: "WITH LOCAL CHECK OPTION", wantStored: "LOCAL"},
	{name: "the local level in lower case, with a comment", clause: "with /* level */ local check option", wantStored: "LOCAL"},
}

// writeCheckOptionSchema writes content as a schema file and returns its path.
func writeCheckOptionSchema(c *qt.C, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	return path
}

// storedCheckOption reads back the check option the server holds for view v
// of the schema url selects, empty when there is no such view.
func storedCheckOption(c *qt.C, engine checkOptionEngine, url string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var checkOption string
	err = conn.QueryRowContext(c.Context(), `SELECT
		COALESCE((SELECT check_option FROM information_schema.views WHERE table_name = 'v' AND table_schema = `+engine.currentSchema+`), '')`,
	).Scan(&checkOption)
	c.Assert(err, qt.IsNil)
	return checkOption
}

// TestSchemaApplyFindsAViewCheckOptionSyncedE2E plans a schema file whose view
// ends in a check option against the database the server built from the same
// file: nothing is planned, at either level. The server stores the clause
// apart from the body, so a declaration that kept it in the body replaced the
// view on every plan.
func TestSchemaApplyFindsAViewCheckOptionSyncedE2E(t *testing.T) {
	for _, engine := range checkOptionEngines {
		for _, spelling := range checkOptionSpellings {
			t.Run(engine.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				schema := checkOptionView(spelling.clause)
				target := engine.builtDB(c, schema)

				plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", writeCheckOptionSchema(c, schema), "--dry-run")
				stored := storedCheckOption(c, engine, target)

				c.Assert(plan, qt.Contains, "Schema is synced")
				c.Assert(stored, qt.Equals, spelling.wantStored)
			})
		}
	}
}

// checkOptionChanges add and remove a view's check option and change its
// level. They are the control for the synced rows above: the comparison sees
// the check option and its level, and the plan gives the server the one
// declared. A held LOCAL check option compared with a declared CASCADED one
// planned nothing while the model held no level, so the view kept checking
// less than the declaration asked (stokaro/ptah#5647).
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
	{name: "LOCAL added", live: checkOptionView(""), declared: checkOptionView("WITH LOCAL CHECK OPTION"),
		wantPlan: "SELECT id, n FROM t WHERE n > 0\nWITH LOCAL CHECK OPTION;", wantStored: "LOCAL"},
	{name: "LOCAL removed", live: checkOptionView("WITH LOCAL CHECK OPTION"), declared: checkOptionView(""),
		wantPlan: "SELECT id, n FROM t WHERE n > 0;", wantStored: "NONE"},
	{name: "LOCAL to CASCADED", live: checkOptionView("WITH LOCAL CHECK OPTION"), declared: checkOptionView("WITH CHECK OPTION"),
		wantPlan: "SELECT id, n FROM t WHERE n > 0\nWITH CHECK OPTION;", wantStored: "CASCADED"},
	{name: "CASCADED to LOCAL", live: checkOptionView("WITH CHECK OPTION"), declared: checkOptionView("WITH LOCAL CHECK OPTION"),
		wantPlan: "SELECT id, n FROM t WHERE n > 0\nWITH LOCAL CHECK OPTION;", wantStored: "LOCAL"},
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
				stored := storedCheckOption(c, engine, target)
				replan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

				c.Assert(applied, qt.Contains, change.wantPlan)
				c.Assert(stored, qt.Equals, change.wantStored)
				c.Assert(replan, qt.Contains, "Schema is synced")
			})
		}
	}
}

// checkOptionInspections are the levels a database's view holds, with what
// each export of the database writes for it.
var checkOptionInspections = []struct {
	name    string
	clause  string
	wantHCL string
	wantSQL string
	wantGo  string
}{
	{name: "CASCADED", clause: "WITH CHECK OPTION", wantHCL: "check_option = CASCADED", wantSQL: "\nWITH CHECK OPTION\n", wantGo: `with_check="true"`},
	{name: "LOCAL", clause: "WITH LOCAL CHECK OPTION", wantHCL: "check_option = LOCAL", wantSQL: "\nWITH LOCAL CHECK OPTION\n", wantGo: `with_check="local"`},
}

// TestSchemaInspectKeepsTheViewCheckOptionLevelE2E exports a database whose
// view holds each level as HCL, SQL and Go, and plans each export against the
// same database: every export writes the level the view holds, and nothing is
// planned. An HCL export wrote LOCAL for every check option and a SQL export
// wrote WITH CHECK OPTION for every one, so one level in each came back as the
// other.
func TestSchemaInspectKeepsTheViewCheckOptionLevelE2E(t *testing.T) {
	for _, engine := range checkOptionEngines {
		for _, level := range checkOptionInspections {
			t.Run(engine.name+"/"+level.name, func(t *testing.T) {
				c := qt.New(t)
				target := engine.builtDB(c, checkOptionView(level.clause))
				documents := c.TempDir()
				hclPath, sqlPath, models := filepath.Join(documents, "schema.hcl"), filepath.Join(documents, "schema.sql"), filepath.Join(documents, "models")

				hcl, hclNotes, hclErr := runPtahSplitStreams(c.Context(), []string{"schema", "inspect", "--db-url", target, "--format", "hcl"})
				c.Assert(hclErr, qt.IsNil, qt.Commentf("%s", hclNotes))
				described, sqlNotes, sqlErr := runPtahSplitStreams(c.Context(), []string{"schema", "inspect", "--db-url", target, "--format", "sql"})
				c.Assert(sqlErr, qt.IsNil, qt.Commentf("%s", sqlNotes))
				c.Assert(os.WriteFile(hclPath, []byte(hcl), 0o600), qt.IsNil)       // #nosec G703 -- the path is c.TempDir() plus a constant name
				c.Assert(os.WriteFile(sqlPath, []byte(described), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
				runPtahNative(c, "introspect", "--db-url", target, "--out", models)
				goSource, err := os.ReadFile(filepath.Join(models, "schema_objects.go")) // #nosec G304 -- the path is c.TempDir() plus a constant name
				c.Assert(err, qt.IsNil)
				hclPlan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", hclPath, "--dry-run")
				sqlPlan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", sqlPath, "--dry-run")
				goPlan := runPtahNative(c, "schema", "apply", "--db-url", target, "--root-dir", models, "--dry-run")

				c.Assert(hcl, qt.Contains, level.wantHCL)
				c.Assert(described, qt.Contains, level.wantSQL)
				c.Assert(string(goSource), qt.Contains, level.wantGo)
				c.Assert(hclPlan, qt.Contains, "Schema is synced")
				c.Assert(sqlPlan, qt.Contains, "Schema is synced")
				c.Assert(goPlan, qt.Contains, "Schema is synced")
			})
		}
	}
}
