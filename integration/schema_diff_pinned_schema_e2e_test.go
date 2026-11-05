//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `ptah schema diff --from <file> --to <database>` with no --dev-url compares
// a document naming its tables with their schema against a database read
// through a URL that pins that schema, which reports the same tables without
// it. The two have to pair as one table each: paired as two, the plan created
// the table under its bare name and dropped the one the database holds.

// TestSchemaDiffPairsAFileWithAMySQLDatabaseItNamesE2E diffs a document
// naming `<db>.t1` and `<db>.t2` to a database holding t1 and t3. The plan
// drops t2, creates t3 and leaves t1, which both sides hold, alone.
func TestSchemaDiffPairsAFileWithAMySQLDatabaseItNamesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, database := scratch.builtFrom(c, "CREATE TABLE t1 (id int PRIMARY KEY); CREATE TABLE t3 (id int PRIMARY KEY);")
			file := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(file, []byte(fmt.Sprintf(
				"CREATE TABLE %[1]s.t1 (id int PRIMARY KEY);\nCREATE TABLE %[1]s.t2 (id int PRIMARY KEY);\n", name)), 0o600), qt.IsNil)

			diff := runPtahIn(c, "schema", "diff", "--from", "file://"+file, "--to", database)

			c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
			c.Assert(diff.Stdout, qt.Contains, "DROP TABLE `"+name+"`.`t2`;")
			c.Assert(diff.Stdout, qt.Contains, "CREATE TABLE `t3`")
			c.Assert(diff.Stdout, qt.Not(qt.Contains), "`t1`")
			c.Assert(scratch.tablesOf(c, name), qt.Equals, "t1,t3")
		})
	}
}

// TestSchemaDiffPairsAFileWithAPostgresSchemaItNamesE2E is the same document
// on PostgreSQL, against a URL whose search_path pins schema app. The plan
// drops app.t2, creates t3 and plans no table statement for t1.
func TestSchemaDiffPairsAFileWithAPostgresSchemaItNamesE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "pinned_pairing")
	for _, statement := range []string{
		"CREATE SCHEMA app",
		"CREATE TABLE app.t1 (id integer PRIMARY KEY)",
		"CREATE TABLE app.t3 (id integer PRIMARY KEY)",
	} {
		execPostgresFamilySQL(c, c.Context(), database, statement)
	}
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte("CREATE TABLE app.t1 (id integer PRIMARY KEY);\n"+
		"CREATE TABLE app.t2 (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)

	diff := runPtahIn(c, "schema", "diff", "--from", "file://"+file, "--to", planE2ESearchPath(c, database, "app"))

	c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
	c.Assert(diff.Stdout, qt.Contains, `DROP TABLE "app"."t2";`)
	c.Assert(diff.Stdout, qt.Contains, `CREATE TABLE "t3"`)
	c.Assert(diff.Stdout, qt.Not(qt.Contains), `CREATE TABLE "t1"`)
	c.Assert(diff.Stdout, qt.Not(qt.Contains), `"app"."t1"`)
}
