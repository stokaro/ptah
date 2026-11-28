//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// SQL Server creates a name written without a schema in the default schema
// of the connection's user. A connection whose URL names no schema works in
// that schema, so an unqualified table or routine is planned once for an
// account whose default schema is not dbo, and read back where the server put
// it.

// sqlServerObjectsBySchema reads the user tables and scalar functions a
// database holds as schema.name, in byte order.
const sqlServerObjectsBySchema = `SELECT ISNULL(STRING_AGG(CONVERT(nvarchar(max), SCHEMA_NAME(schema_id) + N'.' + name), N',')
	WITHIN GROUP (ORDER BY SCHEMA_NAME(schema_id) + N'.' + name COLLATE Latin1_General_BIN), N'')
	FROM sys.objects WHERE type IN ('U', 'FN') AND name <> N'ptah_schema_apply_log'`

// TestSchemaApplySQLServerUnqualifiedObjectsForAnAccountOutsideDboE2E applies
// an unqualified table and function twice as an account whose default schema
// is app. The first run creates both in app, and the second finds them there.
func TestSchemaApplySQLServerUnqualifiedObjectsForAnAccountOutsideDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t1 (id INT NOT NULL);\nGO\n"+
		"CREATE FUNCTION f() RETURNS int AS BEGIN RETURN 1 END;\nGO\n"), 0o600), qt.IsNil)
	apply := func() clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
			"schema", "apply", "--db-url", account, "--schema-file", schema, "--auto-approve")
	}

	applied := apply()
	again := apply()

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerObjectsBySchema), qt.Equals, "app.f,app.t1")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// sqlServerTwoSchemaDatabase is a scratch database holding dbo.d1 and
// app.t1, and a schema file declaring both.
func sqlServerTwoSchemaDatabase(c *qt.C) (dbURL, schema string) {
	c.Helper()
	dbURL = sqlServerRoleScratchDatabase(c)
	conn := connectDevDialect(c, dbURL)
	for _, batch := range []string{"CREATE SCHEMA app", "CREATE TABLE dbo.d1 (id INT NOT NULL)", "CREATE TABLE app.t1 (id INT NOT NULL)"} {
		_, err := conn.ExecContext(c.Context(), batch)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", batch))
	}
	schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE dbo.d1 (id INT NOT NULL);\nGO\nCREATE TABLE app.t1 (id INT NOT NULL);\nGO\n"), 0o600), qt.IsNil)
	return dbURL, schema
}

// TestSchemaDiffSQLServerSaysWhichSchemaItReadE2E diffs a database holding
// dbo.d1 and app.t1 against a file declaring both. Read at dbo alone, the
// database lacks app.t1, so the plan creates it, and the run says which
// schema it read and which declared schema it did not.
func TestSchemaDiffSQLServerSaysWhichSchemaItReadE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, schema := sqlServerTwoSchemaDatabase(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "diff", "--from", dbURL, "--to", schema)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "CREATE TABLE [app].[t1]")
	c.Assert(got.Stderr, qt.Contains, `Note: the --from database was read at schema "dbo" alone; pass --schemas to compare others. `+
		`--to declares objects in "app", which this diff compares as absent from the database.`)
}

// TestSchemaDiffSQLServerReadsTheSchemasItIsGivenE2E is the control: with
// --schemas naming both, the diff finds both tables and says nothing about
// its scope.
func TestSchemaDiffSQLServerReadsTheSchemasItIsGivenE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, schema := sqlServerTwoSchemaDatabase(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "diff", "--from", dbURL, "--to", schema, "--schemas", "dbo,app")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
	c.Assert(got.Stderr, qt.Not(qt.Contains), "Note:")
}

// TestSchemaDiffSQLServerSaysNothingOfADatabaseHoldingOnlyDboE2E diffs a
// database holding dbo.d1 alone against a file declaring dbo.d1 and app.t1.
// The database holds no schema the diff did not read, so the plan creates
// app.t1 and the run writes no note about its scope.
func TestSchemaDiffSQLServerSaysNothingOfADatabaseHoldingOnlyDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	_, err := connectDevDialect(c, dbURL).ExecContext(c.Context(), "CREATE TABLE dbo.d1 (id INT NOT NULL)")
	c.Assert(err, qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE dbo.d1 (id INT NOT NULL);\nGO\nCREATE TABLE app.t1 (id INT NOT NULL);\nGO\n"), 0o600), qt.IsNil)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"schema", "diff", "--from", dbURL, "--to", schema)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "CREATE TABLE [app].[t1]")
	c.Assert(got.Stderr, qt.Not(qt.Contains), "Note:")
}
