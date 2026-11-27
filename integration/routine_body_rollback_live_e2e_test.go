//go:build integration

package integration_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// oracleRoutineDatabase is [oracleDevDatabase] for an account that may also
// create procedures.
func oracleRoutineDatabase(c *qt.C) devDialectDatabase {
	c.Helper()
	database := oracleDevDatabase(c)
	address, err := url.Parse(database.url)
	c.Assert(err, qt.IsNil)
	admin := connectDevDialect(c, dbtarget.URL(c, dbtarget.OracleAdmin))
	execOracle(c.Context(), c, admin, "GRANT CREATE PROCEDURE TO "+address.User.Username())
	return database
}

// changedProcedure is a procedure rt as the first migration creates it, the
// schema file that changes it next, the query that reads back what the server
// holds for it, and what that query reads after the change.
type changedProcedure struct {
	created    string
	schemaFile string
	schema     string
	source     string
	changed    string
}

// assertProcedureComesBack generates the migration from the first migration's
// procedure rt on database to the one procedure.schema declares, applies it
// and rolls it back. What the server holds after the down migration is what it
// held before the up migration. The first migration writes the procedure as
// Ptah writes it, so the two compare as text.
func assertProcedureComesBack(c *qt.C, database devDialectDatabase, procedure changedProcedure) {
	c.Helper()
	dir := c.TempDir()
	writeRoutineMigration(c, dir, "1700000000_init.up.sql", procedure.created)
	writeRoutineMigration(c, dir, "1700000000_init.down.sql", "DROP PROCEDURE rt;\n")
	desired := filepath.Join(c.TempDir(), procedure.schemaFile)
	c.Assert(os.WriteFile(desired, []byte(procedure.schema), 0o600), qt.IsNil)
	initialized, err := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", initialized))
	source := func() string {
		c.Helper()
		var text string
		c.Assert(database.conn.QueryRowContext(c.Context(), procedure.source).Scan(&text), qt.IsNil)
		return text
	}
	held := source()
	generated, err := runPtahNativeWithError("migrations", "generate", "--db-url", database.url,
		"--migrations-dir", dir, "--schema-file", desired, "--name", "body")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))

	applied, upErr := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir,
		"--allow-destructive")
	changed := source()
	rolledBack, downErr := runPtahNativeWithError("migrations", "down", "--db-url", database.url,
		"--migrations-dir", dir, "--limit", "1", "--confirm")

	c.Assert(upErr, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(changed, qt.Contains, procedure.changed)
	c.Assert(downErr, qt.IsNil, qt.Commentf("%s", rolledBack))
	c.Assert(routineMigrationNamed(c, dir, "_body.down.sql"), qt.Not(qt.Contains), "empty_rollback")
	c.Assert(source(), qt.Equals, held)
}

// The down migration of a body change to a SQL Server procedure creates the
// procedure again with the body it had. The declaration leaves the schema out,
// and the read names the procedure in dbo.
func TestMigrationsDownRestoresAChangedSQLServerProcedureLiveE2E(t *testing.T) {
	c := qt.New(t)
	assertProcedureComesBack(c, sqlServerDevDatabase(c), changedProcedure{
		created:    "CREATE OR ALTER PROCEDURE [rt] \nAS\nBEGIN SELECT 1 END;\n",
		schemaFile: "schema.hcl",
		schema:     "procedure \"rt\" {\n  as = \"BEGIN SELECT 2 END\"\n}\n",
		source:     "SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.rt'))",
		changed:    "BEGIN SELECT 2 END",
	})
}

// The down migration of a change to the options alone of a SQL Server
// procedure creates the procedure again with the options it had. The schema
// file adds RECOMPILE and keeps the body, so the comparison reports the
// change as a routine option rather than as a field of the procedure.
func TestMigrationsDownRestoresTheOptionsOfASQLServerProcedureLiveE2E(t *testing.T) {
	c := qt.New(t)
	assertProcedureComesBack(c, sqlServerDevDatabase(c), changedProcedure{
		created:    "CREATE OR ALTER PROCEDURE [rt] \nAS\nBEGIN SELECT 1 END;\n",
		schemaFile: "schema.sql",
		schema:     "CREATE PROCEDURE rt WITH RECOMPILE AS BEGIN SELECT 1 END\n",
		source: "SELECT CONCAT(N'is_recompiled=', m.is_recompiled, N' ', m.definition) " +
			"FROM sys.sql_modules AS m WHERE m.object_id = OBJECT_ID('dbo.rt')",
		changed: "is_recompiled=1",
	})
}

// The down migration of a body change to an Oracle procedure creates the
// procedure again with the body it had. The declaration leaves the schema out,
// and the read names the procedure in the schema of the connected account.
func TestMigrationsDownRestoresAChangedOracleProcedureLiveE2E(t *testing.T) {
	c := qt.New(t)
	assertProcedureComesBack(c, oracleRoutineDatabase(c), changedProcedure{
		created:    "CREATE OR REPLACE PROCEDURE rt AUTHID DEFINER IS\nBEGIN NULL; END;\n",
		schemaFile: "schema.hcl",
		schema:     "procedure \"rt\" {\n  as = \"BEGIN DBMS_OUTPUT.PUT_LINE(2); END;\"\n}\n",
		source:     "SELECT LISTAGG(text) WITHIN GROUP (ORDER BY line) FROM user_source WHERE name = 'RT'",
		changed:    "BEGIN DBMS_OUTPUT.PUT_LINE(2); END;",
	})
}
