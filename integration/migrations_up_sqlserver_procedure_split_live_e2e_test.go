//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/microsoft/go-mssqldb" // registers the sqlserver driver for database/sql

	"ptah.run/internal/dbtarget"
)

// TestMigrationsUpEndsATSQLProcedureWhereItsBlocksEndLiveE2E is
// stokaro/ptah#4461 on a server.
//
// A T-SQL procedure body runs to the end of its batch, so whatever the
// splitter leaves in the procedure's statement becomes part of the procedure.
// The splitter counted BEGIN TRAN and the BEGIN of a CATCH as blocks that
// never closed, so the CREATE TABLE after each procedure below reached the
// server inside it: the migration succeeded, the procedure held a CREATE TABLE,
// and the table did not exist.
func TestMigrationsUpEndsATSQLProcedureWhereItsBlocksEndLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "TRY and CATCH", body: "BEGIN TRY\n    SELECT 1;\n  END TRY\n  BEGIN CATCH\n    SELECT 2;\n  END CATCH;"},
		{name: "a transaction", body: "BEGIN TRAN;\n  SELECT 1;\n  COMMIT;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			up := fmt.Sprintf("CREATE PROCEDURE dbo.split_probe AS\nBEGIN\n  %s\nEND;\nCREATE TABLE dbo.after_probe (id int);\n", test.body)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(up), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"),
				[]byte("DROP TABLE dbo.after_probe;\nDROP PROCEDURE dbo.split_probe;\n"), 0o600), qt.IsNil)

			out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var table, definition sql.NullString
			c.Assert(db.QueryRowContext(ctx, "SELECT OBJECT_NAME(OBJECT_ID('dbo.after_probe', 'U')), "+
				"OBJECT_DEFINITION(OBJECT_ID('dbo.split_probe'))").Scan(&table, &definition), qt.IsNil)
			c.Assert(table.String, qt.Equals, "after_probe")
			c.Assert(definition.String, qt.Not(qt.Contains), "after_probe")
		})
	}
}

// newSQLServerMigrationDatabase creates a throwaway database and returns its
// URL and an open handle on it.
func newSQLServerMigrationDatabase(c *qt.C, ctx context.Context, dbURL, collation string) (string, *sql.DB) {
	c.Helper()

	adminDB, err := sql.Open("sqlserver", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_split_%d", time.Now().UnixNano())
	// Set the collation before opening the target pool. SQL Server rejects
	// resetting a pooled connection when its database collation differs from
	// the login state, with errors 4021 and 596.
	// #nosec G202 -- The database name is generated above; collation comes from test constants.
	statement := "CREATE DATABASE [" + name + "]"
	if collation != "" {
		statement += " COLLATE " + collation
	}
	_, err = adminDB.ExecContext(ctx, statement)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := adminDB.ExecContext(context.Background(),
			"ALTER DATABASE ["+name+"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["+name+"]")
		c.Check(dropErr, qt.IsNil)
	})

	databaseURL := replaceSQLServerDatabase(c, dbURL, name)
	db, err := sql.Open("sqlserver", databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	return databaseURL, db
}
