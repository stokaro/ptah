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

	"ptah.run/internal/dbtarget"
)

// languageFreeProcedure is a procedure as an Atlas HCL document writes it on
// MySQL, MariaDB and SQL Server: HCL has no attribute for a routine's language,
// because the body is in the engine's own. The schema block names the schema
// the procedure is created in.
func languageFreeProcedure(schema string) string {
	return fmt.Sprintf(`schema %q {}
procedure "language_probe" {
  schema = schema.%s
  as     = "BEGIN SELECT 1; END"
}
`, schema, schema)
}

// writeLanguageFreeProcedure writes the document for schema to a file.
func writeLanguageFreeProcedure(c *qt.C, schema string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(languageFreeProcedure(schema)), 0o600), qt.IsNil)
	return path
}

// TestSchemaRenderCreatesAProcedureThatDeclaresNoLanguageFS renders the
// document on each engine HCL describes procedures for.
//
// The model gave a routine that declares no language PostgreSQL's plpgsql, so
// on MySQL, MariaDB and SQL Server the procedure was rendered as a skip, a
// language the target does not run, and could never be created.
func TestSchemaRenderCreatesAProcedureThatDeclaresNoLanguageFS(t *testing.T) {
	for _, test := range []struct {
		dialect string
		want    string
	}{
		{dialect: "mysql", want: "CREATE PROCEDURE `app`.`language_probe`"},
		{dialect: "mariadb", want: "CREATE PROCEDURE `app`.`language_probe`"},
		{dialect: "sqlserver", want: "CREATE OR ALTER PROCEDURE [app].[language_probe]"},
		{dialect: "postgres", want: "LANGUAGE plpgsql"},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := writeLanguageFreeProcedure(c, "app")

			out, err := runPtahNativeWithError("schema", "render", "--dialect", test.dialect, "--schema-file", schema)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.want)
			c.Assert(out, qt.Not(qt.Contains), "skipped")
		})
	}
}

// TestSchemaApplyCreatesAProcedureThatDeclaresNoLanguageLiveE2E applies the
// document to a server, reads the procedure back from the catalog, and plans
// again: the second plan is empty.
//
// The document states no security mode either, so the procedure runs with the
// rights the engine gives one: its definer's on MySQL and MariaDB, its
// caller's on SQL Server.
func TestSchemaApplyCreatesAProcedureThatDeclaresNoLanguageLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
		// security reads the rights the procedure runs with, and want is the
		// engine's own answer for a procedure that states no mode.
		security string
		want     string
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, want: "DEFINER",
			security: "SELECT SECURITY_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'language_probe'"},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, want: "DEFINER",
			security: "SELECT SECURITY_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'language_probe'"},
		{name: "sqlserver", engine: dbtarget.SQLServer, want: "CALLER",
			security: "SELECT CASE WHEN m.execute_as_principal_id IS NULL THEN 'CALLER' ELSE 'OWNER' END " +
				"FROM sys.sql_modules m JOIN sys.procedures p ON p.object_id = m.object_id WHERE p.name = 'language_probe'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, schema, db := languageDatabase(c, ctx, test.engine)
			document := writeLanguageFreeProcedure(c, schema)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", document, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var security string
			c.Assert(db.QueryRowContext(ctx, test.security).Scan(&security), qt.IsNil)
			c.Assert(security, qt.Equals, test.want)
			second, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", document, "--dry-run")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
			c.Assert(second, qt.Contains, "Schema is synced")
		})
	}
}

// languageDatabase creates a throwaway database on the server engine reaches,
// and returns its URL, the schema the document names, and a handle on it. On
// the MySQL family a schema is a database, so the document names the database
// itself; on SQL Server it names dbo.
func languageDatabase(c *qt.C, ctx context.Context, engine dbtarget.Engine) (url, schema string, db *sql.DB) {
	c.Helper()
	if engine == dbtarget.SQLServer {
		url, db := newSQLServerMigrationDatabase(c, ctx, dbtarget.URL(c, engine), "")
		return url, "dbo", db
	}
	adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	name := fmt.Sprintf("ptah_language_%d", time.Now().UnixNano())
	createMySQLDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), adminDB, name) })
	db, err = sql.Open("mysql", replaceMySQLDatabaseName(c, dbtarget.DriverDSN(c, engine), name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	return replaceMySQLDatabaseName(c, dbtarget.URL(c, engine), name), name, db
}
