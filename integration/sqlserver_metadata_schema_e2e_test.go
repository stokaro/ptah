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

// SQL Server keeps Ptah's own tables where a URL's schema parameter, or else
// dbo, puts them, whatever the default schema of the connection's user is. A
// database migrated as sa keeps its history in dbo, and an account whose
// default schema is app reads that history: placed in app, the history read
// as empty and the account ran every migration again.

// sqlServerOwnTables lists Ptah's own tables a database holds as
// schema.name, in byte order.
const sqlServerOwnTables = `SELECT ISNULL(STRING_AGG(CONVERT(nvarchar(max), SCHEMA_NAME(schema_id) + N'.' + name), N',')
	WITHIN GROUP (ORDER BY SCHEMA_NAME(schema_id) + N'.' + name COLLATE Latin1_General_BIN), N'')
	FROM sys.tables
	WHERE name IN (N'schema_migrations', N'schema_migrations_log', N'atlas_schema_revisions', N'ptah_schema_apply_log')`

// sqlServerHistoryRows counts the rows the migrations of
// sqlServerHistoryMigrations inserted.
const sqlServerHistoryRows = `SELECT CONVERT(nvarchar(20), COUNT(*)) FROM dbo.acct`

// sqlServerHistoryMigrations creates a table once and inserts one row into
// it, so a run that applies them again leaves an extra row. The third inserts
// a second row; the account applies it, so its revision is written as well as
// read.
var sqlServerHistoryMigrations = []string{
	"IF OBJECT_ID(N'dbo.acct', N'U') IS NULL CREATE TABLE dbo.acct (id INT NOT NULL, amount INT NOT NULL);\n",
	"INSERT INTO dbo.acct VALUES (1, 100);\n",
	"INSERT INTO dbo.acct VALUES (2, 200);\n",
}

// writeFiles writes each file of files, by name, into dir.
func writeFiles(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
}

// TestMigrationsUpSQLServerKeepsTheHistoryForAnAccountOutsideDboE2E migrates
// a database as sa and then runs again as an account whose default schema is
// app, with one more migration in the directory. The second run reads the
// history sa wrote to dbo, applies the new migration alone, and records it
// there: two rows, and one revision table.
func TestMigrationsUpSQLServerKeepsTheHistoryForAnAccountOutsideDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{
		"1_create.up.sql":   sqlServerHistoryMigrations[0],
		"1_create.down.sql": "DROP TABLE dbo.acct;\n",
		"2_seed.up.sql":     sqlServerHistoryMigrations[1],
		"2_seed.down.sql":   "DELETE FROM dbo.acct;\n",
	})
	up := func(target string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
			"migrations", "up", "--db-url", target, "--migrations-dir", dir)
	}

	asSA := up(dbURL)
	writeFiles(c, dir, map[string]string{
		"3_more.up.sql":   sqlServerHistoryMigrations[2],
		"3_more.down.sql": "DELETE FROM dbo.acct WHERE id = 2;\n",
	})
	asAccount := up(account)

	c.Assert(asSA.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asSA.Stdout, asSA.Stderr))
	c.Assert(asAccount.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asAccount.Stdout, asAccount.Stderr))
	c.Assert(asAccount.Stdout, qt.Contains, "Database is now at version: 3")
	c.Assert(heldColumns(c, dbURL, sqlServerHistoryRows), qt.Equals, "2")
	c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, "dbo.schema_migrations,dbo.schema_migrations_log")
	c.Assert(heldColumns(c, dbURL, `SELECT STRING_AGG(CONVERT(nvarchar(20), version) + N':' + state, N',')
		WITHIN GROUP (ORDER BY version) FROM dbo.schema_migrations`), qt.Equals, "1:applied,2:applied,3:applied")
}

// TestMigrateApplySQLServerKeepsTheHistoryForAnAccountOutsideDboE2E is the
// same sequence through ptah-compat migrate apply and the Atlas revision
// table.
func TestMigrateApplySQLServerKeepsTheHistoryForAnAccountOutsideDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{
		"20260101000001_create.sql": sqlServerHistoryMigrations[0],
		"20260101000002_seed.sql":   sqlServerHistoryMigrations[1],
	})
	dirURL := "file://" + filepath.ToSlash(dir)
	hash := func() clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{}, "migrate", "hash", "--dir", dirURL)
	}
	apply := func(target string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Timeout: time.Minute},
			"migrate", "apply", "--url", target, "--dir", dirURL)
	}

	hashed := hash()
	asSA := apply(dbURL)
	writeFiles(c, dir, map[string]string{"20260101000003_more.sql": sqlServerHistoryMigrations[2]})
	rehashed := hash()
	asAccount := apply(account)

	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
	c.Assert(asSA.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asSA.Stdout, asSA.Stderr))
	c.Assert(rehashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", rehashed.Stdout, rehashed.Stderr))
	c.Assert(asAccount.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asAccount.Stdout, asAccount.Stderr))
	c.Assert(asAccount.Stdout, qt.Contains, "-- 1 migration\n")
	c.Assert(heldColumns(c, dbURL, sqlServerHistoryRows), qt.Equals, "2")
	c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, "dbo.atlas_schema_revisions")
	c.Assert(heldColumns(c, dbURL, `SELECT STRING_AGG(CONVERT(nvarchar(max), version), N',')
		WITHIN GROUP (ORDER BY version) FROM dbo.atlas_schema_revisions`), qt.Equals, "20260101000001,20260101000002,20260101000003")
}

// TestMigrationsUpSQLServerKeepsTheHistoryInTheSchemaTheURLNamesE2E runs the
// account through a URL whose schema parameter names app: the history goes
// to app, where the URL puts it.
func TestMigrationsUpSQLServerKeepsTheHistoryInTheSchemaTheURLNamesE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app") + "&schema=app"
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{
		"1_create.up.sql":   sqlServerHistoryMigrations[0],
		"1_create.down.sql": "DROP TABLE dbo.acct;\n",
	})

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
		"migrations", "up", "--db-url", account, "--migrations-dir", dir)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, "app.schema_migrations,app.schema_migrations_log")
}

// TestSchemaApplySQLServerKeepsOneApplyLogForAnAccountOutsideDboE2E applies
// a schema as sa and another as an account whose default schema is app. Both
// runs write their entries to the one apply log in dbo.
func TestSchemaApplySQLServerKeepsOneApplyLogForAnAccountOutsideDboE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := sqlServerRoleScratchDatabase(c)
	account := sqlServerAccount(c, dbURL, "app")
	dir := c.TempDir()
	writeFiles(c, dir, map[string]string{
		"sa.sql":      "CREATE TABLE t1 (id INT NOT NULL);\nGO\n",
		"account.sql": "CREATE TABLE t2 (id INT NOT NULL);\nGO\n",
	})
	apply := func(target, file string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Timeout: time.Minute},
			"schema", "apply", "--db-url", target, "--schema-file", filepath.Join(dir, file), "--auto-approve")
	}

	asSA := apply(dbURL, "sa.sql")
	asAccount := apply(account, "account.sql")

	c.Assert(asSA.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asSA.Stdout, asSA.Stderr))
	c.Assert(asAccount.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", asAccount.Stdout, asAccount.Stderr))
	c.Assert(heldColumns(c, dbURL, sqlServerObjectsBySchema), qt.Equals, "app.t2,dbo.t1")
	c.Assert(heldColumns(c, dbURL, sqlServerOwnTables), qt.Equals, "dbo.ptah_schema_apply_log")
	c.Assert(heldColumns(c, dbURL, `SELECT CONVERT(nvarchar(20), COUNT(DISTINCT run_id)) FROM dbo.ptah_schema_apply_log`), qt.Equals, "2")
}
