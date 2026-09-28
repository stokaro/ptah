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

	"ptah.run/internal/atlasurl"
)

// nativeStatusMigrationDir writes a one-migration directory in the native
// format.
func nativeStatusMigrationDir(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE status_probe (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE status_probe;\n"), 0o600), qt.IsNil)
	return dir
}

// selectOnlyAccount creates an account that may only read database, dropped
// when the test ends, and answers url with that account's credentials.
func selectOnlyAccount(c *qt.C, scratch mysqlScratch, database, url string) string {
	c.Helper()
	account := fmt.Sprintf("ptah_ro_%d", time.Now().UnixNano()%1_000_000_000)
	password := account + "_pw"
	createMySQLUser(c, c.Context(), scratch.admin, account, password)
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), scratch.admin, account) })
	_, err := scratch.admin.ExecContext(c.Context(),
		fmt.Sprintf("GRANT SELECT ON `%s`.* TO '%s'@'%%'", database, account))
	c.Assert(err, qt.IsNil)
	return replaceMySQLCredentials(c, url, account, password)
}

// TestMigrationsStatusCreatesNothingE2E reads the status of an empty database
// with `ptah migrations status` and leaves it empty. A status is a read: read
// with a writing migrator, it creates schema_migrations in the database, on
// MySQL 8.4.11 and PostgreSQL 18 alike (stokaro/ptah#3893). The tables are
// counted from each engine's catalog after the run.
func TestMigrationsStatusCreatesNothingE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, url := scratch.database(c, "nstatus")

			out, err := runPtahNativeWithError("migrations", "status", "--db-url", url,
				"--migrations-dir", nativeStatusMigrationDir(c))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Current Version: 0")
			c.Assert(scratch.tablesOf(c, name), qt.Equals, "")
		})
	}
	t.Run("PostgreSQL", func(t *testing.T) {
		c := qt.New(t)
		url := postgresScratchDevURL(c, "ptah_nstatus")

		out, err := runPtahNativeWithError("migrations", "status", "--db-url", url,
			"--migrations-dir", nativeStatusMigrationDir(c))

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Current Version: 0")
		db, err := sql.Open("pgx", url)
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(db.Close(), qt.IsNil) }()
		var tables int
		err = db.QueryRowContext(c.Context(),
			`SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`).Scan(&tables)
		c.Assert(err, qt.IsNil)
		c.Assert(tables, qt.Equals, 0)
	})
	t.Run("SQLite", func(t *testing.T) {
		c := qt.New(t)
		path := filepath.Join(c.TempDir(), "status.db")

		out, err := runPtahNativeWithError("migrations", "status", "--db-url", atlasurl.SQLiteURLFromPath(path),
			"--migrations-dir", nativeStatusMigrationDir(c))

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Current Version: 0")
		db, err := sql.Open("sqlite", path)
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(db.Close(), qt.IsNil) }()
		var tables int
		c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables), qt.IsNil)
		c.Assert(tables, qt.Equals, 0)
	})
}

// TestMigrationsStatusReadsAnEmptyDatabaseWithASelectOnlyAccountE2E asks for
// the status of an empty database through an account that may only read it.
// A status that creates schema_migrations is refused with CREATE command
// denied, measured on MySQL 8.4.11.
func TestMigrationsStatusReadsAnEmptyDatabaseWithASelectOnlyAccountE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, url := scratch.database(c, "nstatus_ro")

			out, err := runPtahNativeWithError("migrations", "status",
				"--db-url", selectOnlyAccount(c, scratch, name, url), "--migrations-dir", nativeStatusMigrationDir(c))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Current Version: 0")
			c.Assert(scratch.tablesOf(c, name), qt.Equals, "")
		})
	}
}

// TestMigrationsStatusReadsTheHistoryWithASelectOnlyAccountE2E applies the
// directory as the administrator, then asks for the status through an account
// that may only read the database. The status reads the migration the apply
// recorded. Measured on MySQL 8.4.11, a status that initializes the table is
// refused with CREATE command denied even where the table exists.
func TestMigrationsStatusReadsTheHistoryWithASelectOnlyAccountE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, url := scratch.database(c, "nstatus_hist")
			dir := nativeStatusMigrationDir(c)
			applied, err := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))

			out, err := runPtahNativeWithError("migrations", "status",
				"--db-url", selectOnlyAccount(c, scratch, name, url), "--migrations-dir", dir)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Current Version: 1")
		})
	}
}
