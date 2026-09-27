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
	"ptah.run/internal/dbtarget"
)

// statusMigrationDir writes and hashes a one-file migration directory.
func statusMigrationDir(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000001_init.sql"),
		[]byte("CREATE TABLE status_probe (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir
}

// TestCompatMigrateStatusCreatesNothingE2E reads the status of an empty
// database and leaves it empty, as the pinned community binary v1.3.0 does:
// measured on MySQL 8.4.11 and PostgreSQL 18, `migrate status` against an
// empty database reports PENDING and creates no revision table
// (stokaro/ptah#3881). Read with a writing migrator, the status creates one
// in the connected database, and on PostgreSQL the atlas_schema_revisions
// schema with it. The tables are counted from each engine's catalog after the
// run.
func TestCompatMigrateStatusCreatesNothingE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, url := scratch.database(c, "status")

			out, err := runCompatVerb("migrate", "status", "--url", url, "--dir", "file://"+statusMigrationDir(c))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Migration Status: PENDING")
			c.Assert(scratch.tablesOf(c, name), qt.Equals, "")
		})
	}
	t.Run("PostgreSQL", func(t *testing.T) {
		c := qt.New(t)
		url := postgresScratchDevURL(c, "ptah_status")

		out, err := runCompatVerb("migrate", "status", "--url", url, "--dir", "file://"+statusMigrationDir(c))

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Migration Status: PENDING")
		db, err := sql.Open("pgx", url)
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(db.Close(), qt.IsNil) }()
		var schemas, tables int
		err = db.QueryRowContext(c.Context(), `
			SELECT
				(SELECT count(*) FROM pg_namespace WHERE nspname = 'atlas_schema_revisions'),
				(SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema'))`).Scan(&schemas, &tables)
		c.Assert(err, qt.IsNil)
		c.Assert(schemas, qt.Equals, 0)
		c.Assert(tables, qt.Equals, 0)
	})
	t.Run("SQLite", func(t *testing.T) {
		c := qt.New(t)
		path := filepath.Join(c.TempDir(), "status.db")

		out, err := runCompatVerb("migrate", "status", "--url", atlasurl.SQLiteURLFromPath(path),
			"--dir", "file://"+statusMigrationDir(c))

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Migration Status: PENDING")
		db, err := sql.Open("sqlite", path)
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(db.Close(), qt.IsNil) }()
		var tables int
		c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables), qt.IsNil)
		c.Assert(tables, qt.Equals, 0)
	})
}

// TestCompatMigrateStatusReadsWithASelectOnlyAccountE2E asks for a status
// through an account that may only read the database. The pinned community
// binary v1.3.0 reports PENDING, measured on MySQL 8.4.11; a status that
// creates the revision table is refused with CREATE command denied.
func TestCompatMigrateStatusReadsWithASelectOnlyAccountE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, url := scratch.database(c, "status_ro")
			account := fmt.Sprintf("ptah_ro_%d", time.Now().UnixNano()%1_000_000_000)
			password := account + "_pw"
			createMySQLUser(c, c.Context(), scratch.admin, account, password)
			c.Cleanup(func() { dropMySQLUser(c, context.Background(), scratch.admin, account) })
			_, err := scratch.admin.ExecContext(c.Context(),
				fmt.Sprintf("GRANT SELECT ON `%s`.* TO '%s'@'%%'", name, account))
			c.Assert(err, qt.IsNil)

			out, err := runCompatVerb("migrate", "status", "--url", replaceMySQLCredentials(c, url, account, password),
				"--dir", "file://"+statusMigrationDir(c))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Migration Status: PENDING")
			c.Assert(scratch.tablesOf(c, name), qt.Equals, "")
		})
	}
}

// TestCompatMigrateStatusReadsTheHistoryApplyWroteE2E is the control: a
// status after an apply reads the revision the apply recorded, so the read
// did not stop reading an existing table.
func TestCompatMigrateStatusReadsTheHistoryApplyWroteE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
	_, url := scratch.database(c, "status")
	dir := statusMigrationDir(c)

	applied, err := runCompatVerb("migrate", "apply", "--url", url, "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	out, err := runCompatVerb("migrate", "status", "--url", url, "--dir", "file://"+dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Migration Status: OK")
	c.Assert(out, qt.Contains, "Current Version: 20260101000001")
}
