package migratestatus_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite" // registers the "sqlite" driver the catalog is read back through
)

// sqliteTables answers the tables the SQLite database at path holds.
func sqliteTables(c *qt.C, path string) int {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var tables int
	c.Assert(db.QueryRowContext(c.Context(), `SELECT count(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables), qt.IsNil)
	return tables
}

// TestMigrateStatus_ReadsWithoutWriting asks for the status of an empty
// database and of one the directory was applied to. A status is a read: the
// empty database stays empty, and the report carries no dry-run narration.
// Read with a writing migrator, the status of the empty database creates
// schema_migrations (stokaro/ptah#3893); read with the writer in dry-run mode
// and the migrator's Info records kept, it prints `[DRY RUN] Would initialize
// migrations metadata`.
func TestMigrateStatus_ReadsWithoutWriting(t *testing.T) {
	t.Run("an empty database", func(t *testing.T) {
		c := qt.New(t)
		dir := writeStatusMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "empty.db")

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Current Version: 0")
		c.Assert(out, qt.Not(qt.Contains), "DRY RUN")
		c.Assert(sqliteTables(c, dbPath), qt.Equals, 0)
	})
	t.Run("a database the directory was applied to", func(t *testing.T) {
		c := qt.New(t)
		dir := writeStatusMigrations(c)
		dbPath := filepath.Join(c.TempDir(), "applied.db")
		applyStatusMigrations(c, dir, dbPath)

		out, err := runStatus(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		c.Assert(out, qt.Contains, "Current Version: 1")
		c.Assert(out, qt.Not(qt.Contains), "DRY RUN")
	})
}
