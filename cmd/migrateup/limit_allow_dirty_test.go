package migrateup_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrateup"
	"go.5x5.cz/ptah/dbschema"
)

// writeUpMigrations writes a two-migration ptah-format directory.
func writeUpMigrations(t *testing.T) string {
	c := qt.New(t)
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":    "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql":  "DROP TABLE users;\n",
		"0000000002_orders.up.sql":   "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000002_orders.down.sql": "DROP TABLE orders;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

func runUp(args ...string) (string, error) {
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func queryCurrentVersion(tb testing.TB, dbPath string) int64 {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var version int64
	err = conn.QueryRowContext(context.Background(),
		`SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE state = 'applied'`).Scan(&version)
	c.Assert(err, qt.IsNil)
	return version
}

func TestMigrateUpLimitAppliesFirstNPending(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "limit.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--limit", "1")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Database is now at version: 1")
	c.Assert(queryCurrentVersion(c.TB, dbPath), qt.Equals, int64(1))

	// A second limited run applies the next pending migration.
	out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--limit", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c.TB, dbPath), qt.Equals, int64(2))
}

func TestMigrateUpLimitDryRunReportsLimitedCount(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "limit.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--limit", "1", "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Would have applied 1 migrations")
	// Nothing was executed: the revision table itself was never created.
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestMigrateUpDryRunUsesStoredRevisionState(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "stored-state.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--limit", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c.TB, dbPath), qt.Equals, int64(1))

	out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Current version: 1")
	c.Assert(out, qt.Contains, "Pending migrations: 1")
	c.Assert(out, qt.Contains, "Would have applied 1 migrations")
	c.Assert(queryCurrentVersion(c.TB, dbPath), qt.Equals, int64(1))
}

// insertOrphanDirtyRevision records a failed revision row for a version that
// no migration file provides, modeling a crashed migration whose file was
// later rebased or removed: the dirty guard trips on it, but no pending work
// references it.
func insertOrphanDirtyRevision(tb testing.TB, dbPath string, version int64) {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(),
		`INSERT INTO schema_migrations (version, description, applied_at, state, applied, total, error, error_stmt, execution_time_ms, checksum)
VALUES (?, 'crashed', CURRENT_TIMESTAMP, 'failed', 0, 1, 'boom', '', 0, '')`, version)
	c.Assert(err, qt.IsNil)
}

func TestMigrateUpAllowDirtyBypassesDirtyGuard(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "dirty.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--limit", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	insertOrphanDirtyRevision(c.TB, dbPath, 99)

	// The default guard refuses to run over a dirty revision.
	out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)
	c.Assert(err, qt.ErrorMatches, "(?s).*dirty.*", qt.Commentf("%s", out))

	// The explicit recovery escape hatch proceeds with the pending work.
	out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--allow-dirty")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c.TB, dbPath), qt.Equals, int64(2))
}
