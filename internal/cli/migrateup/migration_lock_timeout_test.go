package migrateup_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A `--migration-lock-timeout` on a dialect with no session advisory lock
// bounds a wait the target never makes, and the migration lock is what keeps
// two runners off one history. `ptah migrations up` took the request and
// applied anyway, which is what stokaro/ptah#3417 reports.
//
// Each refusal is measured against the database as well as the exit code. The
// exit code alone passes against a build that refuses after applying, which is
// the half the reporter cares about.

// TestMigrateUpRefusesMigrationLockTimeoutBeforeConnecting drives every dialect
// with no advisory lock at an address nothing is listening on. The refusal,
// rather than a connection failure, is what says the decision is made from the
// URL.
func TestMigrateUpRefusesMigrationLockTimeoutBeforeConnecting(t *testing.T) {
	tests := []struct {
		name    string
		dbURL   string
		dialect string
	}{
		{name: "sqlite", dbURL: "sqlite://target.db", dialect: "sqlite"},
		{name: "clickhouse", dbURL: "clickhouse://127.0.0.1:1/db", dialect: "clickhouse"},
		{name: "cockroachdb", dbURL: "cockroachdb://root@127.0.0.1:1/db", dialect: "cockroachdb"},
		{name: "spanner", dbURL: "spanner://127.0.0.1:1/db", dialect: "spanner"},
		{name: "oracle", dbURL: "oracle://ptah@127.0.0.1:1/db", dialect: "oracle"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// The sqlite row names a relative path, so the run is held in a
			// directory of its own: a build that stopped refusing would write
			// the database into the package source tree.
			t.Chdir(t.TempDir())
			migrationsDir := writeUpMigrations(t)

			out, err := runUp(
				"--db-url", test.dbURL,
				"--migrations-dir", migrationsDir,
				"--migration-lock-timeout", "10s",
			)

			c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("--migration-lock-timeout", test.dialect),
				qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Not(qt.Contains), "error connecting to database")
		})
	}
}

// TestMigrateUpRefusedMigrationLockTimeoutCreatesNoDatabase repeats the
// reporter's own command, where the SQLite file does not exist yet. An absent
// file is the strongest reading of "nothing was applied": the connection that
// would have created it was never opened.
func TestMigrateUpRefusedMigrationLockTimeoutCreatesNoDatabase(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "real.db")

	out, err := runUp(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--migration-lock-timeout", "10s",
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("--migration-lock-timeout", "sqlite"),
		qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "Migrations completed successfully")
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateUpRefusedMigrationLockTimeoutLeavesTheHistoryAlone measures a
// database that already exists, so the claim is about the migration history
// rather than about a file nothing created.
func TestMigrateUpRefusedMigrationLockTimeoutLeavesTheHistoryAlone(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "history.db")

	applied, applyErr := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--limit", "1")
	c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(1))

	out, err := runUp(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--migration-lock-timeout", "10s",
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("--migration-lock-timeout", "sqlite"),
		qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(1))
}

// TestMigrateUpRefusesEmptyMigrationLockTimeout pins the predicate to presence.
// An empty value asks for an unbounded wait, which is a wait this target never
// makes either, so reading the string would let this spelling through in
// silence.
func TestMigrateUpRefusesEmptyMigrationLockTimeout(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "empty.db")

	out, err := runUp(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--migration-lock-timeout", "",
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("--migration-lock-timeout", "sqlite"),
		qt.Commentf("%s", out))
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateUpRefusesEnvironmentMigrationLockTimeout covers the second
// spelling. PTAH_MIGRATION_LOCK_TIMEOUT names this lock on every command that
// reads it, so a value arriving that way asks for the same lock a typed flag
// asks for, and the message names the variable so the operator can find it.
func TestMigrateUpRefusesEnvironmentMigrationLockTimeout(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "10s")
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "env.db")

	out, err := runUpThroughRoot(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("PTAH_MIGRATION_LOCK_TIMEOUT", "sqlite"),
		qt.Commentf("%s", out))
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateUpRefusesProjectConfigMigrationLockTimeout covers the third
// spelling. A project file configures the run as much as a flag does, and the
// message names the key rather than a flag the operator never typed.
func TestMigrateUpRefusesProjectConfigMigrationLockTimeout(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	work := t.TempDir()
	dbPath := filepath.Join(work, "config.db")
	configPath := filepath.Join(work, "ptah.yaml")
	c.Assert(os.WriteFile(configPath,
		[]byte("migration:\n  migration_lock_timeout: 10s\n"), 0o600), qt.IsNil)

	out, err := runUp(
		"--config", configPath,
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("migration.migration_lock_timeout", "sqlite"),
		qt.Commentf("%s", out))
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateUpWithoutMigrationLockTimeoutStillApplies is the control for the
// refusals above: a dialect that cannot lock keeps applying, silently, when
// nobody asked for a lock.
func TestMigrateUpWithoutMigrationLockTimeoutStillApplies(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "control.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Migrations completed successfully")
	c.Assert(out, qt.Not(qt.Contains), "migration advisory lock")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateUpKeepsMigrationLockTimeoutOnLockingDialect is the second control:
// the refusal must not fire where the lock exists. The address has no server,
// so the run gets as far as the connection and fails there, which is the part
// worth asserting -- a refusal that swallowed every dialect would never reach
// it.
func TestMigrateUpKeepsMigrationLockTimeoutOnLockingDialect(t *testing.T) {
	tests := []struct {
		name  string
		dbURL string
	}{
		{name: "postgres", dbURL: "postgres://ptah@127.0.0.1:1/db?sslmode=disable"},
		{name: "mysql", dbURL: "mysql://ptah@127.0.0.1:1/db"},
		{name: "mariadb", dbURL: "mariadb://ptah@127.0.0.1:1/db"},
		{name: "sqlserver", dbURL: "sqlserver://ptah@127.0.0.1:1?database=db"},
		{name: "yugabytedb", dbURL: "yugabytedb://ptah@127.0.0.1:1/db?sslmode=disable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir := writeUpMigrations(t)

			out, err := runUp(
				"--db-url", test.dbURL,
				"--migrations-dir", migrationsDir,
				"--migration-lock-timeout", "10s",
				"--connect-timeout", "2s",
			)

			c.Assert(err, qt.ErrorMatches, `(?s)error connecting to database:.*`, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Not(qt.Contains), "session advisory lock")
		})
	}
}

// fmtMigrationLockRefusal renders the refusal a dialect with no advisory lock
// answers a migration lock timeout with, for the spelling that carried it.
func fmtMigrationLockRefusal(request, dialect string) string {
	return regexp.QuoteMeta(
		request + ` requested the migration advisory lock, and dialect "` + dialect + `" has none: ` +
			`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
			`Remove ` + request + ` to run without a lock`)
}
