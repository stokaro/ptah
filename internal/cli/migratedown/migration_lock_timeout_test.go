package migratedown_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/cli/root"
)

// `migrations down` takes the same session advisory lock as `migrations up`,
// on the same history, and is the destructive half of the pair. A lock timeout
// aimed at a dialect that has no such lock is refused here too
// (stokaro/ptah#3417).

// TestMigrateDownRefusesMigrationLockTimeoutBeforeConnecting drives every
// dialect with no advisory lock at an address nothing is listening on. The
// refusal, rather than a connection failure, is what says the decision is made
// from the URL.
func TestMigrateDownRefusesMigrationLockTimeoutBeforeConnecting(t *testing.T) {
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
			migrationsDir := writeDownMigrations(c)

			out, err := runDown(
				"--db-url", test.dbURL,
				"--migrations-dir", migrationsDir,
				"--target", "0",
				"--confirm",
				"--migration-lock-timeout", "10s",
			)

			c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("--migration-lock-timeout", test.dialect),
				qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Not(qt.Contains), "error connecting to database")
		})
	}
}

// TestMigrateDownRefusedMigrationLockTimeoutRollsBackNothing measures the
// database rather than the exit code: a rollback that ran and then reported a
// refusal would satisfy the exit code alone.
func TestMigrateDownRefusedMigrationLockTimeoutRollsBackNothing(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeDownMigrations(c)
	dbPath := filepath.Join(t.TempDir(), "rollback.db")
	seedAppliedMigrations(c, dbPath, migrationsDir)

	out, err := runDown(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--target", "0",
		"--confirm",
		"--migration-lock-timeout", "10s",
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("--migration-lock-timeout", "sqlite"),
		qt.Commentf("%s", out))
	c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(2))
	c.Assert(tableCensus(c, dbPath), qt.Contains, "orders")
}

// TestMigrateDownRefusesEnvironmentMigrationLockTimeout covers the spelling a
// deployment pipeline is most likely to use. The variable names this lock on
// every command that reads it, so it asks for the same lock a typed flag does.
func TestMigrateDownRefusesEnvironmentMigrationLockTimeout(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "10s")
	c := qt.New(t)
	migrationsDir := writeDownMigrations(c)
	dbPath := filepath.Join(t.TempDir(), "env-rollback.db")
	seedAppliedMigrations(c, dbPath, migrationsDir)

	out, err := runDownThroughRoot(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--target", "0",
		"--confirm",
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("PTAH_MIGRATION_LOCK_TIMEOUT", "sqlite"),
		qt.Commentf("%s", out))
	c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateDownWithoutMigrationLockTimeoutStillRollsBack is the control: the
// same rollback, on the same target, when nobody asked for a lock.
func TestMigrateDownWithoutMigrationLockTimeoutStillRollsBack(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	migrationsDir := writeDownMigrations(c)
	dbPath := filepath.Join(t.TempDir(), "control.db")
	seedAppliedMigrations(c, dbPath, migrationsDir)

	out, err := runDown(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--target", "0",
		"--confirm",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(0))
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "orders")
}

// TestMigrateDownRefusesProjectConfigMigrationLockTimeout covers the third
// spelling on the destructive verb. A project file configures the run as much
// as a flag does, and the refusal names the key rather than a flag the operator
// never typed.
//
// It is the field constant that this pins. `migration.migration_lock_timeout`
// and `migration.lock_timeout` are neighbors in projectconfig, they mean
// different locks, and reading the wrong one leaves the rollback applying
// unlocked under a configuration that asked for a lock.
func TestMigrateDownRefusesProjectConfigMigrationLockTimeout(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	migrationsDir := writeDownMigrations(c)
	work := t.TempDir()
	dbPath := filepath.Join(work, "config-rollback.db")
	seedAppliedMigrations(c, dbPath, migrationsDir)
	configPath := filepath.Join(work, "ptah.yaml")
	c.Assert(os.WriteFile(configPath,
		[]byte("migration:\n  migration_lock_timeout: 10s\n"), 0o600), qt.IsNil)

	out, err := runDownThroughRoot(
		"--config", configPath,
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--target", "0",
		"--confirm",
	)

	c.Assert(err, qt.ErrorMatches, fmtMigrationLockRefusal("migration.migration_lock_timeout", "sqlite"),
		qt.Commentf("%s", out))
	c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateDownKeepsMigrationLockTimeoutOnLockingDialect is the second
// control: the refusal must not fire where the lock exists, so the run reaches
// the connection and fails there instead.
func TestMigrateDownKeepsMigrationLockTimeoutOnLockingDialect(t *testing.T) {
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
			migrationsDir := writeDownMigrations(c)

			out, err := runDown(
				"--db-url", test.dbURL,
				"--migrations-dir", migrationsDir,
				"--target", "0",
				"--confirm",
				"--migration-lock-timeout", "10s",
				"--connect-timeout", "2s",
			)

			c.Assert(err, qt.ErrorMatches, `(?s)error connecting to database:.*`, qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Not(qt.Contains), "session advisory lock")
		})
	}
}

// writeDownMigrations writes a two-migration ptah-format directory whose second
// version is the one a rollback removes.
func writeDownMigrations(c *qt.C) string {
	c.Helper()

	dir := c.TempDir()
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

// seedAppliedMigrations brings the target to the newest version through
// `migrations up`, so the rollback under test has real history to remove rather
// than rows a fixture wrote.
//
// It drives the leaf command, which carries no PTAH_* binding, so a variable a
// test sets for the rollback cannot reach the setup that has to succeed first.
func seedAppliedMigrations(c *qt.C, dbPath, migrationsDir string) {
	c.Helper()

	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(2))
}

// runDownThroughRoot drives the shipped command tree. The PTAH_* binding is
// installed on the root command, so a run assembled from the leaf alone reads
// no environment variable and can say nothing about a rule over one.
func runDownThroughRoot(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"migrations", "down"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

func downCurrentVersion(c *qt.C, dbPath string) int64 {
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

// fmtMigrationLockRefusal renders the refusal a dialect with no advisory lock
// answers a migration lock timeout with, for the spelling that carried it.
func fmtMigrationLockRefusal(request, dialect string) string {
	return regexp.QuoteMeta(
		request + ` requested the migration advisory lock, and dialect "` + dialect + `" has none: ` +
			`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
			`Remove ` + request + ` to run without a lock`)
}
