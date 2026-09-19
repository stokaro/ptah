package migratebaseline_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migratebaseline"
	"ptah.run/internal/cli/root"
)

// `migrations baseline` writes revision rows under the same session advisory
// lock the other versioned verbs take, so a lock timeout aimed at a dialect
// that has none is refused here too (stokaro/ptah#3417). It reads no project
// config for the timeout, so the command line and the environment variable are
// the two spellings it can carry.

// TestMigrateBaselineRefusesMigrationLockTimeoutBeforeConnecting drives every
// dialect with no advisory lock at an address nothing is listening on. The
// refusal, rather than a connection failure, is what says the decision is made
// from the URL.
func TestMigrateBaselineRefusesMigrationLockTimeoutBeforeConnecting(t *testing.T) {
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
			migrationsDir := writeBaselineMigrations(c)

			out, err := runBaseline(
				"--db-url", test.dbURL,
				"--migrations-dir", migrationsDir,
				"--migration-lock-timeout", "10s",
			)

			c.Assert(err, qt.ErrorMatches, fmtBaselineLockRefusal("--migration-lock-timeout", test.dialect),
				qt.Commentf("%s", out))
			c.Assert(err.Error(), qt.Not(qt.Contains), "error connecting to database")
		})
	}
}

// TestMigrateBaselineRefusedMigrationLockTimeoutRecordsNothing measures the
// target: baseline's whole output is revision rows, and an absent database file
// is the strongest reading of "none were written".
func TestMigrateBaselineRefusedMigrationLockTimeoutRecordsNothing(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeBaselineMigrations(c)
	dbPath := filepath.Join(t.TempDir(), "baseline.db")

	out, err := runBaseline(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--migration-lock-timeout", "10s",
	)

	c.Assert(err, qt.ErrorMatches, fmtBaselineLockRefusal("--migration-lock-timeout", "sqlite"),
		qt.Commentf("%s", out))
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateBaselineRefusesEnvironmentMigrationLockTimeout covers the second
// spelling, named in the message so an operator who exported it can find it.
func TestMigrateBaselineRefusesEnvironmentMigrationLockTimeout(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "10s")
	c := qt.New(t)
	migrationsDir := writeBaselineMigrations(c)
	dbPath := filepath.Join(t.TempDir(), "baseline-env.db")

	out, err := runBaselineThroughRoot(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
	)

	c.Assert(err, qt.ErrorMatches, fmtBaselineLockRefusal("PTAH_MIGRATION_LOCK_TIMEOUT", "sqlite"),
		qt.Commentf("%s", out))
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateBaselineWithoutMigrationLockTimeoutStillRecords is the control: a
// dialect that cannot lock keeps recording the baseline when nobody asked for a
// lock.
func TestMigrateBaselineWithoutMigrationLockTimeoutStillRecords(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	migrationsDir := writeBaselineMigrations(c)
	dbPath := filepath.Join(t.TempDir(), "baseline-control.db")

	out, err := runBaseline(
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", migrationsDir,
		"--force",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(baselineRecordedVersion(c, dbPath), qt.Equals, int64(2))
}

// writeBaselineMigrations writes a two-migration ptah-format directory whose
// newest version is the one a baseline records.
func writeBaselineMigrations(c *qt.C) string {
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

func runBaseline(args ...string) (string, error) {
	cmd := migratebaseline.NewMigrateBaselineCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// runBaselineThroughRoot drives the shipped command tree. The PTAH_* binding is
// installed on the root command, so a run assembled from the leaf alone reads
// no environment variable and can say nothing about a rule over one.
func runBaselineThroughRoot(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"migrations", "baseline"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

func baselineRecordedVersion(c *qt.C, dbPath string) int64 {
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

// fmtBaselineLockRefusal renders the refusal a dialect with no advisory lock
// answers a migration lock timeout with, for the spelling that carried it.
func fmtBaselineLockRefusal(request, dialect string) string {
	return regexp.QuoteMeta(
		request + ` requested the migration advisory lock, and dialect "` + dialect + `" has none: ` +
			`only postgres, yugabytedb, mysql, mariadb, sqlserver take a session advisory lock. ` +
			`Remove ` + request + ` to run without a lock`)
}
