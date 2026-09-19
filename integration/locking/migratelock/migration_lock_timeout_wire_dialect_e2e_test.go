//go:build integration

package migratelock_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

// A PostgreSQL-wire URL does not name its product, and the server does. These
// tests hand the versioned commands an address every URL reader calls
// PostgreSQL, pointing at a CockroachDB that has no session advisory lock, and
// measure what an explicit --migration-lock-timeout does with it
// (stokaro/ptah#3417).
//
// A server decides this, so no unit test can reach it: the dialect comes from
// the banner CockroachDB writes, and nothing offline produces one.
//
// Each test reads the database back. The exit code alone would pass against a
// build that refused after applying, which is the half the reporter cares
// about: two runners on one history is what the lock exists to prevent.

// probeUp and probeDown are the one migration these tests apply and roll back.
const (
	probeUp   = "CREATE TABLE lock_probe (id INT PRIMARY KEY, name TEXT NOT NULL);\n"
	probeDown = "DROP TABLE lock_probe;\n"
)

// probeTable is the table probeUp declares, read back by name.
const probeTable = "lock_probe"

// revisionTable is Ptah's own bookkeeping table. A refused run must not create
// it either: initializing it is the first write a migrator makes.
const revisionTable = "schema_migrations"

// flagRefusal and environmentRefusal are the part of each refusal that carries
// the finding: the dialect named is the one the server reported, not the one
// the URL spelled.
const (
	flagRefusal = `--migration-lock-timeout requested the migration advisory lock, ` +
		`and dialect "cockroachdb" has none`
	environmentRefusal = `PTAH_MIGRATION_LOCK_TIMEOUT requested the migration advisory lock, ` +
		`and dialect "cockroachdb" has none`
)

func TestMigrationsUpLockTimeoutOnPostgresWireTargetE2E_FailurePath(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	work := c.TempDir()
	migrationsDir := writeProbeMigrations(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_migration_lock_wire_up")

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "up",
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
		"--migration-lock-timeout", "10s",
	)

	c.Assert(refused.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, flagRefusal)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.HasLen, 0)
}

// TestMigrationsUpEnvironmentLockTimeoutOnPostgresWireTargetE2E_FailurePath is
// where the versioned commands part from `ptah schema apply`, which notes an
// environment-set value and applies unlocked. PTAH_MIGRATION_LOCK_TIMEOUT means
// this one lock on every command that reads it, so a value arriving that way is
// addressed to this run.
func TestMigrationsUpEnvironmentLockTimeoutOnPostgresWireTargetE2E_FailurePath(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "10s")
	c := qt.New(t)
	work := c.TempDir()
	migrationsDir := writeProbeMigrations(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_migration_lock_wire_env")

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "up",
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
	)

	c.Assert(refused.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, environmentRefusal)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.HasLen, 0)
}

// TestMigrationsDownLockTimeoutOnPostgresWireTargetE2E_FailurePath is the
// destructive half, on a target the same binary brought up first.
func TestMigrationsDownLockTimeoutOnPostgresWireTargetE2E_FailurePath(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	work := c.TempDir()
	migrationsDir := writeProbeMigrations(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_migration_lock_wire_down")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "up",
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
	)
	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "down",
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
		"--target", "0",
		"--confirm",
		"--migration-lock-timeout", "10s",
	)

	c.Assert(refused.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, flagRefusal)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.Contains, probeTable)
}

// TestMigrationsUpWithoutLockTimeoutOnPostgresWireTargetE2E_HappyPath is the
// control. Without it the failure-path tests cannot tell a refusal from a
// target nothing could reach.
func TestMigrationsUpWithoutLockTimeoutOnPostgresWireTargetE2E_HappyPath(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	work := c.TempDir()
	migrationsDir := writeProbeMigrations(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_migration_lock_wire_control")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "up",
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
	)

	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Not(qt.Contains), flagRefusal)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.DeepEquals, []string{probeTable, revisionTable})
}

// TestMigrationsUpLockTimeoutOnAliasTargetE2E_FailurePath is the other half of
// the pair. Here the URL names the product, so the decision taken before the
// connection answers on its own and the target is never opened.
func TestMigrationsUpLockTimeoutOnAliasTargetE2E_FailurePath(t *testing.T) {
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")
	c := qt.New(t)
	work := c.TempDir()
	migrationsDir := writeProbeMigrations(c, work)
	wireURL := emptyTargetDatabase(c, "ptah_migration_lock_alias")
	aliasURL := productNamingURL(c, wireURL)

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"migrations", "up",
		"--db-url", aliasURL,
		"--migrations-dir", migrationsDir,
		"--migration-lock-timeout", "10s",
	)

	c.Assert(refused.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, flagRefusal)
	c.Assert(tableNames(c, c.Context(), wireURL), qt.HasLen, 0)
}

// writeProbeMigrations puts a one-migration ptah-format directory where the
// command can read it and returns the path.
func writeProbeMigrations(c *qt.C, dir string) string {
	c.Helper()

	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	files := map[string]string{
		"0000000001_probe.up.sql":   probeUp,
		"0000000001_probe.down.sql": probeDown,
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	return migrationsDir
}

// emptyTargetDatabase creates a database of its own on the live CockroachDB and
// returns its PostgreSQL-wire URL.
//
// The database is the test's own so a refusal can be asserted as "this database
// holds nothing", which a shared one could never answer.
//
// The address comes from [dbtarget.DriverDSN], which owns the wire spelling of
// a CockroachDB address -- it rewrites a `cockroachdb://` alias to the scheme
// pgx reads, and the result is also a URL ptah connects with. That is the shape
// these tests are about: an address that says PostgreSQL to everything reading
// the string, in front of a server that is not PostgreSQL.
func emptyTargetDatabase(c *qt.C, name string) string {
	c.Helper()

	serverURL := dbtarget.DriverDSN(c, dbtarget.CockroachDB)

	// The premise. Where the URL's own dialect has no migration lock, the
	// decision taken from --db-url refuses on its own and these tests measure
	// nothing about what the server said.
	urlDialect, err := atlasurl.DialectFromURL(serverURL)
	c.Assert(err, qt.IsNil)
	c.Assert(dblock.Supported(urlDialect), qt.IsTrue,
		qt.Commentf("the wire URL resolves to dialect %q, which must be one that locks", urlDialect))

	execOnServer(c, c.Context(), serverURL, "DROP DATABASE IF EXISTS "+name+" CASCADE")
	execOnServer(c, c.Context(), serverURL, "CREATE DATABASE "+name)
	c.Cleanup(func() {
		// t.Context is canceled before cleanup runs, so the drop carries its
		// own bounded context.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		execOnServer(c, ctx, serverURL, "DROP DATABASE IF EXISTS "+name+" CASCADE")
	})

	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// productNamingURL rewrites a wire address to the scheme that names the
// product, and asserts that the rewrite did name it.
func productNamingURL(c *qt.C, wireURL string) string {
	c.Helper()

	parsed, err := url.Parse(wireURL)
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "cockroachdb"
	aliasURL := parsed.String()

	urlDialect, err := atlasurl.DialectFromURL(aliasURL)
	c.Assert(err, qt.IsNil)
	c.Assert(urlDialect, qt.Equals, "cockroachdb")
	return aliasURL
}

// execOnServer runs one statement through a driver of its own, so the setup and
// the read-back never pass through the reader the command used.
func execOnServer(c *qt.C, ctx context.Context, dsn, statement string) {
	c.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, statement)
	c.Assert(err, qt.IsNil)
}

// tableNames lists what the target database holds, sorted, so an empty answer
// and a populated answer are both assertable values.
func tableNames(c *qt.C, ctx context.Context, dsn string) []string {
	c.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx,
		`SELECT table_name FROM information_schema.tables `+
			`WHERE table_schema = 'public' ORDER BY table_name`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	names := []string{}
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
