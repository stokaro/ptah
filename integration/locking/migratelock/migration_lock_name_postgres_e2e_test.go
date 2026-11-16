//go:build integration

package migratelock_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/clirun"
	pglock "ptah.run/internal/dblockext/postgres"
	"ptah.run/internal/dbtarget"
)

// Two migration histories in one PostgreSQL database are two schemas with a
// revision table each. Nothing in a schema name separates their lock: the
// PostgreSQL advisory lock key comes from the lock name alone, so both
// histories wait for each other under the default name `ptah_migrate`.
// `--migration-lock-name` is what separates them, and the same name is what
// keeps two runners of one history in line.
//
// The database decides whether a session holds a lock, so these tests hold one
// from a session of their own, on the key the migrator derives from the name
// ([pglock.Key]), and measure what each run does while it is held.

const (
	lockNameHistoryA = "svc_a"
	lockNameHistoryB = "svc_b"
	lockNameA        = "svc_a_lock"
	lockNameB        = "svc_b_lock"
	// defaultLockName is the name a run takes when nobody chose one.
	defaultLockName = "ptah_migrate"
	// heldTimeout is how long a bounded run waits for a lock a test holds.
	heldTimeout = "1s"
	// stillWaiting is how long an unbounded run is watched for completing
	// while its lock is held.
	stillWaiting = 2 * time.Second
)

// lockNameTarget is a scratch PostgreSQL database holding the two schemas.
type lockNameTarget struct {
	url string
	// dir is where each history's migrations live, keyed by schema.
	dir map[string]string
}

// newLockNameTarget creates a database of the test's own with one schema per
// history, and a one-migration directory for each.
func newLockNameTarget(c *qt.C) lockNameTarget {
	c.Helper()

	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	name := fmt.Sprintf("ptah_lock_name_%d", time.Now().UnixNano())
	execOnServer(c, c.Context(), adminURL, "CREATE DATABASE "+name)
	c.Cleanup(func() {
		// The test's context is canceled before cleanup runs, so the drop
		// carries its own bounded one.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		execOnServer(c, ctx, adminURL, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	target := lockNameTarget{url: parsed.String(), dir: make(map[string]string)}

	for _, schema := range []string{lockNameHistoryA, lockNameHistoryB} {
		execOnServer(c, c.Context(), target.url, "CREATE SCHEMA "+schema)
		target.dir[schema] = writeHistory(c, schema)
	}
	return target
}

// writeHistory writes a one-migration directory whose table lives in schema.
func writeHistory(c *qt.C, schema string) string {
	c.Helper()

	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	files := map[string]string{
		"0000000001_items.up.sql":   "CREATE TABLE " + schema + ".items (id BIGINT PRIMARY KEY);\n",
		"0000000001_items.down.sql": "DROP TABLE " + schema + ".items;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// verb is `ptah migrations <verb>` against one history of the target, with the
// flags every verb here takes. extra carries the verb's own flags.
func (t lockNameTarget) verb(schema, verb string, extra ...string) []string {
	return append([]string{
		"migrations", verb,
		"--db-url", t.url,
		"--migrations-dir", t.dir[schema],
		"--migrations-schema", schema,
	}, extra...)
}

// tablesIn lists the tables of schema, sorted.
func (t lockNameTarget) tablesIn(c *qt.C, schema string) []string {
	c.Helper()

	conn, err := pgx.Connect(c.Context(), t.url)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(c.Context(),
		`SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name`, schema)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// holdLock takes the advisory lock a migrator takes under name, on a session of
// the test's own, and returns the function that releases it. It is released at
// the end of the test too, so a failed assertion cannot leave a run waiting.
func holdLock(c *qt.C, target lockNameTarget, name string) (release func()) {
	c.Helper()

	conn, err := pgx.Connect(c.Context(), target.url)
	c.Assert(err, qt.IsNil)
	_, err = conn.Exec(c.Context(), "SELECT pg_advisory_lock($1)", pglock.Key(name))
	c.Assert(err, qt.IsNil)
	// Closing the session releases a session-level advisory lock, and closing a
	// closed one is harmless, so the cleanup is safe after an explicit release.
	release = func() { _ = conn.Close(context.Background()) }
	c.Cleanup(release)
	return release
}

// cleanEnv is the environment of a run with no PTAH_* variable in it, so the
// flags a test passes are the only inputs.
func cleanEnv(extra ...string) []string {
	env := slices.DeleteFunc(os.Environ(), func(entry string) bool { return strings.HasPrefix(entry, "PTAH_") })
	return append(env, extra...)
}

// run is one `ptah` run, started in the background.
type run struct {
	done chan clirun.Result
}

// start runs args as `ptah` in the background. Building happens first, on the
// test's own goroutine, which is the one a failed build can be reported on.
func start(c *qt.C, env []string, args ...string) *run {
	c.Helper()
	clirun.Build(c, clirun.Ptah)
	r := &run{done: make(chan clirun.Result, 1)}
	go func() { r.done <- clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: env}, args...) }()
	return r
}

// stillRunning fails the test when the run finished within stillWaiting, which
// is how a run that did not wait for the held lock shows itself.
func (r *run) stillRunning(c *qt.C) {
	c.Helper()
	select {
	case got := <-r.done:
		c.Fatalf("the run finished while its lock was held: exit %d\nstdout:\n%s\nstderr:\n%s",
			got.ExitCode, got.Stdout, got.Stderr)
	case <-time.After(stillWaiting):
	}
}

// finished waits for the run to end and returns what it produced.
func (r *run) finished(c *qt.C) clirun.Result {
	c.Helper()
	select {
	case got := <-r.done:
		return got
	case <-time.After(time.Minute):
		c.Fatal("the run did not finish after its lock was released")
		return clirun.Result{}
	}
}

func runPtah(c *qt.C, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: env}, args...)
}

func described(got clirun.Result) qt.Comment {
	return qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", got.ExitCode, got.Stdout, got.Stderr)
}

// TestMigrationsUpWithDifferentLockNamesProgressesTogetherE2E_HappyPath holds
// history A's lock, as a long-running runner of A does. History B, under its
// own name, completes while A is held, and A itself waits until the lock goes.
func TestMigrationsUpWithDifferentLockNamesProgressesTogetherE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)
	release := holdLock(c, target, lockNameA)
	runnerA := start(c, cleanEnv(),
		target.verb(lockNameHistoryA, "up", "--migration-lock-name", lockNameA)...)

	gotB := runPtah(c, cleanEnv(),
		target.verb(lockNameHistoryB, "up", "--migration-lock-name", lockNameB, "--migration-lock-timeout", heldTimeout)...)

	c.Assert(gotB.ExitCode, qt.Equals, 0, described(gotB))
	c.Assert(target.tablesIn(c, lockNameHistoryB), qt.DeepEquals, []string{"items", "schema_migrations", "schema_migrations_log"})
	runnerA.stillRunning(c)
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)

	release()
	gotA := runnerA.finished(c)

	c.Assert(gotA.ExitCode, qt.Equals, 0, described(gotA))
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.DeepEquals, []string{"items", "schema_migrations", "schema_migrations_log"})
}

// TestMigrationsUpWithTheSameLockNameWaitsE2E_FailurePath holds the lock a name
// selects and runs the history under that name: the run waits and gives up when
// its timeout ends, before it creates the revision table. The timeout names the
// lock, so the operator can tell which lock another runner holds.
func TestMigrationsUpWithTheSameLockNameWaitsE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)
	holdLock(c, target, lockNameA)

	got := runPtah(c, cleanEnv(),
		target.verb(lockNameHistoryA, "up", "--migration-lock-name", lockNameA, "--migration-lock-timeout", heldTimeout)...)

	c.Assert(got.ExitCode, qt.Equals, 2, described(got))
	c.Assert(got.Stderr, qt.Contains, `timed out acquiring migration lock "`+lockNameA+`" for postgres after 1s`)
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
}

// TestMigrationsUpWithoutALockNameKeepsTheDefaultE2E_FailurePath holds the
// default lock and runs two histories: the one that names its lock proceeds,
// and the one that does not waits on the default, which is what two histories
// that never chose a name have always done.
func TestMigrationsUpWithoutALockNameKeepsTheDefaultE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)
	holdLock(c, target, defaultLockName)

	unnamed := runPtah(c, cleanEnv(),
		target.verb(lockNameHistoryA, "up", "--migration-lock-timeout", heldTimeout)...)
	named := runPtah(c, cleanEnv(),
		target.verb(lockNameHistoryB, "up", "--migration-lock-name", lockNameB, "--migration-lock-timeout", heldTimeout)...)

	c.Assert(unnamed.ExitCode, qt.Equals, 2, described(unnamed))
	c.Assert(unnamed.Stderr, qt.Contains, `timed out acquiring migration lock "`+defaultLockName+`" for postgres after 1s`)
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
	c.Assert(named.ExitCode, qt.Equals, 0, described(named))
	c.Assert(target.tablesIn(c, lockNameHistoryB), qt.Contains, "items")
}

// TestMigrationsUpWithTheSameLockNameSerializesE2E_HappyPath starts two runners
// of one history under one name while the lock is held, so both queue before
// either has created the revision table. Once the lock goes, they run one after
// the other: both succeed, and the migration is recorded once. Racing the first
// CREATE of the revision table is what the lock prevents.
func TestMigrationsUpWithTheSameLockNameSerializesE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)
	release := holdLock(c, target, lockNameA)
	args := target.verb(lockNameHistoryA, "up", "--migration-lock-name", lockNameA)
	first := start(c, cleanEnv(), args...)
	second := start(c, cleanEnv(), args...)

	first.stillRunning(c)
	second.stillRunning(c)
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
	release()
	gotFirst := first.finished(c)
	gotSecond := second.finished(c)

	c.Assert(gotFirst.ExitCode, qt.Equals, 0, described(gotFirst))
	c.Assert(gotSecond.ExitCode, qt.Equals, 0, described(gotSecond))
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.DeepEquals, []string{"items", "schema_migrations", "schema_migrations_log"})
	c.Assert(strings.Count(gotFirst.Stdout+gotSecond.Stdout, "Migrations completed successfully"), qt.Equals, 2)
}

// TestMigrationVerbsTakeTheSelectedLockE2E_FailurePath holds the lock a name
// selects and runs each verb that writes revision state under that name, on a
// schema with no revision table yet. A verb that did not pass the name on would
// take the default lock, which nobody holds, and finish; one that read the
// history before taking the lock would create the revision table while another
// runner holds it.
func TestMigrationVerbsTakeTheSelectedLockE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		verb string
		args []string
	}{
		{name: "up", verb: "up"},
		{name: "baseline", verb: "baseline", args: []string{"--version", "1", "--force"}},
		{name: "down", verb: "down", args: []string{"--target", "0", "--confirm"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newLockNameTarget(c)
			holdLock(c, target, lockNameA)
			flags := append([]string{"--migration-lock-name", lockNameA, "--migration-lock-timeout", heldTimeout}, test.args...)

			got := runPtah(c, cleanEnv(), target.verb(lockNameHistoryA, test.verb, flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 2, described(got))
			c.Assert(got.Stderr, qt.Contains, `timed out acquiring migration lock "`+lockNameA+`"`)
			c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
		})
	}
}

// TestMigrationsDownTakesTheSelectedLockE2E_FailurePath applies history A, then
// holds its lock and rolls back under that name.
func TestMigrationsDownTakesTheSelectedLockE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)
	applied := runPtah(c, cleanEnv(), target.verb(lockNameHistoryA, "up", "--migration-lock-name", lockNameA)...)
	c.Assert(applied.ExitCode, qt.Equals, 0, described(applied))
	holdLock(c, target, lockNameA)

	got := runPtah(c, cleanEnv(), target.verb(lockNameHistoryA, "down",
		"--target", "0", "--confirm",
		"--migration-lock-name", lockNameA, "--migration-lock-timeout", heldTimeout)...)

	c.Assert(got.ExitCode, qt.Equals, 2, described(got))
	c.Assert(got.Stderr, qt.Contains, `timed out acquiring migration lock "`+lockNameA+`"`)
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.Contains, "items")
}

// lockNameConfig writes a project config whose migration lock name is name,
// and returns its path.
func lockNameConfig(c *qt.C, name string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(path, []byte("migration:\n  migration_lock_name: "+name+"\n"), 0o600), qt.IsNil)
	return path
}

// TestMigrationVerbsReadTheLockNameFromConfigE2E_FailurePath holds the lock the
// project config names and runs each verb with a timeout that reads the config,
// with no flag. A verb that did not read the key would take the default lock,
// which nobody holds, and finish.
func TestMigrationVerbsReadTheLockNameFromConfigE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		verb string
		args []string
	}{
		{name: "up", verb: "up"},
		{name: "down", verb: "down", args: []string{"--target", "0", "--confirm"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newLockNameTarget(c)
			holdLock(c, target, lockNameA)
			flags := append([]string{"--config", lockNameConfig(c, lockNameA), "--migration-lock-timeout", heldTimeout}, test.args...)

			got := runPtah(c, cleanEnv(), target.verb(lockNameHistoryA, test.verb, flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 2, described(got))
			c.Assert(got.Stderr, qt.Contains, `timed out acquiring migration lock "`+lockNameA+`"`)
			c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
		})
	}
}

// TestMigrationsRepairAndSetTakeTheSelectedLockE2E_HappyPath covers the two
// verbs that take no lock timeout: they wait for as long as the lock is held.
// Each is watched while the lock a name selects is held, finishes once it goes,
// and wrote nothing before. The name comes from the flag, which outranks the
// config naming a lock nobody holds, or from the config alone.
func TestMigrationsRepairAndSetTakeTheSelectedLockE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		verb   string
		config string
		args   []string
	}{
		{name: "repair, flag", verb: "repair", config: lockNameB,
			args: []string{"--migration-lock-name", lockNameA, "--version", "1", "--force"}},
		{name: "repair, config", verb: "repair", config: lockNameA, args: []string{"--version", "1", "--force"}},
		{name: "set, flag", verb: "set", config: lockNameB, args: []string{"--migration-lock-name", lockNameA, "--version", "1"}},
		{name: "set, config", verb: "set", config: lockNameA, args: []string{"--version", "1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newLockNameTarget(c)
			release := holdLock(c, target, lockNameA)
			flags := append([]string{"--config", lockNameConfig(c, test.config)}, test.args...)
			verb := start(c, cleanEnv(), target.verb(lockNameHistoryA, test.verb, flags...)...)

			verb.stillRunning(c)
			c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
			release()
			got := verb.finished(c)

			c.Assert(got.ExitCode, qt.Equals, 0, described(got))
			c.Assert(target.tablesIn(c, lockNameHistoryA), qt.Contains, "schema_migrations")
		})
	}
}

// TestMigrationsUpLockNamePrecedenceE2E_HappyPath reads the lock a dry run
// reports under each combination of flag, environment variable and project
// config. The flag outranks the variable, and the variable outranks the file.
func TestMigrationsUpLockNamePrecedenceE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		flag   []string
		env    []string
		config string
		want   string
	}{
		{name: "flag over environment and config", flag: []string{"--migration-lock-name", "from_flag"},
			env: []string{"PTAH_MIGRATION_LOCK_NAME=from_env"}, config: "from_config", want: "from_flag"},
		{name: "environment over config", env: []string{"PTAH_MIGRATION_LOCK_NAME=from_env"},
			config: "from_config", want: "from_env"},
		{name: "config alone", config: "from_config", want: "from_config"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newLockNameTarget(c)
			configPath := filepath.Join(c.TempDir(), "ptah.yaml")
			c.Assert(os.WriteFile(configPath, []byte("migration:\n  migration_lock_name: "+test.config+"\n"), 0o600), qt.IsNil)
			flags := append([]string{"--dry-run", "--config", configPath}, test.flag...)

			got := runPtah(c, cleanEnv(test.env...), target.verb(lockNameHistoryA, "up", flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, described(got))
			c.Assert(got.Stdout, qt.Contains, "Migration lock: "+test.want+"\n")
		})
	}
}

// TestMigrationVerbsReportTheSelectedLockE2E_HappyPath reads the lock line each
// verb that prints a report writes, on a dry run, when a name was chosen.
func TestMigrationVerbsReportTheSelectedLockE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		verb string
		args []string
	}{
		{name: "up", verb: "up"},
		{name: "down", verb: "down", args: []string{"--target", "0"}},
		{name: "baseline", verb: "baseline", args: []string{"--version", "1", "--force"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newLockNameTarget(c)
			flags := append([]string{"--dry-run", "--migration-lock-name", lockNameA}, test.args...)

			got := runPtah(c, cleanEnv(), target.verb(lockNameHistoryA, test.verb, flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, described(got))
			c.Assert(got.Stdout+got.Stderr, qt.Contains, "Migration lock: "+lockNameA+"\n")
			c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
		})
	}
}

// TestMigrationsUpOmittedLockNameReportsNoLockE2E_HappyPath is the control for
// the precedence test: nobody chose a name, so the report carries no lock line
// and the run takes the default.
func TestMigrationsUpOmittedLockNameReportsNoLockE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)

	got := runPtah(c, cleanEnv(), target.verb(lockNameHistoryA, "up", "--dry-run")...)

	c.Assert(got.ExitCode, qt.Equals, 0, described(got))
	c.Assert(got.Stdout, qt.Not(qt.Contains), "Migration lock:")
}
