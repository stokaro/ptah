//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	pglock "ptah.run/internal/dblockext/postgres"
	"ptah.run/internal/testutils"
)

// `ptah migrations down --plan` derives its rollback before the prompt and
// runs it after, holding no lock while a person reads the plan. These runs
// drive the shipped binary against PostgreSQL and change the target in that
// window: the statements that run are the ones shown, and a rollback derived
// for versions that are no longer the applied ones is refused under the
// migration lock before any of them runs.

// migrationLockName is the advisory lock `ptah migrations up` and `down` take,
// named as the migrator names it by default.
const migrationLockName = "ptah_migrate"

// downPlanTargetRun is a PostgreSQL target at version 2 of
// writeDownPlanMigrations, and an empty dev database.
type downPlanTargetRun struct {
	migrationsDir, target, dev string
}

func newDownPlanTargetRun(c *qt.C) downPlanTargetRun {
	c.Helper()
	run := downPlanTargetRun{
		migrationsDir: writeDownPlanMigrations(c, c.TempDir()),
		target:        postgresScratchDevURL(c, "ptah_downtc_target"),
		dev:           postgresScratchDevURL(c, "ptah_downtc_dev"),
	}
	run.up(c)
	return run
}

// up is `ptah migrations up` over the directory, as another operator runs it.
func (r downPlanTargetRun) up(c *qt.C) {
	c.Helper()
	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()},
		"migrations", "up", "--db-url", r.target, "--migrations-dir", r.migrationsDir)
	c.Assert(got.ExitCode, qt.Equals, 0, historyComment(got))
}

// addCommentsMigration writes version 3 into the directory.
func (r downPlanTargetRun) addCommentsMigration(c *qt.C) {
	c.Helper()
	for name, body := range map[string]string{
		"0000000000003_create_comments.up.sql":   "CREATE TABLE comments (id SERIAL PRIMARY KEY);\n",
		"0000000000003_create_comments.down.sql": "DROP TABLE comments;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(r.migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}
}

// down runs `ptah migrations down --plan --confirm` back to version 1.
func (r downPlanTargetRun) down(c *qt.C, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables(), Timeout: 2 * time.Minute},
		append([]string{"migrations", "down", "--db-url", r.target, "--migrations-dir", r.migrationsDir,
			"--target", "1", "--plan", "--shadow-db", r.dev, "--confirm"}, args...)...)
}

// userTables is the target's public tables, without the revision bookkeeping.
func (r downPlanTargetRun) userTables(c *qt.C) []string {
	c.Helper()
	return slices.DeleteFunc(postgresPublicTables(c, r.target), func(table string) bool {
		return strings.HasPrefix(table, "schema_migrations")
	})
}

// appliedVersions reads the applied versions back from the revision table,
// oldest first.
func (r downPlanTargetRun) appliedVersions(c *qt.C) []int64 {
	c.Helper()
	rows, err := openReadScopeDatabase(c, r.target).QueryContext(c.Context(),
		"SELECT version FROM schema_migrations WHERE state = 'applied' ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

// promptedDown is `ptah migrations down --plan` without --confirm, running
// while the test decides when the operator answers.
type promptedDown struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *lockedBuffer
	stderr *lockedBuffer
}

// startPromptedDown starts the rollback back to version 1 and waits until it
// asks for confirmation.
func (r downPlanTargetRun) startPromptedDown(c *qt.C) *promptedDown {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	c.Cleanup(cancel)
	// #nosec G204 -- clirun.Build builds the binary from this tree, and every
	// argument is a path or URL the test created.
	cmd := exec.CommandContext(ctx, clirun.Build(c, clirun.Ptah),
		"migrations", "down", "--db-url", r.target, "--migrations-dir", r.migrationsDir,
		"--target", "1", "--plan", "--shadow-db", r.dev)
	cmd.Dir = c.TempDir()
	cmd.Env = environmentWithoutPtahVariables()
	stdin, err := cmd.StdinPipe()
	c.Assert(err, qt.IsNil)
	run := &promptedDown{cmd: cmd, stdin: stdin, stdout: &lockedBuffer{}, stderr: &lockedBuffer{}}
	cmd.Stdout, cmd.Stderr = run.stdout, run.stderr
	c.Assert(cmd.Start(), qt.IsNil)
	c.Assert(run.stdout.waitFor("Type 'YES' to confirm", 2*time.Minute), qt.IsTrue, run.comment())
	return run
}

// answer types YES and returns the exit status the run ends with.
func (p *promptedDown) answer(c *qt.C) int {
	c.Helper()
	_, err := io.WriteString(p.stdin, "YES\n")
	c.Assert(err, qt.IsNil)
	c.Assert(p.stdin.Close(), qt.IsNil)
	waitErr := p.cmd.Wait()
	_, exited := errors.AsType[*exec.ExitError](waitErr)
	c.Assert(waitErr == nil || exited, qt.IsTrue, qt.Commentf("wait: %v", waitErr), p.comment())
	return exitCodeOf(waitErr)
}

func (p *promptedDown) comment() qt.Comment {
	return qt.Commentf("stdout:\n%s\nstderr:\n%s", p.stdout.String(), p.stderr.String())
}

// lockedBuffer is a buffer a running command writes to while the test reads
// it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitFor reports whether the buffer comes to hold text within limit.
func (b *lockedBuffer) waitFor(text string, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if strings.Contains(b.String(), text) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// TestMigrationsDownPlanRunsTheShownPlanE2E_HappyPath creates a table on the
// target while the prompt waits, outside any migration. The run executes the
// plan it showed: the table nobody was shown stays.
func TestMigrationsDownPlanRunsTheShownPlanE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	run := newDownPlanTargetRun(c)
	prompted := run.startPromptedDown(c)

	execPostgresFamilySQL(c, c.Context(), run.target, "CREATE TABLE late_tool (id BIGINT PRIMARY KEY)")
	code := prompted.answer(c)

	c.Assert(code, qt.Equals, 0, prompted.comment())
	c.Assert(prompted.stdout.String(), qt.Matches, `(?s).*Derived rollback plan: 1 statements\n  DROP TABLE "posts"[^\n]*\n.*`)
	c.Assert(run.userTables(c), qt.DeepEquals, []string{"late_tool", "users"})
	c.Assert(run.appliedVersions(c), qt.DeepEquals, []int64{1})
}

// TestMigrationsDownPlanRefusesAChangedTargetE2E_FailurePath applies version
// 3 while the prompt waits. That `migrations up` succeeds, so the waiting run
// holds no lock. Once answered, the run takes the migration lock, reads the
// applied migrations again and refuses before any statement runs: every table
// and every revision row is still there.
func TestMigrationsDownPlanRefusesAChangedTargetE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	run := newDownPlanTargetRun(c)
	prompted := run.startPromptedDown(c)

	run.addCommentsMigration(c)
	run.up(c)
	code := prompted.answer(c)

	c.Assert(code, qt.Equals, 2, prompted.comment())
	c.Assert(stderrErrors(prompted.stderr.String()), qt.DeepEquals, []string{`error: error running down migrations: ` +
		`the rollback to version 1 was derived for the applied migrations [2], ` +
		`and the migrations applied above version 1 are now [3 2]; nothing was rolled back. ` +
		`Run migrations down --plan again to derive the rollback from the database as it is now`}, prompted.comment())
	c.Assert(run.userTables(c), qt.DeepEquals, []string{"comments", "posts", "users"})
	c.Assert(run.appliedVersions(c), qt.DeepEquals, []int64{1, 2, 3})
}

// TestMigrationsDownPlanWaitsForTheMigrationLockE2E_FailurePath holds the
// migration lock on a session of the test's own. The rollback waits for it
// as long as --migration-lock-timeout allows and then gives up, before any
// statement runs.
func TestMigrationsDownPlanWaitsForTheMigrationLockE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	run := newDownPlanTargetRun(c)
	holder := holdAdvisoryLock(c, run.target, migrationLockName)

	got := run.down(c, "--migration-lock-timeout", "2s")

	c.Assert(got.ExitCode, qt.Equals, 2, historyComment(got))
	c.Assert(stderrErrors(got.Stderr), qt.DeepEquals, []string{`error: error running down migrations: ` +
		`failed to acquire migration lock for migrate down: ` +
		`timed out acquiring migration lock "ptah_migrate" for postgres after 2s`}, historyComment(got))
	c.Assert(run.userTables(c), qt.DeepEquals, []string{"posts", "users"})
	c.Assert(run.appliedVersions(c), qt.DeepEquals, []int64{1, 2})
	c.Assert(holder.PingContext(c.Context()), qt.IsNil)
}

// TestMigrationsDownPlanRunsThePreDownHookE2E_FailurePath refuses in the
// pre-down hook. The derived rollback stops there, as one through the down
// files does.
func TestMigrationsDownPlanRunsThePreDownHookE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	run := newDownPlanTargetRun(c)

	got := run.down(c, "--pre-down-hook", testutils.FailingHookCommand("hook-refused", 9))

	c.Assert(got.ExitCode, qt.Equals, 2, historyComment(got))
	c.Assert(stderrErrors(got.Stderr), qt.DeepEquals, []string{
		`error: error running down migrations: down pre-flight custom command hook failed: exit status 9`,
	}, historyComment(got))
	c.Assert(got.Stderr, qt.Contains, "hook-refused")
	c.Assert(run.userTables(c), qt.DeepEquals, []string{"posts", "users"})
	c.Assert(run.appliedVersions(c), qt.DeepEquals, []int64{1, 2})
}

// holdAdvisoryLock takes the PostgreSQL advisory lock name on a session of
// its own, which it releases when the test ends.
func holdAdvisoryLock(c *qt.C, dbURL, name string) *sql.Conn {
	c.Helper()
	conn, err := openReadScopeDatabase(c, dbURL).Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	_, err = conn.ExecContext(c.Context(), "SELECT pg_advisory_lock($1)", pglock.Key(name))
	c.Assert(err, qt.IsNil)
	return conn
}
