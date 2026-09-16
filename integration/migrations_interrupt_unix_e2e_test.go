//go:build integration && unix

package integration_test

// Unix only: the interrupt is delivered with Process.Signal(syscall.SIGINT),
// and Windows cannot send that signal to another process.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// interruptSleepSeconds is how long the interrupted migration body sleeps. It
// has to outlast the time between the statement starting and the signal
// landing by a wide margin, so the signal always arrives mid-statement.
const interruptSleepSeconds = 12

// interruptPromptness is the longest an interrupted run may take to exit after
// the signal. A run that lets the statement finish takes the rest of the sleep,
// which is several seconds more than this.
const interruptPromptness = interruptSleepSeconds * time.Second / 2

// interruptSleepingQuery counts sessions in one database that are executing the
// sleep. The signal is sent on this evidence rather than after a pause, so a
// slow start cannot make the interrupt land before the migration runs.
const interruptSleepingQuery = `
SELECT count(*) FROM pg_stat_activity
WHERE datname = $1 AND state = 'active' AND query LIKE '%pg_sleep%' AND pid <> pg_backend_pid()`

// interruptRevisionsQuery reads the revision table through the raw driver, so
// the answer does not pass through the reader the command itself used. The
// statement counts are part of the answer: a failed row that counts no applied
// statement is what separates a rolled-back migration from a partial one.
const interruptRevisionsQuery = `
SELECT version::text || ' ' || state || ' ' || applied::text || '/' || total::text
FROM schema_migrations ORDER BY version`

// interruptTablesQuery reads which fixture tables exist, from the catalog.
const interruptTablesQuery = `
SELECT tablename::text FROM pg_tables
WHERE schemaname = 'public' AND tablename IN ('widgets', 'gadgets')
ORDER BY tablename COLLATE "C"`

// TestMigrationsUpInterruptedDuringAMigrationE2E sends SIGINT to `ptah
// migrations up` while its second migration is executing, and checks that the
// exit status and the revision table tell the same story.
//
// The status alone cannot show the defect: the root command reports 130 for
// any interrupted process, whether or not the verb noticed. The defect is a
// run that keeps executing after the signal, commits migration 2, prints
// success, and then exits 130 against a database it fully migrated. A run that
// honors the interrupt rolls migration 2 back and records it as failed, which
// is what the migrator records for any failed transactional migration.
//
// The uninterrupted run over a fresh database is the control. Without it, "no
// gadgets table" is also what a migration 2 that never applies would produce.
func TestMigrationsUpInterruptedDuringAMigrationE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	bodies := interruptMigrationBodies{
		up:   fmt.Sprintf("SELECT pg_sleep(%d);\nCREATE TABLE gadgets (id integer PRIMARY KEY);\n", interruptSleepSeconds),
		down: "DROP TABLE gadgets;\n",
	}

	t.Run("an interrupt during migration 2 rolls it back and records the failure", func(t *testing.T) {
		c := qt.New(t)
		fixture := newInterruptFixture(c, ctx, dbURL, "up", bodies)

		run := runInterruptedDuringSleep(c, ctx, fixture, fixture.upArgs()...)

		c.Assert(run.exitCode, qt.Equals, 130, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
		c.Assert(run.stderr, qt.Contains, "interrupt received")
		// The operator stopped the run, so that is what the run reports.
		// Whichever call noticed the cancelation first answers in its own
		// words, and those words are not the answer (docs/exit_codes.md).
		c.Assert(run.stderr, qt.Contains, "error: canceled")
		c.Assert(run.stderr, qt.Not(qt.Contains), "SQL execution failed")
		c.Assert(run.stdout, qt.Not(qt.Contains), "Migrations completed successfully")
		c.Assert(run.afterSignal < interruptPromptness, qt.IsTrue, qt.Commentf("exited %s after the signal", run.afterSignal))
		c.Assert(fixture.revisions(c, ctx), qt.DeepEquals, []string{"1 applied 1/1", "2 failed 0/2"})
		c.Assert(fixture.tables(c, ctx), qt.DeepEquals, []string{"widgets"})
	})

	t.Run("the same directory, not interrupted, applies migration 2", func(t *testing.T) {
		c := qt.New(t)
		fixture := newInterruptFixture(c, ctx, dbURL, "up_control", bodies)

		run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: fixture.workDir}, fixture.upArgs()...)

		c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
		c.Assert(run.Stdout, qt.Contains, "Migrations completed successfully")
		c.Assert(fixture.revisions(c, ctx), qt.DeepEquals, []string{"1 applied 1/1", "2 applied 2/2"})
		c.Assert(fixture.tables(c, ctx), qt.DeepEquals, []string{"gadgets", "widgets"})
	})
}

// TestMigrationsDownInterruptedDuringARollbackE2E sends SIGINT to `ptah
// migrations down` while the down body of migration 2 is executing.
//
// This is the destructive direction: an operator who presses Ctrl-C to stop a
// rollback must not find the DROP committed behind a signal status. The
// uninterrupted rollback over a fresh database is the control, proving the down
// body does drop the table when nobody stops it.
func TestMigrationsDownInterruptedDuringARollbackE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	bodies := interruptMigrationBodies{
		up:   "CREATE TABLE gadgets (id integer PRIMARY KEY);\n",
		down: fmt.Sprintf("SELECT pg_sleep(%d);\nDROP TABLE gadgets;\n", interruptSleepSeconds),
	}

	t.Run("an interrupt during the rollback of migration 2 keeps its table", func(t *testing.T) {
		c := qt.New(t)
		fixture := newInterruptFixture(c, ctx, dbURL, "down", bodies)
		fixture.applyAll(c, ctx)

		run := runInterruptedDuringSleep(c, ctx, fixture, fixture.downToOneArgs()...)

		c.Assert(run.exitCode, qt.Equals, 130, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
		c.Assert(run.stderr, qt.Contains, "interrupt received")
		// The operator stopped the run, so that is what the run reports.
		// Whichever call noticed the cancelation first answers in its own
		// words, and those words are not the answer (docs/exit_codes.md).
		c.Assert(run.stderr, qt.Contains, "error: canceled")
		c.Assert(run.stderr, qt.Not(qt.Contains), "SQL execution failed")
		c.Assert(run.stdout, qt.Not(qt.Contains), "Migration rollback completed successfully")
		c.Assert(run.afterSignal < interruptPromptness, qt.IsTrue, qt.Commentf("exited %s after the signal", run.afterSignal))
		c.Assert(fixture.revisions(c, ctx), qt.DeepEquals, []string{"1 applied 1/1", "2 failed:down 0/2"})
		c.Assert(fixture.tables(c, ctx), qt.DeepEquals, []string{"gadgets", "widgets"})
	})

	t.Run("the same rollback, not interrupted, removes migration 2", func(t *testing.T) {
		c := qt.New(t)
		fixture := newInterruptFixture(c, ctx, dbURL, "down_control", bodies)
		fixture.applyAll(c, ctx)

		run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: fixture.workDir}, fixture.downToOneArgs()...)

		c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
		c.Assert(run.Stdout, qt.Contains, "Migration rollback completed successfully")
		c.Assert(fixture.revisions(c, ctx), qt.DeepEquals, []string{"1 applied 1/1"})
		c.Assert(fixture.tables(c, ctx), qt.DeepEquals, []string{"widgets"})
	})
}

// interruptMigrationBodies are the up and down bodies of migration 2. Migration
// 1 is fixed: it creates widgets, which no interrupt in these tests reaches.
type interruptMigrationBodies struct {
	up   string
	down string
}

// interruptFixture is one throwaway database and one migration directory.
type interruptFixture struct {
	database      string
	url           string
	workDir       string
	migrationsDir string
	adminDB       *sql.DB
	targetDB      *sql.DB
}

// newInterruptFixture creates a database of its own for one run, because the
// signal is sent on the evidence of a sleep in that database, and a shared one
// would let another test's session answer for this one.
func newInterruptFixture(c *qt.C, ctx context.Context, dbURL, label string, bodies interruptMigrationBodies) interruptFixture {
	c.Helper()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	database := fmt.Sprintf("ptah_interrupt_3286_%s_%d", label, time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, database)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, database) })
	scopedURL := replaceDatabaseName(c, dbURL, database)

	targetDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = targetDB.Close() })

	workDir := c.TempDir()
	migrationsDir := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	files := map[string]string{
		"0000000001_widgets.up.sql":   "CREATE TABLE widgets (id integer PRIMARY KEY);\n",
		"0000000001_widgets.down.sql": "DROP TABLE widgets;\n",
		"0000000002_gadgets.up.sql":   bodies.up,
		"0000000002_gadgets.down.sql": bodies.down,
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}

	return interruptFixture{
		database:      database,
		url:           scopedURL,
		workDir:       workDir,
		migrationsDir: migrationsDir,
		adminDB:       adminDB,
		targetDB:      targetDB,
	}
}

func (f interruptFixture) upArgs() []string {
	return []string{"migrations", "up", "--db-url", f.url, "--migrations-dir", f.migrationsDir}
}

func (f interruptFixture) downToOneArgs() []string {
	return []string{
		"migrations", "down", "--db-url", f.url, "--migrations-dir", f.migrationsDir,
		"--target", "1", "--confirm",
	}
}

// applyAll brings the fixture database to version 2 without an interrupt.
func (f interruptFixture) applyAll(c *qt.C, ctx context.Context) {
	c.Helper()
	run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: f.workDir}, f.upArgs()...)
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(f.revisions(c, ctx), qt.DeepEquals, []string{"1 applied 1/1", "2 applied 1/1"})
}

func (f interruptFixture) revisions(c *qt.C, ctx context.Context) []string {
	c.Helper()
	return interruptQueryStrings(c, ctx, f.targetDB, interruptRevisionsQuery)
}

func (f interruptFixture) tables(c *qt.C, ctx context.Context) []string {
	c.Helper()
	return interruptQueryStrings(c, ctx, f.targetDB, interruptTablesQuery)
}

func interruptQueryStrings(c *qt.C, ctx context.Context, db *sql.DB, query string) []string {
	c.Helper()

	rows, err := db.QueryContext(ctx, query)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	values := []string{}
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// interruptedRun is what one interrupted process produced.
type interruptedRun struct {
	stdout      string
	stderr      string
	exitCode    int
	afterSignal time.Duration
}

// runInterruptedDuringSleep starts the native binary, waits until the fixture
// database shows the migration body sleeping, sends SIGINT, and waits for the
// process to exit.
func runInterruptedDuringSleep(c *qt.C, ctx context.Context, f interruptFixture, args ...string) interruptedRun {
	c.Helper()
	return runTargetInterruptedDuringSleep(c, ctx, f, clirun.Ptah, args...)
}

// runTargetInterruptedDuringSleep is the same interrupt for either shipped
// binary. The compatibility surface is interrupted exactly here too, and one
// implementation is what keeps the two answers comparable.
func runTargetInterruptedDuringSleep(
	c *qt.C,
	ctx context.Context,
	f interruptFixture,
	target clirun.Target,
	args ...string,
) interruptedRun {
	c.Helper()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, clirun.Build(c, target), args...)
	cmd.Dir = f.workDir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	c.Assert(cmd.Start(), qt.IsNil)
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	awaitInterruptSleep(c, ctx, f, exited, &stdout, &stderr)
	signaledAt := time.Now()
	c.Assert(cmd.Process.Signal(syscall.SIGINT), qt.IsNil)
	waitErr := <-exited
	afterSignal := time.Since(signaledAt)

	return interruptedRun{
		stdout:      stdout.String(),
		stderr:      stderr.String(),
		exitCode:    interruptExitStatus(c, waitErr),
		afterSignal: afterSignal,
	}
}

// awaitInterruptSleep polls until a session in the fixture database is
// executing the sleep. A process that exits first, or a context that expires,
// fails the test with what the process printed; the buffers are read only once
// the process has exited, since exec writes them until then.
func awaitInterruptSleep(c *qt.C, ctx context.Context, f interruptFixture, exited chan error, stdout, stderr *bytes.Buffer) {
	c.Helper()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var sleeping int
		c.Assert(f.adminDB.QueryRowContext(ctx, interruptSleepingQuery, f.database).Scan(&sleeping), qt.IsNil)
		if sleeping > 0 {
			return
		}
		select {
		case err := <-exited:
			c.Fatalf("the command exited before the migration started sleeping: %v\nstdout:\n%s\nstderr:\n%s",
				err, stdout.String(), stderr.String())
		case <-ctx.Done():
			c.Fatalf("no session in %s started sleeping: %v", f.database, ctx.Err())
		case <-ticker.C:
		}
	}
}

// interruptExitStatus reads the status out of what Wait returned. Anything
// other than an ExitError means the process did not run to an exit.
func interruptExitStatus(c *qt.C, err error) int {
	c.Helper()
	if err == nil {
		return 0
	}
	exitErr, ok := errors.AsType[*exec.ExitError](err)
	c.Assert(ok, qt.IsTrue, qt.Commentf("the command did not exit: %v", err))
	return exitErr.ExitCode()
}
