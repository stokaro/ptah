//go:build integration && unix

package integration_test

// Unix only: the interrupt is delivered with Process.Signal(syscall.SIGINT),
// and Windows cannot send that signal to another process.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// atlasInterruptRevisionsQuery reads the Atlas revision table through the raw
// driver, so the answer does not pass through the reader the command used.
const atlasInterruptRevisionsQuery = `
SELECT version || ' ' || applied::text || '/' || total::text
FROM atlas_schema_revisions.atlas_schema_revisions ORDER BY version`

// TestAtlasMigrateApplyInterruptedDuringAMigrationE2E is the Atlas half of
// stokaro/ptah#3315: what an interrupted `ptah-compat migrate apply` leaves
// behind in the revision table.
//
// Atlas discards the revision of a failed migration whose transaction rolled
// back, and Ptah matches it. An interrupt rolls the transaction back too -- it
// is database/sql that does it, on the canceled context -- so the row has to go
// the same way. Kept, it names a migration that changed nothing, and the next
// run refuses to continue past it.
//
// The ordinary failure is the control. Without it, "no row for migration 2" is
// also what a run that never reached migration 2 would leave.
func TestAtlasMigrateApplyInterruptedDuringAMigrationE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Run("an interrupt during migration 2 leaves no revision for it", func(t *testing.T) {
		c := qt.New(t)
		fixture := newAtlasInterruptFixture(c, ctx, dbURL, "apply",
			fmt.Sprintf("SELECT pg_sleep(%d);\nCREATE TABLE gadgets (id integer PRIMARY KEY);\n", interruptSleepSeconds))

		run := runTargetInterruptedDuringSleep(c, ctx, fixture.interruptFixture, clirun.Compat, fixture.applyArgs()...)

		c.Assert(run.exitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.stdout, run.stderr))
		c.Assert(run.stderr, qt.Contains, "Error: canceled")
		c.Assert(run.afterSignal < interruptPromptness, qt.IsTrue, qt.Commentf("exited %s after the signal", run.afterSignal))
		c.Assert(atlasInterruptRevisions(c, ctx, fixture), qt.DeepEquals, []string{"20260101000001 1/1"})
		c.Assert(fixture.tables(c, ctx), qt.DeepEquals, []string{"widgets"})
	})

	t.Run("a migration that fails on its own SQL leaves no revision either", func(t *testing.T) {
		c := qt.New(t)
		fixture := newAtlasInterruptFixture(c, ctx, dbURL, "apply_control",
			"CREATE TABLE gadgets (id integer PRIMARY KEY);\nTHIS IS NOT SQL;\n")

		run := clirun.Run(c, clirun.Compat, clirun.Options{Dir: fixture.workDir}, fixture.applyArgs()...)

		c.Assert(run.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
		c.Assert(run.Stderr, qt.Contains, "syntax error")
		c.Assert(atlasInterruptRevisions(c, ctx, fixture), qt.DeepEquals, []string{"20260101000001 1/1"})
		c.Assert(fixture.tables(c, ctx), qt.DeepEquals, []string{"widgets"})
	})

	t.Run("the run after an interrupt applies the migration that was stopped", func(t *testing.T) {
		c := qt.New(t)
		fixture := newAtlasInterruptFixture(c, ctx, dbURL, "apply_retry",
			fmt.Sprintf("SELECT pg_sleep(%d);\nCREATE TABLE gadgets (id integer PRIMARY KEY);\n", interruptSleepSeconds))
		interrupted := runTargetInterruptedDuringSleep(c, ctx, fixture.interruptFixture, clirun.Compat, fixture.applyArgs()...)
		c.Assert(interrupted.exitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", interrupted.stderr))

		run := clirun.Run(c, clirun.Compat, clirun.Options{Dir: fixture.workDir}, fixture.applyArgs()...)

		c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
		c.Assert(atlasInterruptRevisions(c, ctx, fixture), qt.DeepEquals,
			[]string{"20260101000001 1/1", "20260101000002 2/2"})
		c.Assert(fixture.tables(c, ctx), qt.DeepEquals, []string{"gadgets", "widgets"})
	})
}

// atlasInterruptFixture is an Atlas-format migration directory over the
// throwaway database the interrupt harness provides.
type atlasInterruptFixture struct {
	interruptFixture
}

func (f atlasInterruptFixture) applyArgs() []string {
	return []string{"migrate", "apply", "--url", f.url, "--dir", "file://" + f.migrationsDir}
}

func newAtlasInterruptFixture(c *qt.C, ctx context.Context, dbURL, label, second string) atlasInterruptFixture {
	c.Helper()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	database := fmt.Sprintf("ptah_interrupt_3315_%s_%d", label, time.Now().UnixNano())
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
		"20260101000001_widgets.sql": "CREATE TABLE widgets (id integer PRIMARY KEY);\n",
		"20260101000002_gadgets.sql": second,
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name), []byte(body), 0o600), qt.IsNil)
	}
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: workDir},
		"migrate", "hash", "--dir", "file://"+migrationsDir)
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))

	return atlasInterruptFixture{interruptFixture{
		database:      database,
		url:           scopedURL,
		workDir:       workDir,
		migrationsDir: migrationsDir,
		adminDB:       adminDB,
		targetDB:      targetDB,
	}}
}

func atlasInterruptRevisions(c *qt.C, ctx context.Context, f atlasInterruptFixture) []string {
	c.Helper()
	return interruptQueryStrings(c, ctx, f.targetDB, atlasInterruptRevisionsQuery)
}
