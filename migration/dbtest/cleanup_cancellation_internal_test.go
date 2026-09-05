package dbtest

// White-box testing required: cleanup on a canceled run cannot be reached
// through the public API. RunMigrationTest connects before it runs any case, so
// a context canceled beforehand fails at "connect to ephemeral test database"
// and no case executes; a context canceled DURING a case needs a hook inside
// the step loop, and adding one to the public Step type would put a test's
// needs into an embedder's model. The property under test -- that teardown runs
// against a context detached from the cancellation rather than against the
// canceled one -- is only observable by calling the cleanup path directly.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/internal/shadowdb"
)

// TestRunCaseCleanup_RunsOnACanceledContext is the fourth lifecycle path.
//
// Running teardown on the canceled context is the implementation an ordinary
// reading produces, and it is wrong in a way that looks right: every statement
// fails instantly, so the report shows a cleanup that ran and failed rather
// than one that never had a chance, and the database stays dirty either way.
//
// The assertion is therefore that the cleanup step PASSED. Nothing but a
// detached context can produce that, so the test cannot be satisfied by an
// implementation that merely reports the attempt.
func TestRunCaseCleanup_RunsOnACanceledContext(t *testing.T) {
	c := qt.New(t)

	database, err := shadowdb.Open(context.Background(), "", "ptah-dbtest-cleanup-*")
	c.Assert(err, qt.IsNil)
	defer database.Close()

	r := &runner{conn: database.Connection()}

	// The table exists, so a cleanup that actually reaches the database
	// succeeds and one that runs on a canceled context does not.
	passed, detail := r.execStep(context.Background(), Step{Exec: "CREATE TABLE leftover (id INTEGER)"})
	c.Assert(passed, qt.IsTrue, qt.Commentf("setup: %s", detail))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := CaseResult{Name: "canceled", Passed: true}
	r.runCaseCleanup(ctx, Case{
		Name:    "canceled",
		Cleanup: []Step{{Name: "drop it", Exec: "DROP TABLE leftover"}},
	}, &result)

	c.Assert(result.Steps, qt.HasLen, 1)
	c.Assert(result.Steps[0].Name, qt.Equals, "drop it")
	c.Assert(result.Steps[0].Kind, qt.Equals, StepKindCleanup)
	c.Assert(result.Steps[0].Passed, qt.IsTrue,
		qt.Commentf("detail: %s", result.Steps[0].Detail))
	c.Assert(result.CleanupFailed, qt.IsFalse)

	// The control: the statement really did run, so the assertion above is
	// about the teardown reaching the database rather than about a step that
	// reported success without doing anything.
	repeated, _ := r.execStep(context.Background(), Step{Exec: "DROP TABLE leftover"})
	c.Assert(repeated, qt.IsFalse)
}

// TestRunCaseCleanup_LeavesAnUncanceledContextAlone is the other half.
//
// The detachment exists for a canceled run and must not become the ordinary
// path: a cleanup on a healthy run should inherit the caller's deadline and
// cancellation, so an operator who interrupts a suite mid-teardown is still
// obeyed on everything that has not started.
func TestRunCaseCleanup_LeavesAnUncanceledContextAlone(t *testing.T) {
	c := qt.New(t)

	database, err := shadowdb.Open(context.Background(), "", "ptah-dbtest-cleanup-*")
	c.Assert(err, qt.IsNil)
	defer database.Close()

	r := &runner{conn: database.Connection()}
	result := CaseResult{Name: "healthy", Passed: true}

	ctx := t.Context()

	r.runCaseCleanup(ctx, Case{
		Name:    "healthy",
		Cleanup: []Step{{Name: "harmless", Exec: "SELECT 1"}},
	}, &result)

	c.Assert(result.Steps, qt.HasLen, 1)
	c.Assert(result.Steps[0].Passed, qt.IsTrue)
	c.Assert(ctx.Err(), qt.IsNil)
}
