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

	"ptah.run/migration/internal/shadowdb"
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

// TestExpectedFailure_AnInterruptionIsNotACaughtRefusal is the misclassification
// #2866 names, and it cannot be reached through the public API for the reason
// this file already gives: a context canceled before the run fails at connect,
// and one canceled during a step needs a hook the public Step type must not
// grow.
//
// A caught step passes because the DATABASE refused what it was asked. A
// canceled context is not that -- the statement may never have reached the
// server -- and counting it as the expected failure makes an interrupted suite
// report that all its refusals occurred, which is the worst possible answer
// from a run nobody watched.
func TestExpectedFailure_AnInterruptionIsNotACaughtRefusal(t *testing.T) {
	database, err := shadowdb.Open(context.Background(), "", "ptah-dbtest-interrupt-*")
	qt.New(t).Assert(err, qt.IsNil)
	defer database.Close()

	r := &runner{conn: database.Connection()}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name       string
		wantDetail string
	}{
		{name: "expect any error", wantDetail: "interrupted rather than the statement refused"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			passed, detail := r.assertAnyError(canceled, "SELECT * FROM missing_table")

			c.Assert(passed, qt.IsFalse)
			c.Assert(detail, qt.Contains, test.wantDetail)
		})
	}

	t.Run("error matches", func(t *testing.T) {
		c := qt.New(t)

		passed, detail := r.assertErrorMatches(canceled, "SELECT * FROM missing_table", "missing")

		c.Assert(passed, qt.IsFalse)
		c.Assert(detail, qt.Contains, "interrupted rather than the statement refused")
	})

	// The control: a real refusal on a healthy context still passes, so the
	// rule was not achieved by making every expected failure fail.
	t.Run("a real refusal still passes", func(t *testing.T) {
		c := qt.New(t)

		passed, detail := r.assertAnyError(context.Background(), "SELECT * FROM missing_table")

		c.Assert(passed, qt.IsTrue, qt.Commentf("detail: %s", detail))
		c.Assert(detail, qt.Contains, "error occurred")
	})
}
