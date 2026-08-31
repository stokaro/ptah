package embedengine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedrun"
)

// TestPause_StopsARunningBackfillAtItsNextCommit is the property the pause verb
// exists for, and the one a test calling Pause on an idle run cannot see.
//
// An operator pauses a backfill that is running. What must happen is that the
// backfill stops: the pause claims the run, which moves the fencing token past
// the token that worker holds, and the store refuses its next commit. A pause
// that only wrote "paused" into the row would be overwritten by that same
// commit, and the run would read paused for a moment and then read running
// again while the provider bill went on.
func TestPause_StopsARunningBackfillAtItsNextCommit(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	h.target.beforeCommit = pauseAfter(c, h, 1)

	run, _, err := h.engine.Backfill(context.Background(), "run-1")

	c.Assert(err, qt.ErrorIs, embedengine.ErrFenced)
	// The transaction that was already in flight landed; nothing after it did.
	c.Assert(h.target.commits, qt.HasLen, 1)
	// Stopped, not failed. The run is durable at its last checkpoint and a
	// resume picks it up from there.
	c.Assert(run.Status, qt.Equals, embedrun.StatusPaused)
	c.Assert(run.FailureDetail, qt.Equals, "the provider is rate limiting us")
}

// TestPause_RecordsWhyAndKeepsWhereItGotTo is the same verb against an idle
// run, which is the ordinary case.
func TestPause_RecordsWhyAndKeepsWhereItGotTo(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	ctx := context.Background()
	_, _, err := h.engine.Backfill(ctx, "run-1")
	c.Assert(err, qt.IsNil)

	paused, err := runsOf(h).Pause(ctx, "run-1", "waiting on a budget approval")

	c.Assert(err, qt.IsNil)
	c.Assert(paused.Status, qt.Equals, embedrun.StatusPaused)
	c.Assert(paused.FailureDetail, qt.Equals, "waiting on a budget approval")
	// The checkpoint is untouched: a pause stops a run, it does not rewind one.
	c.Assert(paused.Cursor, qt.DeepEquals, []string{"4"})

	stored, err := h.store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Status, qt.Equals, embedrun.StatusPaused)
	c.Assert(stored.LeaseOwner, qt.Equals, "operator")
}

// TestResume_FencesTheWorkerThePauseStopped is the half a resume that only set
// the status would get wrong.
//
// The worker fenced by the pause is not necessarily gone. Returning the run to
// running while that worker's token is still current would put it back exactly
// where the fence exists to stop it: able to commit into a run somebody else
// now owns.
func TestResume_FencesTheWorkerThePauseStopped(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	ctx := context.Background()
	runs := runsOf(h)

	paused, err := runs.Pause(ctx, "run-1", "waiting on a budget approval")
	c.Assert(err, qt.IsNil)
	resumed, err := runs.Resume(ctx, "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(resumed.Status, qt.Equals, embedrun.StatusRunning)
	// Cleared, because a running run carrying the reason it stopped for reads
	// as a run that stopped.
	c.Assert(resumed.FailureDetail, qt.Equals, "")
	c.Assert(resumed.FencingToken > paused.FencingToken, qt.IsTrue)
}

// TestResume_RefusesARunThatIsNotPaused keeps the verb from being a way to set
// a status.
func TestResume_RefusesARunThatIsNotPaused(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())

	_, err := runsOf(h).Resume(context.Background(), "run-1")

	c.Assert(err, qt.ErrorMatches, `resume run run-1: .*only a paused run resumes, and this one is running`)
}

// TestPause_RefusesAPauseNobodyCanActOn is the reason requirement, at the layer
// that persists it rather than only at the one that formats it.
func TestPause_RefusesAPauseNobodyCanActOn(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	ctx := context.Background()

	_, err := runsOf(h).Pause(ctx, "run-1", "")

	c.Assert(err, qt.ErrorMatches, `pause run run-1: .*a pause without a reason cannot be acted on`)
	// And the run is untouched, so a refused pause is not a half-applied one.
	stored, err := h.store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Status, qt.Equals, embedrun.StatusRunning)
}

// runsOf is the store-only surface, held by somebody other than the worker in
// the harness. The name matters to every assertion about who took the run.
func runsOf(h *harness) embedengine.Runs {
	return embedengine.Runs{Store: h.store, Worker: "operator"}
}

// pauseAfter pauses the run once that many transactions have landed.
//
// The branch lives here rather than in the test body because a test asserts and
// does not branch, which is the rule scripts/check-test-style.sh enforces.
func pauseAfter(c *qt.C, h *harness, commits int) func() {
	return func() {
		if len(h.target.commits) != commits {
			return
		}
		_, err := runsOf(h).Pause(context.Background(), "run-1", "the provider is rate limiting us")
		c.Assert(err, qt.IsNil)
	}
}
