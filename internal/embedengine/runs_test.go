package embedengine_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
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

// TestAbandon_FencesTheWorkerAndKeepsTheGeneration ends the run rather than
// retiring the vectors it built.
func TestAbandon_FencesTheWorkerAndKeepsTheGeneration(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	ctx := context.Background()
	registerHarnessGeneration(c, h)
	working, err := h.store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)

	abandoned, err := runsOf(h).Abandon(ctx, "run-1", "the migration was superseded")

	c.Assert(err, qt.IsNil)
	c.Assert(abandoned.Status, qt.Equals, embedrun.StatusAbandoned)
	c.Assert(abandoned.FailureDetail, qt.Equals, "the migration was superseded")
	c.Assert(abandoned.LeaseOwner, qt.Equals, "")
	c.Assert(h.store.SaveRun(ctx, working), qt.ErrorIs, embedstore.ErrConflict)
	generation, err := h.store.Generation(ctx, abandoned.GenerationIdentity)
	c.Assert(err, qt.IsNil)
	c.Assert(generation.Retired(), qt.IsFalse)
	_, _, err = runsOf(h).Claim(ctx, "run-1")
	c.Assert(err, qt.ErrorIs, embedrun.ErrTerminal)
}

// TestAbandon_IsIdempotent preserves the first recorded reason and does not
// take another fencing token on an operator retry.
func TestAbandon_IsIdempotent(t *testing.T) {
	c := qt.New(t)
	h := newHarness(c, defaultBounds())
	ctx := context.Background()
	registerHarnessGeneration(c, h)

	first, err := runsOf(h).Abandon(ctx, "run-1", "the migration was superseded")
	c.Assert(err, qt.IsNil)
	second, err := runsOf(h).Abandon(ctx, "run-1", "a retry after the response was lost")

	c.Assert(err, qt.IsNil)
	c.Assert(second.FencingToken, qt.Equals, first.FencingToken)
	c.Assert(second.FailureDetail, qt.Equals, "the migration was superseded")
}

// TestAbandon_ProtectsTheLastActiveOrMaintainedRun keeps the promise attached
// to those generation states without letting a superseded duplicate pin the
// outbox floor.
func TestAbandon_ProtectsTheLastActiveOrMaintainedRun(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	active := newHarness(c, defaultBounds())
	registerHarnessGeneration(c, active)
	identity := spec().Identity().Digest
	c.Assert(active.store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: identity,
	}, ""), qt.IsNil)
	_, err := runsOf(active).Abandon(ctx, "run-1", "the migration was superseded")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
	c.Assert(err.Error(), qt.Contains,
		"is active for public.articles and no other usable live feeder remains")
	c.Assert(err.Error(), qt.Not(qt.Contains), "abandon run run-1: abandon run run-1:")
	createSiblingRun(c, active, "active-replacement")
	abandoned, err := runsOf(active).Abandon(ctx, "run-1", "the replacement remains")
	c.Assert(err, qt.IsNil)
	c.Assert(abandoned.Status, qt.Equals, embedrun.StatusAbandoned)

	maintained := newHarness(c, defaultBounds())
	registerHarnessGeneration(c, maintained)
	c.Assert(maintained.store.Maintain(ctx, identity, time.Now().UTC().Add(time.Hour)), qt.IsNil)
	_, err = runsOf(maintained).Abandon(ctx, "run-1", "the migration was superseded")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
	c.Assert(err.Error(), qt.Contains, "is maintained until")
	createSiblingRun(c, maintained, "maintained-replacement")
	abandoned, err = runsOf(maintained).Abandon(ctx, "run-1", "the replacement remains")
	c.Assert(err, qt.IsNil)
	c.Assert(abandoned.Status, qt.Equals, embedrun.StatusAbandoned)
}

// runsOf is the store-only surface, held by somebody other than the worker in
// the harness. The name matters to every assertion about who took the run.
func runsOf(h *harness) embedengine.Runs {
	return embedengine.Runs{Store: h.store, Worker: "operator"}
}

// registerHarnessGeneration supplies the registry facts abandonment uses for
// its active and maintenance guards.
func registerHarnessGeneration(c *qt.C, h *harness) {
	c.Helper()
	identity := spec().Identity().Digest
	_, err := h.store.RegisterGeneration(context.Background(), embedstore.Generation{
		Identity: identity, SpecDigest: identity,
		TargetSchema: "public", TargetTable: "articles", TargetColumn: "embedding",
		CreatedAt: time.Now().UTC(),
	})
	c.Assert(err, qt.IsNil)
}

// createSiblingRun leaves another nonterminal feeder for the same generation.
func createSiblingRun(c *qt.C, h *harness, id string) {
	c.Helper()
	run, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	run.ID = id
	run.LeaseOwner = ""
	run.LeaseExpires = time.Time{}
	c.Assert(h.store.CreateRun(context.Background(), run), qt.IsNil)
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
