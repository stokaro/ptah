package embedengine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedrun"
)

// pendingChange is one settled event waiting to be caught up, which is what
// makes "nothing was spent" a statement about the guard rather than about an
// empty outbox.
func pendingChange() *fakeChanges {
	return &fakeChanges{
		pages:    [][]embedcatchup.Event{{changed(101, 1, "1", embedcatchup.OperationUpdate)}},
		horizons: []uint64{102},
	}
}

// atPhase puts the run at a phase, with the boundary a prepared run holds.
func atPhase(c *qt.C, h *harness, phase embedrun.Phase) {
	c.Helper()
	stored, err := h.store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	stored.Phase = phase
	stored.SnapshotWatermark = "100"
	c.Assert(h.store.SaveRun(context.Background(), stored), qt.IsNil)
}

// TestCatchUp_RefusesBeforeSpendingAnythingFailurePath covers
// stokaro/ptah#2737.
//
// Catch-up covers what changed after the snapshot the backfill walked. Asked
// before that walk finished there is no such range, and the engine served the
// request anyway: it embedded the row, wrote the vector into the operator's
// table, moved the catch-up watermark PAST the snapshot boundary, and returned
// a nil error. The only refusal was the CLI reaching for `caught_up`
// afterwards, so a command that reported failure had already spent the money.
//
// The assertions are on what was NOT done, because an error alone would pass
// against the reported behavior too -- the CLI raised one. What distinguishes
// the fix is the provider never being called.
func TestCatchUp_RefusesBeforeSpendingAnythingFailurePath(t *testing.T) {
	tests := []struct {
		name  string
		phase embedrun.Phase
	}{
		{name: "prepared", phase: embedrun.PhasePrepared},
		{name: "the boundary is captured and nothing walked", phase: embedrun.PhaseBoundaryCaptured},
		{name: "the walk started and did not finish", phase: embedrun.PhaseBackfilling},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newHarness(c, defaultBounds())
			atPhase(c, h, test.phase)

			_, _, err := h.engine.CatchUp(context.Background(), "run-1",
				pendingChange(), livingRows("1"))

			c.Assert(err, qt.ErrorMatches,
				`catch-up needs a backfill that reached the end of its snapshot, and this run is at ".*"`)
			c.Assert(err, qt.ErrorMatches, ".*"+string(test.phase)+".*")

			// The money, the operator's table, and the value that clears the
			// "catch-up has not reached the barrier" finding.
			c.Assert(h.provider.calls, qt.HasLen, 0)
			c.Assert(h.target.commits, qt.HasLen, 0)
			stored, err := h.store.Run(context.Background(), "run-1")
			c.Assert(err, qt.IsNil)
			c.Assert(stored.CatchUpWatermark, qt.Equals, "")
			c.Assert(stored.Phase, qt.Equals, test.phase)
		})
	}
}

// TestCatchUp_RunsOncePastTheBackfillHappyPath is the control the refusal needs
// and the regression the guard could introduce.
//
// The guard asks Reached rather than an equality, because catching up again
// after an index, a verification or a cutover is ordinary -- the source keeps
// moving, and [embedrun.Run.Reach] is documented as a high-water mark for that
// reason. An equality would refuse every one of these while looking correct
// against the failure path above.
func TestCatchUp_RunsOncePastTheBackfillHappyPath(t *testing.T) {
	tests := []struct {
		name  string
		phase embedrun.Phase
	}{
		{name: "the walk reached the end of the snapshot", phase: embedrun.PhaseBackfilled},
		{name: "a second catch-up", phase: embedrun.PhaseCaughtUp},
		{name: "after an index", phase: embedrun.PhaseIndexed},
		{name: "after a verification", phase: embedrun.PhaseVerified},
		{name: "after a cutover, with the source still moving", phase: embedrun.PhaseCutOver},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newHarness(c, defaultBounds())
			atPhase(c, h, test.phase)

			run, pass, err := h.engine.CatchUp(context.Background(), "run-1",
				pendingChange(), livingRows("1"))

			c.Assert(err, qt.IsNil)
			c.Assert(h.provider.calls, qt.HasLen, 1)
			c.Assert(pass.RowsEmbedded, qt.Equals, int64(1))
			c.Assert(run.CatchUpWatermark, qt.Equals, "102")
		})
	}
}
