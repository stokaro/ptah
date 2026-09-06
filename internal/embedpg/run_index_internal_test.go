package embedpg

// White-box testing required: indexedRun is the pure transition shared with
// the live PostgreSQL operation. Keeping its fence, lease, and high-water phase
// controls here avoids exporting a persistence implementation detail.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedrun"
)

func TestIndexedRunAdvancesAndFences(t *testing.T) {
	c := qt.New(t)
	lease := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	run := embedrun.Run{
		ID: "run-1", GenerationIdentity: "generation-1",
		Phase: embedrun.PhaseCaughtUp, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", LeaseExpires: lease, FencingToken: 7,
	}

	indexed, err := indexedRun(run, "generation-1")

	c.Assert(err, qt.IsNil)
	c.Assert(indexed.Phase, qt.Equals, embedrun.PhaseIndexed)
	c.Assert(indexed.FencingToken, qt.Equals, int64(8))
	c.Assert(indexed.LeaseOwner, qt.Equals, "")
	c.Assert(indexed.LeaseExpires.IsZero(), qt.IsTrue)
	// The input is a snapshot and must remain untouched.
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseCaughtUp)
	c.Assert(run.FencingToken, qt.Equals, int64(7))
}

func TestIndexedRunFencesAnAlreadyAdvancedRunWithoutMovingItBack(t *testing.T) {
	c := qt.New(t)
	run := embedrun.Run{
		ID: "run-1", GenerationIdentity: "generation-1",
		Phase: embedrun.PhaseVerified, Status: embedrun.StatusPaused,
		FencingToken: 11,
	}

	indexed, err := indexedRun(run, "generation-1")

	c.Assert(err, qt.IsNil)
	c.Assert(indexed.Phase, qt.Equals, embedrun.PhaseVerified)
	c.Assert(indexed.FencingToken, qt.Equals, int64(12))
}

func TestIndexedRunRefusesWorkThatCannotTruthfullyReachIndexing(t *testing.T) {
	tests := []struct {
		name       string
		run        embedrun.Run
		identity   string
		wantTarget error
	}{
		{
			name: "phase before catch-up",
			run: embedrun.Run{
				ID: "too-early", GenerationIdentity: "generation-1",
				Phase: embedrun.PhaseBackfilled, Status: embedrun.StatusRunning,
			},
			identity: "generation-1", wantTarget: embedrun.ErrPhase,
		},
		{
			name: "abandoned run",
			run: embedrun.Run{
				ID: "ended", GenerationIdentity: "generation-1",
				Phase: embedrun.PhaseCaughtUp, Status: embedrun.StatusAbandoned,
			},
			identity: "generation-1", wantTarget: embedrun.ErrTerminal,
		},
		{
			name: "another generation",
			run: embedrun.Run{
				ID: "wrong", GenerationIdentity: "generation-2",
				Phase: embedrun.PhaseCaughtUp, Status: embedrun.StatusRunning,
			},
			identity: "generation-1", wantTarget: embedrun.ErrGeneration,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := indexedRun(test.run, test.identity)
			c.Assert(err, qt.ErrorIs, test.wantTarget,
				qt.Commentf("indexedRun() error = %v, want errors.Is(_, %v)",
					err, test.wantTarget))
		})
	}
}
