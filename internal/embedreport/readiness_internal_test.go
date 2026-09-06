package embedreport

// White-box testing required: terminalReadiness is the pure early-return used
// before database measurement. Its complete-at-an-early-phase control cannot
// be isolated through ReadReadiness without constructing the whole live
// verification stack.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedrun"
)

func TestTerminalReadinessDoesNotMeasureACompletedEarlyPhaseRun(t *testing.T) {
	c := qt.New(t)
	now := time.Date(2026, 9, 2, 15, 0, 0, 0, time.UTC)
	run := embedrun.Run{
		ID: "retired-before-backfill", Phase: embedrun.PhasePrepared,
		Status: embedrun.StatusComplete,
	}

	readiness, terminal := terminalReadiness(run, now)

	c.Assert(terminal, qt.IsTrue)
	c.Assert(readiness.Verified, qt.IsFalse)
	c.Assert(readiness.CutoverReady, qt.IsFalse)
	c.Assert(readiness.Blockers, qt.DeepEquals, []string{
		"run retired-before-backfill is complete because its generation was retired and cannot be cut over",
	})
	c.Assert(readiness.Unmeasured, qt.DeepEquals, []string{
		"every deterministic layer, because the run is terminal",
	})
}

func TestTerminalReadinessLeavesALiveRunForMeasurement(t *testing.T) {
	c := qt.New(t)
	_, terminal := terminalReadiness(embedrun.Run{
		ID: "live", Phase: embedrun.PhasePrepared, Status: embedrun.StatusPaused,
	}, time.Now())

	c.Assert(terminal, qt.IsFalse,
		qt.Commentf("a paused run must proceed to live readiness measurement"))
}
