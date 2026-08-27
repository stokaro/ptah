package embedcutover_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcutover"
)

// cutOverAt is when the current generation took over.
var cutOverAt = time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)

// rollbackReady is a previous generation you could still go back to.
func rollbackReady() (embedcutover.RollbackPolicy, embedcutover.RollbackState, embedcutover.Observed) {
	policy := embedcutover.RollbackPolicy{
		Window:         24 * time.Hour,
		MaxStaleRows:   10,
		MaxMissingRows: 0,
		RequireIndex:   true,
	}
	state := embedcutover.RollbackState{
		Present:     true,
		Maintained:  true,
		StaleRows:   3,
		MissingRows: 0,
		IndexReady:  true,
		VerifiedAt:  cutOverAt.Add(time.Hour),
		CutOverAt:   cutOverAt,
	}
	observed := embedcutover.Observed{Now: cutOverAt.Add(2 * time.Hour)}
	return policy, state, observed
}

// TestEvaluateRollback_AFreshMaintainedGenerationIsEligible is the control.
func TestEvaluateRollback_AFreshMaintainedGenerationIsEligible(t *testing.T) {
	c := qt.New(t)
	policy, state, observed := rollbackReady()

	eligibility := embedcutover.EvaluateRollback(policy, state, observed)

	c.Assert(eligibility.Blockers, qt.HasLen, 0, qt.Commentf("%v", eligibility.Blockers))
	c.Assert(eligibility.Eligible, qt.IsTrue)
	c.Assert(eligibility.Expires, qt.Equals, cutOverAt.Add(24*time.Hour))
}

// TestEvaluateRollback_ExistingIsNotEligible is the epic's rule.
//
// The system must not report rollback as available merely because old tables
// still exist. Every row here is a generation whose tables are perfectly
// present and which is not a place you can go back to.
func TestEvaluateRollback_ExistingIsNotEligible(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcutover.RollbackState)
		want   string
	}{
		{
			name:   "never verified",
			change: func(s *embedcutover.RollbackState) { s.VerifiedAt = time.Time{} },
			want:   "the generation has never been verified, so its freshness is unknown",
		},
		{
			name:   "no longer maintained",
			change: func(s *embedcutover.RollbackState) { s.Maintained = false },
			want:   "the generation is no longer maintained, so it drifts further from the source every write",
		},
		{
			name:   "too many stale rows",
			change: func(s *embedcutover.RollbackState) { s.StaleRows = 11 },
			want:   "11 rows are stale and this policy allows 10",
		},
		{
			name:   "rows it never had",
			change: func(s *embedcutover.RollbackState) { s.MissingRows = 1 },
			want:   "1 rows are missing and this policy allows 0",
		},
		{
			name:   "the index was dropped",
			change: func(s *embedcutover.RollbackState) { s.IndexReady = false },
			want: "the generation's index is absent or invalid, so going back would leave " +
				"queries unindexed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy, state, observed := rollbackReady()
			test.change(&state)

			eligibility := embedcutover.EvaluateRollback(policy, state, observed)

			c.Assert(eligibility.Eligible, qt.IsFalse)
			c.Assert(eligibility.Blockers, qt.Contains, test.want)
		})
	}
}

// TestEvaluateRollback_TheWindowCloses pins the time limit, and pins it as
// closing rather than as never having been open.
func TestEvaluateRollback_TheWindowCloses(t *testing.T) {
	c := qt.New(t)
	policy, state, observed := rollbackReady()
	observed.Now = cutOverAt.Add(25 * time.Hour)

	eligibility := embedcutover.EvaluateRollback(policy, state, observed)

	c.Assert(eligibility.Eligible, qt.IsFalse)
	c.Assert(eligibility.Blockers, qt.Contains,
		"the rollback window closed at 2026-08-28T09:00:00Z")
	c.Assert(eligibility.Expires, qt.Equals, cutOverAt.Add(24*time.Hour))
}

// TestEvaluateRollback_APolicyWithoutAWindowDoesNotExpire is the control for
// the row above.
func TestEvaluateRollback_APolicyWithoutAWindowDoesNotExpire(t *testing.T) {
	c := qt.New(t)
	policy, state, observed := rollbackReady()
	policy.Window = 0
	observed.Now = cutOverAt.Add(10000 * time.Hour)

	eligibility := embedcutover.EvaluateRollback(policy, state, observed)

	c.Assert(eligibility.Eligible, qt.IsTrue, qt.Commentf("%v", eligibility.Blockers))
	c.Assert(eligibility.Expires.IsZero(), qt.IsTrue)
}

// TestEvaluateRollback_AGoneGenerationSaysSoAndStops keeps a report about an
// absent generation from listing everything that is also unknown about it.
//
// A generation that is not there has no stale rows, no index and no
// maintenance either, and reporting four things wrong with a thing that does
// not exist buries the one that matters.
func TestEvaluateRollback_AGoneGenerationSaysSoAndStops(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcutover.RollbackState)
		want   string
	}{
		{
			name:   "retired",
			change: func(s *embedcutover.RollbackState) { s.Retired = true },
			want:   "the generation was retired, which is not something you come back from",
		},
		{
			name:   "gone",
			change: func(s *embedcutover.RollbackState) { s.Present = false },
			want:   "the generation is no longer present",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy, _, observed := rollbackReady()
			state := embedcutover.RollbackState{Present: true, CutOverAt: cutOverAt}
			test.change(&state)

			eligibility := embedcutover.EvaluateRollback(policy, state, observed)

			c.Assert(eligibility.Eligible, qt.IsFalse)
			c.Assert(eligibility.Blockers, qt.DeepEquals, []string{test.want})
		})
	}
}

// TestEvaluateRollback_TheZeroPolicyToleratesNothing pins the default.
//
// A rollback target nobody declared a policy for is one nobody thought about,
// and the safe reading of an unthought-about rollback is that it has not been
// established. Three stale rows are fine under the policy above and are not
// fine under no policy at all.
func TestEvaluateRollback_TheZeroPolicyToleratesNothing(t *testing.T) {
	c := qt.New(t)
	_, state, observed := rollbackReady()

	eligibility := embedcutover.EvaluateRollback(embedcutover.RollbackPolicy{}, state, observed)

	c.Assert(eligibility.Eligible, qt.IsFalse)
	c.Assert(eligibility.Blockers, qt.Contains, "3 rows are stale and this policy allows 0")
}

// TestEvaluateRollback_AnUnmeasuredGenerationDoesNotReportMeasurements keeps
// the report from quoting numbers nobody took.
//
// The stale and missing counts come from a verification that never ran. Saying
// "99 rows are stale" reads as a measurement, and the only true thing here is
// that nobody looked.
func TestEvaluateRollback_AnUnmeasuredGenerationDoesNotReportMeasurements(t *testing.T) {
	c := qt.New(t)
	policy, state, observed := rollbackReady()
	state.VerifiedAt = time.Time{}
	state.StaleRows = 99
	state.MissingRows = 99

	eligibility := embedcutover.EvaluateRollback(policy, state, observed)

	c.Assert(eligibility.Eligible, qt.IsFalse)
	c.Assert(eligibility.Blockers, qt.DeepEquals, []string{
		"the generation has never been verified, so its freshness is unknown",
	})
}

// TestEvaluateRollback_ThePolicyDecidesWhetherTheIndexMatters is the control
// for the dropped-index refusal.
//
// A corpus small enough to scan does not need its index to be a place you can
// go back to, and the policy is where that is said.
func TestEvaluateRollback_ThePolicyDecidesWhetherTheIndexMatters(t *testing.T) {
	c := qt.New(t)
	policy, state, observed := rollbackReady()
	policy.RequireIndex = false
	state.IndexReady = false

	eligibility := embedcutover.EvaluateRollback(policy, state, observed)

	c.Assert(eligibility.Eligible, qt.IsTrue, qt.Commentf("%v", eligibility.Blockers))
}

// TestEvaluateRollback_TheToleranceIsInclusive pins the boundary rather than
// leaving it to whichever comparison somebody typed.
//
// A policy allowing ten stale rows allows ten of them. Off by one here is a
// generation refused for being exactly as fresh as it was required to be.
func TestEvaluateRollback_TheToleranceIsInclusive(t *testing.T) {
	tests := []struct {
		name     string
		change   func(*embedcutover.RollbackState)
		eligible bool
	}{
		{
			name:     "exactly the stale limit",
			change:   func(s *embedcutover.RollbackState) { s.StaleRows = 10 },
			eligible: true,
		},
		{
			name:     "one past it",
			change:   func(s *embedcutover.RollbackState) { s.StaleRows = 11 },
			eligible: false,
		},
		{
			name:     "exactly the missing limit",
			change:   func(s *embedcutover.RollbackState) { s.MissingRows = 0 },
			eligible: true,
		},
		{
			name:     "one past it",
			change:   func(s *embedcutover.RollbackState) { s.MissingRows = 1 },
			eligible: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy, state, observed := rollbackReady()
			test.change(&state)

			eligibility := embedcutover.EvaluateRollback(policy, state, observed)

			c.Assert(eligibility.Eligible, qt.Equals, test.eligible, qt.Commentf("%v", eligibility.Blockers))
		})
	}
}
