package embedcutover

import (
	"time"
)

// RollbackPolicy is what a previous generation has to still satisfy to be
// something you can go back to.
//
// The default -- a zero policy -- requires the generation to be complete and
// fresh with no tolerance at all. That is deliberate: a rollback target nobody
// declared a policy for is one nobody thought about, and the safe reading of an
// unthought-about rollback is that it has not been established.
type RollbackPolicy struct {
	// Window is how long after cutover the previous generation stays eligible,
	// zero for no time limit.
	Window time.Duration
	// MaxStaleRows is how many rows of the previous generation may lag the
	// source before it stops being a place to go back to.
	MaxStaleRows int
	// MaxMissingRows is the same for rows it never had.
	MaxMissingRows int
	// RequireIndex requires the previous generation's index to still be
	// present and valid, which is what decides whether going back is a
	// rollback or an outage.
	RequireIndex bool
}

// RollbackState is what is actually true about a previous generation now.
//
// Read back, every field of it. The epic's rule is that rollback must not be
// reported as available merely because old tables still exist, and every field
// here is a way for the tables to exist and the rollback to be a lie.
type RollbackState struct {
	// Present reports whether the generation's column and rows are still
	// there.
	Present bool
	// Maintained reports whether the previous generation is still being
	// written to as the source changes. Once maintenance stops, it starts
	// drifting from the moment it stopped.
	Maintained bool
	// StaleRows and MissingRows are what verification found against it.
	StaleRows   int
	MissingRows int
	// IndexReady reports its index as the catalog reports it.
	IndexReady bool
	// VerifiedAt is when those counts were measured, zero if never.
	VerifiedAt time.Time
	// CutOverAt is when the current generation took over.
	CutOverAt time.Time
	// Retired reports whether the generation was destroyed, which is terminal.
	Retired bool
}

// RollbackEligibility is whether going back is possible, and why not.
type RollbackEligibility struct {
	// Eligible reports whether the previous generation can be rolled back to.
	Eligible bool
	// Blockers are the reasons it cannot, empty when it can.
	Blockers []string
	// Expires is when the window closes, zero when the policy sets none.
	Expires time.Time
}

// EvaluateRollback answers whether a previous generation is still a place to go
// back to.
//
// It is a measurement, not a status field somebody sets. A generation becomes
// ineligible by the world moving, and nothing writes to it when that happens --
// which is exactly why this has to be asked rather than read.
func EvaluateRollback(
	policy RollbackPolicy,
	state RollbackState,
	observed Observed,
) RollbackEligibility {
	eligibility := RollbackEligibility{}
	decision := Decision{}

	if policy.Window > 0 && !state.CutOverAt.IsZero() {
		eligibility.Expires = state.CutOverAt.Add(policy.Window)
	}

	switch {
	case state.Retired:
		decision.refusef("the generation was retired, which is not something you come back from")
	case !state.Present:
		decision.refusef("the generation is no longer present")
	}
	if !state.Present || state.Retired {
		eligibility.Blockers = decision.settle().Blockers
		return eligibility
	}

	evaluateFreshness(&decision, policy, state)
	if policy.RequireIndex && !state.IndexReady {
		// Going back to a generation whose index was dropped is not a
		// rollback. It is the same queries against a sequential scan, which
		// for a corpus of any size is an outage with a different shape.
		decision.refusef("the generation's index is absent or invalid, so going back would leave queries unindexed")
	}
	if !eligibility.Expires.IsZero() && observed.Now.After(eligibility.Expires) {
		decision.refusef("the rollback window closed at %s", eligibility.Expires.UTC().Format(time.RFC3339))
	}
	if !state.Maintained {
		decision.refusef("the generation is no longer maintained, so it drifts further from the source every write")
	}

	settled := decision.settle()
	eligibility.Eligible = settled.Allowed
	eligibility.Blockers = settled.Blockers
	return eligibility
}

// evaluateFreshness holds the previous generation to the policy's tolerances.
func evaluateFreshness(decision *Decision, policy RollbackPolicy, state RollbackState) {
	if state.VerifiedAt.IsZero() {
		// Never measured is not the same as measured and fine. Reporting a
		// generation nobody has checked as rollback-capable is precisely the
		// "the old tables still exist" answer the epic refuses.
		decision.refusef("the generation has never been verified, so its freshness is unknown")
		return
	}
	if state.StaleRows > policy.MaxStaleRows {
		decision.refusef("%d rows are stale and this policy allows %d", state.StaleRows, policy.MaxStaleRows)
	}
	if state.MissingRows > policy.MaxMissingRows {
		decision.refusef("%d rows are missing and this policy allows %d", state.MissingRows, policy.MaxMissingRows)
	}
}
