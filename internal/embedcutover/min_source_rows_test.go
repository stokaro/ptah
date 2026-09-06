package embedcutover_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedcutover"
)

// TestDecide_ACorpusBelowThePolicysFloorIsRefused is the half of
// stokaro/ptah#2870 an environment asks for.
//
// A verification over an empty corpus passes every layer, because there is
// nothing for any of them to disagree about. Whether that is a generation to
// move queries onto is something only the environment knows, so it says so
// here rather than Ptah assuming it.
func TestDecide_ACorpusBelowThePolicysFloorIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		rows    int
		floor   int
		wantErr string
	}{
		{
			name: "an empty corpus under any floor", rows: 0, floor: 1,
			wantErr: "this generation covers 0 source rows and this policy requires at least 1",
		},
		{
			name: "a corpus one row short", rows: 47999, floor: 48000,
			wantErr: "this generation covers 47999 source rows and this policy requires at least 48000",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, policy, observed, _ := ready()
			plan.Evidence.SourceRows = test.rows
			policy.MinSourceRows = test.floor

			decision := embedcutover.Decide(plan, policy, observed, approvalFor(plan))

			c.Assert(decision.Allowed, qt.IsFalse)
			c.Assert(blockersJoined(decision), qt.Contains, test.wantErr)
		})
	}
}

// TestDecide_ACorpusAtOrAboveTheFloorIsAllowed is the control the refusal
// needs.
//
// Without it, a check comparing the wrong way round, or one refusing whenever a
// floor is set at all, satisfies every row above and blocks every cutover in an
// environment that configured one.
func TestDecide_ACorpusAtOrAboveTheFloorIsAllowed(t *testing.T) {
	tests := []struct {
		name  string
		rows  int
		floor int
	}{
		{name: "exactly the floor", rows: 48000, floor: 48000},
		{name: "above it", rows: 48231, floor: 48000},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, policy, observed, _ := ready()
			plan.Evidence.SourceRows = test.rows
			policy.MinSourceRows = test.floor

			decision := embedcutover.Decide(plan, policy, observed, approvalFor(plan))

			c.Assert(decision.Allowed, qt.IsTrue, qt.Commentf("%v", decision.Blockers))
		})
	}
}

// TestDecide_NoFloorIsNoRequirement is what every existing specification means.
//
// Zero is the default, and it has to mean "do not ask" rather than "at least
// zero, which is everything" by accident. An empty corpus with no floor
// configured still cuts over, because #2870's finding is an advisory and this
// is the opt-in beside it -- not a refusal that arrived without being asked
// for.
func TestDecide_NoFloorIsNoRequirement(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, _ := ready()
	plan.Evidence.SourceRows = 0
	policy.MinSourceRows = 0

	decision := embedcutover.Decide(plan, policy, observed, approvalFor(plan))

	c.Assert(decision.Allowed, qt.IsTrue, qt.Commentf("%v", decision.Blockers))
}

// TestPlan_TheSignedFileSaysHowManyRowsItCovers is why the count is in the
// plan rather than only in the policy.
//
// An approval binds to the plan digest, and an approver reads the file. "The
// verification passed" says nothing about what it passed over, so a plan over
// the whole corpus and one over none of it would have read identically.
func TestPlan_TheSignedFileSaysHowManyRowsItCovers(t *testing.T) {
	c := qt.New(t)
	plan, _, _, _ := ready()
	plan.Evidence.SourceRows = 48231

	c.Assert(plan.IdentityLines(), qt.Contains, "source rows: 48231")
}
