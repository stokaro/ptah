package embedcutover_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcutover"
)

// retirable is a generation that may be destroyed: nothing reads it, nothing
// depends on it, the plan actually destroys something, the caller holds the
// permission and the approval is bound to this exact plan.
func retirable() (
	embedcutover.RetirementPlan,
	embedcutover.RetirementState,
	embedcutover.Observed,
	*embedcutover.Approval,
	embedcutover.Policy,
) {
	plan := embedcutover.RetirementPlan{
		Generation:  "gen-old",
		Schema:      "public",
		Table:       "articles",
		Column:      "embedding_old",
		DropsIndex:  true,
		DropsColumn: true,
		RowCount:    120_000,
	}
	state := embedcutover.RetirementState{}
	observed := embedcutover.Observed{
		ActivePointer: "gen-new",
		Permissions:   []embedcutover.Permission{embedcutover.PermissionRetire},
	}
	approval := &embedcutover.Approval{PlanDigest: plan.Digest(), Approver: "an operator"}
	policy := embedcutover.Policy{RequireExactApproval: true}
	return plan, state, observed, approval, policy
}

// TestDecideRetirement_ARetirableGenerationProceeds is the control.
func TestDecideRetirement_ARetirableGenerationProceeds(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, approval, policy := retirable()

	decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

	c.Assert(decision.Blockers, qt.HasLen, 0, qt.Commentf("%v", decision.Blockers))
	c.Assert(decision.Allowed, qt.IsTrue)
}

// TestDecideRetirement_RefusesWhatIsStillNeeded walks the ways a generation is
// not disposable.
func TestDecideRetirement_RefusesWhatIsStillNeeded(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcutover.RetirementState, *embedcutover.Observed)
		want   string
	}{
		{
			name:   "queries read it",
			change: func(s *embedcutover.RetirementState, _ *embedcutover.Observed) { s.IsActive = true },
			want:   "queries read this generation, so retiring it would leave them nothing to read",
		},
		{
			name: "the pointer says so even when the state does not",
			change: func(_ *embedcutover.RetirementState, o *embedcutover.Observed) {
				o.ActivePointer = "gen-old"
			},
			want: "queries read this generation, so retiring it would leave them nothing to read",
		},
		{
			name: "it is somebody's way back",
			change: func(s *embedcutover.RetirementState, _ *embedcutover.Observed) {
				s.IsRollbackTargetFor = "gen-new"
				s.RollbackEligible = true
			},
			want: `generation "gen-new" can still be rolled back to this one`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, state, observed, approval, policy := retirable()
			test.change(&state, &observed)

			decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

			c.Assert(decision.Allowed, qt.IsFalse)
			c.Assert(decision.Blockers, qt.Contains, test.want)
		})
	}
}

// TestDecideRetirement_AnIneligibleRollbackTargetIsNoLongerADependency keeps a
// generation from being undroppable forever.
//
// The newer generation still names this one as its way back, and that way back
// is already gone -- the window closed, or it stopped being maintained. A
// dependency that protects nothing is how a corpus doubles in size and stays
// that way.
func TestDecideRetirement_AnIneligibleRollbackTargetIsNoLongerADependency(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, approval, policy := retirable()
	state.IsRollbackTargetFor = "gen-new"
	state.RollbackEligible = false

	decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

	c.Assert(decision.Allowed, qt.IsTrue, qt.Commentf("%v", decision.Blockers))
}

// TestDecideRetirement_APlanThatDestroysNothingIsRefused is the trap this
// operation has and the others do not.
//
// A retirement plan dropping neither the index nor the column is not a safer
// retirement. It is a record saying a generation was retired while the rows,
// the storage and the index are all still there -- and the next question
// somebody asks is why the disk did not shrink.
func TestDecideRetirement_APlanThatDestroysNothingIsRefused(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, _, policy := retirable()
	plan.DropsIndex = false
	plan.DropsColumn = false

	decision := embedcutover.DecideRetirement(plan, state, observed, approvalForRetirement(plan), policy)

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains,
		"the plan destroys nothing, so it would record a retirement that did not happen; "+
			"a retirement has to drop the generation's index, and its column or the "+
			"table its vectors are in")
}

// TestDecideRetirement_DroppingTheTableIsDestroyingSomething is the other side
// of the refusal above, and it is the case the rule was written without.
//
// A generation whose vectors live in a relation of its own commonly has no
// index and never has a column of its own to drop: the storage is the
// relation. Judged by the two objects the rule originally named, every such
// retirement destroyed nothing and was refused with a blocker listing two
// things that layout does not have. Measured through the CLI first, on a
// generation prepared and backfilled into a table Ptah created
// (stokaro/ptah#2624).
func TestDecideRetirement_DroppingTheTableIsDestroyingSomething(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, _, policy := retirable()
	plan.DropsIndex = false
	plan.DropsColumn = false
	plan.DropsTable = true

	decision := embedcutover.DecideRetirement(plan, state, observed, approvalForRetirement(plan), policy)

	c.Assert(decision.Allowed, qt.IsTrue, qt.Commentf("%v", decision.Blockers))
}

// TestDecideRetirement_TheApprovalBindsToWhatIsDestroyed is why the digest
// carries the verbs and not just the nouns.
//
// An operator approving the removal of an index did not approve the removal of
// the column and its hundred and twenty thousand vectors, and both plans name
// the same generation, schema, table and column.
func TestDecideRetirement_TheApprovalBindsToWhatIsDestroyed(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcutover.RetirementPlan)
	}{
		{name: "the column too", change: func(p *embedcutover.RetirementPlan) { p.DropsColumn = false }},
		{name: "the index too", change: func(p *embedcutover.RetirementPlan) { p.DropsIndex = false }},
		{name: "how much", change: func(p *embedcutover.RetirementPlan) { p.RowCount = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, state, observed, approval, policy := retirable()
			test.change(&plan)

			decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

			c.Assert(decision.Allowed, qt.IsFalse)
			c.Assert(blockersJoined(decision), qt.Contains, "the approval is bound to plan ")
		})
	}
}

// TestDecideRetirement_NeedsItsOwnPermission keeps a cutover permission from
// authorizing a destruction.
func TestDecideRetirement_NeedsItsOwnPermission(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, approval, policy := retirable()
	observed.Permissions = []embedcutover.Permission{
		embedcutover.PermissionCutover, embedcutover.PermissionRollback,
	}

	decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains, "the caller does not hold inference:retire")
}

// TestDecideRetirement_RequiresAnApprovalWhereThePolicyDoesAndNotWhereItDoesNot
// pins both sides of the policy.
func TestDecideRetirement_RequiresAnApprovalWhereThePolicyDoesAndNotWhereItDoesNot(t *testing.T) {
	tests := []struct {
		name    string
		require bool
		allowed bool
	}{
		{name: "the policy requires one", require: true, allowed: false},
		{name: "the policy does not", require: false, allowed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, state, observed, _, policy := retirable()
			policy.RequireExactApproval = test.require

			decision := embedcutover.DecideRetirement(plan, state, observed, nil, policy)

			c.Assert(decision.Allowed, qt.Equals, test.allowed, qt.Commentf("%v", decision.Blockers))
		})
	}
}

// approvalForRetirement is an approval bound to exactly this retirement plan.
func approvalForRetirement(plan embedcutover.RetirementPlan) *embedcutover.Approval {
	return &embedcutover.Approval{PlanDigest: plan.Digest(), Approver: "an operator"}
}

// TestDecideRetirement_ASignedApprovalPolicyRefusesAName is the finding a
// review caught: the signed requirement reached the cutover's copy of the
// approval check and not the retirement's.
//
// Retirement is the one operation that cannot be undone, so a policy demanding
// a cryptographically verified approver mattering less there than at a cutover
// is exactly backwards.
func TestDecideRetirement_ASignedApprovalPolicyRefusesAName(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, approval, policy := retirable()
	policy.RequireSignedApproval = true

	decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains,
		`this policy requires a signed approval and the one given names "an operator" `+
			`without a signature over the plan`)
}

// TestDecideRetirement_ASignedApprovalSatisfiesThatPolicy is the control.
func TestDecideRetirement_ASignedApprovalSatisfiesThatPolicy(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, approval, policy := retirable()
	policy.RequireSignedApproval = true
	approval.Signed = true

	decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

	c.Assert(decision.Blockers, qt.HasLen, 0, qt.Commentf("%v", decision.Blockers))
	c.Assert(decision.Allowed, qt.IsTrue)
}

// TestDecideRetirement_TwoPlansSharingAShortDigestAreToldApart is the other
// thing the shared check carried over.
//
// The retirement's own copy rendered both sides short, so an operator who typed
// the short form of a plan that had moved read "bound to plan X and this plan
// is X".
func TestDecideRetirement_TwoPlansSharingAShortDigestAreToldApart(t *testing.T) {
	c := qt.New(t)
	plan, state, observed, approval, policy := retirable()
	approval.PlanDigest = plan.Short()

	decision := embedcutover.DecideRetirement(plan, state, observed, approval, policy)

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains, fmt.Sprintf(
		"the approval is bound to plan %s and this plan is %s", plan.Short(), plan.Digest()))
}
