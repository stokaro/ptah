package embedcutover_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcutover"
)

// preparedAt is when every plan below was built.
var preparedAt = time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)

// ready is a cutover that should go ahead: verification passed, the index is
// built, the pointer is where the plan expects it, the caller holds the
// permission and the approval is bound to this exact plan.
//
// Every test below breaks exactly one of those.
func ready() (embedcutover.Plan, embedcutover.Policy, embedcutover.Observed, *embedcutover.Approval) {
	plan := embedcutover.Plan{
		Generation: "gen-new",
		Previous:   "gen-old",
		Schema:     "public",
		Table:      "articles",
		Column:     "embedding_new",
		Evidence: embedcutover.Evidence{
			VerificationDigest:   "report-1",
			VerificationPassed:   true,
			ConsistencyMode:      "outbox",
			ConsistencyWatermark: "lsn-42",
			IndexReady:           true,
			SourceMutable:        true,
		},
		PreparedAt: preparedAt,
	}
	policy := embedcutover.Policy{
		RequireExactApproval:   true,
		MaxPlanAge:             time.Hour,
		RequireConsistencyMode: true,
	}
	observed := embedcutover.Observed{
		ActivePointer:        "gen-old",
		ConsistencyWatermark: "lsn-42",
		IndexReady:           true,
		Permissions:          []embedcutover.Permission{embedcutover.PermissionCutover},
		Now:                  preparedAt.Add(5 * time.Minute),
	}
	approval := &embedcutover.Approval{
		PlanDigest: plan.Digest(),
		Approver:   "an operator",
		GrantedAt:  preparedAt.Add(time.Minute),
	}
	return plan, policy, observed, approval
}

// TestDecide_AReadyCutoverProceeds is the control every other test needs.
//
// Without it a decider that refused everything satisfies each negative row
// below and stops every cutover this feature exists to make possible.
func TestDecide_AReadyCutoverProceeds(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, approval := ready()

	decision := embedcutover.Decide(plan, policy, observed, approval)

	c.Assert(decision.Blockers, qt.HasLen, 0, qt.Commentf("%v", decision.Blockers))
	c.Assert(decision.Allowed, qt.IsTrue)
	c.Assert(decision.PlanDigest, qt.Equals, plan.Digest())
}

// TestDecide_EvidenceFailuresBlock walks what the plan was built from.
func TestDecide_EvidenceFailuresBlock(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcutover.Plan)
		want   string
	}{
		{
			name:   "no verification report",
			change: func(p *embedcutover.Plan) { p.Evidence.VerificationDigest = "" },
			want:   "the plan cites no verification report",
		},
		{
			name:   "verification did not pass",
			change: func(p *embedcutover.Plan) { p.Evidence.VerificationPassed = false },
			want:   "verification did not pass and nothing was accepted",
		},
		{
			name:   "the index is not built",
			change: func(p *embedcutover.Plan) { p.Evidence.IndexReady = false },
			want:   "the required index is absent, invalid or still building",
		},
		{
			name:   "a mutable source with no consistency mode",
			change: func(p *embedcutover.Plan) { p.Evidence.ConsistencyMode = "" },
			want:   "the source is mutable and the run declared no consistency mode",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, policy, observed, _ := ready()
			test.change(&plan)

			decision := embedcutover.Decide(plan, policy, observed, approvalFor(plan))

			c.Assert(decision.Allowed, qt.IsFalse)
			c.Assert(decision.Blockers, qt.Contains, test.want)
		})
	}
}

// TestDecide_DriftBlocks is the gap between what WAS true and what IS.
//
// A plan's evidence is a memory. Everything in Observed is read back, and the
// distance between the two is the only thing that makes a plan able to go
// stale rather than able to be re-run forever.
func TestDecide_DriftBlocks(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedcutover.Observed)
		want   string
	}{
		{
			name:   "somebody else cut over first",
			change: func(o *embedcutover.Observed) { o.ActivePointer = "gen-someone-else" },
			want:   `queries read "gen-someone-else" and the plan was built to replace "gen-old"`,
		},
		{
			name:   "the index was dropped since",
			change: func(o *embedcutover.Observed) { o.IndexReady = false },
			want:   "the index was ready when the plan was built and is not ready now",
		},
		{
			name:   "the run moved past the watermark",
			change: func(o *embedcutover.Observed) { o.ConsistencyWatermark = "lsn-99" },
			want:   "the run has moved past the watermark the plan was built at",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, policy, observed, approval := ready()
			test.change(&observed)

			decision := embedcutover.Decide(plan, policy, observed, approval)

			c.Assert(decision.Allowed, qt.IsFalse)
			c.Assert(decision.Blockers, qt.Contains, test.want)
		})
	}
}

// TestDecide_AStalePlanIsRefused pins the age limit.
func TestDecide_AStalePlanIsRefused(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, approval := ready()
	observed.Now = preparedAt.Add(2 * time.Hour)

	decision := embedcutover.Decide(plan, policy, observed, approval)

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains, "the plan is 2h0m0s old and this policy allows 1h0m0s")
}

// TestDecide_APolicyWithoutAnAgeLimitDoesNotExpire is the control for the row
// above: the refusal is about the limit, not about time passing.
func TestDecide_APolicyWithoutAnAgeLimitDoesNotExpire(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, approval := ready()
	policy.MaxPlanAge = 0
	observed.Now = preparedAt.Add(10000 * time.Hour)

	decision := embedcutover.Decide(plan, policy, observed, approval)

	c.Assert(decision.Allowed, qt.IsTrue, qt.Commentf("%v", decision.Blockers))
}

// TestDecide_AuthorityFailuresBlock walks who may do this.
func TestDecide_AuthorityFailuresBlock(t *testing.T) {
	tests := []struct {
		name     string
		approval *embedcutover.Approval
		want     string
	}{
		{
			name:     "no approval at all",
			approval: nil,
			want:     "this policy requires an approval and none was given",
		},
		{
			name:     "an approval for a different plan",
			approval: &embedcutover.Approval{PlanDigest: "someone-elses-plan", Approver: "an operator"},
			want:     "the approval is bound to plan someone-else and this plan is ",
		},
		{
			name:     "an approval naming nobody",
			approval: &embedcutover.Approval{Approver: ""},
			want:     "the approval is bound to plan (none) and this plan is ",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, policy, observed, _ := ready()

			decision := embedcutover.Decide(plan, policy, observed, test.approval)

			c.Assert(decision.Allowed, qt.IsFalse)
			c.Assert(blockersJoined(decision), qt.Contains, test.want)
		})
	}
}

// TestDecide_AnUnnamedApproverIsRefused is the case the table above cannot
// reach: an approval bound to the right plan and naming nobody.
//
// A record saying a cutover was approved, with no approver, is not an approval.
// It is the shape of one.
func TestDecide_AnUnnamedApproverIsRefused(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, _ := ready()

	decision := embedcutover.Decide(plan, policy, observed,
		&embedcutover.Approval{PlanDigest: plan.Digest(), Approver: "  "})

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains, "the approval names no approver")
}

// TestDecide_TheCallerNeedsThePermission keeps an approval from standing in for
// authority the caller does not have.
func TestDecide_TheCallerNeedsThePermission(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, approval := ready()
	observed.Permissions = []embedcutover.Permission{embedcutover.PermissionRollback}

	decision := embedcutover.Decide(plan, policy, observed, approval)

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains, "the caller does not hold inference:cutover")
}

// TestDecide_ChangingAnyBoundInputInvalidatesTheApproval is the epic's rule,
// tested end to end.
//
// The approval was real and it was given for this plan a moment ago. One field
// moves -- the same generation, the same table, one more accepted finding --
// and the approval stops applying, because what it approved is no longer what
// would run.
func TestDecide_ChangingAnyBoundInputInvalidatesTheApproval(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, approval := ready()
	plan.Evidence.AcceptedFindings = []string{"a finding nobody approved"}

	decision := embedcutover.Decide(plan, policy, observed, approval)

	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(blockersJoined(decision), qt.Contains, "the approval is bound to plan ")
}

// TestDecide_AcceptedFindingsProceedOnlyWhereThePolicyAllowsIt walks the
// override.
//
// Accepting a blocking finding is a real thing an operator does. It is also the
// one door in this design that lets a failing verification through, so it opens
// on the policy rather than on the operator's own say-so.
func TestDecide_AcceptedFindingsProceedOnlyWhereThePolicyAllowsIt(t *testing.T) {
	tests := []struct {
		name    string
		allow   bool
		allowed bool
		want    string
	}{
		{name: "the policy permits it", allow: true, allowed: true},
		{
			name: "the policy does not", allow: false, allowed: false,
			want: "verification did not pass and this policy does not permit accepting findings",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, policy, observed, _ := ready()
			plan.Evidence.VerificationPassed = false
			plan.Evidence.AcceptedFindings = []string{"3 rows are stale"}
			policy.AllowAcceptedFindings = test.allow

			decision := embedcutover.Decide(plan, policy, observed, approvalFor(plan))

			c.Assert(decision.Allowed, qt.Equals, test.allowed, qt.Commentf("%v", decision.Blockers))
			c.Assert(blockersJoined(decision), qt.Contains, test.want)
		})
	}
}

// TestDecide_AFirstCutoverHasNoPrevious pins the case where nothing is being
// replaced.
//
// The pointer is empty because no generation has ever been active, and a
// decider comparing against a previous generation that does not exist would
// refuse the only cutover that cannot conflict with anyone.
func TestDecide_AFirstCutoverHasNoPrevious(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, _ := ready()
	plan.Previous = ""
	observed.ActivePointer = ""

	decision := embedcutover.Decide(plan, policy, observed, approvalFor(plan))

	c.Assert(decision.Allowed, qt.IsTrue, qt.Commentf("%v", decision.Blockers))
}

// TestDecide_EveryBlockerIsReported keeps the answer complete.
//
// An operator who fixes what they were told and comes back to a second refusal
// learns the system one refusal at a time. This plan is wrong in four separate
// ways and has to say so in one answer.
func TestDecide_EveryBlockerIsReported(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, _ := ready()
	plan.Evidence.VerificationPassed = false
	plan.Evidence.IndexReady = false
	observed.ActivePointer = "gen-someone-else"
	observed.Permissions = nil

	decision := embedcutover.Decide(plan, policy, observed, nil)

	c.Assert(decision.Blockers, qt.HasLen, 5)
	c.Assert(decision.Blockers, qt.DeepEquals, []string{
		`queries read "gen-someone-else" and the plan was built to replace "gen-old"`,
		"the caller does not hold inference:cutover",
		"the required index is absent, invalid or still building",
		"this policy requires an approval and none was given",
		"verification did not pass and nothing was accepted",
	})
}

// approvalFor is an approval bound to exactly this plan.
func approvalFor(plan embedcutover.Plan) *embedcutover.Approval {
	return &embedcutover.Approval{PlanDigest: plan.Digest(), Approver: "an operator", GrantedAt: preparedAt}
}

// blockersJoined renders a decision's blockers for a substring assertion.
func blockersJoined(decision embedcutover.Decision) string {
	joined := ""
	for _, blocker := range decision.Blockers {
		joined += blocker + "\n"
	}
	return joined
}
