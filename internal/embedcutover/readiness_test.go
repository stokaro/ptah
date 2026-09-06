package embedcutover_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedcutover"
)

// TestAssessReadiness_AStateWaitingOnlyForAnApprovalIsReady is the question a
// rollout gate asks, and the reason it is not [embedcutover.Decide].
//
// Under `require_exact_approval`, which is what production environments run,
// Decide refuses every generation nobody has signed for yet. A gate built on it
// would wait forever on a state that is finished -- so readiness answers what
// the state proves and reports the approval as owed rather than as a defect.
func TestAssessReadiness_AStateWaitingOnlyForAnApprovalIsReady(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, _ := ready()

	readiness := embedcutover.AssessReadiness(plan, policy, observed)

	c.Assert(readiness.Blockers, qt.HasLen, 0, qt.Commentf("%v", readiness.Blockers))
	c.Assert(readiness.Ready, qt.IsTrue)
	c.Assert(readiness.ApprovalRequired, qt.IsTrue)
	// And what to approve, which is the next thing whoever read this does.
	c.Assert(readiness.PlanDigest, qt.Equals, plan.Digest())
}

// TestAssessReadiness_TheSameStateIsNotYetAllowedToCutOver is the control.
//
// It pins the difference between the two answers: the state is ready and the
// cutover is refused, on the same inputs. Without it, a readiness that simply
// called Decide would satisfy the test above whenever a policy required no
// approval, and the production case -- the one it exists for -- would be the
// one nothing measured.
func TestAssessReadiness_TheSameStateIsNotYetAllowedToCutOver(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, _ := ready()

	readiness := embedcutover.AssessReadiness(plan, policy, observed)
	decision := embedcutover.Decide(plan, policy, observed, nil)

	c.Assert(readiness.Ready, qt.IsTrue)
	c.Assert(decision.Allowed, qt.IsFalse)
	c.Assert(decision.Blockers, qt.Contains,
		"this policy requires an approval and none was given")
}

// TestAssessReadiness_ANoApprovalPolicyOwesNothing is the other half of the
// approval report.
//
// A gate that treated ApprovalRequired as always true would tell an operator to
// go and sign something their policy does not ask for.
func TestAssessReadiness_ANoApprovalPolicyOwesNothing(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, _ := ready()
	policy.RequireExactApproval = false

	readiness := embedcutover.AssessReadiness(plan, policy, observed)

	c.Assert(readiness.Ready, qt.IsTrue)
	c.Assert(readiness.ApprovalRequired, qt.IsFalse)
}

// TestAssessReadiness_EveryStateRefusalIsCarried is what makes the gate agree
// with the verb.
//
// Each row breaks one thing a cutover needs and asserts that readiness reports
// it. A gate that only checked the approval would report a generation with a
// failed verification, a broken index or a pointer somebody else moved as ready
// to deploy against.
func TestAssessReadiness_EveryStateRefusalIsCarried(t *testing.T) {
	tests := []struct {
		name    string
		breakIt func(*embedcutover.Plan, *embedcutover.Observed)
		blocker string
	}{
		{
			name: "verification did not pass",
			breakIt: func(plan *embedcutover.Plan, _ *embedcutover.Observed) {
				plan.Evidence.VerificationPassed = false
			},
			blocker: "verification did not pass and nothing was accepted",
		},
		{
			name: "the index is not there",
			breakIt: func(plan *embedcutover.Plan, _ *embedcutover.Observed) {
				plan.Evidence.IndexReady = false
			},
			blocker: "the required index is absent, invalid or still building",
		},
		{
			name: "somebody else moved the pointer",
			breakIt: func(_ *embedcutover.Plan, observed *embedcutover.Observed) {
				observed.ActivePointer = "gen-somebody-else"
			},
			blocker: `queries read "gen-somebody-else" and the plan was built to replace "gen-old"`,
		},
		{
			name: "the run moved past the watermark",
			breakIt: func(_ *embedcutover.Plan, observed *embedcutover.Observed) {
				observed.ConsistencyWatermark = "lsn-99"
			},
			blocker: "the run has moved past the watermark the plan was built at",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, policy, observed, _ := ready()
			test.breakIt(&plan, &observed)

			readiness := embedcutover.AssessReadiness(plan, policy, observed)

			c.Assert(readiness.Ready, qt.IsFalse)
			c.Assert(readiness.Blockers, qt.Contains, test.blocker)
		})
	}
}

// TestAssessReadiness_TheCallersOwnPermissionIsNotAStateFact is the boundary
// between the two answers, from the other side.
//
// A gate reads the state; it does not hold the permission to move a pointer and
// is not expected to. Reporting the reader's own authority as something wrong
// with the generation would send an operator to look at a corpus that is fine.
func TestAssessReadiness_TheCallersOwnPermissionIsNotAStateFact(t *testing.T) {
	c := qt.New(t)
	plan, policy, observed, approval := ready()
	observed.Permissions = nil

	readiness := embedcutover.AssessReadiness(plan, policy, observed)
	decision := embedcutover.Decide(plan, policy, observed, approval)

	c.Assert(readiness.Ready, qt.IsTrue)
	c.Assert(decision.Allowed, qt.IsFalse)
}
