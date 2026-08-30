package embedcutover

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/embeddigest"
)

// Decision is the answer, with every reason it is what it is.
//
// Every blocker is collected rather than the first one returned. An operator
// who fixes what they were told and comes back to a second refusal learns the
// system one refusal at a time, and the epic's whole position is that a
// migration this shape should say what is wrong with it.
type Decision struct {
	// Allowed reports whether the operation may proceed.
	Allowed bool
	// Blockers are the reasons it may not, empty when it may.
	Blockers []string
	// PlanDigest is the digest the decision was made against, so a record of
	// the decision can be checked later.
	PlanDigest string
}

// refusef records a blocker.
func (d *Decision) refusef(format string, args ...any) {
	d.Blockers = append(d.Blockers, fmt.Sprintf(format, args...))
}

// settle finishes a decision.
func (d *Decision) settle() Decision {
	sort.Strings(d.Blockers)
	d.Allowed = len(d.Blockers) == 0
	return *d
}

// Decide answers whether this plan may execute, against what is true now.
//
// The approval is a pointer because "no approval was given" and "an approval
// was given for something else" are different refusals, and an operator told
// the wrong one looks in the wrong place.
func Decide(plan Plan, policy Policy, observed Observed, approval *Approval) Decision {
	decision := decideState(plan, policy, observed)
	decideAuthority(&decision, policy, observed, approval, decision.PlanDigest)
	return decision.settle()
}

// Readiness is what a caller that is not cutting over can establish.
//
// It is the same answer [Decide] gives, minus the half about the caller: what
// the state proves, without asking whether whoever is looking may act on it.
type Readiness struct {
	// Ready reports whether the state satisfies everything except authority.
	Ready bool
	// Blockers are what it does not satisfy, empty when it does.
	Blockers []string
	// PlanDigest is what an approval would have to bind to.
	PlanDigest string
	// ApprovalRequired says whether a person still has to give one.
	//
	// Reported separately rather than folded into Ready, because an approval
	// nobody has given yet is not a defect in the state. A rollout gate that
	// waited for Ready to include it would wait forever under the policy most
	// production environments run.
	ApprovalRequired bool
}

// AssessReadiness answers whether the state is ready for a cutover, without
// performing or authorizing one.
//
// It exists because "may I cut over" and "is this ready to cut over" are asked
// by different callers -- an operator with an approval, and a rollout gate with
// none -- and answering the second by running the first would report every
// generation under an approval policy as not ready.
//
// The two share [decideState] rather than restating it. A gate that agreed with
// the cutover verb only by coincidence is a gate that will one day let a
// deployment proceed against a generation the cutover then refuses.
func AssessReadiness(plan Plan, policy Policy, observed Observed) Readiness {
	state := decideState(plan, policy, observed)
	decision := state.settle()
	return Readiness{
		Ready: decision.Allowed, Blockers: decision.Blockers,
		PlanDigest: decision.PlanDigest, ApprovalRequired: policy.RequireExactApproval,
	}
}

// decideState is everything a decision says about the world rather than about
// the caller. It is deliberately unsettled, so [Decide] can add to it.
func decideState(plan Plan, policy Policy, observed Observed) Decision {
	decision := Decision{PlanDigest: plan.Digest()}
	decideEvidence(&decision, plan, policy)
	decideDrift(&decision, plan, observed)
	decideStaleness(&decision, plan, policy, observed)
	return decision
}

// decideEvidence answers whether the plan was justified when it was built.
func decideEvidence(decision *Decision, plan Plan, policy Policy) {
	evidence := plan.Evidence
	switch {
	case evidence.VerificationDigest == "":
		decision.refusef("the plan cites no verification report")
	case evidence.VerificationPassed:
	case len(evidence.AcceptedFindings) == 0:
		decision.refusef("verification did not pass and nothing was accepted")
	case !policy.AllowAcceptedFindings:
		decision.refusef("verification did not pass and this policy does not permit accepting findings")
	}
	// The remaining case -- findings accepted under a policy that permits it --
	// is allowed and is not silent: the plan digest covers exactly WHICH
	// findings were accepted, so an approval given here does not carry over to
	// a plan that accepted others.

	if !evidence.IndexReady {
		decision.refusef("the required index is absent, invalid or still building")
	}
	if policy.RequireConsistencyMode && evidence.SourceMutable && normalizeMode(evidence.ConsistencyMode) == "" {
		decision.refusef("the source is mutable and the run declared no consistency mode")
	}
}

// decideDrift answers whether the world still matches the evidence.
func decideDrift(decision *Decision, plan Plan, observed Observed) {
	if observed.ActivePointer != plan.Previous {
		// Somebody has cut over since this plan was built. Executing it now
		// would move the pointer off whatever they put there, which is not
		// what the plan says it does.
		decision.refusef("queries read %q and the plan was built to replace %q",
			observed.ActivePointer, plan.Previous)
	}
	if !observed.IndexReady {
		decision.refusef("the index was ready when the plan was built and is not ready now")
	}
	if plan.Evidence.ConsistencyWatermark != "" &&
		observed.ConsistencyWatermark != plan.Evidence.ConsistencyWatermark {
		decision.refusef("the run has moved past the watermark the plan was built at")
	}
}

// decideStaleness answers whether the plan is still current.
func decideStaleness(decision *Decision, plan Plan, policy Policy, observed Observed) {
	if policy.MaxPlanAge <= 0 {
		return
	}
	age := observed.Now.Sub(plan.PreparedAt)
	if age > policy.MaxPlanAge {
		decision.refusef("the plan is %s old and this policy allows %s", age.Round(time.Second), policy.MaxPlanAge)
	}
}

// decideAuthority answers whether the caller may do this.
func decideAuthority(decision *Decision, policy Policy, observed Observed, approval *Approval, digest string) {
	if !observed.holds(PermissionCutover) {
		decision.refusef("the caller does not hold %s", PermissionCutover)
	}
	if !policy.RequireExactApproval {
		return
	}
	if policy.RequireSignedApproval && approval != nil && !approval.Signed {
		decision.refusef("this policy requires a signed approval and the one given names %q "+
			"without a signature over the plan", approval.Approver)
	}
	switch {
	case approval == nil:
		decision.refusef("this policy requires an approval and none was given")
	case approval.PlanDigest != digest:
		// The approval is real and it is for a different plan. Saying so is
		// the difference between an operator re-approving and an operator
		// hunting for a missing record.
		decision.refusef("the approval is bound to plan %s and this plan is %s",
			shortOrNone(approval.PlanDigest), shortOrNone(digest))
	case strings.TrimSpace(approval.Approver) == "":
		decision.refusef("the approval names no approver")
	}
}

// shortOrNone names a digest for a person, or says there is none.
func shortOrNone(digest string) string {
	if digest == "" {
		return "(none)"
	}
	return embeddigest.Short(digest)
}
