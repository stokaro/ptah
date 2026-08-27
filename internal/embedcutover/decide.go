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
	digest := plan.Digest()
	decision := Decision{PlanDigest: digest}

	decideEvidence(&decision, plan, policy)
	decideDrift(&decision, plan, observed)
	decideStaleness(&decision, plan, policy, observed)
	decideAuthority(&decision, policy, observed, approval, digest)

	return decision.settle()
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
