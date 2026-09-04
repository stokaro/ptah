package embedcutover

import (
	"strconv"

	"go.5x5.cz/ptah/internal/embeddigest"
)

// RetirementPlan is a proposal to destroy a generation.
//
// It is a separate plan with a separate digest and a separate permission,
// because retirement is the one operation here that cannot be undone. A cutover
// that was wrong is a cutover back; a retirement that was wrong is a backfill
// that has to run again from nothing.
type RetirementPlan struct {
	// Generation is what would be destroyed.
	Generation string
	// Schema, Table and Column locate it.
	Schema string
	Table  string
	Column string
	// DropsIndex, DropsColumn and DropsTable say what the retirement actually
	// does, so an approval binds to the destruction rather than to the word.
	//
	// A generation's vectors live in columns on a relation the application
	// keeps, or in a relation of the generation's own, and the two are
	// destroyed by different statements. DropsColumn and DropsTable are
	// therefore alternatives, never both: the plan says which of the two this
	// retirement is, and an approval for one does not authorize the other --
	// which is the whole point of putting the destruction in the digest, and
	// matters most here, because the second removes rows the first leaves.
	DropsIndex  bool
	DropsColumn bool
	DropsTable  bool
	// RowCount is how many rows would be destroyed with it.
	RowCount int
}

// Digest is the retirement plan's exact content.
//
// It carries what the plan DESTROYS, not just what it names. An approval given
// for a plan that drops an index must not authorize one that drops the column.
func (p RetirementPlan) Digest() string {
	return embeddigest.Of(
		"retirement", strconv.Itoa(PlanVersion),
		"generation", p.Generation,
		"schema", p.Schema,
		"table", p.Table,
		"column", p.Column,
		"drops_index", strconv.FormatBool(p.DropsIndex),
		"drops_column", strconv.FormatBool(p.DropsColumn),
		"drops_table", strconv.FormatBool(p.DropsTable),
		"row_count", strconv.Itoa(p.RowCount),
	)
}

// Short is the digest a person quotes in an approval.
func (p RetirementPlan) Short() string {
	return shortOrNone(p.Digest())
}

// RetirementState is what is true about the generation now.
type RetirementState struct {
	// IsActive reports whether queries currently read this generation.
	IsActive bool
	// IsRollbackTargetFor names a live generation that still depends on this
	// one as its way back, empty when none does.
	IsRollbackTargetFor string
	// RollbackEligible reports whether that dependency is still real. A
	// rollback target that is already ineligible protects nothing, and keeping
	// it forever is how a corpus doubles in size and stays that way.
	RollbackEligible bool
}

// DecideRetirement answers whether a generation may be destroyed.
//
// Deleting a manifest, a session, an image tag or a Kubernetes object does not
// reach this function and must not: none of them destroys a generation, and a
// system that treated them as if they did would retire a corpus because
// somebody tidied up a namespace.
func DecideRetirement(
	plan RetirementPlan,
	state RetirementState,
	observed Observed,
	approval *Approval,
	policy Policy,
) Decision {
	digest := plan.Digest()
	decision := Decision{PlanDigest: digest}

	if state.IsActive || observed.ActivePointer == plan.Generation {
		decision.refusef("queries read this generation, so retiring it would leave them nothing to read")
	}
	if state.IsRollbackTargetFor != "" && state.RollbackEligible {
		decision.refusef("generation %q can still be rolled back to this one", state.IsRollbackTargetFor)
	}
	if !plan.DropsIndex && !plan.DropsColumn && !plan.DropsTable {
		// A retirement plan that destroys nothing is not a safer retirement.
		// It is a record saying a generation was retired while the storage,
		// the rows and the index are all still there.
		// Naming what would satisfy it matters more since --drop-column became
		// opt-in: the reachable way to land here is a generation whose index is
		// already gone, retired without asking for the column too, and the
		// blocker has to say that rather than only that nothing happened.
		//
		// The table is in this list because it is the storage under the
		// own-table layout, and a generation there commonly has no index and
		// no column of its own to drop: leaving it out refused every
		// index-less own-table retirement as destroying nothing, with a
		// blocker naming two objects that layout does not have.
		decision.refusef(
			"the plan destroys nothing, so it would record a retirement that did not happen; " +
				"a retirement has to drop the generation's index, and its column or the " +
				"table its vectors are in")
	}
	if !observed.holds(PermissionRetire) {
		decision.refusef("the caller does not hold %s", PermissionRetire)
	}
	DecideApproval(&decision, policy, approval, digest)

	return decision.settle()
}
