// Package embedcutover decides whether a generation may become the one queries
// read, whether an older one can still be rolled back to, and whether one may
// be destroyed.
//
// Three separate decisions on purpose. A backfill finishing does not imply a
// cutover, a cutover does not imply the old generation is disposable, and
// deleting a manifest, a session, an image tag or a Kubernetes object retires
// nothing (stokaro/ptah#2068).
package embedcutover

import (
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/embeddigest"
)

// Evidence is what a cutover plan was built from.
//
// Every field here is bound into the plan's digest, so an approval given for
// this plan stops applying the moment any of it changes. That is the point: an
// approval that survived its evidence would authorize a cutover to a
// generation nobody looked at.
type Evidence struct {
	// VerificationDigest identifies the exact verification report the plan was
	// built on.
	VerificationDigest string
	// VerificationPassed reports whether that report had no blocking findings.
	VerificationPassed bool
	// AcceptedFindings are blocking findings an operator explicitly accepted,
	// by summary. An acceptance names what was accepted; "verification was
	// overridden" is not a record of anything.
	AcceptedFindings []string
	// ConsistencyMode is the mode the specification declares, empty when it
	// declares none.
	//
	// It says what was ASKED FOR and not whether it was reached. Those were one
	// field: a plan blanked the mode when the guarantee was incomplete, and the
	// refusal that reads it then told an operator with `consistency.mode:
	// outbox` that their run "declared no consistency mode"
	// (stokaro/ptah#2646).
	ConsistencyMode string
	// ConsistencyBlockers are the reasons that mode has not reached its
	// completion condition, empty when it has.
	//
	// Carried from the barrier rather than derived here. Whether a backfill is
	// short of its snapshot or a catch-up is short of the barrier is a
	// measurement, and a decision layer restating it would be guessing between
	// two answers it does not have.
	ConsistencyBlockers []string
	// ConsistencyWatermark is how far that proof reached.
	ConsistencyWatermark string
	// IndexReady reports whether the required index exists, is valid and is
	// finished building.
	IndexReady bool
	// SourceMutable reports whether the source can change under the run.
	SourceMutable bool
}

// Plan is a proposal to make one generation the one queries read.
type Plan struct {
	// Generation is the generation to cut over to.
	Generation string
	// Previous is the generation queries read now, empty on a first cutover.
	Previous string
	// Schema, Table and Column locate what the pointer moves over.
	Schema string
	Table  string
	Column string
	// Evidence is what justified the plan.
	Evidence Evidence
	// PreparedAt is when the plan was built, which is what makes it able to go
	// stale.
	PreparedAt time.Time
}

// Digest is the plan's exact content. An approval binds to this value.
func (p Plan) Digest() string {
	return embeddigest.Of(p.digestComponents()...)
}

// digestComponents is the ordered list the digest is taken over.
//
// It is written out rather than reflected so that adding a field is a
// decision: TestPlan_EveryFieldIsBound enumerates the struct and requires each
// field to appear here, so a field that joins Plan without joining the digest
// fails rather than quietly widening what an approval covers.
func (p Plan) digestComponents() []string {
	accepted := make([]string, len(p.Evidence.AcceptedFindings))
	copy(accepted, p.Evidence.AcceptedFindings)
	sort.Strings(accepted)

	components := []string{
		"plan", strconv.Itoa(PlanVersion),
		"generation", p.Generation,
		"previous", p.Previous,
		"schema", p.Schema,
		"table", p.Table,
		"column", p.Column,
		"evidence.verification_digest", p.Evidence.VerificationDigest,
		"evidence.verification_passed", strconv.FormatBool(p.Evidence.VerificationPassed),
		"evidence.consistency_mode", p.Evidence.ConsistencyMode,
		"evidence.consistency_watermark", p.Evidence.ConsistencyWatermark,
		"evidence.index_ready", strconv.FormatBool(p.Evidence.IndexReady),
		"evidence.source_mutable", strconv.FormatBool(p.Evidence.SourceMutable),
		"prepared_at", p.PreparedAt.UTC().Format(time.RFC3339Nano),
	}
	// Two variable-length lists now sit next to each other, and neither carries
	// a count -- because the LABEL between them is the boundary. Components are
	// length-prefixed, so a blocker reading "evidence.accepted_findings" cannot
	// be mistaken for the label that follows it.
	//
	// A count was written here first and no fixture could tell it from this,
	// which is the sign it was doing nothing.
	// TestPlanDigest_TheTwoVariableListsCannotBeConfused is what establishes
	// the property the count appeared to provide: a plan blocked by a sentence
	// and one accepting that same sentence digest differently.
	//
	// Where two such lists sit next to each other with NO label between them --
	// as they do in a generation identity -- a count is load-bearing and is
	// written.
	blockers := append([]string(nil), p.Evidence.ConsistencyBlockers...)
	sort.Strings(blockers)
	components = append(components, "evidence.consistency_blockers")
	components = append(components, blockers...)

	components = append(components, "evidence.accepted_findings")
	components = append(components, accepted...)
	return components
}

// PlanVersion is the schema version of the digest encoding above.
//
// It is in the digest so that a future change to what a plan binds cannot make
// a new plan collide with an old approval.
//
// It moved to 2 when the plan started binding the consistency blockers
// (stokaro/ptah#2646). Every approval given under version 1 stops matching,
// which is the intended outcome: those plans were built by a layer that blanked
// the consistency mode when the guarantee was incomplete, so an approval given
// for one cannot be honored by a build that now distinguishes "no mode
// declared" from "the declared mode has not caught up".
const PlanVersion = 2

// Short is the plan digest a person quotes in an approval.
func (p Plan) Short() string {
	return embeddigest.Short(p.Digest())
}

// Approval is one person authorizing one exact plan.
type Approval struct {
	// PlanDigest is the plan this approval was given for.
	PlanDigest string
	// Approver identifies who gave it.
	Approver string
	// Signed reports whether the approver was established by a signature over
	// the plan rather than typed alongside it.
	//
	// The two are different claims and only one of them is evidence. A name
	// given on a command line says who the operator wrote down; a verified
	// signature says whose key covered these exact bytes, and that is the
	// question an audit six months later is asking.
	Signed bool
	// GrantedAt is when.
	GrantedAt time.Time
}

// Permission is a capability the caller holds.
type Permission string

const (
	// PermissionCutover allows moving the pointer queries read.
	PermissionCutover Permission = "inference:cutover"
	// PermissionRollback allows moving it back.
	PermissionRollback Permission = "inference:rollback"
	// PermissionRetire allows destroying a generation.
	PermissionRetire Permission = "inference:retire"
)

// Policy is what the environment requires before a pointer moves.
type Policy struct {
	// RequireExactApproval requires an approval bound to this plan's digest.
	RequireExactApproval bool
	// RequireSignedApproval requires that approval to be a verified signature
	// rather than a digest and a name somebody typed.
	//
	// Separate from RequireExactApproval because they refuse different things.
	// An exact approval establishes WHAT was approved; a signed one establishes
	// WHO approved it, and an environment can reasonably want the first without
	// the machinery for the second.
	RequireSignedApproval bool
	// MaxPlanAge is how long a plan stays current, zero for no limit.
	MaxPlanAge time.Duration
	// RequireConsistencyMode refuses a run that declared no mode over a
	// mutable source.
	RequireConsistencyMode bool
	// AllowAcceptedFindings permits cutting over with blocking findings an
	// operator accepted. With it false, an acceptance changes nothing and
	// verification has to pass on its own.
	AllowAcceptedFindings bool
}

// Observed is what is true at the moment the cutover would execute, read back
// rather than remembered.
//
// A plan's evidence is what WAS true. Everything here is what IS true, and the
// gap between them is the whole reason a plan can go stale.
type Observed struct {
	// ActivePointer is the generation queries read right now.
	ActivePointer string
	// ConsistencyWatermark is how far the run's completion proof reaches now.
	ConsistencyWatermark string
	// IndexReady reports the index as the catalog reports it now.
	IndexReady bool
	// Permissions are what the caller holds.
	Permissions []Permission
	// Now is the current time.
	Now time.Time
}

// holds reports whether the caller holds a permission.
func (o Observed) holds(permission Permission) bool {
	return slices.Contains(o.Permissions, permission)
}

// normalizeMode folds a consistency mode for comparison.
func normalizeMode(mode string) string {
	return strings.ToLower(strings.TrimSpace(mode))
}
