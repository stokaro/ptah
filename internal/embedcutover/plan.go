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
	// ConsistencyMode is the mode the run proved its completion condition
	// under, empty when the run declared none.
	ConsistencyMode string
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
	// No count precedes the list, because it is the last group: with
	// length-prefixed components and nothing following it, the sequence is
	// already unambiguous and a count would be a second rule saying what the
	// first one says. Where two variable-length lists sit next to each other
	// -- as they do in a generation identity -- the count is load-bearing and
	// is written.
	components = append(components, "evidence.accepted_findings")
	components = append(components, accepted...)
	return components
}

// PlanVersion is the schema version of the digest encoding above.
//
// It is in the digest so that a future change to what a plan binds cannot make
// a new plan collide with an old approval.
const PlanVersion = 1

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
