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
	// UnacceptedFindings are the blocking findings that were not accepted.
	//
	// Both lists, because one of them cannot answer the question. A plan
	// carrying three blocking findings and one acceptance has a non-empty
	// AcceptedFindings, and a decision reading only that lets the other two
	// through -- an acceptance for one finding authorizing a cutover over
	// findings nobody looked at (stokaro/ptah#2649).
	UnacceptedFindings []string
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
	// SourceRows is how many in-scope source rows the verification counted.
	//
	// The measurement rather than a judgement about it: whether a corpus is
	// large enough to cut over to is an environment's question, and
	// [Policy.MinSourceRows] is where the answer lives. It is here because a
	// plan carries what justified it, and "how much was there" is part of
	// that -- an approval given for a plan over 48000 rows should not carry to
	// one over none.
	SourceRows int
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
		"evidence.source_rows", strconv.Itoa(p.Evidence.SourceRows),
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

	unaccepted := append([]string(nil), p.Evidence.UnacceptedFindings...)
	sort.Strings(unaccepted)
	components = append(components, "evidence.unaccepted_findings")
	components = append(components, unaccepted...)
	return components
}

// IdentityLines are the facts an approver reads before signing this plan.
//
// They live here rather than in the command that writes the file because they
// are a description of the PLAN, and the file format -- its header and where
// the digest sits -- is the command's. What the command must not do is decide
// which facts a signature is worth giving over.
//
// Every field the digest binds appears here, and that is the contract rather
// than a convenience. An approval binds to the digest, so a fact the digest
// covers and the file omits is a fact the approver signed for and could not
// have read. Under policy.require_signed_approval the file WAS the whole of
// what a person saw, and two plans differing only in whether both blocking
// findings were accepted rendered byte-identical apart from the digest: the
// `verification:` line named a report and read as evidence that it passed
// (stokaro/ptah#2739).
//
// An empty list still writes its line. Silence about accepted findings is not
// distinguishable from a file whose author had nothing to say, and the reader
// deciding is the one who cannot tell.
func (p Plan) IdentityLines() []string {
	lines := []string{
		"generation: " + p.Generation,
		"replaces: " + p.Previous,
		"target: " + p.Schema + "." + p.Table + "." + p.Column,
		"prepared at: " + p.PreparedAt.UTC().Format(time.RFC3339Nano),
		"verification report: " + p.Evidence.VerificationDigest,
		"verification passed: " + strconv.FormatBool(p.Evidence.VerificationPassed),
		// Beside "passed", because passing says nothing about what was passed
		// over: an empty corpus passes every layer. An approver reading this
		// file is being asked to authorize a pointer move, and how many rows
		// it moves onto is the fact that separates one from a filter typo
		// (stokaro/ptah#2870).
		"source rows: " + strconv.Itoa(p.Evidence.SourceRows),
		"consistency mode: " + p.Evidence.ConsistencyMode,
		"consistency watermark: " + p.Evidence.ConsistencyWatermark,
		"index ready: " + strconv.FormatBool(p.Evidence.IndexReady),
		"source mutable: " + strconv.FormatBool(p.Evidence.SourceMutable),
	}
	lines = append(lines, identityList("consistency blocker", p.Evidence.ConsistencyBlockers)...)
	lines = append(lines, identityList("accepts blocking finding", p.Evidence.AcceptedFindings)...)
	lines = append(lines, identityList("UNACCEPTED blocking finding", p.Evidence.UnacceptedFindings)...)
	return lines
}

// identityList renders one of the evidence lists, sorted the way the digest
// sorts it so the file and the number it carries describe the same plan.
//
// The label is repeated per element rather than written once above an indented
// block: a reader scanning for what they are accepting sees it on the line that
// says it, and a list that ran on past its heading cannot be misread as part of
// the one before.
func identityList(label string, values []string) []string {
	if len(values) == 0 {
		return []string{label + "s: none"}
	}
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	lines := make([]string, 0, len(sorted))
	for _, value := range sorted {
		lines = append(lines, label+": "+value)
	}
	return lines
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
//
// It moved to 3 when the plan started binding the blocking findings that were
// NOT accepted (stokaro/ptah#2649). A version-2 plan recorded which findings an
// operator accepted and nothing about the ones they did not, so an approval
// given under it says nothing about what the cutover would proceed over.
//
// It moved to 4 when the plan started binding how many source rows the
// verification counted (stokaro/ptah#2870). A version-3 plan carried whether
// verification passed and not what it passed over, and an empty corpus passes
// every layer -- so an approval given for a plan over the whole corpus would
// have been honored by a build over none of it.
const PlanVersion = 4

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
	// MinSourceRows is the smallest corpus this environment will cut over to,
	// zero for no requirement.
	//
	// A verification over an empty corpus passes every layer, because there is
	// nothing for any of them to disagree about, and the reachable cause is a
	// `source.filter` with a typo in it rather than an empty table
	// (stokaro/ptah#2870). The report says so as an advisory, because an empty
	// generation is not wrong -- a table backfilled before its first rows
	// arrive is a specification doing what it says -- so the refusal is
	// something an environment asks for rather than something Ptah assumes.
	//
	// Zero means no requirement, which is the default and what every existing
	// specification means.
	MinSourceRows int
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
