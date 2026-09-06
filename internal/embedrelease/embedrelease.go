// Package embedrelease is the evidence a generation change leaves behind: what
// was built, what was measured about it, and what was done with it.
//
// The records are OCI artifacts because that is where Ptah's other evidence
// already lives, and because a verification report that only exists in a
// terminal is a verification nobody can produce six months later when somebody
// asks why a corpus was replaced (stokaro/ptah#2068).
package embedrelease

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"ptah.run/internal/embeddigest"
)

// Artifact types, following the convention internal/ociartifact already
// establishes.
const (
	// ReleaseArtifactType identifies what a generation change proposes: the
	// specification it was built from and the corpus it will be measured with.
	ReleaseArtifactType = "application/vnd.stokaro.ptah.inference.release.v1"
	// VerificationArtifactType identifies what was measured about a generation.
	VerificationArtifactType = "application/vnd.stokaro.ptah.inference.verification.v1"
	// CutoverArtifactType identifies what was done: a pointer moved, and on
	// whose approval.
	CutoverArtifactType = "application/vnd.stokaro.ptah.inference.cutover.v1"
	// RollbackArtifactType identifies a pointer moved back, and what made the
	// generation it moved to still a place to go.
	RollbackArtifactType = "application/vnd.stokaro.ptah.inference.rollback.v1"
	// RetirementArtifactType identifies what was destroyed. It is the record
	// that cannot be reconstructed from the database afterwards, because the
	// thing it describes is gone.
	RetirementArtifactType = "application/vnd.stokaro.ptah.inference.retirement.v1"
)

// RecordVersion is the schema version of the records below.
//
// It is inside every digest so that a future change to what a record holds
// cannot make a new one collide with a signature over an old one.
const RecordVersion = 1

// Release is what a generation change proposes.
type Release struct {
	// Version is the record schema version.
	Version int `json:"version"`
	// Generation is the identity being built.
	Generation string `json:"generation"`
	// Replaces is the generation it is meant to replace, empty on a first one.
	Replaces string `json:"replaces,omitempty"`
	// SpecDigest is the specification's content address.
	SpecDigest string `json:"spec_digest"`
	// CorpusDigest is the evaluation corpus's, empty when none was declared.
	CorpusDigest string `json:"corpus_digest,omitempty"`
	// Target names where the vectors live.
	Target string `json:"target"`
	// Reproducibility is what the identity could promise, and Reason says what
	// is unpinned when it is partial.
	//
	// Recorded rather than omitted, because the answer six months later to
	// "can we rebuild this" is either yes or a sentence, and a record that
	// carried neither would be read as yes.
	Reproducibility string `json:"reproducibility"`
	Reason          string `json:"reason,omitempty"`
	// CreatedAt is when the release was recorded.
	CreatedAt time.Time `json:"created_at"`
}

// Verification is what was measured about a generation.
type Verification struct {
	// Version is the record schema version.
	Version int `json:"version"`
	// Generation is what was measured.
	Generation string `json:"generation"`
	// Passed reports whether every deterministic layer passed.
	Passed bool `json:"passed"`
	// SourceRows and TargetRows are the two counts, kept because a reader
	// checking a report against a database later needs the shape it was taken
	// on.
	SourceRows int `json:"source_rows"`
	TargetRows int `json:"target_rows"`
	// Findings are what it said, blocking first.
	Findings []Finding `json:"findings,omitempty"`
	// Unmeasured names the checks that did not run.
	//
	// A record that listed only findings would read the same whether a layer
	// passed or was never asked, and those are different facts.
	Unmeasured []string `json:"unmeasured,omitempty"`
	// Retrieval is what an evaluation found, when one ran.
	Retrieval *Retrieval `json:"retrieval,omitempty"`
	// MeasuredAt is when.
	MeasuredAt time.Time `json:"measured_at"`
}

// Finding is one thing a verification said.
type Finding struct {
	Layer    string `json:"layer"`
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
	Count    int    `json:"count,omitempty"`
}

// Retrieval is what an evaluation measured.
type Retrieval struct {
	// CorpusDigest is what it was measured against.
	CorpusDigest string `json:"corpus_digest"`
	// QueryParameters are the settings it was measured under.
	//
	// Without them the numbers below are not comparable to any others, which
	// is ADR 0010's measurement and the reason this field is not optional.
	QueryParameters string `json:"query_parameters"`
	// The numbers.
	RecallAtK      float64 `json:"recall_at_k"`
	MRR            float64 `json:"mrr"`
	NDCG           float64 `json:"ndcg"`
	ExactAgreement float64 `json:"exact_agreement"`
	Cases          int     `json:"cases"`
	ExactCases     int     `json:"exact_cases"`
	// Blockers are what it refused for, empty when nothing did.
	Blockers []string `json:"blockers,omitempty"`
}

// Cutover is what was done.
type Cutover struct {
	// Version is the record schema version.
	Version int `json:"version"`
	// Generation is what queries now read, and Replaced what they read before.
	Generation string `json:"generation"`
	Replaced   string `json:"replaced,omitempty"`
	// Target names the table whose pointer moved.
	Target string `json:"target"`
	// PlanDigest is the exact plan that was executed, and Approver who
	// authorized it.
	//
	// The digest rather than the plan: an approval binds to it, so a record
	// carrying a rendered plan and not its digest could not be checked against
	// the approval it claims.
	PlanDigest string `json:"plan_digest"`
	Approver   string `json:"approver,omitempty"`
	// ApprovalSigned reports whether Approver was established by a verified
	// signature over the plan bytes rather than typed beside them.
	//
	// Without it the record cannot tell the two apart, and they are different
	// claims: a name on a command line says who the operator wrote down, and a
	// verified signature says whose key covered these exact bytes. Two cutovers
	// of one target -- one signed by an auditor's key, one authorized by
	// anybody holding the database URL and that auditor's name -- were
	// identical in every field a reader could use (stokaro/ptah#2643).
	//
	// It is omitted when false so an unsigned record says nothing rather than
	// saying "not signed" about a policy that never asked for a signature.
	ApprovalSigned bool `json:"approval_signed,omitempty"`
	// VerificationDigest is the measurement the plan rested on: the value
	// [Verification.MeasurementDigest] answers for that report, which is the
	// same value the plan cited and the approval covered.
	//
	// A measurement rather than an artifact, deliberately. The digest here used
	// to be that of the report restamped at the cutover's own instant, so it
	// named a record no verb ever writes -- and on a cutover that re-verifies
	// after the source moved, the measurement it rested on was recorded nowhere
	// at all (stokaro/ptah#2643). A measurement digest is reproducible by
	// anybody holding the report.
	VerificationDigest string `json:"verification_digest"`
	// Watermark is how far the source had been accounted for when the pointer
	// moved, empty under a consistency mode that records no boundary.
	//
	// It is the answer to "what does this generation cover", which the
	// generation identity does not carry: the identity says how a vector was
	// computed and this says which source state was. A record without it can be
	// read six months later for what was replaced and not for what was in it.
	Watermark string `json:"watermark,omitempty"`
	// StabilizeUntil is how long the replaced generation stays a way back, zero
	// when nothing keeps it.
	StabilizeUntil time.Time `json:"stabilize_until,omitzero"`
	// CutOverAt is when.
	CutOverAt time.Time `json:"cut_over_at"`
}

// Rollback is what was undone.
//
// A separate record from a cutover rather than a cutover with a flag. They are
// answers to different questions -- "why did the corpus change" and "why did we
// go back" -- and a reader looking for the second in a list of the first finds
// a pointer move with no explanation attached to it.
type Rollback struct {
	// Version is the record schema version.
	Version int `json:"version"`
	// Generation is what queries read after, and Replaced what they read
	// before. Both are the reverse of a cutover's, which is the point.
	Generation string `json:"generation"`
	Replaced   string `json:"replaced,omitempty"`
	// Target names the table whose pointer moved.
	Target string `json:"target"`
	// Maintained reports whether the generation returned to was still being
	// kept current when the pointer moved, and VerifiedAt when its freshness
	// was last measured.
	//
	// This is what made going back possible: a previous generation stops
	// receiving changes the moment queries stop reading it, so a record without
	// it says a pointer moved and not whether it moved to something current.
	Maintained bool      `json:"maintained"`
	VerifiedAt time.Time `json:"verified_at,omitzero"`
	// StaleRows and MissingRows are what that measurement found.
	StaleRows   int `json:"stale_rows"`
	MissingRows int `json:"missing_rows"`
	// Expires is when the window over the generation left behind closes, zero
	// when the policy set none.
	Expires time.Time `json:"expires,omitzero"`
	// RolledBackAt is when.
	RolledBackAt time.Time `json:"rolled_back_at"`
}

// Retirement is what was destroyed.
type Retirement struct {
	// Version is the record schema version.
	Version int `json:"version"`
	// Generation is what was destroyed.
	Generation string `json:"generation"`
	// Target names the table it lived in.
	Target string `json:"target"`
	// Objects are what was removed, by name, and Rows how many vectors went
	// with them.
	//
	// Named rather than counted, because this is the one record whose subject
	// cannot be inspected afterwards: everything else here describes something
	// still in the database, and this describes an absence.
	Objects []string `json:"objects"`
	Rows    int64    `json:"rows"`
	// PlanDigest is the retirement plan that was executed, and Approver who
	// authorized it.
	PlanDigest string `json:"plan_digest"`
	Approver   string `json:"approver,omitempty"`
	// ApprovalSigned reports whether Approver was established by a verified
	// signature over the plan bytes rather than typed beside them.
	//
	// Without it the record cannot tell the two apart, and they are different
	// claims: a name on a command line says who the operator wrote down, and a
	// verified signature says whose key covered these exact bytes. Two cutovers
	// of one target -- one signed by an auditor's key, one authorized by
	// anybody holding the database URL and that auditor's name -- were
	// identical in every field a reader could use (stokaro/ptah#2643).
	//
	// It is omitted when false so an unsigned record says nothing rather than
	// saying "not signed" about a policy that never asked for a signature.
	ApprovalSigned bool `json:"approval_signed,omitempty"`
	// RetiredAt is when.
	RetiredAt time.Time `json:"retired_at"`
}

// Digest is the rollback's content address.
func (r Rollback) Digest() string {
	return embeddigest.Of(
		"rollback", strconv.Itoa(RecordVersion),
		"generation", r.Generation, "replaced", r.Replaced, "target", r.Target,
		"maintained", strconv.FormatBool(r.Maintained),
		"verified_at", formatTime(r.VerifiedAt),
		"stale_rows", strconv.Itoa(r.StaleRows),
		"missing_rows", strconv.Itoa(r.MissingRows),
		"expires", formatTime(r.Expires),
		"rolled_back_at", r.RolledBackAt.UTC().Format(time.RFC3339Nano))
}

// Digest is the retirement's content address.
//
// The objects are length-prefixed by embeddigest and preceded by their count,
// because the list is variable-length and followed by more components: without
// the count, one object named "a" and "b" would address the same record as one
// named "a b".
func (r Retirement) Digest() string {
	components := []string{
		"retirement", strconv.Itoa(RecordVersion),
		"generation", r.Generation, "target", r.Target,
		"objects", strconv.Itoa(len(r.Objects)),
	}
	components = append(components, sortedCopy(r.Objects)...)
	return embeddigest.Of(append(components,
		"rows", strconv.FormatInt(r.Rows, 10),
		"plan", r.PlanDigest, "approver", r.Approver,
		"approval_signed", strconv.FormatBool(r.ApprovalSigned),
		"retired_at", r.RetiredAt.UTC().Format(time.RFC3339Nano))...)
}

// Digest is the release's content address.
func (r Release) Digest() string {
	return embeddigest.Of(
		"release", strconv.Itoa(RecordVersion),
		"generation", r.Generation, "replaces", r.Replaces,
		"spec", r.SpecDigest, "corpus", r.CorpusDigest, "target", r.Target,
		"reproducibility", r.Reproducibility, "reason", r.Reason,
		"created_at", r.CreatedAt.UTC().Format(time.RFC3339Nano))
}

// Digest is the verification's content address.
//
// It covers what was found and what was not asked, and not the counts alone: a
// report saying "three source rows, three target rows" is the same two numbers
// whether every layer passed or three of them were never run.
//
// It addresses one ARTIFACT: two runs of the same checks over the same state
// digest differently, because the instant is part of it. That is right for
// naming a record in a registry and wrong for citing a measurement, which is
// what [Verification.MeasurementDigest] is for.
func (v Verification) Digest() string {
	return embeddigest.Of(v.components(
		[]string{"measured_at", v.MeasuredAt.UTC().Format(time.RFC3339Nano)})...)
}

// MeasurementDigest addresses what was measured, without when.
//
// A cutover plan cites it, and the approval an operator signs covers the
// citation, so the approval stops applying the moment the measurement changes
// -- a finding appearing, a count moving, a layer going unmeasured. That is the
// property [Evidence.VerificationDigest] promised and did not have: it held the
// GENERATION identity, so the plan showed the approver the same sixty-four
// characters twice under two labels, it did not move when the report changed,
// and `decideEvidence`'s refusal "the plan cites no verification report" could
// never fire because a generation identity is never empty (stokaro/ptah#2643).
//
// It is deliberately not the artifact digest. A plan is built before the record
// that would carry the artifact exists -- and on a cutover that re-verifies,
// the record may never be written at all -- so citing an artifact means citing
// one that cannot be fetched. A measurement can be recomputed by anyone holding
// the report, which is what a citation in an evidence record is for.
func (v Verification) MeasurementDigest() string {
	return embeddigest.Of(v.components(nil)...)
}

// components is the one encoding both digests use.
//
// timeComponents is what separates them, and it is a parameter rather than a
// second function because the two must not be able to disagree about anything
// else. Passing nil yields the measurement; passing the instant yields the
// artifact.
func (v Verification) components(timeComponents []string) []string {
	components := []string{
		"verification", strconv.Itoa(RecordVersion),
		"generation", v.Generation, "passed", strconv.FormatBool(v.Passed),
		"source_rows", strconv.Itoa(v.SourceRows), "target_rows", strconv.Itoa(v.TargetRows),
	}
	components = append(components, timeComponents...)
	// The retrieval block comes before the two lists because it is the last
	// fixed-length part: an absent one renders two components and a present one
	// eleven, and both begin with a literal that says which, so nothing after
	// the header can be mistaken for it.
	components = append(components, v.Retrieval.components()...)

	// One count, on the first of the two adjacent variable-length lists. The
	// second needs none because nothing follows it, and a second count would
	// answer the same question -- which is how both come to be unmeasurable:
	// remove either and the other still separates the lists, so no fixture can
	// tell the encoding lost a guarantee. Measured, on this shape: with the
	// count removed, one finding of {"1", "2", "3", 4} and no unmeasured notes
	// digest identically to no findings and four notes reading "1", "2", "3",
	// "4".
	components = append(components, "findings", strconv.Itoa(len(v.Findings)))
	for _, finding := range v.Findings {
		components = append(components,
			finding.Layer, finding.Severity, finding.Summary, strconv.Itoa(finding.Count))
	}
	return append(components, sortedCopy(v.Unmeasured)...)
}

// components renders the retrieval half of a verification digest.
func (r *Retrieval) components() []string {
	if r == nil {
		// Absent and present-with-zeroes are different records: one is an
		// evaluation nobody ran, the other is one that found nothing.
		return []string{"retrieval", "none"}
	}
	return []string{
		"retrieval", "present",
		"corpus", r.CorpusDigest, "parameters", r.QueryParameters,
		"recall", formatNumber(r.RecallAtK), "mrr", formatNumber(r.MRR),
		"ndcg", formatNumber(r.NDCG), "exact_agreement", formatNumber(r.ExactAgreement),
		"cases", strconv.Itoa(r.Cases), "exact_cases", strconv.Itoa(r.ExactCases),
		"blockers", strconv.Itoa(len(r.Blockers)),
	}
}

// Digest is the cutover's content address.
func (c Cutover) Digest() string {
	return embeddigest.Of(
		"cutover", strconv.Itoa(RecordVersion),
		"generation", c.Generation, "replaced", c.Replaced, "target", c.Target,
		"plan", c.PlanDigest, "approver", c.Approver,
		// The signed flag is covered, or two records differing only in whether
		// the approver was a key or a keystroke would share one content
		// address -- which is the very thing the field was added to tell apart.
		"approval_signed", strconv.FormatBool(c.ApprovalSigned),
		"verification", c.VerificationDigest, "watermark", c.Watermark,
		"stabilize_until", formatTime(c.StabilizeUntil),
		"cut_over_at", c.CutOverAt.UTC().Format(time.RFC3339Nano))
}

// Encode renders a record as the bytes an artifact layer carries.
//
// Indented, because the thing a person reaches for six months later is `oci
// fetch` and a text editor, and a single line of JSON is evidence nobody reads.
func Encode(record any) ([]byte, error) {
	body, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode the record: %w", err)
	}
	return append(body, '\n'), nil
}

// sortedCopy orders a list without disturbing the caller's.
func sortedCopy(values []string) []string {
	ordered := append([]string(nil), values...)
	sort.Strings(ordered)
	return ordered
}

// formatNumber renders a measurement the same way every time.
func formatNumber(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

// formatTime renders an optional instant, including its absence.
func formatTime(value time.Time) string {
	if value.IsZero() {
		return "none"
	}
	return value.UTC().Format(time.RFC3339Nano)
}
