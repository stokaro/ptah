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

	"go.5x5.cz/ptah/internal/embeddigest"
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
	// VerificationDigest is the report the plan rested on.
	VerificationDigest string `json:"verification_digest"`
	// StabilizeUntil is how long the replaced generation stays a way back, zero
	// when nothing keeps it.
	StabilizeUntil time.Time `json:"stabilize_until,omitzero"`
	// CutOverAt is when.
	CutOverAt time.Time `json:"cut_over_at"`
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
func (v Verification) Digest() string {
	components := []string{
		"verification", strconv.Itoa(RecordVersion),
		"generation", v.Generation, "passed", strconv.FormatBool(v.Passed),
		"source_rows", strconv.Itoa(v.SourceRows), "target_rows", strconv.Itoa(v.TargetRows),
		"measured_at", v.MeasuredAt.UTC().Format(time.RFC3339Nano),
	}
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
	return embeddigest.Of(append(components, sortedCopy(v.Unmeasured)...)...)
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
		"verification", c.VerificationDigest,
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
