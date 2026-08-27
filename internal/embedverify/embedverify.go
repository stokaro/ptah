// Package embedverify decides whether a generation may be cut over to.
//
// # Why a successful backfill is not the answer
//
// A backfill that ran to the end of the source proves that a loop terminated.
// It does not prove that every row has a vector, that the vectors are current,
// that they are finite, or that the index over them exists -- and each of those
// failures produces a corpus that answers queries confidently and wrongly
// (stokaro/ptah#2068).
//
// So verification is layered, each layer answering a question the others
// cannot, and a cutover requires all of them or an explicit acceptance of the
// ones that failed.
//
// # Why counts are not enough
//
// A source count matching a target count is satisfied by a corpus that missed a
// thousand rows and invented a thousand others. Coverage is therefore answered
// key by key -- what is missing, what is unexpected, what is duplicated -- and
// the counts are reported beside it rather than instead of it.
package embedverify

import (
	"fmt"
	"sort"
	"strings"
)

// Severity is how much a finding matters.
type Severity string

const (
	// Blocking stops a cutover unless it is explicitly accepted.
	Blocking Severity = "blocking"
	// Advisory is worth reading and does not stop anything.
	Advisory Severity = "advisory"
)

// Layer names which question a finding came from.
type Layer string

const (
	// LayerStructural is the target objects: the column, its type, the index,
	// the operator class, the extension.
	LayerStructural Layer = "structural"
	// LayerCoverage is which source keys have a target row.
	LayerCoverage Layer = "coverage"
	// LayerFreshness is whether those rows match the source as it is now.
	LayerFreshness Layer = "freshness"
	// LayerVectorValidity is whether the stored vectors are usable.
	LayerVectorValidity Layer = "vector_validity"
	// LayerConsistency is whether the run itself finished what it started.
	LayerConsistency Layer = "consistency"
)

// Finding is one thing verification noticed.
type Finding struct {
	// Layer is which question found it.
	Layer Layer
	// Severity is whether it blocks a cutover.
	Severity Severity
	// Summary is one line an operator reads.
	Summary string
	// Keys are the source keys involved, bounded by [MaxReportedKeys] so a
	// report about a million missing rows stays readable. Count carries the
	// real number.
	Keys []string
	// Count is how many rows the finding covers.
	Count int
}

// MaxReportedKeys bounds the keys one finding lists.
//
// A finding about a million missing rows is not more useful for listing a
// million keys; it is unreadable, and the count is what an operator acts on.
const MaxReportedKeys = 20

// Report is everything verification found about one generation.
type Report struct {
	// Generation is the identity verified.
	Generation string
	// Findings are what it noticed, ordered by layer and then by summary so two
	// runs over one state produce the same report.
	Findings []Finding
	// SourceRows and TargetRows are the counts, reported beside key-level
	// coverage rather than instead of it.
	SourceRows int
	TargetRows int
}

// Blocking lists the findings that stop a cutover.
func (r Report) Blocking() []Finding {
	blocking := make([]Finding, 0, len(r.Findings))
	for _, finding := range r.Findings {
		if finding.Severity == Blocking {
			blocking = append(blocking, finding)
		}
	}
	return blocking
}

// Passed reports whether nothing blocks a cutover.
func (r Report) Passed() bool {
	return len(r.Blocking()) == 0
}

// addf records a finding, bounding the keys it lists.
func (r *Report) addf(layer Layer, severity Severity, count int, keys []string, format string, args ...any) {
	sort.Strings(keys)
	listed := keys
	if len(listed) > MaxReportedKeys {
		listed = listed[:MaxReportedKeys]
	}
	r.Findings = append(r.Findings, Finding{
		Layer:    layer,
		Severity: severity,
		Summary:  fmt.Sprintf(format, args...),
		Keys:     listed,
		Count:    count,
	})
}

// sortFindings orders a report so two runs over one state produce one document.
func (r *Report) sortFindings() {
	sort.SliceStable(r.Findings, func(i, j int) bool {
		if r.Findings[i].Layer != r.Findings[j].Layer {
			return r.Findings[i].Layer < r.Findings[j].Layer
		}
		return strings.Compare(r.Findings[i].Summary, r.Findings[j].Summary) < 0
	})
}
