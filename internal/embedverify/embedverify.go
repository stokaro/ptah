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

	"go.5x5.cz/ptah/internal/embeddigest"
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

// KeyIdentity is the one string every layer compares a row by.
//
// Through [embeddigest.Encode], the length-prefixed encoding the rest of the
// lifecycle already addresses content with. A joiner was used here -- U+001F,
// the ASCII unit separator, chosen for rarity -- and rarity is not the same
// property as safety: a TEXT column may hold that byte, and with `key_fields:
// [tenant, id]` the rows (`a<US>b`, `c`) and (`a`, `b<US>c`) folded onto one
// identity. Coverage then compared one source row against the other's target
// row, and the layer that exists to answer key by key answered about the wrong
// key (stokaro/ptah#2744).
//
// Length prefixes remove the boundary question rather than making it rarer.
// Nothing a component can contain moves where it ends, so there is no value
// left to choose a separator against -- which is why this is the encoder
// [embeddigest.Of] hashes and why [go.5x5.cz/ptah/internal/embedcatchup.KeyIdentity]
// already reached for it.
//
// It is a comparison key, never a display one. Printed raw, a terminal swallows
// a control byte -- `(acme, 2)` and `(globex, 1)` came out as `acme2` and
// `globex1`, so the only line telling an operator which rows to remove was
// neither copy-pasteable nor unambiguous (stokaro/ptah#2649 finding 2). It is
// no longer raw-printable at all: `6:acme1:2` is the identity, and anything
// showing a key to a person calls [RenderKey].
func KeyIdentity(components ...string) string {
	return embeddigest.Encode(components...)
}

// RenderKey is how a key is shown to a person.
//
// A single-component key is its own value, because parentheses around one
// column say nothing. A composite one is rendered as the tuple it is, in the
// specification's key order, which is the shape it would be written in a
// predicate.
//
// The components come back out of the identity rather than being carried
// beside it: a second copy of a key is a second thing that can disagree with
// the first. An identity this package did not produce is returned unchanged --
// there is nothing truer to say about it, and rendering a guess would be worse
// than showing what is there.
func RenderKey(key string) string {
	components, ok := embeddigest.Decode(key)
	if !ok || len(components) == 0 {
		return key
	}
	if len(components) == 1 {
		return components[0]
	}
	return "(" + strings.Join(components, ", ") + ")"
}

// Report is everything verification found about one generation.
type Report struct {
	// Generation is the identity verified.
	Generation string
	// Findings are what it noticed, ordered by layer and then by summary so two
	// runs over one state produce the same report.
	Findings []Finding
	// SourceRows and TargetRows are the counts, reported beside key-level
	// coverage rather than instead of it.
	//
	// TargetRows is every row the walk stood opposite on the target side, which
	// is not the number of vectors: a tombstone, a skip and a row nothing ever
	// wrote each occupy a position and hold none. That is the count the
	// verification record stores as the shape it was taken on, and it is what
	// the three below break down.
	SourceRows int
	TargetRows int
	// TargetVectors is how many of TargetRows actually hold a vector.
	//
	// It exists because the header printed TargetRows and a reader read it as
	// this: `2 source rows, 3 target rows` beside a column holding two vectors,
	// after catch-up tombstoned one row through Ptah's own verbs
	// (stokaro/ptah#2742).
	TargetVectors int
	// Tombstones and SkippedTargets are the DELIBERATE absences among them --
	// a row whose source is gone, and one the specification asked not to embed.
	//
	// Reported separately because they are the reason a healthy generation's
	// two counts differ, and a reader given only the difference has to guess
	// which it was.
	//
	// They do not partition the difference, in either direction. A tombstone
	// that still holds a vector is counted in both TargetVectors and Tombstones
	// and is a finding (stokaro/ptah#2734); a row nothing ever wrote is counted
	// in neither, because it is not a deliberate absence -- the coverage layer
	// reports it, which is where it belongs.
	Tombstones     int
	SkippedTargets int
	// Unmeasured names the checks that did not run at all.
	//
	// A check that could not be made is not a check that passed, and the
	// difference is invisible in a list of findings: a layer that found nothing
	// and a layer nobody asked read exactly the same.
	Unmeasured []string
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
