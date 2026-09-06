package embedrelease_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedrelease"
)

// at is a fixed instant, so a record says what a test means.
var at = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

// aVerification is a report with something in it.
func aVerification() embedrelease.Verification {
	return embedrelease.Verification{
		Generation: "gen-1", Passed: false,
		SourceRows: 3, TargetRows: 3,
		Findings: []embedrelease.Finding{
			{Layer: "freshness", Severity: "blocking", Summary: "1 target rows are stale", Count: 1},
		},
		Unmeasured: []string{"the stored vectors were not read back"},
		MeasuredAt: at,
	}
}

// TestVerification_TheDigestSeparatesWhatWasFoundFromWhatWasNotAsked is
// Decision 13, in the record.
//
// A report that found nothing and a report where a layer never ran carry the
// same counts and the same empty findings list. If the digest could not tell
// them apart, two records saying different things would be one record.
func TestVerification_TheDigestSeparatesWhatWasFoundFromWhatWasNotAsked(t *testing.T) {
	c := qt.New(t)
	found := embedrelease.Verification{Generation: "gen-1", Passed: true, MeasuredAt: at}
	notAsked := found
	notAsked.Unmeasured = []string{"the stored vectors were not read back"}

	c.Assert(notAsked.Digest(), qt.Not(qt.Equals), found.Digest())
}

// TestVerification_AnAbsentRetrievalIsNotAZeroOne keeps an evaluation nobody ran
// from reading as one that scored nothing.
func TestVerification_AnAbsentRetrievalIsNotAZeroOne(t *testing.T) {
	c := qt.New(t)
	absent := embedrelease.Verification{Generation: "gen-1", MeasuredAt: at}
	scoredZero := absent
	scoredZero.Retrieval = &embedrelease.Retrieval{}

	c.Assert(scoredZero.Digest(), qt.Not(qt.Equals), absent.Digest())
}

// TestVerification_TheDigestCoversEveryFieldSeparately is the ratchet on a
// record somebody may one day sign.
//
// A digest that missed a field would let two records with different contents
// share one address, which is the whole thing an address is for.
func TestVerification_TheDigestCoversEveryFieldSeparately(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedrelease.Verification)
	}{
		{name: "generation", change: func(v *embedrelease.Verification) { v.Generation = "gen-2" }},
		{name: "verdict", change: func(v *embedrelease.Verification) { v.Passed = true }},
		{name: "source rows", change: func(v *embedrelease.Verification) { v.SourceRows = 4 }},
		{name: "target rows", change: func(v *embedrelease.Verification) { v.TargetRows = 4 }},
		{
			name:   "a finding's layer",
			change: func(v *embedrelease.Verification) { v.Findings[0].Layer = "coverage" },
		},
		{
			name:   "a finding's severity",
			change: func(v *embedrelease.Verification) { v.Findings[0].Severity = "advisory" },
		},
		{
			name:   "a finding's summary",
			change: func(v *embedrelease.Verification) { v.Findings[0].Summary = "something else" },
		},
		{
			name:   "a finding's count",
			change: func(v *embedrelease.Verification) { v.Findings[0].Count = 9 },
		},
		{
			name: "how many findings",
			change: func(v *embedrelease.Verification) {
				v.Findings = append(v.Findings, embedrelease.Finding{Layer: "coverage"})
			},
		},
		{
			name:   "what was not measured",
			change: func(v *embedrelease.Verification) { v.Unmeasured = []string{"something else"} },
		},
		{
			name:   "when it was measured",
			change: func(v *embedrelease.Verification) { v.MeasuredAt = at.Add(time.Nanosecond) },
		},
	}
	base := aVerification()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := copyVerification(base)
			test.change(&changed)

			c.Assert(changed.Digest(), qt.Not(qt.Equals), base.Digest())
		})
	}
}

// TestVerification_TheRetrievalNumbersAreAllInTheDigest is the same ratchet for
// the half a separate verb produces.
func TestVerification_TheRetrievalNumbersAreAllInTheDigest(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedrelease.Retrieval)
	}{
		{name: "the corpus", change: func(r *embedrelease.Retrieval) { r.CorpusDigest = "other" }},
		{
			name:   "the query parameters",
			change: func(r *embedrelease.Retrieval) { r.QueryParameters = "ivfflat.probes=100" },
		},
		{name: "recall", change: func(r *embedrelease.Retrieval) { r.RecallAtK = 0.5 }},
		{name: "MRR", change: func(r *embedrelease.Retrieval) { r.MRR = 0.5 }},
		{name: "NDCG", change: func(r *embedrelease.Retrieval) { r.NDCG = 0.5 }},
		{name: "exact agreement", change: func(r *embedrelease.Retrieval) { r.ExactAgreement = 0.5 }},
		{name: "how many cases", change: func(r *embedrelease.Retrieval) { r.Cases = 7 }},
		{name: "how many exact cases", change: func(r *embedrelease.Retrieval) { r.ExactCases = 7 }},
		{
			name:   "how many blockers",
			change: func(r *embedrelease.Retrieval) { r.Blockers = []string{"recall is low"} },
		},
	}
	base := aVerification()
	base.Retrieval = &embedrelease.Retrieval{
		CorpusDigest: "corpus-1", QueryParameters: "ivfflat.probes=1",
		RecallAtK: 1, MRR: 1, NDCG: 1, ExactAgreement: 1, Cases: 3, ExactCases: 3,
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := copyVerification(base)
			retrieval := *base.Retrieval
			changed.Retrieval = &retrieval
			test.change(changed.Retrieval)

			c.Assert(changed.Digest(), qt.Not(qt.Equals), base.Digest())
		})
	}
}

// TestVerification_TheDigestDoesNotDependOnUnmeasuredOrder keeps two records of
// one measurement from being two records.
func TestVerification_TheDigestDoesNotDependOnUnmeasuredOrder(t *testing.T) {
	c := qt.New(t)
	first := aVerification()
	first.Unmeasured = []string{"a", "b"}
	second := aVerification()
	second.Unmeasured = []string{"b", "a"}

	c.Assert(second.Digest(), qt.Equals, first.Digest())
}

// TestVerification_SortingDoesNotDisturbTheCaller keeps the record a caller
// holds intact.
//
// The same value is often encoded as well as digested, and reordering it here
// would make the published JSON disagree with what the caller passed.
func TestVerification_SortingDoesNotDisturbTheCaller(t *testing.T) {
	c := qt.New(t)
	verification := aVerification()
	verification.Unmeasured = []string{"b", "a"}

	verification.Digest()

	c.Assert(verification.Unmeasured, qt.DeepEquals, []string{"b", "a"})
}

// TestCutover_TheDigestCoversWhatWasDone is the ratchet for the record a person
// reads when they ask why a corpus changed.
func TestCutover_TheDigestCoversWhatWasDone(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedrelease.Cutover)
	}{
		{name: "generation", change: func(c *embedrelease.Cutover) { c.Generation = "other" }},
		{name: "what it replaced", change: func(c *embedrelease.Cutover) { c.Replaced = "other" }},
		{name: "target", change: func(c *embedrelease.Cutover) { c.Target = "other" }},
		{name: "the plan", change: func(c *embedrelease.Cutover) { c.PlanDigest = "other" }},
		{name: "the approver", change: func(c *embedrelease.Cutover) { c.Approver = "somebody else" }},
		{
			// Two cutovers naming one approver, one authorized by that
			// person's key and one by anybody who could type their name, are
			// two different things done (stokaro/ptah#2643).
			name:   "whether the approval was signed",
			change: func(c *embedrelease.Cutover) { c.ApprovalSigned = true },
		},
		{
			name:   "the verification it rested on",
			change: func(c *embedrelease.Cutover) { c.VerificationDigest = "other" },
		},
		{
			// What the generation covers, which its identity does not carry:
			// the identity says how a vector was computed and this says which
			// source state was. Two cutovers of one generation at different
			// watermarks are two different things done.
			name:   "the watermark it was current to",
			change: func(c *embedrelease.Cutover) { c.Watermark = "4288" },
		},
		{
			name:   "the window it opened",
			change: func(c *embedrelease.Cutover) { c.StabilizeUntil = at.Add(time.Hour) },
		},
		{name: "when", change: func(c *embedrelease.Cutover) { c.CutOverAt = at.Add(time.Nanosecond) }},
	}
	base := embedrelease.Cutover{
		Generation: "gen-2", Replaced: "gen-1", Target: "public.articles",
		PlanDigest: "plan-1", Approver: "an operator",
		VerificationDigest: "report-1", Watermark: "4210", CutOverAt: at,
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := base
			test.change(&changed)

			c.Assert(changed.Digest(), qt.Not(qt.Equals), base.Digest())
		})
	}
}

// TestRelease_TheDigestCoversWhatIsProposed is the same for the first record.
func TestRelease_TheDigestCoversWhatIsProposed(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedrelease.Release)
	}{
		{name: "generation", change: func(r *embedrelease.Release) { r.Generation = "other" }},
		{name: "what it replaces", change: func(r *embedrelease.Release) { r.Replaces = "other" }},
		{name: "the specification", change: func(r *embedrelease.Release) { r.SpecDigest = "other" }},
		{name: "the corpus", change: func(r *embedrelease.Release) { r.CorpusDigest = "other" }},
		{name: "the target", change: func(r *embedrelease.Release) { r.Target = "other" }},
		{
			name:   "whether it can be rebuilt",
			change: func(r *embedrelease.Release) { r.Reproducibility = "partial" },
		},
		{name: "why it cannot", change: func(r *embedrelease.Release) { r.Reason = "no revision" }},
		{name: "when", change: func(r *embedrelease.Release) { r.CreatedAt = at.Add(time.Nanosecond) }},
	}
	base := embedrelease.Release{
		Generation: "gen-2", Replaces: "gen-1", SpecDigest: "spec-1", CorpusDigest: "corpus-1",
		Target: "public.articles.embedding", Reproducibility: "full", CreatedAt: at,
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := base
			test.change(&changed)

			c.Assert(changed.Digest(), qt.Not(qt.Equals), base.Digest())
		})
	}
}

// TestNewVerificationRecord_AnnotatesTheVerdict is what a registry lists without
// pulling a layer.
//
// "Did this pass" is the question somebody scanning a list of reports is asking,
// and an annotation answers it from the manifest.
func TestNewVerificationRecord_AnnotatesTheVerdict(t *testing.T) {
	tests := []struct {
		name   string
		passed bool
		want   string
	}{
		{name: "it passed", passed: true, want: "true"},
		{name: "it did not", passed: false, want: "false"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			verification := aVerification()
			verification.Passed = test.passed

			record, err := embedrelease.NewVerificationRecord(verification)

			c.Assert(err, qt.IsNil)
			c.Assert(record.Annotations["run.ptah.inference.passed"], qt.Equals, test.want)
			c.Assert(record.Annotations["run.ptah.inference.record"], qt.Equals, verification.Digest())
			c.Assert(record.ArtifactType, qt.Equals, embedrelease.VerificationArtifactType)
		})
	}
}

// TestNewVerificationRecord_StampsTheSchemaVersion keeps a record readable by
// whatever reads it next.
//
// The version is in the digest too, so a future change to what a record holds
// cannot make a new one collide with a signature over an old one.
func TestNewVerificationRecord_StampsTheSchemaVersion(t *testing.T) {
	c := qt.New(t)

	record, err := embedrelease.NewVerificationRecord(aVerification())

	c.Assert(err, qt.IsNil)
	var decoded map[string]any
	c.Assert(json.Unmarshal(record.Body, &decoded), qt.IsNil)
	c.Assert(decoded["version"], qt.Equals, float64(embedrelease.RecordVersion))
}

// TestEncode_IsReadable is a small promise with a real reason.
//
// What somebody reaches for six months later is a fetch and a text editor, and
// one line of JSON is evidence nobody reads.
func TestEncode_IsReadable(t *testing.T) {
	c := qt.New(t)

	body, err := embedrelease.Encode(aVerification())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(string(body), "\n") > 5, qt.IsTrue)
	c.Assert(strings.HasSuffix(string(body), "\n"), qt.IsTrue)
}

// copyVerification copies a record deeply enough for one row to change it.
func copyVerification(base embedrelease.Verification) embedrelease.Verification {
	copied := base
	copied.Findings = append([]embedrelease.Finding(nil), base.Findings...)
	copied.Unmeasured = append([]string(nil), base.Unmeasured...)
	return copied
}

// TestVerification_TheBoundaryBetweenTheTwoListsCannotMove is the fixture the
// findings count exists for.
//
// A verification carries two adjacent lists of unbounded length: what was found
// and what was not asked. Without a count on the first, a reader of the digest
// components cannot tell where one ends -- and these two records, which say
// opposite things, would be the same record.
//
// The values are digits because the second list is sorted: a finding's four
// fields have to arrive already in the order sorting would put them in, or the
// collision this pins could not be constructed and the count would look
// load-bearing when it was the sort doing the work.
func TestVerification_TheBoundaryBetweenTheTwoListsCannotMove(t *testing.T) {
	c := qt.New(t)
	at := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)

	found := embedrelease.Verification{
		Generation: "gen-1", MeasuredAt: at,
		Findings: []embedrelease.Finding{
			{Layer: "1", Severity: "2", Summary: "3", Count: 4},
		},
	}
	notAsked := embedrelease.Verification{
		Generation: "gen-1", MeasuredAt: at,
		Unmeasured: []string{"1", "2", "3", "4"},
	}

	c.Assert(found.Digest(), qt.Not(qt.Equals), notAsked.Digest())
}

// TestRollback_TheDigestCoversWhatMadeItPossible walks the record that says why
// going back was allowed.
//
// A rollback record that carried only the pointer move would say a generation
// changed and not whether it changed to something current -- which is the whole
// question a rollback rests on, and the one an auditor asks first.
func TestRollback_TheDigestCoversWhatMadeItPossible(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedrelease.Rollback)
	}{
		{name: "generation", change: func(r *embedrelease.Rollback) { r.Generation = "other" }},
		{name: "what it replaced", change: func(r *embedrelease.Rollback) { r.Replaced = "other" }},
		{name: "target", change: func(r *embedrelease.Rollback) { r.Target = "other" }},
		{
			name:   "whether it was still maintained",
			change: func(r *embedrelease.Rollback) { r.Maintained = false },
		},
		{
			name:   "when its freshness was measured",
			change: func(r *embedrelease.Rollback) { r.VerifiedAt = at.Add(time.Hour) },
		},
		{name: "stale rows", change: func(r *embedrelease.Rollback) { r.StaleRows = 1 }},
		{name: "missing rows", change: func(r *embedrelease.Rollback) { r.MissingRows = 1 }},
		{
			name:   "when the window closes",
			change: func(r *embedrelease.Rollback) { r.Expires = at.Add(2 * time.Hour) },
		},
		{name: "when", change: func(r *embedrelease.Rollback) { r.RolledBackAt = at.Add(time.Nanosecond) }},
	}
	base := embedrelease.Rollback{
		Generation: "gen-1", Replaced: "gen-2", Target: "public.articles",
		Maintained: true, VerifiedAt: at, Expires: at.Add(time.Hour), RolledBackAt: at,
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := base
			test.change(&changed)

			c.Assert(changed.Digest(), qt.Not(qt.Equals), base.Digest())
		})
	}
}

// TestRetirement_TheDigestCoversWhatWasDestroyed walks the one record whose
// subject cannot be inspected afterwards.
func TestRetirement_TheDigestCoversWhatWasDestroyed(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedrelease.Retirement)
	}{
		{name: "generation", change: func(r *embedrelease.Retirement) { r.Generation = "other" }},
		{name: "target", change: func(r *embedrelease.Retirement) { r.Target = "other" }},
		{
			name: "one object more",
			change: func(r *embedrelease.Retirement) {
				r.Objects = append(r.Objects, "column public.articles.embedding")
			},
		},
		{
			name:   "a different object",
			change: func(r *embedrelease.Retirement) { r.Objects = []string{"something else"} },
		},
		{name: "how many rows", change: func(r *embedrelease.Retirement) { r.Rows = 4 }},
		{name: "the plan", change: func(r *embedrelease.Retirement) { r.PlanDigest = "other" }},
		{name: "the approver", change: func(r *embedrelease.Retirement) { r.Approver = "somebody else" }},
		{
			// The same reason as on a cutover, over a record whose subject
			// cannot be inspected afterwards at all (stokaro/ptah#2643).
			name:   "whether the approval was signed",
			change: func(r *embedrelease.Retirement) { r.ApprovalSigned = true },
		},
		{name: "when", change: func(r *embedrelease.Retirement) { r.RetiredAt = at.Add(time.Nanosecond) }},
	}
	base := embedrelease.Retirement{
		Generation: "gen-1", Target: "public.articles.embedding",
		Objects: []string{"index over public.articles.embedding"},
		Rows:    3, PlanDigest: "plan-1", Approver: "an operator", RetiredAt: at,
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := base
			test.change(&changed)

			c.Assert(changed.Digest(), qt.Not(qt.Equals), base.Digest())
		})
	}
}

// TestRetirement_TwoObjectsAreNotOneWithASpaceInIt is why the object list is
// counted before it is hashed.
//
// The list is variable-length and more components follow it, so without the
// count a retirement that removed "a" and "b" would address the same record as
// one that removed a single object called "a b".
func TestRetirement_TwoObjectsAreNotOneWithASpaceInIt(t *testing.T) {
	c := qt.New(t)
	two := embedrelease.Retirement{Generation: "gen-1", Objects: []string{"a", "b"}, RetiredAt: at}
	one := embedrelease.Retirement{Generation: "gen-1", Objects: []string{"a b"}, RetiredAt: at}

	c.Assert(two.Digest(), qt.Not(qt.Equals), one.Digest())
}

// TestNewRetirementRecord_AnnotatesWhatWentWithIt is what a registry lists
// without pulling the layer.
//
// "How much was destroyed" is the question somebody scanning a list of these is
// asking, and it is the one that cannot be answered by going to look.
func TestNewRetirementRecord_AnnotatesWhatWentWithIt(t *testing.T) {
	c := qt.New(t)

	record, err := embedrelease.NewRetirementRecord(embedrelease.Retirement{
		Generation: "gen-1", Objects: []string{"column public.articles.embedding"},
		Rows: 481_204, RetiredAt: at,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(record.ArtifactType, qt.Equals, embedrelease.RetirementArtifactType)
	c.Assert(record.FileName, qt.Equals, embedrelease.RetirementFileName)
	c.Assert(record.Annotations["run.ptah.inference.rows"], qt.Equals, "481204")
	c.Assert(record.Annotations["run.ptah.inference.generation"], qt.Equals, "gen-1")
}
