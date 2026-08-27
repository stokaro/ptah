package embedcorpus_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcorpus"
)

// complete is a corpus with everything an operator would write.
const complete = `
version: 1
name: article retrieval
description: what the search page has to find
default_k: 10
cases:
  - id: pricing
    query: how much does it cost
    k: 5
    required: [doc-price]
    relevant:
      doc-price: 3
      doc-plans: 1
  - id: support
    query: how do I contact support
    required: [doc-support]
    relevant:
      doc-support: 3
`

// TestParse_ReadsACompleteCorpus is the control.
func TestParse_ReadsACompleteCorpus(t *testing.T) {
	c := qt.New(t)

	corpus, err := embedcorpus.Parse([]byte(complete), "corpus.yaml")

	c.Assert(err, qt.IsNil)
	c.Assert(corpus.Name, qt.Equals, "article retrieval")
	c.Assert(corpus.DefaultK, qt.Equals, 10)
	c.Assert(corpus.Cases, qt.HasLen, 2)
	c.Assert(corpus.Cases[0].ID, qt.Equals, "pricing")
	c.Assert(corpus.Cases[0].K, qt.Equals, 5)
	c.Assert(corpus.Cases[0].Required, qt.DeepEquals, []string{"doc-price"})
	c.Assert(corpus.Cases[0].Relevant, qt.DeepEquals, map[string]float64{"doc-price": 3, "doc-plans": 1})
	c.Assert(corpus.Digest, qt.HasLen, 64)
}

// TestParse_RefusesACorpusThatMeasuresNothing walks the shapes that pass every
// threshold by asking nothing.
//
// A gate that passes because it had no question is the failure this whole
// feature exists to avoid, and each of these produces one.
func TestParse_RefusesACorpusThatMeasuresNothing(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "no cases at all",
			document: "version: 1\n",
			want:     `corpus.yaml declares no cases, so it would measure nothing`,
		},
		{
			name:     "a case that expects nothing",
			document: "version: 1\ncases:\n  - id: empty\n    query: anything\n",
			want:     `corpus.yaml: case "empty" expects nothing, so any answer satisfies it`,
		},
		{
			name:     "a case with no query",
			document: "version: 1\ncases:\n  - id: quiet\n    required: [a]\n",
			want:     `corpus.yaml: case "quiet" has no query`,
		},
		{
			name:     "a case with no name",
			document: "version: 1\ncases:\n  - query: anything\n    required: [a]\n",
			want:     `corpus.yaml: case 1 has no id`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := embedcorpus.Parse([]byte(test.document), "corpus.yaml")

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestParse_RefusesACaseNamedTwice keeps a report an operator can act on.
//
// A finding names the case, and two cases with one name give the reader two
// places to look.
func TestParse_RefusesACaseNamedTwice(t *testing.T) {
	c := qt.New(t)
	document := "version: 1\ncases:\n" +
		"  - id: same\n    query: one\n    required: [a]\n" +
		"  - id: same\n    query: two\n    required: [b]\n"

	_, err := embedcorpus.Parse([]byte(document), "corpus.yaml")

	c.Assert(err, qt.ErrorMatches, `corpus.yaml declares case "same" twice`)
}

// TestParse_RefusesAGradeOfZero is the corpus disagreeing with itself.
//
// Zero is what an unlisted key already means. A listed zero reads as "this one
// matters" and scores as "this one does not".
func TestParse_RefusesAGradeOfZero(t *testing.T) {
	c := qt.New(t)
	document := "version: 1\ncases:\n  - id: graded\n    query: q\n    relevant:\n      doc: 0\n"

	_, err := embedcorpus.Parse([]byte(document), "corpus.yaml")

	c.Assert(err, qt.ErrorMatches,
		`corpus.yaml: case "graded" grades "doc" as 0; a key that does not matter is left out `+
			`rather than graded zero`)
}

// TestParse_RefusesAFieldItDoesNotKnow is why unknown fields are an error.
//
// `expected` for `required` produces a corpus that measures something else and
// reports it as a pass.
func TestParse_RefusesAFieldItDoesNotKnow(t *testing.T) {
	c := qt.New(t)
	document := "version: 1\ncases:\n  - id: a\n    query: q\n    expected: [doc]\n"

	_, err := embedcorpus.Parse([]byte(document), "corpus.yaml")

	c.Assert(err, qt.ErrorMatches, `(?s)parse corpus.yaml: .*field expected not found.*`)
}

// TestParse_TheDigestFollowsWhatIsMeasured is what makes two numbers
// comparable.
//
// A case edited between two runs makes them incomparable exactly as a changed
// model would, and a filename says nothing about that.
func TestParse_TheDigestFollowsWhatIsMeasured(t *testing.T) {
	tests := []struct {
		name string
		edit func(string) string
		same bool
	}{
		{
			name: "a different query",
			edit: func(s string) string {
				return strings.Replace(s, "how much does it cost", "what is the price", 1)
			},
			same: false,
		},
		{
			name: "a different depth",
			edit: func(s string) string { return strings.Replace(s, "k: 5", "k: 20", 1) },
			same: false,
		},
		{
			name: "a different grade",
			edit: func(s string) string { return strings.Replace(s, "doc-plans: 1", "doc-plans: 2", 1) },
			same: false,
		},
		{
			name: "a different requirement",
			edit: func(s string) string {
				return strings.Replace(s, "required: [doc-price]", "required: [doc-other]", 1)
			},
			same: false,
		},
		{
			name: "a different name",
			edit: func(s string) string {
				return strings.Replace(s, "name: article retrieval", "name: whatever", 1)
			},
			same: true,
		},
		{
			name: "a different description",
			edit: func(s string) string {
				return strings.Replace(s, "what the search page has to find", "prose", 1)
			},
			same: true,
		},
	}
	base := mustParse(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			edited, err := embedcorpus.Parse([]byte(test.edit(complete)), "corpus.yaml")

			c.Assert(err, qt.IsNil)
			c.Assert(edited.Digest == base.Digest, qt.Equals, test.same)
		})
	}
}

// mustParse reads the control corpus, which every row above is an edit of.
//
// It lives here rather than in the test body because a test asserts and does not
// branch, which is the rule scripts/check-test-style.sh enforces.
func mustParse(t *testing.T) embedcorpus.Corpus {
	t.Helper()
	corpus, err := embedcorpus.Parse([]byte(complete), "corpus.yaml")
	if err != nil {
		t.Fatalf("the control corpus does not parse: %v", err)
	}
	return corpus
}

// TestParse_TheDigestDoesNotDependOnHowAMapWasTyped keeps two files that differ
// only in typing from being two corpora.
//
// A YAML map has no order, so the digest sorts what it reads.
func TestParse_TheDigestDoesNotDependOnHowAMapWasTyped(t *testing.T) {
	c := qt.New(t)
	reordered := strings.Replace(complete,
		"      doc-price: 3\n      doc-plans: 1\n",
		"      doc-plans: 1\n      doc-price: 3\n", 1)

	first, err := embedcorpus.Parse([]byte(complete), "corpus.yaml")
	c.Assert(err, qt.IsNil)
	second, err := embedcorpus.Parse([]byte(reordered), "corpus.yaml")

	c.Assert(err, qt.IsNil)
	c.Assert(second.Digest, qt.Equals, first.Digest)
}

// TestParse_RefusesAFormatVersionItDoesNotRead keeps a file written for a later
// build from being read as one written for this one.
func TestParse_RefusesAFormatVersionItDoesNotRead(t *testing.T) {
	c := qt.New(t)

	_, err := embedcorpus.Parse([]byte("version: 99\n"), "corpus.yaml")

	c.Assert(err, qt.ErrorMatches, `corpus.yaml declares format version 99 and this build reads 1`)
}
