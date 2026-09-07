package inference_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestEvaluate_RefusesHalfAComparison is stokaro/ptah#2640.
//
// `--baseline` was bound to a field nothing read, so the two regression gates
// could never fire: `evaluateCorpus` handed `Evaluate` a literal empty baseline,
// the report short-circuited on `Baseline.Cases == 0`, and
// `--max-ndcg-regression 0` — the strictest allowance there is — refused
// nothing. A generation identity that named nothing was accepted at exit 0 too,
// so the operator was not told the comparison had not happened.
//
// The strategy pages publish this as the model-regression gate. A team
// following them believed a worse model would be refused, and it would pass.
func TestEvaluate_RefusesHalfAComparison(t *testing.T) {
	tests := []struct {
		name    string
		extra   []string
		wantErr string
	}{
		{
			name:    "a generation with nothing to measure it",
			extra:   []string{"--baseline", strings.Repeat("a", 64)},
			wantErr: `--baseline names a generation and --baseline-spec is how it gets measured.*`,
		},
		{
			name:    "a specification nobody said which generation it is",
			extra:   []string{"--baseline-spec", "previous.yaml"},
			wantErr: `--baseline-spec names a specification and --baseline does not say which.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := runEvaluate(c, test.extra...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestEvaluate_RefusesABaselineSpecificationThatIsNotTheGenerationNamed is the
// validation the flag never had.
//
// Without it the pair could name one generation and measure another, which is
// the silent comparison this replaces wearing a second face.
func TestEvaluate_RefusesABaselineSpecificationThatIsNotTheGenerationNamed(t *testing.T) {
	c := qt.New(t)
	previous := writeEvaluateSpec(c, "embedding")

	_, err := runEvaluate(c,
		"--baseline", strings.Repeat("b", 64), "--baseline-spec", previous)

	c.Assert(err, qt.ErrorMatches, `(?s).*--baseline names generation bbbbbbbbbbbb and .* produces .*`)
}

// TestEvaluate_AcceptsNeitherFlag is the control.
//
// Omitting the comparison is the ordinary case and must stay one: a refusal
// that fired without either flag would make the verb unusable, and the report
// already says a comparison it did not make was not measured.
func TestEvaluate_AcceptsNeitherFlag(t *testing.T) {
	c := qt.New(t)

	_, err := runEvaluate(c)

	// It gets past the flag checks and fails on the database instead, which is
	// the fixture having no server rather than the baseline being refused.
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "--baseline")
}

// runEvaluate drives the verb with a corpus and a specification that parse.
func runEvaluate(c *qt.C, extra ...string) (string, error) {
	c.Helper()
	args := []string{
		"evaluate",
		"--spec", writeEvaluateSpec(c, "embedding_v2"),
		"--db-url", "postgres://u:p@127.0.0.1:9/x?sslmode=disable",
		"--corpus", writeEvaluateCorpus(c),
	}
	return runVerb(c, "evaluate", append(args[1:], extra...))
}

// writeEvaluateSpec writes a specification differing only in its column, so two
// of them are two generations.
func writeEvaluateSpec(c *qt.C, column string) string {
	c.Helper()
	document := strings.Replace(
		describeSpecDocument("test-embed", column, "1"),
		"column: "+column, "column: "+column, 1)
	path := filepath.Join(c.TB.(*testing.T).TempDir(), column+".yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// writeEvaluateCorpus writes a corpus with one case.
func writeEvaluateCorpus(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "corpus.yaml")
	c.Assert(os.WriteFile(path, []byte(
		"version: 1\nname: baseline\ndefault_k: 1\n"+
			"cases:\n  - id: one\n    query: anything\n    required: [a]\n"), 0o600), qt.IsNil)
	return path
}
