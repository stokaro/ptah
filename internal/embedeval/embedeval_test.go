package embedeval_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedeval"
)

// corpus is two cases with graded relevance and a hard requirement each.
func corpus() []embedeval.Case {
	return []embedeval.Case{
		{
			ID:       "pricing",
			Query:    "how much does it cost",
			Required: []string{"doc-price"},
			Relevant: map[string]float64{"doc-price": 3, "doc-plans": 1},
		},
		{
			ID:       "support",
			Query:    "how do I contact support",
			Required: []string{"doc-support"},
			Relevant: map[string]float64{"doc-support": 3, "doc-faq": 1},
		},
	}
}

// perfect is what a generation answering both cases ideally returns.
func perfect() []embedeval.Result {
	return []embedeval.Result{
		{
			CaseID:    "pricing",
			Keys:      []string{"doc-price", "doc-plans", "doc-other"},
			ExactKeys: []string{"doc-price", "doc-plans", "doc-other"},
			ExactRun:  true,
		},
		{
			CaseID:    "support",
			Keys:      []string{"doc-support", "doc-faq", "doc-other"},
			ExactKeys: []string{"doc-support", "doc-faq", "doc-other"},
			ExactRun:  true,
		},
	}
}

// strict is a policy that asks for everything.
func strict() embedeval.Policy {
	return embedeval.Policy{
		DefaultK:          3,
		MinRecallAtK:      0.9,
		MinExactAgreement: 0.9,
		RequireEveryCase:  true,
	}
}

// TestEvaluate_APerfectGenerationPasses is the control.
//
// Without it an evaluator that blocked everything satisfies every negative row
// below and makes the feature unusable.
func TestEvaluate_APerfectGenerationPasses(t *testing.T) {
	c := qt.New(t)

	report := embedeval.Evaluate("gen-new", strict(), corpus(), perfect(), embedeval.Scores{})

	c.Assert(report.Blockers, qt.HasLen, 0, qt.Commentf("%v", report.Blockers))
	c.Assert(report.Passed(), qt.IsTrue)
	c.Assert(report.Scores.RecallAtK, qt.Equals, 1.0)
	c.Assert(report.Scores.MRR, qt.Equals, 1.0)
	c.Assert(report.Scores.NDCG, qt.Equals, 1.0)
	c.Assert(report.Scores.ExactAgreement, qt.Equals, 1.0)
	c.Assert(report.Scores.Cases, qt.Equals, 2)
}

// TestEvaluate_ARequiredDocumentMissingFromTopKBlocks is the epic's first
// retrieval condition, and the one a score cannot express.
//
// The generation is otherwise excellent -- it returns the second relevant
// document first, so recall and NDCG stay high -- and the document the corpus
// says must be there is not.
func TestEvaluate_ARequiredDocumentMissingFromTopKBlocks(t *testing.T) {
	c := qt.New(t)
	results := perfect()
	results[0].Keys = []string{"doc-plans", "doc-other", "doc-more"}

	report := embedeval.Evaluate("gen-new", strict(), corpus(), results, embedeval.Scores{})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(report.Blockers, qt.Contains, "pricing did not return doc-price")
}

// TestEvaluate_KBoundsWhatCounts pins the depth.
//
// The required document is returned, in fourth place, and the case asks for
// three. A search nobody scrolls past three results in did not find it.
func TestEvaluate_KBoundsWhatCounts(t *testing.T) {
	c := qt.New(t)
	results := perfect()
	results[0].Keys = []string{"a", "b", "c", "doc-price"}

	report := embedeval.Evaluate("gen-new", strict(), corpus(), results, embedeval.Scores{})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(report.Blockers, qt.Contains, "pricing did not return doc-price")
}

// TestEvaluate_ACasesOwnKBeatsTheDefault keeps a per-case depth from being
// ignored. One query wanting ten results and another wanting three is the
// normal shape of a corpus.
func TestEvaluate_ACasesOwnKBeatsTheDefault(t *testing.T) {
	c := qt.New(t)
	cases := corpus()
	cases[0].K = 4
	results := perfect()
	results[0].Keys = []string{"a", "b", "c", "doc-price"}

	report := embedeval.Evaluate("gen-new", strict(), cases, results, embedeval.Scores{})

	c.Assert(report.Blockers, qt.Not(qt.Contains), "pricing did not return doc-price")
}

// TestEvaluate_RecallBelowPolicyBlocks is the second condition.
func TestEvaluate_RecallBelowPolicyBlocks(t *testing.T) {
	c := qt.New(t)
	cases := corpus()
	cases[0].Required = nil
	results := perfect()
	results[0].Keys = []string{"doc-price", "a", "b"}

	report := embedeval.Evaluate("gen-new", strict(), cases, results, embedeval.Scores{})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(report.Blockers, qt.Contains, "recall is 0.750 and this policy requires 0.900")
}

// TestEvaluate_RegressionAgainstTheGenerationBeingReplacedBlocks is the third.
//
// Both numbers are respectable on their own. They are worse than what the
// corpus already gets, which is the only comparison that matters when the point
// of the migration was to improve retrieval.
func TestEvaluate_RegressionAgainstTheGenerationBeingReplacedBlocks(t *testing.T) {
	tests := []struct {
		name     string
		policy   func(*embedeval.Policy)
		baseline embedeval.Scores
		want     string
	}{
		{
			name:     "MRR",
			policy:   func(p *embedeval.Policy) { p.MaxMRRRegression = 0.05 },
			baseline: embedeval.Scores{Cases: 2, MRR: 1.0, NDCG: 0.5},
			want:     "MRR fell by 0.250 (1.000 to 0.750) and this policy allows 0.050",
		},
		{
			name:     "NDCG",
			policy:   func(p *embedeval.Policy) { p.MaxNDCGRegression = 0.05 },
			baseline: embedeval.Scores{Cases: 2, MRR: 0.5, NDCG: 1.0},
			want:     "NDCG fell by 0.170 (1.000 to 0.830) and this policy allows 0.050",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy := strict()
			policy.MinRecallAtK = 0
			test.policy(&policy)
			results := perfect()
			results[0].Keys = []string{"doc-other", "doc-price", "doc-plans"}
			cases := corpus()
			cases[0].Required = nil

			report := embedeval.Evaluate("gen-new", policy, cases, results, test.baseline)

			c.Assert(report.Passed(), qt.IsFalse)
			c.Assert(report.Blockers, qt.Contains, test.want)
		})
	}
}

// TestEvaluate_AFirstEvaluationHasNothingToRegressFrom is the control for the
// rows above.
//
// Comparing against a baseline nobody measured reports every first evaluation
// as a perfect improvement, which is the most flattering way to say nothing.
func TestEvaluate_AFirstEvaluationHasNothingToRegressFrom(t *testing.T) {
	c := qt.New(t)
	policy := strict()
	policy.MaxMRRRegression = 0
	policy.MaxNDCGRegression = 0

	report := embedeval.Evaluate("gen-new", policy, corpus(), perfect(), embedeval.Scores{})

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", report.Blockers))
}

// TestEvaluate_AnApproximateIndexDivergingFromExactSearchBlocks is the fourth
// condition, and the one that separates a bad corpus from a bad index.
//
// The vectors are fine -- an exhaustive search over them finds the right
// documents. The index does not, which is a recall setting, not a model.
func TestEvaluate_AnApproximateIndexDivergingFromExactSearchBlocks(t *testing.T) {
	c := qt.New(t)
	cases := corpus()
	cases[0].Required = nil
	cases[1].Required = nil
	policy := strict()
	policy.MinRecallAtK = 0
	results := perfect()
	results[0].Keys = []string{"x", "y", "z"}

	report := embedeval.Evaluate("gen-new", policy, cases, results, embedeval.Scores{})

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(report.Blockers, qt.Contains,
		"the index agrees with an exhaustive search on 0.500 of results and this policy requires 0.900")
}

// TestEvaluate_NoExactSearchIsNotPerfectAgreement keeps a check nobody ran from
// reading as one that passed.
//
// This is the epic's "no silent partial analysis" rule at its sharpest: an
// exact search that was never run and one that agreed completely are the same
// zero unless the difference is carried, and one of them is evidence.
func TestEvaluate_NoExactSearchIsNotPerfectAgreement(t *testing.T) {
	c := qt.New(t)
	results := perfect()
	results[0].ExactRun = false
	results[0].ExactKeys = nil
	results[1].ExactRun = false
	results[1].ExactKeys = nil

	report := embedeval.Evaluate("gen-new", strict(), corpus(), results, embedeval.Scores{})

	c.Assert(report.Scores.ExactCases, qt.Equals, 0)
	c.Assert(report.Scores.ExactAgreement, qt.Equals, 0.0)
	c.Assert(report.Blockers, qt.Not(qt.Contains),
		"the index agrees with an exhaustive search on 0.000 of results and this policy requires 0.900")
}

// TestEvaluate_AnIncompleteEvaluationBlocks is the fifth condition, and the one
// a report of averages hides best.
//
// A case that failed is not in the mean. Half a corpus scoring perfectly reads
// as a perfect corpus, and the half that never ran is where the regression was.
func TestEvaluate_AnIncompleteEvaluationBlocks(t *testing.T) {
	tests := []struct {
		name    string
		results []embedeval.Result
		want    string
	}{
		{
			name:    "a case that was never run",
			results: perfect()[:1],
			want:    "1 evaluation cases produced no result: support was never run",
		},
		{
			name: "a case that failed",
			results: append(perfect()[:1], embedeval.Result{
				CaseID: "support", Err: "the provider refused the query",
			}),
			want: "1 evaluation cases produced no result: support failed: the provider refused the query",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := embedeval.Evaluate("gen-new", strict(), corpus(), test.results, embedeval.Scores{})

			c.Assert(report.Passed(), qt.IsFalse)
			c.Assert(report.Blockers, qt.Contains, test.want)
			c.Assert(report.Scores.Cases, qt.Equals, 1)
		})
	}
}

// TestEvaluate_AFailedCaseIsNotScoredAsZero is why the incomplete list exists
// separately from the scores.
//
// A zero is a measurement: it says the generation found nothing. "We could not
// ask" is a different fact with a different fix, and folding one into the other
// makes a provider outage look like a bad model.
func TestEvaluate_AFailedCaseIsNotScoredAsZero(t *testing.T) {
	c := qt.New(t)
	policy := strict()
	policy.RequireEveryCase = false
	results := append(perfect()[:1], embedeval.Result{CaseID: "support", Err: "timed out"})

	report := embedeval.Evaluate("gen-new", policy, corpus(), results, embedeval.Scores{})

	c.Assert(report.Scores.MRR, qt.Equals, 1.0)
	c.Assert(report.Scores.Cases, qt.Equals, 1)
	c.Assert(report.Incomplete, qt.DeepEquals, []string{"support failed: timed out"})
	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", report.Blockers))
}

// TestEvaluate_ACaseWithNoRelevanceExpectationsScoresNothing keeps an empty
// case from carrying a failing evaluation.
//
// Scoring it as a perfect one is how a corpus padded with queries nobody wrote
// expectations for lifts the mean over the policy floor.
func TestEvaluate_ACaseWithNoRelevanceExpectationsScoresNothing(t *testing.T) {
	c := qt.New(t)
	cases := append(corpus(), embedeval.Case{ID: "empty", Query: "anything"})
	results := append(perfect(), embedeval.Result{
		CaseID: "empty", Keys: []string{"a"}, ExactKeys: []string{"a"}, ExactRun: true,
	})
	results[0].Keys = []string{"doc-plans", "doc-other", "doc-more"}
	results[0].ExactKeys = results[0].Keys

	report := embedeval.Evaluate("gen-new", strict(), cases, results, embedeval.Scores{})

	c.Assert(report.Scores.RecallAtK, qt.Equals, 0.75)
	c.Assert(report.Scores.Cases, qt.Equals, 3)
}

// TestEvaluate_RankingIsMeasuredAndNotJustMembership is what separates NDCG
// from recall.
//
// Both generations return exactly the same three documents. One puts the most
// relevant first and the other puts it second, and a measure that only asked
// which documents came back would call them identical.
func TestEvaluate_RankingIsMeasuredAndNotJustMembership(t *testing.T) {
	c := qt.New(t)
	policy := strict()
	policy.MinRecallAtK = 0
	cases := corpus()
	cases[0].Required = nil
	best := perfect()
	worse := perfect()
	worse[0].Keys = []string{"doc-other", "doc-price", "doc-plans"}

	first := embedeval.Evaluate("gen-new", policy, cases, best, embedeval.Scores{})
	second := embedeval.Evaluate("gen-new", policy, cases, worse, embedeval.Scores{})

	c.Assert(first.Scores.RecallAtK, qt.Equals, second.Scores.RecallAtK)
	c.Assert(second.Scores.NDCG < first.Scores.NDCG, qt.IsTrue)
	c.Assert(second.Scores.MRR < first.Scores.MRR, qt.IsTrue)
}
