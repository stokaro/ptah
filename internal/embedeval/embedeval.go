// Package embedeval measures whether a generation still finds what the corpus
// says it should.
//
// The five deterministic layers answer whether a generation is well-formed,
// complete and current. None of them can tell whether it is any GOOD: a corpus
// of perfectly fresh vectors from a worse model passes every one of them, and
// the first person to notice is a user whose search stopped working
// (stokaro/ptah#2068).
package embedeval

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
)

// Case is one query and what it is supposed to find.
//
// The relevance expectations are the user's, not Ptah's. Ptah measures against
// them and reports; it does not decide what a good answer to a query is.
type Case struct {
	// ID names the case, for a report a person reads.
	ID string
	// Query is the text that gets embedded and searched with.
	Query string
	// Required are keys that must appear in the top K. A case with any of them
	// is a hard expectation rather than a score.
	Required []string
	// Relevant maps a key to its graded relevance, for the ranked measures.
	// A key absent from the map is irrelevant, which is not the same as
	// forbidden.
	Relevant map[string]float64
	// K is how deep the search goes for this case, zero to use the run's
	// default.
	K int
}

// Result is what a generation actually returned for one case.
type Result struct {
	// CaseID identifies the case.
	CaseID string
	// Keys are the returned keys, best first.
	Keys []string
	// ExactKeys are what an exhaustive search returned for the same query,
	// best first. Empty when no exact search was run, which is not the same as
	// an exact search that returned nothing.
	ExactKeys []string
	// ExactRun records whether the exact search was run at all.
	ExactRun bool
	// Err is why this case produced nothing, empty when it produced something.
	Err string
}

// Scores are one generation's measured retrieval quality.
type Scores struct {
	// RecallAtK is the share of relevant keys the search found, averaged over
	// the cases that had any.
	RecallAtK float64
	// MRR is the mean reciprocal rank of the first relevant key.
	MRR float64
	// NDCG is the mean normalized discounted cumulative gain.
	NDCG float64
	// ExactAgreement is the mean overlap between the index's answers and an
	// exhaustive search's, over the cases where both ran.
	ExactAgreement float64
	// Cases is how many cases were scored.
	Cases int
	// ExactCases is how many contributed to ExactAgreement.
	ExactCases int
}

// Policy is what the environment requires of those numbers.
type Policy struct {
	// DefaultK is how deep to look when a case does not say.
	DefaultK int
	// MinRecallAtK is the floor, zero for none.
	MinRecallAtK float64
	// MaxMRRRegression and MaxNDCGRegression are how far below the previous
	// generation the new one may fall, zero for no tolerance at all.
	MaxMRRRegression  float64
	MaxNDCGRegression float64
	// MinExactAgreement is how closely the index has to agree with an
	// exhaustive search, zero for none.
	MinExactAgreement float64
	// RequireEveryCase refuses a run where any case failed or was not
	// executed. Its default is the strict one: an evaluation missing a third
	// of its cases is not a passing evaluation, it is a third of one.
	RequireEveryCase bool
}

// Report is what an evaluation says about a generation.
type Report struct {
	// Generation is what was measured.
	Generation string
	// Scores are the numbers.
	Scores Scores
	// Baseline is the previous generation's numbers, for the regression
	// tolerances. Zero when there is nothing to compare against.
	Baseline Scores
	// Blockers are the reasons this generation may not be cut over to.
	Blockers []string
	// Incomplete names the cases that produced nothing, which is a blocker in
	// its own right and is reported separately from a low score.
	Incomplete []string
}

// Passed reports whether retrieval quality permits a cutover.
func (r Report) Passed() bool {
	return len(r.Blockers) == 0
}

// Evaluate scores the results and holds them to the policy.
//
// It never invents a number it could not measure. A case that errored is not
// scored as zero -- a zero is a measurement saying the generation found
// nothing, and "we could not ask" is a different fact with a different fix.
func Evaluate(generation string, policy Policy, cases []Case, results []Result, baseline Scores) Report {
	report := Report{Generation: generation, Baseline: baseline}
	byCase := make(map[string]Result, len(results))
	for _, result := range results {
		byCase[result.CaseID] = result
	}

	var recalls, reciprocals, gains, agreements []float64
	var missingRequired []string
	for _, testCase := range cases {
		result, ran := byCase[testCase.ID]
		if !ran {
			report.Incomplete = append(report.Incomplete, fmt.Sprintf("%s was never run", testCase.ID))
			continue
		}
		if result.Err != "" {
			report.Incomplete = append(report.Incomplete,
				fmt.Sprintf("%s failed: %s", testCase.ID, result.Err))
			continue
		}
		depth := testCase.K
		if depth <= 0 {
			depth = policy.DefaultK
		}
		top := truncate(result.Keys, depth)
		missingRequired = append(missingRequired, missingFrom(testCase, top)...)
		scoreCase(testCase, top, &recalls, &reciprocals, &gains)
		if result.ExactRun {
			agreements = append(agreements, overlap(top, truncate(result.ExactKeys, depth)))
		}
	}

	report.Scores = Scores{
		RecallAtK:      mean(recalls),
		MRR:            mean(reciprocals),
		NDCG:           mean(gains),
		ExactAgreement: mean(agreements),
		Cases:          len(cases) - len(report.Incomplete),
		ExactCases:     len(agreements),
	}
	judge(&report, policy, missingRequired)
	sort.Strings(report.Blockers)
	return report
}

// missingFrom lists the required keys a case did not return.
func missingFrom(testCase Case, top []string) []string {
	var missing []string
	for _, required := range testCase.Required {
		if !contains(top, required) {
			missing = append(missing, fmt.Sprintf("%s did not return %s", testCase.ID, required))
		}
	}
	return missing
}

// scoreCase accumulates the ranked measures for one case.
//
// A case with no relevance expectations contributes to none of them. Scoring it
// as a perfect one would let a corpus of empty cases carry a failing
// evaluation over the line.
func scoreCase(testCase Case, top []string, recalls, reciprocals, gains *[]float64) {
	if len(testCase.Relevant) == 0 {
		return
	}
	*recalls = append(*recalls, recall(testCase, top))
	*reciprocals = append(*reciprocals, reciprocalRank(testCase, top))
	*gains = append(*gains, ndcg(testCase, top))
}

// recall is the share of the case's relevant keys that appear in the top K.
func recall(testCase Case, top []string) float64 {
	found := 0
	for key := range testCase.Relevant {
		if contains(top, key) {
			found++
		}
	}
	return float64(found) / float64(len(testCase.Relevant))
}

// reciprocalRank is one over the position of the first relevant key.
func reciprocalRank(testCase Case, top []string) float64 {
	for position, key := range top {
		if testCase.Relevant[key] > 0 {
			return 1 / float64(position+1)
		}
	}
	return 0
}

// ndcg is the discounted cumulative gain over the ideal one.
func ndcg(testCase Case, top []string) float64 {
	ideal := idealGain(testCase, len(top))
	if ideal == 0 {
		return 0
	}
	gain := 0.0
	for position, key := range top {
		gain += testCase.Relevant[key] / math.Log2(float64(position)+2)
	}
	return gain / ideal
}

// idealGain is the gain a perfect ranking of this case would produce.
func idealGain(testCase Case, depth int) float64 {
	grades := make([]float64, 0, len(testCase.Relevant))
	for _, grade := range testCase.Relevant {
		grades = append(grades, grade)
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(grades)))
	ideal := 0.0
	for position, grade := range truncateGrades(grades, depth) {
		ideal += grade / math.Log2(float64(position)+2)
	}
	return ideal
}

// overlap is the share of the index's answers an exhaustive search agrees with.
func overlap(approximate, exact []string) float64 {
	if len(exact) == 0 {
		return 0
	}
	shared := 0
	for _, key := range approximate {
		if contains(exact, key) {
			shared++
		}
	}
	return float64(shared) / float64(len(exact))
}

// judge holds the numbers to the policy.
func judge(report *Report, policy Policy, missingRequired []string) {
	report.Blockers = append(report.Blockers, missingRequired...)
	if policy.RequireEveryCase && len(report.Incomplete) > 0 {
		// The epic lists an incomplete evaluation among the reasons a
		// migration stays uncutoverable, and it is the one a report of
		// averages hides best: the cases that failed are simply not in the
		// mean.
		report.Blockers = append(report.Blockers,
			fmt.Sprintf("%d evaluation cases produced no result: %s",
				len(report.Incomplete), strings.Join(report.Incomplete, "; ")))
	}
	if policy.MinRecallAtK > 0 && report.Scores.RecallAtK < policy.MinRecallAtK {
		report.Blockers = append(report.Blockers, fmt.Sprintf(
			"recall is %.3f and this policy requires %.3f", report.Scores.RecallAtK, policy.MinRecallAtK))
	}
	if policy.MinExactAgreement > 0 && report.Scores.ExactCases > 0 &&
		report.Scores.ExactAgreement < policy.MinExactAgreement {
		report.Blockers = append(report.Blockers, fmt.Sprintf(
			"the index agrees with an exhaustive search on %.3f of results and this policy requires %.3f",
			report.Scores.ExactAgreement, policy.MinExactAgreement))
	}
	judgeRegression(report, policy)
}

// judgeRegression compares against the generation being replaced.
//
// Regression is only meaningful against a baseline that was measured. Comparing
// against a zero baseline would report every first evaluation as a perfect
// improvement, which is the most flattering way to say nothing.
func judgeRegression(report *Report, policy Policy) {
	if report.Baseline.Cases == 0 {
		return
	}
	if drop := report.Baseline.MRR - report.Scores.MRR; drop > policy.MaxMRRRegression {
		report.Blockers = append(report.Blockers, fmt.Sprintf(
			"MRR fell by %.3f (%.3f to %.3f) and this policy allows %.3f",
			drop, report.Baseline.MRR, report.Scores.MRR, policy.MaxMRRRegression))
	}
	if drop := report.Baseline.NDCG - report.Scores.NDCG; drop > policy.MaxNDCGRegression {
		report.Blockers = append(report.Blockers, fmt.Sprintf(
			"NDCG fell by %.3f (%.3f to %.3f) and this policy allows %.3f",
			drop, report.Baseline.NDCG, report.Scores.NDCG, policy.MaxNDCGRegression))
	}
}

// mean is the average, and zero over nothing.
func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

// truncate returns at most depth keys.
func truncate(keys []string, depth int) []string {
	if depth <= 0 || len(keys) <= depth {
		return keys
	}
	return keys[:depth]
}

// truncateGrades returns at most depth grades.
func truncateGrades(grades []float64, depth int) []float64 {
	if depth <= 0 || len(grades) <= depth {
		return grades
	}
	return grades[:depth]
}

// contains reports whether a key is present.
func contains(keys []string, key string) bool {
	return slices.Contains(keys, key)
}
