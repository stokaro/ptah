package inference

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/embedcorpus"
	"go.5x5.cz/ptah/internal/embedeval"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedprovider"
)

// evaluateOptions are what the evaluate verb takes.
type evaluateOptions struct {
	commonOptions
	corpusPath string
	policy     embedeval.Policy
	baseline   string
	timeout    time.Duration
}

// newEvaluateCommand returns "ptah inference evaluate".
func newEvaluateCommand() *cobra.Command {
	options := evaluateOptions{}

	cmd := &cobra.Command{
		Use:   "evaluate",
		Short: "Measure what the generation actually retrieves, against a corpus you wrote",
		Long: `Run an evaluation corpus against a generation and report what it found.

The deterministic layers answer whether a generation is well-formed, complete
and current. None of them can tell whether it WORKS: a corpus of perfectly fresh
vectors from a worse model passes every one, and the first person to notice is a
user whose search stopped working.

Each case is asked twice -- once the way an application would, and once with the
index switched off. That is what separates a bad corpus from a bad index: if an
exhaustive scan finds the right documents and the index does not, the vectors are
fine and the recall setting is not.

Nothing here gates by default. A threshold Ptah picked would be met or missed by
a session setting Ptah does not own, so the floors are yours to set -- and the
report records the query parameters every number was measured under, because two
taken under different ones are not two numbers about the same thing.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runEvaluate(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addCommonFlags(cmd, &options.commonOptions)
	cmd.Flags().StringVar(&options.corpusPath, "corpus", "",
		"Path to the evaluation corpus (required)")
	cmd.Flags().IntVar(&options.policy.DefaultK, "k", 10,
		"How deep to look when neither the case nor the corpus says")
	cmd.Flags().Float64Var(&options.policy.MinRecallAtK, "min-recall", 0,
		"Refuse below this recall; zero gates nothing and reports the number")
	cmd.Flags().Float64Var(&options.policy.MinExactAgreement, "min-exact-agreement", 0,
		"Refuse when the index agrees with an exhaustive search less than this")
	cmd.Flags().Float64Var(&options.policy.MaxMRRRegression, "max-mrr-regression", 0,
		"Refuse when MRR falls further than this below the baseline")
	cmd.Flags().Float64Var(&options.policy.MaxNDCGRegression, "max-ndcg-regression", 0,
		"Refuse when NDCG falls further than this below the baseline")
	cmd.Flags().BoolVar(&options.policy.RequireEveryCase, "require-every-case", true,
		"Refuse when any case produced no result")
	cmd.Flags().StringVar(&options.baseline, "baseline", "",
		"Identity of the generation to compare against; omitted means no comparison")
	cmd.Flags().DurationVar(&options.timeout, "provider-timeout", 60*time.Second,
		"How long one provider request may take")
	return cmd
}

// runEvaluate loads the corpus, asks the generation, and reports.
func runEvaluate(ctx context.Context, out io.Writer, options evaluateOptions) error {
	if options.corpusPath == "" {
		return fmt.Errorf("--corpus is required")
	}
	corpus, err := embedcorpus.Load(options.corpusPath)
	if err != nil {
		return err
	}
	opened, err := open(ctx, options.commonOptions)
	if err != nil {
		return err
	}
	defer opened.close()

	report, err := evaluateCorpus(ctx, opened, corpus, options)
	if err != nil {
		return err
	}
	if err := printEvaluation(out, corpus, report); err != nil {
		return err
	}
	if !report.Passed() {
		return fmt.Errorf("retrieval evaluation found %d blocking findings", len(report.Blockers))
	}
	return nil
}

// evaluateCorpus asks the generation every case and scores the answers.
func evaluateCorpus(
	ctx context.Context, opened *session, corpus embedcorpus.Corpus, options evaluateOptions,
) (embedeval.Report, error) {
	provider, err := buildProvider(opened, options.timeout, 1)
	if err != nil {
		return embedeval.Report{}, err
	}
	searcher, err := embedpg.NewSearcher(opened.db, opened.loaded.Spec)
	if err != nil {
		return embedeval.Report{}, err
	}
	parameters, err := searcher.QueryParameters(ctx)
	if err != nil {
		return embedeval.Report{}, err
	}

	policy := options.policy
	if corpus.DefaultK > 0 {
		// The corpus's own default wins over the flag, because the depth is
		// part of what the corpus measures: a run at a different depth is a
		// different measurement, and the file is where that is written down.
		policy.DefaultK = corpus.DefaultK
	}
	results := make([]embedeval.Result, 0, len(corpus.Cases))
	for _, testCase := range corpus.Cases {
		results = append(results, askOneCase(ctx, provider, searcher, testCase, policy.DefaultK))
	}

	return embedeval.Evaluate(
		embedeval.Subject{
			Generation: opened.loaded.Spec.Identity().Digest, QueryParameters: parameters,
		},
		policy, corpus.Cases, results, embedeval.Scores{}), nil
}

// askOneCase embeds one query and searches with it.
//
// A case that fails is returned as a failure rather than as a zero score. A zero
// says the generation found nothing; "we could not ask" is a different fact with
// a different fix, and folding one into the other makes a provider outage look
// like a bad model.
func askOneCase(
	ctx context.Context, provider embedprovider.Provider, searcher *embedpg.Searcher,
	testCase embedeval.Case, defaultK int,
) embedeval.Result {
	answered, err := provider.Embed(ctx, []string{testCase.Query})
	if err != nil {
		return embedeval.Result{CaseID: testCase.ID, Err: err.Error()}
	}
	if len(answered.Vectors) != 1 {
		return embedeval.Result{
			CaseID: testCase.ID,
			Err:    fmt.Sprintf("the provider answered %d vectors for one query", len(answered.Vectors)),
		}
	}
	depth := testCase.K
	if depth <= 0 {
		depth = defaultK
	}
	result, err := searcher.Search(ctx, testCase, answered.Vectors[0], depth)
	if err != nil {
		return embedeval.Result{CaseID: testCase.ID, Err: err.Error()}
	}
	return result
}

// printEvaluation renders the report.
func printEvaluation(out io.Writer, corpus embedcorpus.Corpus, report embedeval.Report) error {
	lines := []string{
		fmt.Sprintf("generation %s against corpus %s", report.Generation, corpus.Digest[:12]),
		bullet(fmt.Sprintf("recall %.3f, MRR %.3f, NDCG %.3f over %d cases",
			report.Scores.RecallAtK, report.Scores.MRR, report.Scores.NDCG, report.Scores.Cases)),
		bullet(fmt.Sprintf("the index agrees with an exhaustive search on %.3f of results, over %d cases",
			report.Scores.ExactAgreement, report.Scores.ExactCases)),
		bullet("measured under " + report.Scores.QueryParameters),
	}
	for _, incomplete := range report.Incomplete {
		lines = append(lines, bullet("incomplete: "+incomplete))
	}
	for _, unmeasured := range report.Unmeasured {
		lines = append(lines, bullet("not measured: "+unmeasured))
	}
	for _, blocker := range report.Blockers {
		lines = append(lines, bullet("blocking: "+blocker))
	}
	return writeLines(out, lines...)
}
