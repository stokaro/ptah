package inference

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/embedcorpus"
	"go.5x5.cz/ptah/internal/embeddigest"
	"go.5x5.cz/ptah/internal/embedeval"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedprovider"
	"go.5x5.cz/ptah/internal/embedspec"
)

// evaluateOptions are what the evaluate verb takes.
type evaluateOptions struct {
	commonOptions
	corpusPath string
	policy     embedeval.Policy
	baseline   string
	// baselineSpec is the previous generation's own specification file.
	//
	// A generation identity is not enough to measure one. Scoring a generation
	// means embedding each query with THAT generation's model and searching its
	// column with its metric, and the registry records none of those -- so a
	// baseline named by identity alone was accepted and silently not measured,
	// which made both regression gates unreachable (stokaro/ptah#2640).
	baselineSpec string
	timeout      time.Duration
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
		"Identity of the generation to compare against; needs --baseline-spec")
	cmd.Flags().StringVar(&options.baselineSpec, "baseline-spec", "",
		"Path to that generation's own specification, which is what measures it")
	cmd.Flags().DurationVar(&options.timeout, "provider-timeout", 60*time.Second,
		"How long one provider request may take")
	return cmd
}

// runEvaluate loads the corpus, asks the generation, and reports.
func runEvaluate(ctx context.Context, out io.Writer, options evaluateOptions) error {
	if options.corpusPath == "" {
		return fmt.Errorf("--corpus is required")
	}
	// Before the corpus is read and before anything is dialed. A baseline
	// nobody can answer is a configuration error, and answering it after a
	// connection attempt means the operator meets a network error instead of
	// the sentence that says what they asked for cannot be done.
	baseline, err := options.resolveBaseline()
	if err != nil {
		return err
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

	report, err := evaluateCorpus(ctx, opened, corpus, options, baseline)
	if err != nil {
		return err
	}
	if err := printEvaluation(out, corpus, report); err != nil {
		return err
	}
	if !report.Passed() {
		// Exit 1 rather than 2: a tolerance exceeded or a required case
		// unanswered is the verb ANSWERING, and the exit-code reference says
		// so. Two means the verb could not run -- a usage error, an unreadable
		// corpus, a provider failure -- and a pipeline that gates on this
		// cannot tell a regression from a broken invocation when both are 2
		// (stokaro/ptah#2639).
		return exitcode.New(1, fmt.Errorf(
			"retrieval evaluation found %d blocking findings", len(report.Blockers)))
	}
	return nil
}

// evaluateCorpus asks the generation every case and scores the answers.
func evaluateCorpus(
	ctx context.Context, opened *session, corpus embedcorpus.Corpus,
	options evaluateOptions, previous *embedspec.Loaded,
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

	baseline, err := baselineScores(ctx, opened, corpus, previous, options.timeout, policy)
	if err != nil {
		return embedeval.Report{}, err
	}

	return embedeval.Evaluate(
		embedeval.Subject{
			Generation: opened.loaded.Spec.Identity().Digest, QueryParameters: parameters,
		},
		policy, corpus.Cases, results, baseline), nil
}

// baselineScores measures the generation being replaced over the same cases.
//
// A nil previous means no comparison, which is what an omitted --baseline asks
// for and what the report then says it did not measure.
//
// It runs the corpus a second time rather than reading a number from anywhere.
// A stored score would have been taken under other query parameters, and ADR
// 0010 measured a 26.5%-100% recall span on one unchanged index from
// `ivfflat.probes` alone: two numbers taken under different settings are not
// two numbers about the same thing, which is why the report records the
// parameters beside every score.
func baselineScores(
	ctx context.Context, opened *session, corpus embedcorpus.Corpus,
	previous *embedspec.Loaded, timeout time.Duration, policy embedeval.Policy,
) (embedeval.Scores, error) {
	if previous == nil {
		return embedeval.Scores{}, nil
	}
	provider, err := buildProviderFor(*previous, previous.Spec.Model.Identifier, timeout)
	if err != nil {
		return embedeval.Scores{}, err
	}
	searcher, err := embedpg.NewSearcher(opened.db, previous.Spec)
	if err != nil {
		return embedeval.Scores{}, err
	}
	parameters, err := searcher.QueryParameters(ctx)
	if err != nil {
		return embedeval.Scores{}, err
	}

	results := make([]embedeval.Result, 0, len(corpus.Cases))
	for _, testCase := range corpus.Cases {
		results = append(results, askOneCase(ctx, provider, searcher, testCase, policy.DefaultK))
	}
	// Scored through Evaluate rather than through a second scorer, so the two
	// sides of a comparison cannot be computed differently.
	scored := embedeval.Evaluate(
		embedeval.Subject{
			Generation: previous.Spec.Identity().Digest, QueryParameters: parameters,
		},
		policy, corpus.Cases, results, embedeval.Scores{})
	return scored.Scores, nil
}

// resolveBaseline reads the previous generation's specification, or answers nil
// when no comparison was asked for.
//
// Every refusal here is a configuration error, so it is reached before a corpus
// is read and before a connection is attempted.
func (o evaluateOptions) resolveBaseline() (*embedspec.Loaded, error) {
	switch {
	case o.baseline == "" && o.baselineSpec == "":
		return nil, nil
	case o.baseline == "":
		return nil, fmt.Errorf(
			"--baseline-spec names a specification and --baseline does not say which " +
				"generation it is expected to be")
	case o.baselineSpec == "":
		return nil, fmt.Errorf(
			"--baseline names a generation and --baseline-spec is how it gets measured: " +
				"scoring a generation embeds every query with ITS model and searches ITS " +
				"column, and an identity alone carries neither")
	}

	loaded, err := embedspec.Load(o.baselineSpec)
	if err != nil {
		return nil, err
	}
	// The file has to BE the generation named. Without this the flag pair could
	// name one generation and measure another, which is the silent comparison
	// this replaces wearing a second face.
	if identity := loaded.Spec.Identity().Digest; identity != o.baseline {
		return nil, fmt.Errorf(
			"--baseline names generation %s and --baseline-spec %s produces %s",
			embeddigest.Short(o.baseline), o.baselineSpec, embeddigest.Short(identity))
	}
	return &loaded, nil
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
