package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/cmd/internal/exitcode"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedcutover"
	"ptah.run/internal/embeddigest"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedrelease"
	"ptah.run/internal/embedreport"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// newPrepareCommand returns "ptah inference prepare".
func newPrepareCommand() *cobra.Command {
	var options commonOptions
	var runID string
	var worker string

	cmd := &cobra.Command{
		Use:   "prepare",
		Short: "Create the run's own tables, the outbox, and record the snapshot boundary",
		Long: `Create what a run needs before it can start, and record where it starts from.

The order is not arbitrary. The outbox is installed BEFORE the boundary is
recorded, because a change made between the two would be captured by nothing at
all: the backfill has not started, so it will not see it, and the outbox does not
exist yet, so it leaves no event.

Running this twice with the SAME specification is safe. It is what happens when a
run is restarted, and it leaves the run as it is.

Running it with a different specification under the same run id is refused, and
refused before anything is written. A run records the generation it was prepared
for; a second generation needs a run of its own.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPrepare(cmd.Context(), cmd.OutOrStdout(), options, runID, worker)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier for this run (required)")
	cmd.Flags().StringVar(&worker, "worker", "ptah-cli", "Name recorded as the lease holder")
	return cmd
}

// runPrepare creates the run's own state and records the boundary.
func runPrepare(
	ctx context.Context, out io.Writer, options commonOptions, runID, worker string,
) error {
	if runID == "" {
		return fmt.Errorf("--run-id is required")
	}
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	if err := opened.store.EnsureSchema(ctx); err != nil {
		return err
	}
	spec := opened.loaded.Spec
	generation := embedstore.Generation{
		Identity: spec.Identity().Digest, SpecDigest: opened.loaded.Digest,
		SpecDocument: string(opened.loaded.Document),
		Name:         spec.Name, Reproducibility: string(spec.Identity().Reproducibility),
		ReproducibilityReason: spec.Identity().ReproducibilityReason,
		Dimension:             spec.Model.ReportedDimension,
		// The resolved relations rather than the authored spellings: these
		// say where the data physically is, and they are what the source
		// digest below is taken over (stokaro/ptah#2806).
		TargetSchema: opened.target.Schema,
		TargetTable:  opened.target.Table, TargetColumn: spec.Target.Column,
		SourceSchema: opened.source.Schema, SourceTable: opened.source.Table,
		ConsistencyMode: string(opened.loaded.Mode),
		CreatedAt:       time.Now().UTC(),
	}
	run := embedrun.Run{
		ID: runID, SpecDigest: spec.Identity().Digest, GenerationIdentity: spec.Identity().Digest,
		// The digest the outbox is keyed on, not the bare table name. Recorded
		// bare, `public.docs` and `archive.docs` were one source string and two
		// outboxes, so each run held the other's floor (stokaro/ptah#2724).
		Environment: "cli", Source: embedpg.SourceIdentity(opened.source.Schema, opened.source.Table),
		Target:          spec.Target.Table + "." + spec.Target.Column,
		ProviderProfile: spec.Model.Provider, PtahVersion: "cli", PolicyDigest: "",
		Phase: embedrun.PhasePrepared, Status: embedrun.StatusRunning,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	run.Claim(worker, defaultPrepareLease)
	result, err := opened.store.PrepareRun(
		ctx, spec, generation, run, opened.loaded.Mode)
	if err != nil {
		return err
	}
	if !result.Created {
		return writeLines(out,
			fmt.Sprintf("run %s already exists; leaving it as it is", runID))
	}
	return writeLines(out,
		fmt.Sprintf("prepared run %s for generation %s", runID, spec.Identity().Short()),
		bullet("snapshot boundary: "+boundaryText(
			result.Run.SnapshotWatermark, opened.loaded.Mode)))
}

// defaultPrepareLease is how long the lease prepare takes is good for.
//
// Prepare does no long work, so this is short: it names the worker that
// prepared the run and leaves a token a later claim moves past. The engine
// takes its own, longer lease when it starts embedding.
const defaultPrepareLease = time.Minute

// boundaryText renders the recorded boundary, or says why there is none.
//
// The mode travels with it: an absent watermark is "catch-up has not run yet"
// under an outbox and "this mode records none" under the other two
// (stokaro/ptah#2646).
func boundaryText(boundary string, mode embedcatchup.Mode) string {
	return embedreport.BoundaryText(boundary, mode)
}

// newIndexCommand returns "ptah inference index".
func newIndexCommand() *cobra.Command {
	var options commonOptions
	var runID string

	cmd := &cobra.Command{
		Use:   "index",
		Short: "Build the generation's vector index and wait for it to be valid",
		Long: `Build the vector index the specification declares, concurrently.

After the backfill rather than before it. An IVFFlat index trains its lists on
the data present when it is built, so one built over an empty column is valid
and useless.

Concurrently, because the table is one an application is reading and writing
while this runs. That is also why the index is read back afterwards: a
concurrent build that fails leaves an index behind that PostgreSQL will not use,
and it reports no error of its own.

A specification naming no index method has no index to build, and this says so
rather than failing. Every query over that generation is then a sequential scan
over the whole corpus, which is a choice its author made.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runIndex(cmd.Context(), cmd.OutOrStdout(), options, runID)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	return cmd
}

// runIndex builds it and says what happened.
func runIndex(ctx context.Context, out io.Writer, options commonOptions, runID string) error {
	if runID == "" {
		return fmt.Errorf("--run-id is required")
	}
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	// Index DDL and the run transition are one lifecycle operation even though
	// CREATE INDEX CONCURRENTLY cannot be in the run transaction. The store's
	// generation lock prevents abandonment or retirement between them, and its
	// fenced row update preserves a checkpoint committed during the build.
	outcome, err := opened.store.EnsureRunIndex(ctx, runID, opened.loaded.Spec)
	if err != nil {
		return err
	}
	return writeLines(out, indexOutcomeText(outcome, opened.loaded.Spec))
}

// indexOutcomeText says what the build did, including having nothing to do.
func indexOutcomeText(outcome embedpg.IndexOutcome, spec embedgen.Spec) string {
	generation := spec.Identity().Short()
	messages := map[embedpg.IndexOutcome]string{
		embedpg.IndexNotDeclared: "the specification declares no index method, so generation " +
			generation + " has no index: every query over it is a sequential scan over the " +
			"whole corpus",
		embedpg.IndexAlreadyValid: "generation " + generation + " already has a valid index",
		embedpg.IndexRebuilt: "generation " + generation + " had an index that was not valid, " +
			"left by a concurrent build that failed; it was dropped and built again",
		embedpg.IndexBuilt: "generation " + generation + " has a valid index",
	}
	return messages[outcome]
}

// newStatusCommand returns "ptah inference status".
func newStatusCommand() *cobra.Command {
	var options statusOptions

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show what a run has done, how far it got, and what it is waiting for",
		Long: `Print a run's phase, progress, and whether it is ready to cut over.

It reads and changes nothing, and it is the verb to reach for when a cutover has
been refused: the refusal names what is missing, and this says how far the run
got.

Two of the answers are measured rather than read off the run. "verified" runs
the deterministic layers now, and "cutover ready" decides with the same code the
cutover verb decides with -- a gate that agreed with that verb only by
coincidence is one that will eventually let a deployment proceed against a
generation the cutover then refuses. Both cost what verify costs, which is a
read of the target.

Cutover readiness excludes the approval. An approval nobody has given yet is not
a defect in the state, and a rollout gate waiting for one would wait forever
under the policy most production environments run; the answer says separately
whether one is owed, and names the plan digest it would bind to.

--format json is what a rollout system consumes, and --require-ready is what one
gates on: exit 1 until both conditions hold, so an init container that keeps
failing is the whole of the gate.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStatus(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addCommonFlags(cmd, &options.commonOptions)
	cmd.Flags().StringVar(&options.runID, "run-id", "", "Identifier of the run (required)")
	cmd.Flags().StringVar(&options.format, "format", "text", "Output format: text or json")
	cmd.Flags().BoolVar(&options.requireReady, "require-ready", false,
		"Return 1 unless the generation is verified and ready to cut over; errors still return 2")
	return cmd
}

// statusOptions are what the verb was asked for.
//
// A struct rather than four parameters, because two of them are booleans and a
// signature carrying one is a signature a caller reads backwards.
type statusOptions struct {
	commonOptions
	runID  string
	format string
	// requireReady turns the two conditions into the process's exit status.
	requireReady bool
}

// statusDocument is what --format json emits.
//
// The stored run and what is true now are separate objects, because they are
// answers of different kinds: one is a record and the other is a measurement,
// and a reader deciding whether to deploy needs to know which is which.
type statusDocument struct {
	Run       embedreport.Status    `json:"run"`
	Readiness embedreport.Readiness `json:"readiness"`
}

// runStatus prints one run.
func runStatus(ctx context.Context, out io.Writer, options statusOptions) error {
	if options.runID == "" {
		return fmt.Errorf("--run-id is required")
	}
	if options.format != "text" && options.format != "json" {
		return fmt.Errorf("invalid --format value %q: text or json", options.format)
	}
	opened, err := open(ctx, options.commonOptions)
	if err != nil {
		return err
	}
	defer opened.close()
	run, err := opened.store.Run(ctx, options.runID)
	if err != nil {
		return err
	}
	if err := run.DescribesGeneration(opened.loaded.Spec.Identity().Digest); err != nil {
		return err
	}

	status, err := embedreport.ReadStatus(ctx, opened.store, options.runID, opened.loaded.Mode)
	if err != nil {
		return err
	}
	readiness, err := embedreport.ReadReadiness(
		ctx, opened.db, opened.store, opened.loaded, options.runID, time.Now())
	if err != nil {
		return err
	}
	if err := reportStatus(out, options.format, status, readiness); err != nil {
		return err
	}
	if !options.requireReady {
		return nil
	}
	return gateOnReadiness(readiness)
}

// reportStatus writes the answer in whichever form was asked for.
func reportStatus(
	out io.Writer, format string, status embedreport.Status, readiness embedreport.Readiness,
) error {
	if format == "json" {
		return writeStatusJSON(out, statusDocument{Run: status, Readiness: readiness})
	}
	if err := printStatus(out, status); err != nil {
		return err
	}
	return printReadiness(out, readiness)
}

// gateOnReadiness turns the two conditions into the exit code a rollout waits
// on.
//
// Exit 1 rather than a message a caller has to parse, because the caller is a
// container that has to keep failing until the state is there. It is the
// documented code for an expected negative result, and the report is on stdout
// either way: a gate that failed silently would leave whoever reads the pod's
// logs with nothing but a number.
//
// The approval is deliberately not part of it. A generation waiting for a
// person to sign is finished, and a gate holding a deployment for a signature
// somebody gives in the same breath as the cutover would never open.
func gateOnReadiness(readiness embedreport.Readiness) error {
	if readiness.Verified && readiness.CutoverReady {
		return nil
	}
	return exitcode.New(1, fmt.Errorf(
		"the generation is not ready: verified=%t, cutover ready=%t",
		readiness.Verified, readiness.CutoverReady))
}

// writeStatusJSON is the form a rollout system consumes.
//
// Indented, because the reader of a failed gate is a person looking at a log.
func writeStatusJSON(out io.Writer, document statusDocument) error {
	body, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("render the status: %w", err)
	}
	_, err = fmt.Fprintf(out, "%s\n", body)
	return err
}

// printReadiness renders the two conditions a rollout waits on.
func printReadiness(out io.Writer, readiness embedreport.Readiness) error {
	lines := []string{
		section(fmt.Sprintf("verified: %t, cutover ready: %t",
			readiness.Verified, readiness.CutoverReady)),
	}
	if readiness.ApprovalRequired {
		lines = append(lines, bullet("an approval is required, for plan "+
			shortDigest(readiness.PlanDigest)))
	}
	for _, finding := range readiness.Findings {
		lines = append(lines, bullet(finding))
	}
	for _, blocker := range readiness.Blockers {
		lines = append(lines, bullet("blocked: "+blocker))
	}
	// What was not measured, always. A report saying only what it found reads
	// as though it looked at everything.
	for _, unmeasured := range readiness.Unmeasured {
		lines = append(lines, bullet("not measured: "+unmeasured))
	}
	return writeLines(out, lines...)
}

// shortDigest renders a plan digest for a person, or says there is none.
func shortDigest(digest string) string {
	if digest == "" {
		return "(none)"
	}
	return embeddigest.Short(digest)
}

// printStatus renders a run for a person.
//
// The status itself is read by internal/embedreport, which both this verb and
// the agent surface consume. What is here is the rendering.
func printStatus(out io.Writer, status embedreport.Status) error {
	lines := []string{
		fmt.Sprintf("run %s: %s, %s", status.RunID, status.Phase, status.State),
		bullet("generation: " + status.Generation),
		bullet(fmt.Sprintf("scanned %d, embedded %d, skipped %d, deleted %d",
			status.Progress.RowsScanned, status.Progress.RowsEmbedded,
			status.Progress.RowsSkipped, status.Progress.RowsDeleted)),
		bullet(fmt.Sprintf("%d batches committed, %d retries since the last one",
			status.Progress.BatchesCommitted, status.Progress.RetryCount)),
		// The number an operator compares against an invoice, and the reason
		// the capacity strategy page has a Cost section at all. It was
		// recorded on the run and reachable only through --format json, while
		// the page said `status` showed it and illustrated the promise with
		// the batches line above -- so a hurried reader took a count of
		// provider round trips for a count of tokens (stokaro/ptah#2648).
		//
		// Ptah counts no tokens of its own, so the line says whose numbers
		// these are -- and where the provider gave none, it says THAT instead
		// of printing two zeros under the same sentence. A missing `usage`
		// object and one carrying zeros both leave the counts at zero, and an
		// operator reading a bill needs to know which happened.
		bullet(tokenText(status.Progress)),
		bullet("snapshot boundary: " + status.SnapshotWatermark),
		bullet("catch-up watermark: " + status.CatchUpWatermark),
		bullet(fmt.Sprintf("lease: %s, fencing token %d",
			leaseText(status), status.FencingToken)),
	}
	return writeLines(out, append(lines, stoppedLines(status)...)...)
}

// tokenText is the token line, or the reason there is none.
//
// The batch count is what separates the two answers: a run that committed
// batches and carries no usage-bearing one was told nothing by its provider.
// A run that has committed nothing yet has no answer either way, and says so
// the same way -- there is nothing to report until something is asked.
func tokenText(progress embedreport.Progress) string {
	if progress.ProviderUsageBatches == 0 {
		return "the provider reported no token usage"
	}
	return fmt.Sprintf("%d prompt tokens, %d total, as the provider reported them",
		progress.ProviderPromptTokens, progress.ProviderTotalTokens)
}

// stoppedLines says why a run is not running, if it is not.
//
// A pause and a failure both leave their explanation in FailureDetail, and only
// a failure classifies it. Reading the class alone printed nothing at all for a
// paused run, so "why did this stop" -- the first question a paused run's
// operator asks, and the reason a pause without one is refused -- had an answer
// stored and no verb that showed it (stokaro/ptah#2474).
func stoppedLines(status embedreport.Status) []string {
	if status.FailureClass != "" {
		return []string{bullet("failed in " + status.FailureClass + ": " + status.FailureDetail)}
	}
	if status.State == string(embedrun.StatusPaused) {
		return []string{bullet("paused: " + status.FailureDetail)}
	}
	if status.State == string(embedrun.StatusAbandoned) {
		return []string{bullet("abandoned: " + status.FailureDetail)}
	}
	return nil
}

// leaseText renders who holds the run.
func leaseText(status embedreport.Status) string {
	if status.LeaseHolder == "" {
		return "held by nobody"
	}
	return status.LeaseHolder
}

// newRollbackCommand returns "ptah inference rollback".
func newRollbackCommand() *cobra.Command {
	var options commonOptions
	var toGeneration string
	var window time.Duration
	var evidence evidenceOptions

	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Put the previous generation back, if it is still a place to go back to",
		Long: `Move the active pointer back to the generation recorded as its previous generation.

Whether that is possible is measured rather than assumed. A generation whose
tables still exist is not necessarily one you can return to: it may never have
been verified, it may have stopped being maintained and drifted from the source,
or its index may have been dropped -- which makes going back the same queries
against a sequential scan.

Naming --publish-evidence, --attach-to or --evidence-file leaves a record of
what was undone: a separate record from a cutover, because "why did the corpus
change" and "why did we go back" are different questions and a reader looking
for the second in a list of the first finds a pointer move with nothing attached
to it.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRollback(cmd.Context(), cmd.OutOrStdout(), options,
				toGeneration, window, evidence)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&toGeneration, "to", "",
		"Identity recorded as the active pointer's previous generation (required)")
	cmd.Flags().DurationVar(&window, "window", 0,
		"How long after a cutover the previous generation stays eligible; zero for no limit")
	addEvidenceFlags(cmd.Flags(), &evidence)
	addSubjectFlag(cmd, &evidence)
	return cmd
}

// runRollback evaluates eligibility, moves the pointer, and records the
// displaced generation's rollback atomically. An abandonment therefore sees
// either the state before the move or the fully recorded state after it.
func runRollback(
	ctx context.Context, out io.Writer, options commonOptions,
	toGeneration string, window time.Duration, evidence evidenceOptions,
) error {
	if toGeneration == "" {
		return fmt.Errorf("--to is required")
	}
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	schema := opened.loaded.Spec.Target.Schema
	table := opened.loaded.Spec.Target.Table
	pointer, err := opened.store.Pointer(ctx, schema, table)
	if err != nil {
		return err
	}
	switch {
	case toGeneration == pointer.Active:
		return refusal(out, "rollback refused", []string{
			"queries already read that generation",
		})
	case pointer.Previous == "":
		return refusal(out, "rollback refused", []string{
			"the active pointer has no previous generation",
		})
	case toGeneration != pointer.Previous:
		return refusal(out, "rollback refused", []string{
			fmt.Sprintf("--to names %s, but the active pointer's previous generation is %s",
				toGeneration, pointer.Previous),
		})
	}
	state, err := embedpg.RollbackState(ctx, opened.db, toGeneration, pointer)
	if err != nil {
		return err
	}
	destination, err := opened.store.Generation(ctx, toGeneration)
	if err != nil {
		return err
	}
	policy, err := rollbackPolicy(destination, window)
	if err != nil {
		return err
	}
	eligibility := embedcutover.EvaluateRollback(policy, state,
		embedcutover.Observed{Now: time.Now().UTC()})
	if !eligibility.Eligible {
		return refusal(out, "rollback refused", eligibility.Blockers)
	}

	rolledBackAt, err := opened.store.MovePointerWithRollback(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: toGeneration, Previous: pointer.Active,
		CutOverBy: "ptah-cli",
	}, pointer.Active, destination.MaintainedUntil, eligibility.Expires)
	if err != nil {
		return err
	}
	if err := writeLines(out, fmt.Sprintf("queries now read %s, which replaced %s",
		toGeneration, pointer.Active)); err != nil {
		return err
	}
	return publishRollback(ctx, out, options, embedrelease.Rollback{
		Generation: toGeneration, Replaced: pointer.Active,
		Target:     opened.loaded.Spec.Target.Table,
		Maintained: state.Maintained, VerifiedAt: state.VerifiedAt,
		StaleRows: state.StaleRows, MissingRows: state.MissingRows,
		Expires: eligibility.Expires, RolledBackAt: rolledBackAt,
	}, evidence)
}

// publishRollback records what was undone, where a destination was named.
//
// After the pointer has moved, and reported rather than fatal for the reason
// every other record here is: the rollback happened, and a registry being
// unreachable is not a fact about it.
func publishRollback(
	ctx context.Context, out io.Writer, options commonOptions,
	rollback embedrelease.Rollback, evidence evidenceOptions,
) error {
	if !evidence.destinationNamed() {
		return nil
	}
	record, buildErr := embedrelease.NewRollbackRecord(rollback)
	return publishRecord(ctx, out, options, evidence, record, buildErr, swallowed)
}

// rollbackPolicy is what a previous generation has to satisfy to be one you can
// go back to.
//
// RequireIndex follows the specification rather than being asserted. A
// generation that declares no index method has none to be missing, and
// demanding one refuses every rollback to it while naming an index nobody
// configured -- the same defect the cutover decision had, one verb over.
//
// The index method comes from the destination generation's recorded
// specification. The file supplied to this invocation describes the current
// operator intent; it is not evidence of how a previous generation was built.
func rollbackPolicy(
	destination embedstore.Generation, window time.Duration,
) (embedcutover.RollbackPolicy, error) {
	recorded, err := embedpg.RecordedSpec(destination,
		"checked for the index required to roll back to it")
	if err != nil {
		return embedcutover.RollbackPolicy{}, err
	}
	objects, err := recorded.Spec.TargetObjects()
	if err != nil {
		return embedcutover.RollbackPolicy{}, err
	}
	return embedcutover.RollbackPolicy{Window: window, RequireIndex: objects.HasIndex}, nil
}

// refusal prints why an operation was refused, and fails.
//
// Every blocker rather than the first: an operator who fixes what they were told
// and comes back to a second refusal learns the system one refusal at a time.
func refusal(out io.Writer, headline string, blockers []string) error {
	lines := []string{headline + ":"}
	for _, blocker := range blockers {
		lines = append(lines, bullet(blocker))
	}
	if err := writeLines(out, lines...); err != nil {
		return err
	}
	return fmt.Errorf("%s", headline)
}
