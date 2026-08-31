package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embeddigest"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrelease"
	"go.5x5.cz/ptah/internal/embedreport"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
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
	// Asked BEFORE any of the work below, because all of it is durable and
	// none of it is undone by the conflict branch that used to notice. A second
	// prepare under one run id -- which is the documented second-generation
	// workflow, the guide deriving the id from a date and the quick start
	// exporting PTAH_RUN_ID -- added five columns to the user's table and
	// registered a second generation, then said "leaving it as it is" and
	// exited 0 (stokaro/ptah#2637).
	if err := prepareAgreesWithTheRun(ctx, opened, runID, spec.Identity().Digest); err != nil {
		return err
	}
	// The target columns, which is the step the plan names and nothing
	// performed. Before the generation is registered, because a registry row
	// for a generation with nowhere to write is a row every later verb trusts
	// (stokaro/ptah#2390).
	if err := embedpg.EnsureTarget(ctx, opened.db, spec); err != nil {
		return err
	}
	if _, err := opened.store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: spec.Identity().Digest, SpecDigest: spec.Identity().Digest,
		Name: spec.Name, Reproducibility: string(spec.Identity().Reproducibility),
		ReproducibilityReason: spec.Identity().ReproducibilityReason,
		Dimension:             spec.Model.ReportedDimension,
		TargetTable:           spec.Target.Table, TargetColumn: spec.Target.Column,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		return err
	}

	boundary, err := prepareConsistency(ctx, opened)
	if err != nil {
		return err
	}
	return createRun(ctx, out, opened, runID, worker, boundary)
}

// prepareAgreesWithTheRun refuses a prepare whose run is for another generation.
//
// A run that does not exist yet is the ordinary case and agrees with anything;
// a run that exists for this same generation is a restart, which prepare is
// documented to be safe for and remains so.
func prepareAgreesWithTheRun(
	ctx context.Context, opened *session, runID, identity string,
) error {
	run, err := opened.store.Run(ctx, runID)
	if errorsIs(err, embedstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return run.DescribesGeneration(identity)
}

// prepareConsistency installs what the selected mode needs and records the
// boundary it starts from.
func prepareConsistency(ctx context.Context, opened *session) (string, error) {
	if opened.loaded.Mode != embedcatchup.ModeOutbox {
		return "", nil
	}
	outbox, err := embedpg.NewOutbox(opened.db, opened.loaded.Spec)
	if err != nil {
		return "", err
	}
	if err := outbox.Install(ctx); err != nil {
		return "", err
	}
	// Only now. Everything before this point is captured by the outbox; the
	// boundary says where the backfill's own reading begins.
	boundary, err := outbox.Horizon(ctx)
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(boundary, 10), nil
}

// createRun records the run, or reports the one that is already there.
// defaultPrepareLease is how long the lease prepare takes is good for.
//
// Prepare does no long work, so this is short: it names the worker that
// prepared the run and leaves a token a later claim moves past. The engine
// takes its own, longer lease when it starts embedding.
const defaultPrepareLease = time.Minute

func createRun(
	ctx context.Context, out io.Writer, opened *session, runID, worker, boundary string,
) error {
	spec := opened.loaded.Spec
	run := embedrun.Run{
		ID: runID, SpecDigest: spec.Identity().Digest, GenerationIdentity: spec.Identity().Digest,
		Environment: "cli", Source: spec.Source.Table,
		Target:          spec.Target.Table + "." + spec.Target.Column,
		ProviderProfile: spec.Model.Provider, PtahVersion: "cli", PolicyDigest: "",
		Phase: embedrun.PhasePrepared, Status: embedrun.StatusRunning,
		SnapshotWatermark: boundary,
		CreatedAt:         time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	// Claimed rather than written. `FencingToken: 1` in a literal is the
	// mechanism's starting value and never its second, so a run created that
	// way is one no later claim has anything to move past -- which is how a
	// fence that is enforced on every write came to fence nothing
	// (stokaro/ptah#2474).
	run.Claim(worker, defaultPrepareLease)

	err := opened.store.CreateRun(ctx, run)
	switch {
	case err == nil:
		// The boundary-capture step ran, so the run reaches it -- the phase
		// names the step rather than the artifact, and a mode that records no
		// boundary answered the step rather than skipping it. Without this a
		// backfill's move would be a jump, and jumps are refused.
		if err := opened.store.ReachPhase(ctx, runID, embedrun.PhaseBoundaryCaptured); err != nil {
			return err
		}
		return writeLines(out,
			fmt.Sprintf("prepared run %s for generation %s", runID, spec.Identity().Short()),
			bullet("snapshot boundary: "+boundaryText(boundary)))
	case isConflict(err):
		// A restarted prepare finds its own run. Saying so beats failing, and
		// beats overwriting a run that may be halfway through a backfill.
		return writeLines(out,
			fmt.Sprintf("run %s already exists; leaving it as it is", runID))
	default:
		return err
	}
}

// boundaryText renders the recorded boundary, or says there is none.
func boundaryText(boundary string) string {
	return embedreport.BoundaryText(boundary)
}

// isConflict reports whether an error is the store refusing a duplicate.
func isConflict(err error) bool {
	return err != nil && errorsIs(err, embedstore.ErrConflict)
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

	run, err := opened.store.Run(ctx, runID)
	if err != nil {
		return err
	}
	// The index is built for the SPECIFICATION's generation and the phase is
	// marked on the RUN, so a disagreement advances one run's lifecycle on the
	// strength of another generation's index (stokaro/ptah#2637).
	if err := run.DescribesGeneration(opened.loaded.Spec.Identity().Digest); err != nil {
		return err
	}

	outcome, err := embedpg.EnsureIndex(ctx, opened.db, opened.loaded.Spec)
	if err != nil {
		return err
	}
	// The phase names the step rather than the artifact, so a specification
	// that declares no index method still reaches it: the indexing step is
	// done, and a run left short of it could never be verified.
	if err := opened.store.ReachPhase(ctx, runID, embedrun.PhaseIndexed); err != nil {
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

	status, err := embedreport.ReadStatus(ctx, opened.store, options.runID)
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
		bullet("snapshot boundary: " + status.SnapshotWatermark),
		bullet("catch-up watermark: " + status.CatchUpWatermark),
		bullet(fmt.Sprintf("lease: %s, fencing token %d",
			leaseText(status), status.FencingToken)),
	}
	return writeLines(out, append(lines, stoppedLines(status)...)...)
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
		Long: `Move the active pointer back to a previous generation.

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
	cmd.Flags().StringVar(&toGeneration, "to", "", "Identity of the generation to return to (required)")
	cmd.Flags().DurationVar(&window, "window", 0,
		"How long after a cutover the previous generation stays eligible; zero for no limit")
	addEvidenceFlags(cmd.Flags(), &evidence)
	addSubjectFlag(cmd, &evidence)
	return cmd
}

// runRollback evaluates eligibility and moves the pointer.
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

	table := opened.loaded.Spec.Target.Table
	pointer, err := opened.store.Pointer(ctx, table)
	if err != nil {
		return err
	}
	state, err := embedpg.RollbackState(ctx, opened.db, opened.loaded.Spec, toGeneration, pointer)
	if err != nil {
		return err
	}
	policy, err := rollbackPolicy(opened, window)
	if err != nil {
		return err
	}
	eligibility := embedcutover.EvaluateRollback(policy, state,
		embedcutover.Observed{Now: time.Now().UTC()})
	if !eligibility.Eligible {
		return refusal(out, "rollback refused", eligibility.Blockers)
	}

	now := time.Now().UTC()
	if err := opened.store.MovePointer(ctx, embedstore.Pointer{
		TargetTable: table, Active: toGeneration, Previous: pointer.Active,
		CutOverAt: now, CutOverBy: "ptah-cli",
	}, pointer.Active); err != nil {
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
		Expires: eligibility.Expires, RolledBackAt: now,
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
	return publishRecord(ctx, out, options, evidence, record, buildErr)
}

// rollbackPolicy is what a previous generation has to satisfy to be one you can
// go back to.
//
// RequireIndex follows the specification rather than being asserted. A
// generation that declares no index method has none to be missing, and
// demanding one refuses every rollback to it while naming an index nobody
// configured -- the same defect the cutover decision had, one verb over.
//
// The index method comes from the specification in hand rather than from the
// registry, which records what a generation IS and not how it was built. Two
// generations over one table with different index methods would need the
// registry to carry it, and that is a change to make when something needs it.
func rollbackPolicy(opened *session, window time.Duration) (embedcutover.RollbackPolicy, error) {
	objects, err := opened.loaded.Spec.TargetObjects()
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
