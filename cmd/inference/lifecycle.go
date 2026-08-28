package inference

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
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

Running this twice is safe. It is what happens when a run is restarted.`,
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
		LeaseOwner: worker, FencingToken: 1, SnapshotWatermark: boundary,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	err := opened.store.CreateRun(ctx, run)
	switch {
	case err == nil:
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
			return runIndex(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addCommonFlags(cmd, &options)
	return cmd
}

// runIndex builds it and says what happened.
func runIndex(ctx context.Context, out io.Writer, options commonOptions) error {
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	outcome, err := embedpg.EnsureIndex(ctx, opened.db, opened.loaded.Spec)
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
	var options commonOptions
	var runID string

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show what a run has done and what it is waiting for",
		Long: `Print a run's phase, progress and the evidence it has gathered.

It reads and changes nothing, and it is the verb to reach for when a cutover has
been refused: the refusal names what is missing, and this says how far the run
got.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runStatus(cmd.Context(), cmd.OutOrStdout(), options, runID)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	return cmd
}

// runStatus prints one run.
func runStatus(ctx context.Context, out io.Writer, options commonOptions, runID string) error {
	if runID == "" {
		return fmt.Errorf("--run-id is required")
	}
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	status, err := embedreport.ReadStatus(ctx, opened.store, runID)
	if err != nil {
		return err
	}
	return printStatus(out, status)
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
	if status.FailureClass != "" {
		lines = append(lines, bullet("failed in "+status.FailureClass+": "+status.FailureDetail))
	}
	return writeLines(out, lines...)
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

	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Put the previous generation back, if it is still a place to go back to",
		Long: `Move the active pointer back to a previous generation.

Whether that is possible is measured rather than assumed. A generation whose
tables still exist is not necessarily one you can return to: it may never have
been verified, it may have stopped being maintained and drifted from the source,
or its index may have been dropped -- which makes going back the same queries
against a sequential scan.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRollback(cmd.Context(), cmd.OutOrStdout(), options, toGeneration, window)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&toGeneration, "to", "", "Identity of the generation to return to (required)")
	cmd.Flags().DurationVar(&window, "window", 0,
		"How long after a cutover the previous generation stays eligible; zero for no limit")
	return cmd
}

// runRollback evaluates eligibility and moves the pointer.
func runRollback(
	ctx context.Context, out io.Writer, options commonOptions, toGeneration string, window time.Duration,
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

	if err := opened.store.MovePointer(ctx, embedstore.Pointer{
		TargetTable: table, Active: toGeneration, Previous: pointer.Active,
		CutOverAt: time.Now().UTC(), CutOverBy: "ptah-cli",
	}, pointer.Active); err != nil {
		return err
	}
	return writeLines(out, fmt.Sprintf("queries now read %s, which replaced %s",
		toGeneration, pointer.Active))
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
