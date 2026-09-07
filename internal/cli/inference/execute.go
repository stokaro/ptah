package inference

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedprovider"
	"ptah.run/internal/embedreport"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// errorsIs is errors.Is, named so the file that uses it does not have to import
// errors for one call.
func errorsIs(err, target error) bool { return errors.Is(err, target) }

// executeOptions are what the two running verbs take.
type executeOptions struct {
	commonOptions
	runID     string
	batchRows int
	batchSize int
	timeout   time.Duration
	// maintainFor extends the generation's stabilization window after a
	// successful catch-up.
	maintainFor time.Duration
}

// newBackfillCommand returns "ptah inference backfill".
func newBackfillCommand() *cobra.Command {
	options := executeOptions{}

	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Embed the source into the new generation, resumably",
		Long: `Scan the source, embed it, and write the vectors with their checkpoints.

The scan is by keyset, so it is safe over a table that changes while it runs,
and it resumes from where it stopped rather than from the beginning. The vectors
and the checkpoint that records them are one transaction: a checkpoint that
outlived its vectors would produce a resumed run that skips them and looks
completely healthy.

Interrupting it is safe. The run is durable at its last checkpoint.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runBackfill(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addExecuteFlags(cmd, &options)
	return cmd
}

// newCatchUpCommand returns "ptah inference catchup".
func newCatchUpCommand() *cobra.Command {
	options := executeOptions{}

	cmd := &cobra.Command{
		Use:   "catchup",
		Short: "Process the source changes made while the backfill ran",
		Long: `Read the changes recorded since the snapshot boundary and apply them.

Every changed row is REREAD rather than acted on from the change event. That is
what collapses a row updated five times during a backfill into one provider
request, and what stops a stale delete tombstoning a row that has since been
re-inserted.

It requires a consistency mode that records changes. Over a source with none,
there is nothing to read and the cutover will refuse for the same reason.

Run it against a PREVIOUS generation's specification with --maintain-for to keep
that generation a way back: a rollback target only stays one while something is
feeding it, and the window moves with the catch-up rather than being promised
once.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCatchUp(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addExecuteFlags(cmd, &options)
	addMaintainForFlag(cmd, &options.maintainFor)
	return cmd
}

// addExecuteFlags registers the flags the running verbs share.
func addExecuteFlags(cmd *cobra.Command, options *executeOptions) {
	addCommonFlags(cmd, &options.commonOptions)
	cmd.Flags().StringVar(&options.runID, "run-id", "", "Identifier of the run (required)")
	cmd.Flags().IntVar(&options.batchRows, "batch-rows", 200,
		"Source rows read in one query, which bounds how long a cancellation waits")
	cmd.Flags().IntVar(&options.batchSize, "batch-inputs", 32,
		"Inputs sent to the provider in one request")
	cmd.Flags().DurationVar(&options.timeout, "provider-timeout", 60*time.Second,
		"How long one provider request may take")
}

// addMaintainForFlag registers --maintain-for, which "catchup" alone has.
//
// It sits here rather than in addExecuteFlags because a backfill cannot keep
// the promise the flag makes. [extendMaintenance] says why: a window is kept
// true by the catch-up that moves it, so a window extended without one behind
// it is a promise nobody kept. Registered on "backfill" it was accepted,
// advertised in --help and in the flag reference, and read by nothing --
// maintained_until stayed NULL, the run exited 0, and the operator learned
// their way back did not exist when "rollback" refused
// (stokaro/ptah#2648 finding 10). Not registering it is what makes
// `backfill --maintain-for 2h` an unknown flag, and PTAH_MAINTAIN_FOR reach
// the verb that acts on it and no other.
func addMaintainForFlag(cmd *cobra.Command, maintainFor *time.Duration) {
	cmd.Flags().DurationVar(maintainFor, "maintain-for", 0,
		"After catching up, extend this generation's stabilization window by this much")
}

// runBackfill runs the scan-embed-commit loop.
func runBackfill(ctx context.Context, out io.Writer, options executeOptions) error {
	opened, engine, err := buildEngine(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	// Reached BEFORE the walk, because that is when it is true. It used to be
	// set afterwards, which made `backfilling` the phase of a backfill that had
	// FINISHED -- and verification, asking `Phase != PhaseBackfilling`, told a
	// completed backfill it had not reached the end of its snapshot while
	// telling a run that had never backfilled that it had (stokaro/ptah#2649).
	//
	// Reaching a phase the run has passed is a no-op, so a resumed backfill
	// asks for this and is told nothing new.
	if err := opened.store.ReachPhase(ctx, options.runID, embedrun.PhaseBackfilling); err != nil {
		return err
	}
	run, pass, err := engine.Backfill(ctx, options.runID)
	if err != nil {
		return reportRun(out, run, err)
	}
	// And this is the fact nothing recorded: the walk reached the end of the
	// snapshot. An interrupted backfill returns above and never gets here, which
	// is the answer -- it did not.
	if err := opened.store.ReachPhase(ctx, options.runID, embedrun.PhaseBackfilled); err != nil {
		return err
	}
	// A mode that records no changes has nothing for catch-up to process, so
	// the completed backfill IS the run reaching the barrier: the snapshot
	// covers the source, and nothing happened after it that anyone is claiming
	// otherwise about.
	//
	// Without this the run could never leave `backfilling`. `catchup` is the
	// only verb that reaches `caught_up` and it is refused for exactly these
	// modes, so `index`, `verify` and `cutover` were all unreachable and
	// verification blocked with "the backfill has not reached the end of its
	// snapshot" -- a statement about a backfill that had finished
	// (stokaro/ptah#2632).
	if !recordsChanges(opened.loaded.Mode) {
		if err := opened.store.ReachPhase(ctx, options.runID, embedrun.PhaseCaughtUp); err != nil {
			return err
		}
	}
	// This invocation's work, not the run's. A resumed backfill that scanned
	// nothing reported the same numbers as the one that did everything
	// (stokaro/ptah#2645).
	return writeLines(out,
		fmt.Sprintf("backfill finished: %d scanned, %d embedded, %d skipped",
			pass.RowsScanned, pass.RowsEmbedded, pass.RowsSkipped))
}

// runCatchUp processes the changes made since the boundary.
func runCatchUp(ctx context.Context, out io.Writer, options executeOptions) error {
	opened, engine, err := buildEngine(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	if !recordsChanges(opened.loaded.Mode) {
		// Structural absence rather than a silent no-op: a catch-up that
		// "succeeded" over a mode that records nothing is a run reporting
		// itself caught up on a source it never watched.
		return fmt.Errorf(
			"catch-up needs a consistency mode that records changes, and this specification "+
				"selects %q", embedreport.ModeName(opened.loaded.Mode))
	}
	// The outbox is named after the PHYSICAL source, so it is built from the
	// specification resolved against this server rather than from the
	// spelling the document used (stokaro/ptah#2806).
	physical, err := embedpg.WithResolvedRelations(ctx, opened.db, opened.loaded.Spec)
	if err != nil {
		return err
	}
	outbox, err := embedpg.NewOutbox(opened.db, physical)
	if err != nil {
		return err
	}
	source, err := embedpg.NewSource(opened.db, opened.loaded.Spec)
	if err != nil {
		return err
	}

	run, pass, err := engine.CatchUp(ctx, options.runID, outbox, source)
	if err != nil {
		return reportRun(out, run, err)
	}
	if err := opened.store.ReachPhase(ctx, options.runID, embedrun.PhaseCaughtUp); err != nil {
		return err
	}
	// This pass's changed rows. The run's counters already include the
	// backfill, so a catch-up with nothing to do printed the backfill's row
	// count as "changed rows" -- and the documented stop condition,
	// "0 changed rows", could never appear on any run whose backfill scanned
	// anything (stokaro/ptah#2645).
	lines := []string{fmt.Sprintf("caught up to transaction %s: %d changed rows, %d tombstoned",
		boundaryText(run.CatchUpWatermark, opened.loaded.Mode),
		pass.RowsScanned, pass.RowsDeleted)}
	lines = append(lines, pruneOutbox(ctx, opened, outbox, run)...)
	return extendMaintenance(ctx, out, opened, run.GenerationIdentity, options.maintainFor, lines)
}

// pruneOutbox removes the events every live reader of this source has passed.
//
// Here rather than inside the engine, and after the phase is recorded, because
// the floor has to be read from watermarks already on disk: this run's own
// position reached disk with the vectors it belongs to when CatchUp returned,
// and the floor query and delete then commit together under the source
// lifecycle lock. Pruning from a position held in memory, or between pages,
// would leave the events gone and the durable watermark behind them, which a
// resumed run reads as an empty range and reports as caught up.
//
// Called on the concrete PostgreSQL store rather than through embedengine's
// Changes interface, deliberately. The store owns the durable reader set and
// is the only layer that can bind its floor to the deleting statement.
//
// Reported and never fatal, for the reason extendMaintenance gives: the
// catch-up's work is committed, and failing here would report a run that did
// not do what it did. Nothing is lost when a prune fails; the table is bigger.
func pruneOutbox(
	ctx context.Context, opened *session, outbox *embedpg.Outbox, run embedrun.Run,
) []string {
	floor, ok, removed, err := opened.store.PruneOutbox(ctx, outbox)
	if err != nil {
		return []string{bullet(fmt.Sprintf("the outbox was not pruned: %v", err))}
	}
	if !ok {
		// No live reader answered, which is not a license to empty the table.
		return nil
	}
	lines := make([]string, 0, 2)
	if removed > 0 {
		lines = append(lines, bullet(fmt.Sprintf(
			"pruned %d processed event(s) from the outbox", removed)))
	}
	// This run has moved past the floor: something else still reading this
	// table is holding the remaining events. Report it even when this pass
	// deleted an older prefix; otherwise a normally advancing floor says only
	// what went away and hides why the rest remains.
	if reached, resumable, _ := embedcatchup.ResumeFrom(
		run.CatchUpWatermark, run.SnapshotWatermark); resumable && floor.Position.Before(reached) {
		lines = append(lines, bullet(fmt.Sprintf(
			"the outbox keeps events this run has processed: floor %s for %s is held by %s",
			floor.Position.String(),
			embedstore.QualifiedName(
				opened.loaded.Spec.Source.Schema, opened.loaded.Spec.Source.Table),
			outboxFloorHolders(floor.Holders))))
	}
	return lines
}

// outboxFloorHolders renders the exact runs an operator can inspect or
// abandon. The query returns them in generation and run order, so the message
// is stable across repeated catch-up passes.
func outboxFloorHolders(holders []embedpg.OutboxFloorHolder) string {
	named := make([]string, 0, len(holders))
	for _, holder := range holders {
		named = append(named, fmt.Sprintf(
			"run %s (generation %s)", holder.RunID, holder.Generation))
	}
	return strings.Join(named, ", ")
}

// recordsChanges reports whether a mode leaves something for catch-up to read.
//
// Only the outbox does, today. The immutable mode has nothing to catch up on by
// definition, and dual write is the application's own business -- Ptah reads the
// writer's reports rather than a change log.
func recordsChanges(mode embedcatchup.Mode) bool {
	return mode == embedcatchup.ModeOutbox
}

// extendMaintenance renews the promise that this generation is current.
//
// Phase K's window says a generation stays a way back for a period. Nothing
// makes that true on its own: an old generation stops receiving changes the
// moment queries stop reading it, so keeping it current means catching it up,
// and the window has to move with the catch-up rather than being set once and
// hoped over.
//
// So the two happen together. A window extended without a catch-up behind it is
// a promise nobody kept, and a catch-up whose window has expired left a
// generation current and unusable -- which are the two halves of the same
// mistake, made in opposite directions.
func extendMaintenance(
	ctx context.Context, out io.Writer, opened *session,
	generation string, window time.Duration, lines []string,
) error {
	if window <= 0 {
		return writeLines(out, lines...)
	}
	until := time.Now().UTC().Add(window)
	err := opened.store.Maintain(ctx, generation, until)
	if errorsIs(err, embedstore.ErrNotFound) || errorsIs(err, embedstore.ErrRetired) ||
		errorsIs(err, embedstore.ErrNoLiveRun) {
		// The catch-up already happened and its work is committed. Retirement or
		// abandonment may have ended the generation while this command moved from
		// its barrier to the maintenance decision; failing here would report a run
		// that did not do what it did.
		return writeLines(out, append(lines, bullet(fmt.Sprintf(
			"the window was not extended: %v", err)))...)
	}
	if err != nil {
		return err
	}
	return writeLines(out, append(lines, bullet(fmt.Sprintf(
		"generation %s stays a way back until %s", generation, until.Format(time.RFC3339))))...)
}

// buildEngine assembles the engine from the specification.
func buildEngine(
	ctx context.Context, options executeOptions,
) (*session, *embedengine.Engine, error) {
	if options.runID == "" {
		return nil, nil, fmt.Errorf("--run-id is required")
	}
	opened, err := open(ctx, options.commonOptions)
	if err != nil {
		return nil, nil, err
	}
	provider, err := buildProvider(opened, options.timeout, options.batchSize)
	if err != nil {
		opened.close()
		return nil, nil, err
	}
	source, err := embedpg.NewSource(opened.db, opened.loaded.Spec)
	if err != nil {
		opened.close()
		return nil, nil, err
	}
	target, err := embedpg.NewTarget(opened.db, opened.loaded.Spec)
	if err != nil {
		opened.close()
		return nil, nil, err
	}
	return opened, &embedengine.Engine{
		Spec: opened.loaded.Spec, Source: source, Provider: provider,
		Target: target, Store: opened.store,
		Bounds: embedrun.BatchBounds{
			MaxRows: options.batchRows, MaxInputs: options.batchSize,
		},
		Worker: "ptah-cli",
	}, nil
}

// buildProvider resolves the endpoint the specification names.
//
// The credential is passed as a REFERENCE, not a value. The provider resolves it
// at the moment of each request, so nothing in this process holds a token
// between calls and nothing here can put one in a log line.
func buildProvider(
	opened *session, timeout time.Duration, maxBatch int,
) (embedprovider.Provider, error) {
	reference, err := embedprovider.ParseCredentialRef(opened.loaded.Credential)
	if err != nil {
		return nil, err
	}
	return embedprovider.NewOpenAICompatible(embedprovider.OpenAICompatibleOptions{
		Name:               opened.loaded.Spec.Model.Provider,
		BaseURL:            opened.loaded.Endpoint,
		Model:              opened.loaded.Spec.Model.Identifier,
		Revision:           opened.loaded.Spec.Model.Revision,
		Dimension:          opened.loaded.Spec.Model.ReportedDimension,
		RequestedDimension: opened.loaded.Spec.Model.RequestedDimension,
		EndpointClass:      string(opened.loaded.Spec.Model.EndpointClass),
		Credential:         reference,
		MaxBatch:           maxBatch,
		Timeout:            timeout,
	})
}

// reportRun prints how far a failed run got, and returns the failure.
//
// The number is what an operator needs next: a run that failed after four
// batches is resumed, and a run that failed on the first is investigated.
func reportRun(out io.Writer, run embedrun.Run, cause error) error {
	if run.ID != "" {
		_ = writeLines(out, fmt.Sprintf(
			"the run stopped after %d batches, %d rows embedded; it resumes from where it is",
			run.Progress.BatchesCommitted, run.Progress.RowsEmbedded))
	}
	return cause
}
