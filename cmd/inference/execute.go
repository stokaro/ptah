package inference

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedprovider"
	"go.5x5.cz/ptah/internal/embedrun"
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
there is nothing to read and the cutover will refuse for the same reason.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCatchUp(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addExecuteFlags(cmd, &options)
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

// runBackfill runs the scan-embed-commit loop.
func runBackfill(ctx context.Context, out io.Writer, options executeOptions) error {
	opened, engine, err := buildEngine(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	run, err := engine.Backfill(ctx, options.runID)
	if err != nil {
		return reportRun(out, run, err)
	}
	return writeLines(out,
		fmt.Sprintf("backfill finished: %d scanned, %d embedded, %d skipped",
			run.Progress.RowsScanned, run.Progress.RowsEmbedded, run.Progress.RowsSkipped))
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
				"selects %q", modeName(opened.loaded.Mode))
	}
	outbox, err := embedpg.NewOutbox(opened.db, opened.loaded.Spec)
	if err != nil {
		return err
	}
	source, err := embedpg.NewSource(opened.db, opened.loaded.Spec)
	if err != nil {
		return err
	}

	run, err := engine.CatchUp(ctx, options.runID, outbox, source)
	if err != nil {
		return reportRun(out, run, err)
	}
	return writeLines(out,
		fmt.Sprintf("caught up to transaction %s: %d changed rows, %d tombstoned",
			boundaryText(run.CatchUpWatermark), run.Progress.RowsScanned, run.Progress.RowsDeleted))
}

// recordsChanges reports whether a mode leaves something for catch-up to read.
//
// Only the outbox does, today. The immutable mode has nothing to catch up on by
// definition, and dual write is the application's own business -- Ptah reads the
// writer's reports rather than a change log.
func recordsChanges(mode embedcatchup.Mode) bool {
	return mode == embedcatchup.ModeOutbox
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
