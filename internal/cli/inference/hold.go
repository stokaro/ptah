package inference

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedrun"
)

// newPauseCommand returns "ptah inference pause".
func newPauseCommand() *cobra.Command {
	var options commonOptions
	var runID string
	var reason string
	var worker string

	cmd := &cobra.Command{
		Use:   "pause",
		Short: "Stop a run where its last checkpoint left it, and fence whoever is working on it",
		Long: `Stop a run at the boundary it has already reached.

Nothing is lost and nothing is undone. A run is durable at its last checkpoint,
so a pause leaves the work that is committed committed and the position it
reached recorded; ` + "`resume`" + ` picks it up from there.

A pause takes the run for this process, which moves the fencing token past the
worker that held it. That is what makes a pause take effect rather than take
note: without it the pause lands in a row the running worker overwrites at its
next checkpoint, and the run reads paused for a few seconds while the provider
bill goes on.

So a backfill that was running when this ran will fail at its next commit,
saying the run has a newer fencing token. That is the pause working.

The reason is required. A paused run whose reason is empty is one nobody can act
on, and "why did this stop" is the first question its operator asks.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runPause(cmd.Context(), cmd.OutOrStdout(), options, runID, reason, worker)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	cmd.Flags().StringVar(&reason, "reason", "", "Why the run is being stopped (required)")
	cmd.Flags().StringVar(&worker, "worker", "ptah-cli", "Name recorded as the lease holder")
	return cmd
}

// newResumeCommand returns "ptah inference resume".
func newResumeCommand() *cobra.Command {
	var options commonOptions
	var runID string
	var worker string

	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Return a paused run to running",
		Long: `Make a paused run workable again, and clear the reason it stopped for.

Nothing starts working here. This changes what the run is, not what is happening
to it: the verb that does the work -- ` + "`backfill`" + ` or ` + "`catchup`" + ` -- takes the run in
turn and continues from the checkpoint the pause left.

It claims the run for the same reason the pause did, and for one more. The
worker fenced by the pause is not necessarily gone: a resume that left the token
where the pause put it would return the run to running with that worker still
able to commit into it, which is the state the fence exists to prevent.

Only a paused run resumes. A run that is running, finished or failed is refused
by name rather than quietly set to running.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runResume(cmd.Context(), cmd.OutOrStdout(), options, runID, worker)
		},
	}
	addCommonFlags(cmd, &options)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	cmd.Flags().StringVar(&worker, "worker", "ptah-cli", "Name recorded as the lease holder")
	return cmd
}

// newAbandonCommand returns "ptah inference abandon".
func newAbandonCommand() *cobra.Command {
	var dbURL string
	var runID string
	var reason string

	cmd := &cobra.Command{
		Use:   "abandon",
		Short: "End one run and release its outbox position without deleting its vectors",
		Args:  cobra.NoArgs,
		Long: `Permanently end one run without retiring its generation.

The run keeps its checkpoint, progress, and generation vectors. It stops
holding shared outbox events, and a worker already running is fenced at its next
commit. The run cannot resume; start another attempt under a new run identifier.

Ending the last usable live feeder for an active generation is refused because
its vectors must keep following the source. The same guard applies inside a
maintenance window, whose rollback promise still needs a feeder. For outbox
consistency, a replacement counts only after it has a durable, readable resume
position; an unprepared or damaged sibling does not make abandonment safe.

The reason is required and appears in status. This command needs the run-state
database and run identifier, not the original specification.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAbandon(cmd.Context(), cmd.OutOrStdout(), dbURL, runID, reason)
		},
	}
	addDatabaseFlag(cmd, &dbURL)
	cmd.Flags().StringVar(&runID, "run-id", "", "Identifier of the run (required)")
	cmd.Flags().StringVar(&reason, "reason", "", "Why the run is being ended (required)")
	return cmd
}

// runPause stops the run and says what it stopped.
func runPause(
	ctx context.Context, out io.Writer, options commonOptions, runID, reason, worker string,
) error {
	if runID == "" {
		return fmt.Errorf("--run-id is required")
	}
	if reason == "" {
		return fmt.Errorf("--reason is required")
	}
	opened, err := open(ctx, options)
	if err != nil {
		return err
	}
	defer opened.close()

	run, err := embedengine.Runs{Store: opened.store, Worker: worker}.Pause(ctx, runID, reason)
	if err != nil {
		return err
	}
	return writeRunHold(out, "paused", run, reason)
}

// runResume returns the run to running.
func runResume(
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

	run, err := embedengine.Runs{Store: opened.store, Worker: worker}.Resume(ctx, runID)
	if err != nil {
		return err
	}
	return writeRunHold(out, "resumed", run, "")
}

// runAbandon ends one run and reports what remains.
func runAbandon(
	ctx context.Context, out io.Writer, dbURL, runID, reason string,
) error {
	if runID == "" {
		return fmt.Errorf("--run-id is required")
	}
	if reason == "" {
		return fmt.Errorf("--reason is required")
	}
	if err := validateDatabaseURL(dbURL); err != nil {
		return err
	}
	db, err := connectDatabase(ctx, dbURL)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	run, err := (embedengine.Runs{Store: embedpg.NewStore(db)}).Abandon(ctx, runID, reason)
	if err != nil {
		return err
	}
	return writeLines(out,
		fmt.Sprintf("abandoned run %s at phase %s, fencing token %d",
			run.ID, run.Phase, run.FencingToken),
		bullet("this command did not delete generation "+run.GenerationIdentity+" or its vectors"),
		bullet("the run no longer holds shared outbox events"),
		bullet(run.FailureDetail))
}

// writeRunHold reports what changed, including the token.
//
// The token is printed because it is the thing an operator investigating "why
// did my backfill fail" needs: the failure names a number, and this is where
// that number came from.
func writeRunHold(out io.Writer, verb string, run embedrun.Run, reason string) error {
	lines := []string{fmt.Sprintf(
		"%s run %s at phase %s, fencing token %d",
		verb, run.ID, run.Phase, run.FencingToken)}
	if reason != "" {
		lines = append(lines, bullet(reason))
	}
	return writeLines(out, lines...)
}
