// Package inference implements "ptah inference", which plans, runs, verifies
// and cuts over an embedding-generation migration.
//
// The verbs mirror the lifecycle rather than the implementation: each one is a
// phase an operator decides about separately, and the separation is the point.
// A single "migrate" that resolved, backfilled and cut over would take the three
// decisions the epic keeps apart and make them one (stokaro/ptah#2068).
package inference

import (
	"github.com/spf13/cobra"
)

// NewCommand returns the inference command tree.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inference",
		Short: "Plan, run and cut over embedding-generation migrations",
		Long: `Manage the persistent state an inference path reads.

Changing an embedding model rewrites every vector in a corpus. The schema change
is the smallest part of it: the rest is execution over data, with a provider
outside the database in the middle of the loop, and a result that cannot be
derived from the input.

The verbs follow the lifecycle. Each one is a decision an operator takes
separately:

  describe   what a specification says, without a database
  probe      what the embedding provider answers, without sending a source row
  plan       what would happen, and where each of its answers came from
  prepare    create the target column, its metadata and the outbox
  backfill   embed the source into the new generation, resumably
  catchup    process the source changes made while the backfill ran
  index      build the vector index and wait for it to be valid
  pause      stop a run where its last checkpoint left it
  resume     return a paused run to running
  abandon    end a run and release its outbox position without deleting vectors
  verify     the deterministic checks a cutover rests on
  evaluate   what the generation actually retrieves, against a corpus you wrote
  status     what a run has done and what it is waiting for
  cutover    make the new generation the one queries read
  rollback   put the previous generation back
  retire     destroy a generation, which cannot be undone

None of them is implied by another. A backfill finishing does not mean the
corpus is right; verification passing does not mean anything has cut over; and
cutting over does not make the old generation disposable.`,
		SilenceUsage: true,
	}

	cmd.AddCommand(newDescribeCommand())
	cmd.AddCommand(newProbeCommand())
	cmd.AddCommand(newPlanCommand())
	cmd.AddCommand(newPrepareCommand())
	cmd.AddCommand(newBackfillCommand())
	cmd.AddCommand(newCatchUpCommand())
	cmd.AddCommand(newIndexCommand())
	cmd.AddCommand(newPauseCommand())
	cmd.AddCommand(newResumeCommand())
	cmd.AddCommand(newAbandonCommand())
	cmd.AddCommand(newVerifyCommand())
	cmd.AddCommand(newEvaluateCommand())
	cmd.AddCommand(newStatusCommand())
	cmd.AddCommand(newCutoverCommand())
	cmd.AddCommand(newRollbackCommand())
	cmd.AddCommand(newRetireCommand())
	return cmd
}
