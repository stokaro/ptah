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

	"ptah.run/internal/cli/internal/providercommand"
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
cutting over does not make the old generation disposable.

The database is PostgreSQL with pgvector, YDB, or an engine whose executable
provider keeps an embedding store, and the --db-url scheme decides which. Each
keeps the run state in tables of its own beside the vectors. On YDB a vector is
stored as a float vector in a String column and indexed with vector_kmeans_tree,
and the consistency mode is immutable: the outbox mode records changes with row
triggers, which YDB does not have.

--provider-config and --provider-engine select an executable provider, as they
do for ptah schema and ptah migrations. The provider then owns the run-state
layout, the vector storage, the index and the searches, and builds immutable
generations; Ptah keeps every lifecycle decision.`,
		SilenceUsage: true,
	}

	cmd.AddCommand(withProvider(newDescribeCommand()))
	// probe asks the embedding endpoint and reaches no database, so it has no
	// engine for a provider to serve.
	cmd.AddCommand(newProbeCommand())
	for _, verb := range []*cobra.Command{
		newPlanCommand(), newPrepareCommand(), newBackfillCommand(), newCatchUpCommand(), newIndexCommand(),
		newPauseCommand(), newResumeCommand(), newAbandonCommand(), newVerifyCommand(), newEvaluateCommand(),
		newStatusCommand(), newCutoverCommand(), newRollbackCommand(), newRetireCommand(),
	} {
		cmd.AddCommand(withProvider(verb))
	}
	return cmd
}

// withProvider gives a verb the shared --provider-config and
// --provider-engine flags. A selected provider serves the verb's database URL
// and, for describe, the engine the target objects are described for. A
// failure to select or start one is the verb's own error.
func withProvider(verb *cobra.Command) *cobra.Command {
	return providercommand.Configure(verb, func(_ *cobra.Command, err error) error { return err })
}
