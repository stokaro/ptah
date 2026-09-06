package embedengine_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// TestClaim_DoesNotRewindACheckpointCommittedWhileItWasDeciding is
// stokaro/ptah#2636.
//
// Claiming used to read the whole run, raise the token in memory and write
// every column back. A worker that committed a checkpoint between that read and
// that write was still unfenced, so its transaction landed — and the claim then
// passed the `fencing_token <= n` guard and overwrote the cursor and every
// counter with the snapshot it had read. Measured on a live backfill: twenty
// vectors committed, four checkpoints in the event trail, and a run row saying
// three batches and fifteen rows. The resumed run then read the rows behind the
// rewound cursor and paid the provider for them again, and nothing said so.
//
// The interleaving is expressed as a store whose read answers with an older
// snapshot, which is exactly what a claimer holding a pre-commit read has. A
// claim built on that snapshot writes it back; a claim that writes the lease
// alone cannot.
func TestClaim_DoesNotRewindACheckpointCommittedWhileItWasDeciding(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()

	c.Assert(store.CreateRun(ctx, aClaimableRun()), qt.IsNil)
	stale, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)

	// The working worker commits a checkpoint. It is not fenced yet, so this
	// lands, and it is the state the run is really in.
	committed := stale
	committed.Cursor = []string{"20"}
	committed.Progress.RowsScanned = 20
	committed.Progress.RowsEmbedded = 20
	committed.Progress.BatchesCommitted = 4
	c.Assert(store.SaveRun(ctx, committed), qt.IsNil)

	runs := embedengine.Runs{
		Store:  &staleReadStore{Store: store, snapshot: stale},
		Worker: "operator",
	}
	claimed, token, err := runs.Claim(ctx, "run-1")

	c.Assert(err, qt.IsNil)
	// What the claimer is handed is the committed state, not the snapshot the
	// store's read offered it.
	c.Assert(claimed.Cursor, qt.DeepEquals, []string{"20"})
	c.Assert(claimed.Progress.RowsEmbedded, qt.Equals, int64(20))
	c.Assert(claimed.Progress.BatchesCommitted, qt.Equals, int64(4))
	// And so is what the store holds afterwards, which is what a resume reads.
	stored, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Cursor, qt.DeepEquals, []string{"20"})
	c.Assert(stored.Progress.RowsEmbedded, qt.Equals, int64(20))
	c.Assert(stored.Progress.BatchesCommitted, qt.Equals, int64(4))
	// The claim still did its job: the lease moved and the token fences the
	// worker that was running.
	c.Assert(stored.LeaseOwner, qt.Equals, "operator")
	c.Assert(token, qt.Equals, stale.FencingToken+1)
	c.Assert(stored.FencingToken, qt.Equals, token)
}

// TestClaim_FencesTheWorkerItTookTheRunFrom is the control for the test above.
//
// A claim that stopped writing the run entirely would satisfy every assertion
// there and fence nobody, which is the failure the whole mechanism exists to
// prevent: a pause that takes note instead of taking effect.
func TestClaim_FencesTheWorkerItTookTheRunFrom(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aClaimableRun()), qt.IsNil)
	working, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)

	runs := embedengine.Runs{Store: store, Worker: "operator"}
	_, _, err = runs.Claim(ctx, "run-1")
	c.Assert(err, qt.IsNil)

	// The worker's next checkpoint carries the token it was given, and the
	// store is the only place that knows it has been superseded.
	working.Progress.RowsEmbedded = 25
	c.Assert(store.SaveRun(ctx, working), qt.ErrorIs, embedstore.ErrConflict)
}

// aClaimableRun is a run part-way through a backfill.
//
// The progress is non-zero on purpose: a claim that zeroed the counters would
// pass against a run that had none.
func aClaimableRun() embedrun.Run {
	return embedrun.Run{
		ID: "run-1", SpecDigest: "spec-1", GenerationIdentity: "gen-1",
		Environment: "test", Source: "public.articles",
		Target: "public.articles.embedding", ProviderProfile: "fake",
		PtahVersion: "test", PolicyDigest: "policy",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 3,
		Cursor: []string{"15"},
		Progress: embedrun.Progress{
			RowsScanned: 15, RowsEmbedded: 15, BatchesCommitted: 3,
		},
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
}

// staleReadStore answers Run with a snapshot from before the last commit.
//
// It is the claimer's own pre-commit read, made explicit. A claim that builds
// its write from what Run returns rewinds the run to this snapshot; a claim
// that writes the lease alone never consults it.
type staleReadStore struct {
	embedstore.Store
	snapshot embedrun.Run
}

// Run answers with the snapshot rather than with what the store holds.
func (s *staleReadStore) Run(_ context.Context, _ string) (embedrun.Run, error) {
	return s.snapshot, nil
}

// TestClaim_NamesTheOperationOnce covers stokaro/ptah#2648 finding 3.
//
// Both the store and this layer wrapped the failure with `claim run <id>:`, so
// an operator saw the clause twice: `claim run r: claim run r: not found: run
// r`. The doubling is why the troubleshooting page could not be keyed on the
// message -- nobody searches for a sentence that stutters.
//
// The memory store names no operation, so what this pins is that the engine
// adds none of its own: whatever the store said arrives unchanged.
func TestClaim_NamesTheOperationOnce(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	runs := embedengine.Runs{Store: embedstore.NewMemory(), Worker: "operator"}

	_, _, err := runs.Claim(ctx, "a-run-nobody-prepared")

	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	c.Assert(err.Error(), qt.Equals, "not found: run a-run-nobody-prepared")
}
