//go:build integration

package integration_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedconnect"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedspec"
	"ptah.run/internal/embedstore"
)

// TestYDBStore_ConcurrentLifecycleLive is the YDB store under the races the
// PostgreSQL store answers with locks: claims taken at once, a worker whose
// lease was taken, a commit from that worker, and two cutovers expecting the
// same pointer. YDB answers them with serializable transactions that abort on
// a conflict and run again, and every answer has to be the one the rules give.
func TestYDBStore_ConcurrentLifecycleLive(t *testing.T) {
	for _, line := range inferenceYDBLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			dbURL := enterYDBRealm(c, ctx, line)
			db := openYDBRealm(c, ctx, dbURL)
			seedYDBArticles(c, ctx, db)
			backend, err := embedconnect.Open(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = backend.Close() })
			c.Assert(backend.EnsureSchema(ctx), qt.IsNil)

			assertConcurrentClaimsEachTakeAToken(c, ctx, backend)
			assertAFencedWorkerCannotSave(c, ctx, backend)
			assertAFencedWorkerCannotCommitVectors(c, ctx, backend)
			assertOnlyOneFirstCutoverWins(c, ctx, backend)
		})
	}
}

// ydbStoreRun is a run in mid-backfill over the realm's articles table.
func ydbStoreRun(id, generation string) embedrun.Run {
	return embedrun.Run{
		ID: id, SpecDigest: generation, GenerationIdentity: generation,
		Environment: "test", Source: embedstore.SourceIdentity("", "articles"), Target: "articles.embedding",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
}

// ydbStoreGeneration is a registry row for the realm's articles table.
func ydbStoreGeneration(identity string) embedstore.Generation {
	return embedstore.Generation{
		Identity: identity, SpecDigest: identity, Name: identity, Reproducibility: "full", Dimension: 4,
		TargetTable: "articles", TargetColumn: "embedding_" + identity, SourceTable: "articles",
		ConsistencyMode: "immutable", CreatedAt: time.Now().UTC(),
	}
}

// assertConcurrentClaimsEachTakeAToken claims one run from eight workers at
// once. Every claim lands, each with a token of its own: a claim that read the
// token another one was writing would compute the same successor, and two
// workers would hold the run at once.
func assertConcurrentClaimsEachTakeAToken(c *qt.C, ctx context.Context, backend embedbackend.Backend) {
	c.Helper()
	store := backend.Store()
	_, err := store.RegisterGeneration(ctx, ydbStoreGeneration("claims"))
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, ydbStoreRun("claimed", "claims")), qt.IsNil)

	const workers = 8
	tokens := make([]int64, workers)
	errs := make([]error, workers)
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Go(func() {
			_, tokens[worker], errs[worker] = store.ClaimRun(ctx, "claimed", "worker", time.Now().Add(time.Minute))
		})
	}
	wait.Wait()

	c.Assert(errs, qt.DeepEquals, make([]error, workers))
	slices.Sort(tokens)
	c.Assert(tokens, qt.DeepEquals, []int64{1, 2, 3, 4, 5, 6, 7, 8})
	stored, err := store.Run(ctx, "claimed")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.FencingToken, qt.Equals, int64(workers))
}

// assertAFencedWorkerCannotSave takes a run over from a worker and then has the
// worker save with the token it still holds: the save is refused, and the run
// keeps the taker's state.
func assertAFencedWorkerCannotSave(c *qt.C, ctx context.Context, backend embedbackend.Backend) {
	c.Helper()
	store := backend.Store()
	_, err := store.RegisterGeneration(ctx, ydbStoreGeneration("fenced"))
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, ydbStoreRun("fenced-run", "fenced")), qt.IsNil)
	stale, _, err := store.ClaimRun(ctx, "fenced-run", "worker-a", time.Now().Add(time.Minute))
	c.Assert(err, qt.IsNil)
	_, _, err = store.ClaimRun(ctx, "fenced-run", "worker-b", time.Now().Add(time.Minute))
	c.Assert(err, qt.IsNil)

	stale.Progress.RowsScanned = 3
	c.Assert(store.SaveRun(ctx, stale), qt.ErrorIs, embedstore.ErrConflict)

	stored, err := store.Run(ctx, "fenced-run")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.LeaseOwner, qt.Equals, "worker-b")
	c.Assert(stored.Progress.RowsScanned, qt.Equals, int64(0))
}

// assertAFencedWorkerCannotCommitVectors is the same takeover at the write
// path: the batch a fenced worker commits carries its vectors and its
// checkpoint in one transaction, and the refusal of the checkpoint leaves no
// vector behind.
func assertAFencedWorkerCannotCommitVectors(c *qt.C, ctx context.Context, backend embedbackend.Backend) {
	c.Helper()
	endpoint := httptest.NewServer(http.HandlerFunc(topicEmbeddingsHandler(c)))
	defer endpoint.Close()
	loaded, err := embedspec.Load(writeYDBSpec(c, ydbSpec{endpoint: endpoint.URL, column: "embedding_fenced", revision: "9"}))
	c.Assert(err, qt.IsNil)
	spec := loaded.Spec
	identity := spec.Identity().Digest
	generation := embedstore.Generation{
		Identity: identity, SpecDigest: loaded.Digest, SpecDocument: string(loaded.Document),
		Name: spec.Name, Reproducibility: string(spec.Identity().Reproducibility), Dimension: 4,
		TargetTable: "articles", TargetColumn: "embedding_fenced", SourceTable: "articles",
		ConsistencyMode: string(loaded.Mode), CreatedAt: time.Now().UTC(),
	}
	run := ydbStoreRun("commit-run", identity)
	run.Phase = embedrun.PhasePrepared
	run.Target = "articles.embedding_fenced"
	_, err = backend.PrepareRun(ctx, spec, generation, run, loaded.Mode)
	c.Assert(err, qt.IsNil)

	store := backend.Store()
	stale, _, err := store.ClaimRun(ctx, "commit-run", "worker-a", time.Now().Add(time.Minute))
	c.Assert(err, qt.IsNil)
	_, _, err = store.ClaimRun(ctx, "commit-run", "worker-b", time.Now().Add(time.Minute))
	c.Assert(err, qt.IsNil)
	target, err := backend.NewTarget(spec)
	c.Assert(err, qt.IsNil)

	stale.Progress.BatchesCommitted, stale.Progress.RowsScanned = 1, 1
	stale.Cursor = []string{"1"}
	err = target.Commit(ctx, []embedrun.TargetWrite{{
		Key: []string{"1"}, Generation: identity, InputHash: "hash", Kind: embedrun.WriteUpsert,
		Vector: []float32{1, 0, 0, 0},
	}}, stale)

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	rows, err := backend.CountGenerationRows(ctx, generation)
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.Equals, 0)
}

// assertOnlyOneFirstCutoverWins moves one pointer from nothing to two
// generations at once. The pointer is a compare-and-set: one move lands and
// the other is refused, because it expected no pointer and found one.
func assertOnlyOneFirstCutoverWins(c *qt.C, ctx context.Context, backend embedbackend.Backend) {
	c.Helper()
	store := backend.Store()
	for _, identity := range []string{"left", "right"} {
		_, err := store.RegisterGeneration(ctx, ydbStoreGeneration(identity))
		c.Assert(err, qt.IsNil)
	}
	errs := make([]error, 2)
	var wait sync.WaitGroup
	for index, identity := range []string{"left", "right"} {
		wait.Go(func() {
			errs[index] = store.MovePointer(ctx, embedstore.Pointer{
				TargetTable: "articles", Active: identity, CutOverAt: time.Now().UTC(),
			}, "")
		})
	}
	wait.Wait()

	// Exactly one of the two landed, and the other was refused as a conflict.
	c.Assert(errs[0] == nil, qt.Not(qt.Equals), errs[1] == nil, qt.Commentf("%v", errs))
	c.Assert(errors.Is(errs[0], embedstore.ErrConflict) || errors.Is(errs[1], embedstore.ErrConflict), qt.IsTrue,
		qt.Commentf("%v", errs))
	pointer, err := store.Pointer(ctx, "", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert([]string{"left", "right"}, qt.Any(qt.Equals), pointer.Active)
}
