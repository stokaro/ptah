//go:build integration

package integration_test

// Live PostgreSQL coverage for the claim that makes fencing an event rather
// than a field.
//
// The fencing token is enforced in the store's own WHERE clause and has been
// since the store was written. Nothing ever issued a new one: `prepare` wrote
// `FencingToken: 1` into the run literal and every verb read it back unchanged,
// so a mechanism that refuses a worker the run has moved past was never given a
// run that had moved past one (stokaro/ptah#2474).

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// TestClaim_AWorkerStartingMovesTheTokenLive is the event that was missing.
//
// A run whose token never moves is a run no later worker can be fenced against.
// This asserts the number the store enforces against actually changes when a
// worker starts, and that the lease names who took it.
func TestClaim_AWorkerStartingMovesTheTokenLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, spec, store := claimFixture(c, ctx)

	before, err := store.Run(ctx, "claim-run")
	c.Assert(err, qt.IsNil)

	_, _, err = claimEngine(c, db, spec, store, "worker-b").Backfill(ctx, "claim-run")
	c.Assert(err, qt.IsNil)

	after, err := store.Run(ctx, "claim-run")
	c.Assert(err, qt.IsNil)
	c.Assert(after.FencingToken > before.FencingToken, qt.IsTrue,
		qt.Commentf("the token stayed at %d, so nothing was fenced", before.FencingToken))
	c.Assert(after.LeaseOwner, qt.Equals, "worker-b")
	c.Assert(after.LeaseExpires.After(time.Now().UTC()), qt.IsTrue,
		qt.Commentf("the lease is already expired, so the claim recorded no hold"))
}

// TestClaim_TheEarlierWorkerCannotCommitAfterwardsLive is the property the
// claim exists for.
//
// Worker A holds the run. Worker B starts. A's next commit carries the token it
// took, and the store refuses it -- not because A's lease expired, which stops
// nothing, but because the token moved past it.
func TestClaim_TheEarlierWorkerCannotCommitAfterwardsLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, spec, store := claimFixture(c, ctx)

	// A takes the run and the token it holds.
	_, _, err := claimEngine(c, db, spec, store, "worker-a").Backfill(ctx, "claim-run")
	c.Assert(err, qt.IsNil)
	held, err := store.Run(ctx, "claim-run")
	c.Assert(err, qt.IsNil)

	// B starts, which moves the token past A.
	_, _, err = claimEngine(c, db, spec, store, "worker-b").Backfill(ctx, "claim-run")
	c.Assert(err, qt.IsNil)

	// A commits with what it still believes it holds.
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	err = target.Commit(ctx, []embedrun.TargetWrite{{
		Key: []string{"1"}, Generation: spec.Identity().Digest,
		InputHash: "stale", Version: "9",
		Vector: make([]float32, spec.Model.ReportedDimension),
		Kind:   embedrun.WriteUpsert,
	}}, held)

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
}

// TestClaim_TheSecondWorkerIsNotFencedByTheFirstLive is the control.
//
// Every assertion above is satisfied by a store that refused everything after
// one claim. B took the run, so B commits.
func TestClaim_TheSecondWorkerIsNotFencedByTheFirstLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, spec, store := claimFixture(c, ctx)

	_, _, err := claimEngine(c, db, spec, store, "worker-a").Backfill(ctx, "claim-run")
	c.Assert(err, qt.IsNil)
	_, _, err = claimEngine(c, db, spec, store, "worker-b").Backfill(ctx, "claim-run")
	c.Assert(err, qt.IsNil)
	taken, err := store.Run(ctx, "claim-run")
	c.Assert(err, qt.IsNil)

	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	err = target.Commit(ctx, []embedrun.TargetWrite{{
		Key: []string{"1"}, Generation: spec.Identity().Digest,
		InputHash: "fresh", Version: "9",
		Vector: make([]float32, spec.Model.ReportedDimension),
		Kind:   embedrun.WriteUpsert,
	}}, taken)

	c.Assert(err, qt.IsNil)
}

// claimEngine builds one for a named worker.
func claimEngine(
	c *qt.C, db *sql.DB, spec embedgen.Spec, store *embedpg.Store, worker string,
) *embedengine.Engine {
	c.Helper()
	source, err := embedpg.NewSource(db, spec)
	c.Assert(err, qt.IsNil)
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	return &embedengine.Engine{
		Spec: spec, Source: source, Provider: &liveProvider{dimension: 4},
		Target: target, Store: store,
		Bounds: embedrun.BatchBounds{MaxRows: 2, MaxInputs: 2},
		Worker: worker,
	}
}

// claimFixture builds a database of its own with one prepared run.
func claimFixture(c *qt.C, ctx context.Context) (*sql.DB, embedgen.Spec, *embedpg.Store) {
	c.Helper()
	dbURL := dbtarget.URL(c, dbtarget.TimescaleDB)
	admin, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = admin.Close() })

	name := fmt.Sprintf("ptah_claim_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })

	spec := liveSpec()
	seedArticles(c, ctx, db, spec)
	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)
	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: "claim-run", SpecDigest: "spec-1", GenerationIdentity: spec.Identity().Digest,
		Environment: "test", Source: "public.articles",
		Target: "public.articles.embedding",
		Phase:  embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 1,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}), qt.IsNil)
	return db, spec, store
}
