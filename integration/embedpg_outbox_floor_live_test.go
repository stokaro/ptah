//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// floorAt is when the seeded generations were created.
var floorAt = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// TestEmbedPGOutboxFloorLive measures the bound that decides what a catch-up
// may delete from an outbox.
//
// An outbox belongs to a source table, so two generations over one table share
// it. The events that are dead are the ones EVERY live reader has passed, which
// is the minimum of their positions -- and it can only be measured against a
// live server, because the answer is a query joining two tables on a condition
// that includes rows the other table does not have.
//
// Each assertion below removes exactly one clause of that query. Together they
// are the difference between a floor and a statement that happens to be a
// number: a query returning nothing at all satisfies three of them.
//
// Plain PostgreSQL: the run and generation tables hold text and timestamps, and
// nothing here needs pgvector.
func TestEmbedPGOutboxFloorLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_floor_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)

	assertNoReaderIsNotAFloor(c, ctx, store)
	assertFloorIsTheEarliestReader(c, ctx, store)
	assertAnotherSourceDoesNotLowerTheFloor(c, ctx, store)
	assertARetiredGenerationDoesNotLowerTheFloor(c, ctx, store)
	assertAnUncaughtRunReadsFromItsBoundary(c, ctx, store)
	assertARunWithNoPositionIsSkipped(c, ctx, store)
}

// assertNoReaderIsNotAFloor is the control the others need.
//
// An empty reader set has to report absence rather than the zero cursor. Zero
// as a floor is not a conservative answer, it is the whole table: every event
// ever captured sits above it.
func assertNoReaderIsNotAFloor(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	floor, ok, err := store.OutboxFloor(ctx, "nothing_reads_this")
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsFalse)
	c.Assert(floor, qt.Equals, embedcatchup.Cursor{})
}

// assertFloorIsTheEarliestReader is the property the whole change rests on.
//
// Two live generations over one source, at different positions. The floor is
// the one that has read LESS, because the events between them are still owed by
// somebody.
func assertFloorIsTheEarliestReader(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	seedReader(c, ctx, store, "gen-ahead", "run-ahead", "articles", "4446", floorAt)
	seedReader(c, ctx, store, "gen-behind", "run-behind", "articles", "1200", floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
}

// assertAnotherSourceDoesNotLowerTheFloor keys the answer to the source table.
//
// Without this a query that ignored its argument would satisfy every other
// assertion here, and in production one busy migration would pin the floor of
// every other outbox on the server.
func assertAnotherSourceDoesNotLowerTheFloor(
	c *qt.C, ctx context.Context, store *embedpg.Store,
) {
	c.Helper()
	seedReader(c, ctx, store, "gen-elsewhere", "run-elsewhere", "invoices", "7", floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
}

// assertARetiredGenerationDoesNotLowerTheFloor is what lets an outbox ever
// shrink.
//
// A retired generation reads nothing, so it stops holding events. Retirement is
// the operator's lever here, and this is the assertion that says the lever is
// connected.
func assertARetiredGenerationDoesNotLowerTheFloor(
	c *qt.C, ctx context.Context, store *embedpg.Store,
) {
	c.Helper()
	seedReader(c, ctx, store, "gen-retired", "run-retired", "articles", "3", floorAt)
	c.Assert(store.RetireGeneration(ctx, "gen-retired", floorAt), qt.IsNil)

	floor, ok, err := store.OutboxFloor(ctx, "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
}

// assertAnUncaughtRunReadsFromItsBoundary counts a prepared run as a reader.
//
// A generation that has been prepared and backfilled but never caught up owes
// every change since its snapshot boundary. Reading only the catch-up watermark
// would leave it out of the reader set and delete the whole backlog it has yet
// to process -- and the other assertions here cannot see that, because a query
// selecting only caught-up runs still answers them correctly.
func assertAnUncaughtRunReadsFromItsBoundary(
	c *qt.C, ctx context.Context, store *embedpg.Store,
) {
	c.Helper()
	seedRun(c, ctx, store, "gen-fresh", "run-fresh", "articles", embedrun.Run{
		SnapshotWatermark: "44",
	}, floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor, qt.Equals, embedcatchup.Cursor{Transaction: 44})
}

// assertARunWithNoPositionIsSkipped keeps a run that watches nothing out of the
// reader set.
//
// prepare writes both watermarks empty for a mode that records no changes. Such
// a run is not a reader sitting at zero; treating it as one would take the
// floor to zero and authorize deleting the whole table -- the one mistake with
// no recovery, which is why it is asserted rather than reasoned about.
func assertARunWithNoPositionIsSkipped(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	seedRun(c, ctx, store, "gen-immutable", "run-immutable", "articles", embedrun.Run{}, floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor, qt.Equals, embedcatchup.Cursor{Transaction: 44})
}

// seedReader registers a live generation and a run that has caught up to a
// position.
func seedReader(
	c *qt.C, ctx context.Context, store *embedpg.Store,
	generation, runID, source, watermark string, at time.Time,
) {
	c.Helper()
	seedRun(c, ctx, store, generation, runID, source, embedrun.Run{
		SnapshotWatermark: "1", CatchUpWatermark: watermark,
	}, at)
}

// seedRun registers a generation and the run over it, taking the watermarks
// from the caller.
func seedRun(
	c *qt.C, ctx context.Context, store *embedpg.Store,
	generation, runID, source string, positioned embedrun.Run, at time.Time,
) {
	c.Helper()
	_, err := store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: generation, SpecDigest: generation, Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: source, TargetColumn: "embedding", CreatedAt: at,
	})
	c.Assert(err, qt.IsNil)
	run := positioned
	run.ID = runID
	run.GenerationIdentity = generation
	run.SpecDigest = generation
	run.Source = source
	run.Phase = embedrun.PhasePrepared
	run.Status = embedrun.StatusRunning
	run.CreatedAt = at
	run.UpdatedAt = at
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
}
