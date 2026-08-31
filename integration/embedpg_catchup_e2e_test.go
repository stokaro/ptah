//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedverify"
)

// TestEmbedPGCatchUpE2E is the whole live-source lifecycle against a real
// server: install the outbox, record the boundary, backfill, change the source
// underneath, catch up, and verify.
//
// The changes are made DURING the window the backfill covers and after it, and
// they are the changes a migration actually meets: a row updated after it was
// embedded, a row updated repeatedly, a row inserted that the backfill never
// saw, and a row deleted out from under it. What makes this a test rather than
// a demonstration is the verification at the end, which is the same
// deterministic check a cutover would run and which fails on every one of those
// four if catch-up got it wrong (stokaro/ptah#2068).
func TestEmbedPGCatchUpE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_catchup_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := liveSpec()
	seedArticles(c, ctx, db, spec)

	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)
	source, err := embedpg.NewSource(db, spec)
	c.Assert(err, qt.IsNil)
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)

	// The order is the epic's, and it is the order because of what sits in the
	// gaps: installing after recording a boundary leaves the changes between
	// them captured by nothing at all.
	c.Assert(outbox.Install(ctx), qt.IsNil)
	boundary, err := outbox.Horizon(ctx)
	c.Assert(err, qt.IsNil)

	run := embedrun.Run{
		ID: "catchup-run", SpecDigest: "spec-1", GenerationIdentity: spec.Identity().Digest,
		Environment: "test", Source: "public.articles", Target: "public.articles.embedding",
		ProviderProfile: "fake", PtahVersion: "test", PolicyDigest: "policy",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 1,
		SnapshotWatermark: strconv.FormatUint(boundary, 10),
		CreatedAt:         time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)

	engine := &embedengine.Engine{
		Spec: spec, Source: source, Provider: &liveProvider{dimension: 4},
		Target: target, Store: store,
		Bounds: embedrun.BatchBounds{MaxRows: 2, MaxInputs: 2}, Worker: "worker-a",
	}

	backfilled, _, err := engine.Backfill(ctx, "catchup-run")
	c.Assert(err, qt.IsNil)
	c.Assert(backfilled.Progress.RowsEmbedded, qt.Equals, int64(4))

	changeTheSourceUnderneath(c, ctx, db)

	caught, _, err := engine.CatchUp(ctx, "catchup-run", outbox, source)

	c.Assert(err, qt.IsNil)
	c.Assert(caught.CatchUpWatermark, qt.Not(qt.Equals), "")
	assertTheBarrierIsReached(c, ctx, outbox, caught)
	assertTheCorpusMatchesTheSourceNow(c, ctx, db, spec)
}

// changeTheSourceUnderneath makes the four changes a live migration meets.
func changeTheSourceUnderneath(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	statements := []string{
		// Embedded, then changed. The vector the backfill wrote is now stale.
		`UPDATE articles SET title = 'First rewritten', updated_at = '8' WHERE id = 1`,
		// Changed repeatedly: three events, one row, one vector that should be
		// asked for.
		`UPDATE articles SET body = 'about support v2', updated_at = '8' WHERE id = 2`,
		`UPDATE articles SET body = 'about support v3', updated_at = '9' WHERE id = 2`,
		`UPDATE articles SET body = 'about support v4', updated_at = '10' WHERE id = 2`,
		// Inserted after the backfill had passed. Nothing embedded it.
		`INSERT INTO articles (id, title, body, updated_at) VALUES (6, 'Sixth', 'about renewals', '8')`,
		// Deleted out from under a vector that was written for it.
		`DELETE FROM articles WHERE id = 4`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// assertTheBarrierIsReached asks the completion condition a cutover would ask.
func assertTheBarrierIsReached(
	c *qt.C, ctx context.Context, outbox *embedpg.Outbox, run embedrun.Run,
) {
	c.Helper()
	processed, err := embedcatchup.ParseCursor(run.CatchUpWatermark, "catch-up watermark")
	c.Assert(err, qt.IsNil)
	installed, err := outbox.Installed(ctx)
	c.Assert(err, qt.IsNil)
	unprocessed, err := outbox.Unprocessed(ctx, processed)
	c.Assert(err, qt.IsNil)
	horizon, err := outbox.Horizon(ctx)
	c.Assert(err, qt.IsNil)
	snapshot, err := strconv.ParseUint(run.SnapshotWatermark, 10, 64)
	c.Assert(err, qt.IsNil)

	barrier := embedcatchup.Barrier{
		Installed: installed, Snapshot: snapshot, Processed: processed.Transaction,
		Horizon: horizon, Unprocessed: unprocessed,
	}
	reached, blockers := barrier.Reached()

	c.Assert(reached, qt.IsTrue, qt.Commentf("%v", blockers))
	guarantee := embedcatchup.Assess(embedcatchup.ModeOutbox,
		embedcatchup.SourceState{Mutable: true}, barrier,
		embedcatchup.DualWriteEvidence{}, time.Now())
	c.Assert(guarantee.Complete, qt.IsTrue, qt.Commentf("%v", guarantee.Blockers))
}

// assertTheCorpusMatchesTheSourceNow is the check a cutover runs, over a source
// that moved while the backfill was reading it.
//
// Every one of the four changes fails a different layer if catch-up missed it:
// the rewritten row fails freshness, the repeatedly-updated row fails freshness
// at a different version, the inserted row fails coverage, and the deleted row
// fails as a target row outside the source's scope.
func assertTheCorpusMatchesTheSourceNow(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec,
) {
	c.Helper()
	source, target := readVerificationRows(c, ctx, db, spec)
	report := embedverify.Verify(
		embedverify.Expectation{
			Generation: spec.Identity().Digest,
			ColumnType: fmt.Sprintf("vector(%d)", spec.Model.ReportedDimension),
			Dimension:  spec.Model.ReportedDimension,
		},
		embedverify.Structure{
			ColumnExists: true, ColumnType: fmt.Sprintf("vector(%d)", spec.Model.ReportedDimension),
			Dimension: spec.Model.ReportedDimension, ExtensionPresent: true,
		},
		source, target,
		embedverify.RunState{SnapshotComplete: true, CatchUpReached: true, ConsistencyMode: "outbox",
			SourceMutable: true},
	)

	c.Assert(report.Blocking(), qt.HasLen, 0, qt.Commentf("%v", report.Findings))
	c.Assert(report.Passed(), qt.IsTrue)
	// Five rows now: one was deleted and one was inserted.
	c.Assert(report.SourceRows, qt.Equals, 5)
}
