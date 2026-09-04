//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
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

// TestEmbedPGStoreE2E holds the PostgreSQL store to the same rules the
// in-memory one is tested against.
//
// The in-memory store proves the rules are expressible. This proves they
// survive being written as SQL, and every one of them is a WHERE clause rather
// than a read followed by a write -- which is the difference between a rule and
// a race. A store whose fencing check was `SELECT` then `UPDATE` passes every
// single-threaded test ever written.
//
// It runs against the plain PostgreSQL target because none of these tables
// holds a vector: the store is text, integers and timestamps, and it has to
// work wherever a run does (stokaro/ptah#2068).
func TestEmbedPGStoreE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_embedstore_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)
	// Twice, because a worker starting is the normal time to call it and
	// several of them start at once.
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)

	// One database and one schema for all of them, because creating a database
	// per rule is a minute of setup for tests that take a second, and every
	// rule below addresses its own rows.
	assertRunRoundTrips(c, ctx, store)
	assertFencingRefusesStaleWrites(c, ctx, store)
	assertSavePreservesResumeProgressAndLifecycle(c, ctx, store)
	assertSaveCannotCompleteRun(c, ctx, store)
	assertAClaimWritesTheLeaseAndNothingElse(c, ctx, store)
	assertAbandonmentIsAtomicAndKeepsAFeeder(c, ctx, db, store)
	assertOutboxProtectionRequiresUsableFeeders(c, ctx, db, store)
	assertCutoverReturnsCommittedMaintenanceWindow(c, ctx, db, store)
	assertRegistrationIsIdempotent(c, ctx, store)
	assertRegistrationCannotHideALiveOrTerminalOnlyRun(c, ctx, db, store)
	assertRetirementIsTerminal(c, ctx, db, store)
	assertRetirementUsesDatabaseTime(c, ctx, db, store)
	assertRetirementRollsBackDDLAndRunState(c, ctx, db, store)
	assertRetirementRecountsAfterAnInFlightTargetWrite(c, ctx, db, store)
	assertRetirementKeepsAnOutboxForAnOrphanReader(c, ctx, db, store)
	assertCrossedRetirementsLockPhysicalRelationsInOneOrder(c, ctx, db, store)
	assertPointerIsCompareAndSet(c, ctx, store)
	assertPointerMoveAndRetirementCannotCross(c, ctx, db, store)
	assertRollbackIsAtomicAndRevalidatesMaintenance(c, ctx, db, store)
	assertRollbackCannotOutwaitItsDeadlines(c, ctx, db, store)
	assertEventTrailIsOrdered(c, ctx, store)
	assertAbsenceIsNotEmptiness(c, ctx, db, store)
}

// assertAbandonmentIsAtomicAndKeepsAFeeder exercises the PostgreSQL boundary
// added for issue #2723. The targeted UPDATE must preserve the checkpoint, and
// the generation lock must make the last-feeder decision agree for concurrent
// callers.
func assertAbandonmentIsAtomicAndKeepsAFeeder(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	run := liveRun("abandon")
	registerLiveGeneration(c, ctx, store, run.GenerationIdentity, "abandon_articles")
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.AbandonRun(ctx, run.ID, "superseded")
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		c.Assert(err, qt.IsNil)
	}
	abandoned, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(abandoned.Status, qt.Equals, embedrun.StatusAbandoned)
	c.Assert(abandoned.FencingToken, qt.Equals, run.FencingToken+1)
	c.Assert(abandoned.Cursor, qt.DeepEquals, run.Cursor)
	c.Assert(abandoned.Progress, qt.DeepEquals, run.Progress)
	c.Assert(abandoned.SnapshotWatermark, qt.Equals, run.SnapshotWatermark)
	c.Assert(abandoned.CatchUpWatermark, qt.Equals, run.CatchUpWatermark)
	c.Assert(abandoned.FailureDetail, qt.Equals, "superseded")
	c.Assert(store.SaveRun(ctx, run), qt.ErrorIs, embedstore.ErrConflict)
	_, token, err := store.ClaimRun(ctx, run.ID, "late worker", liveAt.Add(time.Hour))
	c.Assert(err, qt.ErrorIs, embedrun.ErrTerminal)
	c.Assert(token, qt.Equals, int64(0))
	afterClaim, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(afterClaim.FencingToken, qt.Equals, abandoned.FencingToken)
	c.Assert(afterClaim.FailureDetail, qt.Equals, abandoned.FailureDetail)

	// A terminal-only generation cannot be made active or maintained.
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "abandon_articles",
		Active: run.GenerationIdentity, CutOverAt: liveAt,
	}, ""), qt.ErrorIs, embedstore.ErrNoLiveRun)
	c.Assert(store.Maintain(ctx, run.GenerationIdentity, liveAt.Add(time.Hour)),
		qt.ErrorIs, embedstore.ErrNoLiveRun)

	protected := liveRun("protected-first")
	protected.GenerationIdentity = "gen-protected"
	registerLiveGeneration(c, ctx, store, protected.GenerationIdentity, "protected_articles")
	c.Assert(store.CreateRun(ctx, protected), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "protected_articles",
		Active: protected.GenerationIdentity, CutOverAt: liveAt,
	}, ""), qt.IsNil)
	_, err = store.AbandonRun(ctx, protected.ID, "obsolete")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)

	other := protected
	other.ID = "protected-second"
	other.FencingToken = 1
	c.Assert(store.CreateRun(ctx, other), qt.IsNil)
	_, err = store.AbandonRun(ctx, protected.ID, "the second run remains")
	c.Assert(err, qt.IsNil)

	clockRun := liveRun("abandon-clock")
	registerLiveGeneration(c, ctx, store, clockRun.GenerationIdentity, "abandon_clock_articles")
	c.Assert(store.CreateRun(ctx, clockRun), qt.IsNil)
	var databaseNow time.Time
	c.Assert(db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow), qt.IsNil)
	maintainedUntil := databaseNow.UTC().Add(time.Hour)
	c.Assert(store.Maintain(ctx, clockRun.GenerationIdentity, maintainedUntil), qt.IsNil)
	_, err = store.AbandonRun(ctx, clockRun.ID, "protected by database clock")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
	c.Assert(store.Maintain(ctx, clockRun.GenerationIdentity, time.Time{}), qt.IsNil)
	clockAbandoned, err := store.AbandonRun(ctx, clockRun.ID, "database clock wins")
	c.Assert(err, qt.IsNil)
	c.Assert(clockAbandoned.UpdatedAt.After(databaseNow.UTC()), qt.IsTrue)
	c.Assert(clockAbandoned.UpdatedAt.Equal(databaseNow.Add(-time.Hour)), qt.IsFalse)

	assertCutoverWindowIsAtomicAgainstAbandonment(c, ctx, db, store)
}

func assertCutoverWindowIsAtomicAgainstAbandonment(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	oldRun := liveRun("atomic-old")
	oldRun.GenerationIdentity = "gen-atomic-old"
	newRun := liveRun("atomic-new")
	newRun.GenerationIdentity = "gen-atomic-new"
	newRun.Phase = embedrun.PhaseVerified
	registerLiveGeneration(c, ctx, store, oldRun.GenerationIdentity, "atomic_articles")
	registerLiveGeneration(c, ctx, store, newRun.GenerationIdentity, "atomic_articles")
	c.Assert(store.CreateRun(ctx, oldRun), qt.IsNil)
	c.Assert(store.CreateRun(ctx, newRun), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "atomic_articles",
		Active: oldRun.GenerationIdentity, CutOverAt: liveAt,
	}, ""), qt.IsNil)

	blocker, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = blocker.ExecContext(ctx, `SELECT active_generation FROM `+embedstore.PointerTable+`
		WHERE target_schema = 'public' AND target_table = 'atomic_articles' FOR UPDATE`)
	c.Assert(err, qt.IsNil)
	var blockedAt time.Time
	c.Assert(db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&blockedAt), qt.IsNil)
	blockedAt = blockedAt.UTC()
	stabilizeFor := 2 * time.Second
	type moveCall struct {
		move embedstore.CutoverMove
		err  error
	}
	moveResult := make(chan moveCall, 1)
	abandonResult := make(chan error, 1)
	go func() {
		move, callErr := store.MovePointerWithMaintenance(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: "atomic_articles",
			Active: newRun.GenerationIdentity, Previous: oldRun.GenerationIdentity,
			CutOverAt: liveAt.Add(time.Minute),
		}, oldRun.GenerationIdentity, newRun.ID, stabilizeFor)
		moveResult <- moveCall{move: move, err: callErr}
	}()
	waitForBlockedQuery(c, ctx, db, "SELECT active_generation, COALESCE(previous_generation")
	go func() {
		_, err := store.AbandonRun(ctx, oldRun.ID, "obsolete")
		abandonResult <- err
	}()
	waitForBlockedQuery(c, ctx, db, "pg_advisory_xact_lock")
	select {
	case callErr := <-abandonResult:
		c.Assert(false, qt.IsTrue,
			qt.Commentf("abandonment completed before the pointer transaction: %v", callErr))
	default:
	}
	waitUntilDatabaseTime(c, ctx, db, blockedAt.Add(stabilizeFor+100*time.Millisecond))
	c.Assert(blocker.Commit(), qt.IsNil)

	move := <-moveResult
	c.Assert(move.err, qt.IsNil)
	c.Assert(move.move.PreviousMaintainedUntil.Sub(move.move.CutOverAt), qt.Equals, stabilizeFor)
	c.Assert(<-abandonResult, qt.ErrorIs, embedstore.ErrNoLiveRun)
	pointer, err := store.Pointer(ctx, "public", "atomic_articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, newRun.GenerationIdentity)
	oldGeneration, err := store.Generation(ctx, oldRun.GenerationIdentity)
	c.Assert(err, qt.IsNil)
	c.Assert(oldGeneration.MaintainedUntil, qt.Equals, move.move.PreviousMaintainedUntil)
	c.Assert(pointer.CutOverAt, qt.Equals, move.move.CutOverAt)
	storedOldRun, err := store.Run(ctx, oldRun.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(storedOldRun.Terminal(), qt.IsFalse)
	storedNewRun, err := store.Run(ctx, newRun.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(storedNewRun.LeaseOwner, qt.Equals, "")
	c.Assert(storedNewRun.LeaseExpires.IsZero(), qt.IsTrue)
}

func assertCutoverReturnsCommittedMaintenanceWindow(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	tests := []struct {
		name         string
		stabilizeFor time.Duration
		wantClear    bool
	}{
		{name: "keeps-later", stabilizeFor: time.Hour},
		{name: "zero-clears", wantClear: true},
	}
	for i, test := range tests {
		oldRun := liveRun(fmt.Sprintf("window-old-%d", i))
		oldRun.GenerationIdentity = fmt.Sprintf("gen-window-old-%d", i)
		newRun := liveRun(fmt.Sprintf("window-new-%d", i))
		newRun.GenerationIdentity = fmt.Sprintf("gen-window-new-%d", i)
		newRun.Phase = embedrun.PhaseVerified
		table := fmt.Sprintf("window_%d_articles", i)
		registerLiveGeneration(c, ctx, store, oldRun.GenerationIdentity, table)
		registerLiveGeneration(c, ctx, store, newRun.GenerationIdentity, table)
		c.Assert(store.CreateRun(ctx, oldRun), qt.IsNil)
		c.Assert(store.CreateRun(ctx, newRun), qt.IsNil)
		var databaseNow time.Time
		c.Assert(db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow), qt.IsNil)
		later := databaseNow.UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
		c.Assert(store.Maintain(ctx, oldRun.GenerationIdentity, later), qt.IsNil)
		c.Assert(store.MovePointer(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: table, Active: oldRun.GenerationIdentity,
		}, ""), qt.IsNil)

		move, err := store.MovePointerWithMaintenance(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: table, Active: newRun.GenerationIdentity,
			Previous: oldRun.GenerationIdentity,
		}, oldRun.GenerationIdentity, newRun.ID, test.stabilizeFor)
		c.Assert(err, qt.IsNil, qt.Commentf("case %s", test.name))
		stored, err := store.Generation(ctx, oldRun.GenerationIdentity)
		c.Assert(err, qt.IsNil)
		if test.wantClear {
			c.Assert(move.PreviousMaintainedUntil.IsZero(), qt.IsTrue)
			c.Assert(stored.MaintainedUntil.IsZero(), qt.IsTrue)
		} else {
			c.Assert(move.PreviousMaintainedUntil, qt.Equals, later)
			c.Assert(stored.MaintainedUntil, qt.Equals, later)
		}
	}
}

func assertOutboxProtectionRequiresUsableFeeders(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	tests := []struct {
		name       string
		positioned bool
		mutateSQL  string
		wantOK     bool
	}{
		{name: "positioned", positioned: true, wantOK: true},
		{name: "unpositioned"},
		{name: "malformed", positioned: true,
			mutateSQL: `UPDATE ` + embedstore.RunTable + ` SET catch_up_watermark = 'bad' WHERE id = $1`},
		{name: "wrong-source", positioned: true,
			mutateSQL: `UPDATE ` + embedstore.RunTable + ` SET source = 'wrong' WHERE id = $1`},
	}
	for i, test := range tests {
		identity := fmt.Sprintf("gen-usable-%d", i)
		table := fmt.Sprintf("usable_%d_articles", i)
		generation := embedstore.Generation{
			Identity: identity, SpecDigest: "spec-" + identity,
			Reproducibility: "full", Dimension: 8,
			TargetSchema: "public", TargetTable: table, TargetColumn: "embedding",
			SourceSchema: "public", SourceTable: "articles",
			ConsistencyMode: "outbox", CreatedAt: liveAt,
		}
		_, err := store.RegisterGeneration(ctx, generation)
		c.Assert(err, qt.IsNil)
		current := liveRun(fmt.Sprintf("usable-current-%d", i))
		current.GenerationIdentity = identity
		c.Assert(store.CreateRun(ctx, current), qt.IsNil)
		sibling := liveRun(fmt.Sprintf("usable-sibling-%d", i))
		sibling.GenerationIdentity = identity
		if !test.positioned {
			sibling.SnapshotWatermark, sibling.CatchUpWatermark = "", ""
		}
		c.Assert(store.CreateRun(ctx, sibling), qt.IsNil)
		if test.mutateSQL != "" {
			_, err = db.ExecContext(ctx, test.mutateSQL, sibling.ID)
			c.Assert(err, qt.IsNil)
		}
		c.Assert(store.MovePointer(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: table, Active: identity,
		}, ""), qt.IsNil)
		_, err = store.AbandonRun(ctx, current.ID, "superseded")
		if test.wantOK {
			c.Assert(err, qt.IsNil, qt.Commentf("case %s", test.name))
		} else {
			c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun,
				qt.Commentf("case %s", test.name))
		}
	}

	// Lifecycle protection itself uses the same definition. An unpositioned
	// outbox history cannot become active, maintained, or imported as maintained.
	identity := "gen-unusable-lifecycle"
	generation := embedstore.Generation{
		Identity: identity, SpecDigest: "spec-" + identity,
		Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: "unusable_lifecycle_articles",
		TargetColumn: "embedding", SourceSchema: "public", SourceTable: "articles",
		ConsistencyMode: "outbox", CreatedAt: liveAt,
	}
	_, err := store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	unpositioned := liveRun("unusable-lifecycle")
	unpositioned.GenerationIdentity = identity
	unpositioned.SnapshotWatermark, unpositioned.CatchUpWatermark = "", ""
	c.Assert(store.CreateRun(ctx, unpositioned), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: generation.TargetTable, Active: identity,
	}, ""), qt.ErrorIs, embedstore.ErrNoLiveRun)
	c.Assert(store.Maintain(ctx, identity, time.Now().UTC().Add(time.Hour)),
		qt.ErrorIs, embedstore.ErrNoLiveRun)

	importRun := liveRun("unusable-maintained-import")
	importRun.GenerationIdentity = "gen-unusable-maintained-import"
	importRun.SnapshotWatermark, importRun.CatchUpWatermark = "", ""
	c.Assert(store.CreateRun(ctx, importRun), qt.IsNil)
	imported := generation
	imported.Identity = importRun.GenerationIdentity
	imported.TargetTable = "unusable_maintained_import_articles"
	imported.MaintainedUntil = time.Now().UTC().Add(time.Hour)
	_, err = store.RegisterGeneration(ctx, imported)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
}

func waitForBlockedQuery(c *qt.C, ctx context.Context, db *sql.DB, fragment string) {
	c.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock' AND query LIKE '%' || $1 || '%'`, fragment).Scan(&waiting)
		c.Assert(err, qt.IsNil)
		if waiting > 0 {
			return
		}
		c.Assert(time.Now().Before(deadline), qt.IsTrue,
			qt.Commentf("no blocked query containing %q", fragment))
		time.Sleep(10 * time.Millisecond)
	}
}

func registerLiveGeneration(
	c *qt.C, ctx context.Context, store *embedpg.Store, identity, table string,
) {
	c.Helper()
	_, err := store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: identity, SpecDigest: "spec-" + identity, Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: table, TargetColumn: "embedding", CreatedAt: liveAt,
		SourceSchema: "public", SourceTable: "articles",
	})
	c.Assert(err, qt.IsNil)
}

// assertAClaimWritesTheLeaseAndNothingElse is stokaro/ptah#2636 at the
// statement.
//
// A claim used to write every column of the run, so a checkpoint committed
// between the claimer's read and its write was erased. The statement now names
// the lease columns alone and derives the token from the stored value, so there
// is no snapshot for it to write back and no window in which to hold one.
//
// The run is created mid-backfill, with a cursor and non-zero counters, because
// a claim that zeroed them would pass against a run that had none.
func assertAClaimWritesTheLeaseAndNothingElse(
	c *qt.C, ctx context.Context, store *embedpg.Store,
) {
	c.Helper()
	run := liveRun("claim-lease")
	registerLiveGeneration(c, ctx, store, run.GenerationIdentity, "claim_lease_articles")
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	before, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(before.Progress.RowsEmbedded > 0, qt.IsTrue,
		qt.Commentf("the fixture must carry progress, or this asserts nothing"))
	c.Assert(before.Cursor, qt.Not(qt.HasLen), 0)

	expires := liveAt.Add(time.Hour)
	claimed, token, err := store.ClaimRun(ctx, run.ID, "operator", expires)

	c.Assert(err, qt.IsNil)
	c.Assert(token, qt.Equals, before.FencingToken+1)
	c.Assert(claimed.FencingToken, qt.Equals, token)
	c.Assert(claimed.LeaseOwner, qt.Equals, "operator")
	c.Assert(claimed.LeaseExpires.UTC(), qt.Equals, expires.UTC())
	// Everything the run was doing is untouched, in the returned copy and in
	// the row a resume would read.
	c.Assert(claimed.Cursor, qt.DeepEquals, before.Cursor)
	c.Assert(claimed.Progress, qt.DeepEquals, before.Progress)
	c.Assert(claimed.Phase, qt.Equals, before.Phase)
	c.Assert(claimed.Status, qt.Equals, before.Status)
	c.Assert(claimed.SnapshotWatermark, qt.Equals, before.SnapshotWatermark)
	c.Assert(claimed.CatchUpWatermark, qt.Equals, before.CatchUpWatermark)

	stored, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Cursor, qt.DeepEquals, before.Cursor)
	c.Assert(stored.Progress, qt.DeepEquals, before.Progress)
	c.Assert(stored.FencingToken, qt.Equals, token)

	// A second claim moves the token again, which is what makes two operators
	// racing for a run resolve rather than tie.
	_, second, err := store.ClaimRun(ctx, run.ID, "another operator", expires)
	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.Equals, token+1)
}

// liveAt is a fixed instant, rounded to microseconds because that is what
// PostgreSQL stores and a nanosecond a test wrote would not come back.
var liveAt = time.Date(2026, 8, 27, 12, 0, 0, 123456000, time.UTC)

// liveRun is a run mid-backfill, with every field set to something
// distinguishable so a column bound to the wrong placeholder shows up as a
// value in the wrong place rather than as a zero.
func liveRun(id string) embedrun.Run {
	return embedrun.Run{
		ID: id, SpecDigest: "spec-" + id, GenerationIdentity: "gen-" + id,
		Environment: "production", Source: embedpg.SourceIdentity("public", "articles"),
		Target:          "public.articles.embedding",
		ProviderProfile: "local", ResolvedModel: "fake-model@1",
		PtahVersion: "test", PolicyDigest: "policy-1",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", LeaseExpires: liveAt.Add(time.Minute), FencingToken: 7,
		SnapshotWatermark: "100", CatchUpWatermark: "200",
		Cursor: []string{"2026-01-01", `a key with "quotes", a comma and a \backslash`},
		Progress: embedrun.Progress{
			RowsScanned: 10, RowsEmbedded: 8, RowsSkipped: 1, RowsDeleted: 1,
			BatchesCommitted: 4, ProviderPromptTokens: 100, ProviderTotalTokens: 200, RetryCount: 2,
		},
		VerificationRef: "report-1", CutoverPlanRef: "plan-1", ApprovalRef: "approval-1",
		ActivePointer: "gen-old", RollbackEligible: true,
		FailureClass: "", FailureDetail: "",
		CreatedAt: liveAt, UpdatedAt: liveAt,
	}
}

// assertRunRoundTrips writes a run and reads it back whole.
//
// DeepEquals rather than a field or two: thirty-five columns bound by position
// is exactly the shape where two of them get swapped, and a spot check reads
// the ones that were not.
func assertRunRoundTrips(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	run := liveRun("roundtrip")
	registerLiveGeneration(c, ctx, store, run.GenerationIdentity, "roundtrip_articles")
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)

	loaded, err := store.Run(ctx, "roundtrip")

	c.Assert(err, qt.IsNil)
	c.Assert(loaded, qt.DeepEquals, run)
	// A second create is refused rather than resetting a run a restarted worker
	// is resuming.
	c.Assert(store.CreateRun(ctx, run), qt.ErrorIs, embedstore.ErrConflict)
}

// assertFencingRefusesStaleWrites is the rule that cannot be tested
// single-threaded and cannot be written as a read.
func assertFencingRefusesStaleWrites(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	run := liveRun("fencing")
	registerLiveGeneration(c, ctx, store, run.GenerationIdentity, "fencing_articles")
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	takenOver := run
	takenOver.FencingToken = 8
	takenOver.LeaseOwner = "worker-b"
	c.Assert(store.SaveRun(ctx, takenOver), qt.IsNil)

	err := store.SaveRun(ctx, run)

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err, qt.ErrorMatches, `.*run fencing is fenced at token 8 and this write carries 7.*`)
	current, err := store.Run(ctx, "fencing")
	c.Assert(err, qt.IsNil)
	c.Assert(current.LeaseOwner, qt.Equals, "worker-b")
	// The control: the holder still writes. A store that refused everything
	// would satisfy the assertion above and stop every backfill checkpointing.
	advanced := takenOver
	advanced.Progress.RowsEmbedded = 5000
	c.Assert(store.SaveRun(ctx, advanced), qt.IsNil)
	current, err = store.Run(ctx, "fencing")
	c.Assert(err, qt.IsNil)
	c.Assert(current.Progress.RowsEmbedded, qt.Equals, int64(5000))

	moved := advanced
	moved.GenerationIdentity = "gen-another"
	c.Assert(store.SaveRun(ctx, moved), qt.ErrorIs, embedrun.ErrGeneration)
}

func assertSavePreservesResumeProgressAndLifecycle(
	c *qt.C, ctx context.Context, store *embedpg.Store,
) {
	c.Helper()
	tests := []struct {
		name   string
		mutate func(*embedrun.Run)
	}{
		{name: "source", mutate: func(run *embedrun.Run) { run.Source = "wrong" }},
		{name: "snapshot", mutate: func(run *embedrun.Run) { run.SnapshotWatermark = "101" }},
		{name: "position removal", mutate: func(run *embedrun.Run) {
			run.SnapshotWatermark, run.CatchUpWatermark = "", ""
		}},
		{name: "catch-up rewind", mutate: func(run *embedrun.Run) {
			run.CatchUpWatermark = "199"
		}},
		{name: "counter rewind", mutate: func(run *embedrun.Run) {
			run.Progress.RowsEmbedded--
		}},
		{name: "retry rewind", mutate: func(run *embedrun.Run) {
			run.Progress.RetryCount--
		}},
		{name: "cursor rewind", mutate: func(run *embedrun.Run) {
			run.Cursor = []string{"stale"}
		}},
		{name: "enter cutover", mutate: func(run *embedrun.Run) {
			run.Phase = embedrun.PhaseCutOver
		}},
		{name: "enter rollback", mutate: func(run *embedrun.Run) {
			run.Phase = embedrun.PhaseRolledBack
		}},
	}
	for i, test := range tests {
		run := liveRun(fmt.Sprintf("save-invariant-%d", i))
		registerLiveGeneration(c, ctx, store, run.GenerationIdentity,
			fmt.Sprintf("save_invariant_%d_articles", i))
		c.Assert(store.CreateRun(ctx, run), qt.IsNil)
		offered := run
		test.mutate(&offered)
		c.Assert(store.SaveRun(ctx, offered), qt.ErrorIs, embedstore.ErrConflict,
			qt.Commentf("mutation %s", test.name))
		stored, err := store.Run(ctx, run.ID)
		c.Assert(err, qt.IsNil)
		c.Assert(stored, qt.DeepEquals, run, qt.Commentf("mutation %s", test.name))
	}

	unpositioned := liveRun("save-position-add")
	unpositioned.SnapshotWatermark, unpositioned.CatchUpWatermark = "", ""
	registerLiveGeneration(c, ctx, store, unpositioned.GenerationIdentity,
		"save_position_add_articles")
	c.Assert(store.CreateRun(ctx, unpositioned), qt.IsNil)
	positioned := unpositioned
	positioned.SnapshotWatermark = "100"
	c.Assert(store.SaveRun(ctx, positioned), qt.ErrorIs, embedstore.ErrConflict)

	// RetryCount is per-batch. A new checkpoint may reset it while advancing
	// the cumulative batch count and cursor.
	base := liveRun("save-next-batch")
	registerLiveGeneration(c, ctx, store, base.GenerationIdentity, "save_next_batch_articles")
	c.Assert(store.CreateRun(ctx, base), qt.IsNil)
	next := base
	next.Progress.BatchesCommitted++
	next.Progress.RetryCount = 0
	next.Cursor = []string{"next"}
	c.Assert(store.SaveRun(ctx, next), qt.IsNil)

	// Once the atomic pointer operation has established a special phase, a
	// same-phase checkpoint is allowed, but stale copies cannot leave it or
	// replace rollback with cutover.
	for i, phase := range []embedrun.Phase{embedrun.PhaseCutOver, embedrun.PhaseRolledBack} {
		run := liveRun(fmt.Sprintf("save-special-%d", i))
		run.Phase = phase
		registerLiveGeneration(c, ctx, store, run.GenerationIdentity,
			fmt.Sprintf("save_special_%d_articles", i))
		c.Assert(store.CreateRun(ctx, run), qt.IsNil)
		checkpoint := run
		checkpoint.Progress.RowsEmbedded++
		c.Assert(store.SaveRun(ctx, checkpoint), qt.IsNil)
		stale := checkpoint
		if phase == embedrun.PhaseCutOver {
			stale.Phase = embedrun.PhaseVerified
		} else {
			stale.Phase = embedrun.PhaseCutOver
		}
		c.Assert(store.SaveRun(ctx, stale), qt.ErrorIs, embedstore.ErrConflict)
	}
}

func assertSaveCannotCompleteRun(
	c *qt.C, ctx context.Context, store *embedpg.Store,
) {
	c.Helper()
	run := liveRun("terminal-generation-guard")
	registerLiveGeneration(c, ctx, store, run.GenerationIdentity, "terminal_guard_articles")
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	write := run
	write.Phase = embedrun.PhaseRetired
	write.Status = embedrun.StatusComplete
	write.Source = "wrong-source"
	write.Progress.RowsEmbedded = 9999
	c.Assert(store.SaveRun(ctx, write), qt.ErrorIs, embedrun.ErrTerminal)
	stored, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(stored, qt.DeepEquals, run)

	retiredButRunning := run
	retiredButRunning.Phase = embedrun.PhaseRetired
	c.Assert(store.SaveRun(ctx, retiredButRunning), qt.ErrorIs, embedrun.ErrPhase)
	stored, err = store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(stored, qt.DeepEquals, run)
	terminalCreate := run
	terminalCreate.ID = "create-already-abandoned"
	terminalCreate.Status = embedrun.StatusAbandoned
	c.Assert(store.CreateRun(ctx, terminalCreate), qt.ErrorIs, embedrun.ErrTerminal)
	terminalCreate.ID = "create-already-complete"
	terminalCreate.Status = embedrun.StatusComplete
	terminalCreate.Phase = embedrun.PhaseRetired
	c.Assert(store.CreateRun(ctx, terminalCreate), qt.ErrorIs, embedrun.ErrTerminal)
}

// assertRegistrationIsIdempotent is what a content address means for a
// registry.
func assertRegistrationIsIdempotent(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	generation := embedstore.Generation{
		Identity: "gen-idempotent", SpecDigest: "spec-1", Name: "articles v2",
		Reproducibility: "full", Dimension: 1024,
		TargetSchema: "public", TargetTable: "articles", TargetColumn: "embedding_v2", CreatedAt: liveAt,
	}
	first, err := store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	renamed := generation
	renamed.Name = "something else"
	renamed.CreatedAt = liveAt.Add(24 * time.Hour)

	second, err := store.RegisterGeneration(ctx, renamed)

	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.DeepEquals, first)
	c.Assert(second.Name, qt.Equals, "articles v2")
	c.Assert(second.CreatedAt, qt.Equals, liveAt)
}

func assertRegistrationCannotHideALiveOrTerminalOnlyRun(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	run := liveRun("registration-lifecycle")
	run.SnapshotWatermark = ""
	run.CatchUpWatermark = ""
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	retired := embedstore.Generation{
		Identity: run.GenerationIdentity, SpecDigest: "spec-registration-lifecycle",
		Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: "registration_lifecycle_articles",
		TargetColumn: "embedding", CreatedAt: liveAt, RetiredAt: liveAt,
	}
	_, err := store.RegisterGeneration(ctx, retired)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	_, err = store.Generation(ctx, retired.Identity)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	_, err = store.AbandonRun(ctx, run.ID, "obsolete")
	c.Assert(err, qt.IsNil)
	maintained := retired
	maintained.RetiredAt = time.Time{}
	maintained.MaintainedUntil = liveAt.Add(time.Hour)
	_, err = store.RegisterGeneration(ctx, maintained)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
	_, err = store.Generation(ctx, maintained.Identity)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	const activeMissing = "gen-registration-active-missing"
	_, err = db.ExecContext(ctx, `INSERT INTO `+embedstore.PointerTable+` (
		target_schema, target_table, active_generation, cut_over_at)
		VALUES ('public', 'registration_active_missing', $1, $2)`, activeMissing, liveAt)
	c.Assert(err, qt.IsNil)
	activeRetired := retired
	activeRetired.Identity = activeMissing
	_, err = store.RegisterGeneration(ctx, activeRetired)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	_, err = store.Generation(ctx, activeMissing)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
}

// assertRetirementIsTerminal keeps the record of when a corpus was destroyed.
func assertRetirementIsTerminal(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	_, err := db.ExecContext(ctx, `CREATE TABLE public.retire_articles (
		id BIGINT PRIMARY KEY, embedding_old_generation TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: "gen-retire", SpecDigest: "spec-1", Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: "retire_articles",
		TargetColumn: "embedding_old", SourceSchema: "public", SourceTable: "articles",
		CreatedAt: liveAt,
	})
	c.Assert(err, qt.IsNil)
	run := liveRun("retire-run")
	run.GenerationIdentity = "gen-retire"
	run.Phase = embedrun.PhaseCutOver
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	// Simulate a historical malformed pointer. Retirement must search every
	// target row for this active identity, not only the generation's target.
	_, err = db.ExecContext(ctx, `INSERT INTO `+embedstore.PointerTable+` (
		target_schema, target_table, active_generation, cut_over_at)
		VALUES ('public', 'wrong_retire_articles', $1, $2)`, "gen-retire", liveAt)
	c.Assert(err, qt.IsNil)
	_, err = store.RetireGenerationObjects(
		ctx, "gen-retire", embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	_, err = db.ExecContext(ctx, `DELETE FROM `+embedstore.PointerTable+`
		WHERE target_schema = 'public' AND target_table = 'wrong_retire_articles'`)
	c.Assert(err, qt.IsNil)
	release, err := store.RetireGenerationObjects(
		ctx, "gen-retire", embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
	c.Assert(err, qt.IsNil)

	_, err = store.RetireGenerationObjects(
		ctx, "gen-retire", embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})

	c.Assert(err, qt.ErrorIs, embedstore.ErrRetired)
	generation, readErr := store.Generation(ctx, "gen-retire")
	c.Assert(readErr, qt.IsNil)
	c.Assert(generation.RetiredAt, qt.Equals, release.RetiredAt)
	c.Assert(generation.Retired(), qt.IsTrue)
	completed, readErr := store.Run(ctx, run.ID)
	c.Assert(readErr, qt.IsNil)
	c.Assert(completed.Status, qt.Equals, embedrun.StatusComplete)
	c.Assert(completed.Phase, qt.Equals, embedrun.PhaseRetired)
	c.Assert(completed.FencingToken, qt.Equals, run.FencingToken+1)
	c.Assert(completed.LeaseOwner, qt.Equals, "")
	c.Assert(completed.LeaseExpires.IsZero(), qt.IsTrue)
	_, _, claimErr := store.ClaimRun(ctx, run.ID, "late worker", liveAt.Add(time.Hour))
	c.Assert(claimErr, qt.ErrorIs, embedrun.ErrTerminal)
	_, err = store.RegisterGeneration(ctx, embedstore.Generation{Identity: "gen-retire"})
	c.Assert(err, qt.ErrorIs, embedstore.ErrRetired)
	newRun := liveRun("run-after-retirement")
	newRun.GenerationIdentity = "gen-retire"
	c.Assert(store.CreateRun(ctx, newRun), qt.ErrorIs, embedstore.ErrRetired)
}

func assertRetirementUsesDatabaseTime(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	const table = "retire_clock_articles"
	_, err := db.ExecContext(ctx, `CREATE TABLE public.retire_clock_articles (
		id BIGINT PRIMARY KEY, embedding_clock TEXT,
		embedding_clock_generation TEXT)`)
	c.Assert(err, qt.IsNil)
	generation := embedstore.Generation{
		Identity: "gen-retire-clock", SpecDigest: "spec-retire-clock",
		Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: table, TargetColumn: "embedding_clock",
		SourceSchema: "public", SourceTable: "articles", CreatedAt: liveAt,
	}
	_, err = store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	run := liveRun("retire-clock-run")
	run.GenerationIdentity = generation.Identity
	run.Phase = embedrun.PhaseCutOver
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	var databaseNow time.Time
	c.Assert(db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow), qt.IsNil)
	maintainedUntil := databaseNow.UTC().Add(time.Hour)
	c.Assert(store.Maintain(ctx, generation.Identity, maintainedUntil), qt.IsNil)

	// The database samples time only after acquiring lifecycle locks and still
	// sees the maintenance window open.
	_, err = store.RetireGenerationObjects(ctx, generation.Identity,
		embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	registered, readErr := store.Generation(ctx, generation.Identity)
	c.Assert(readErr, qt.IsNil)
	c.Assert(registered.Retired(), qt.IsFalse)

	c.Assert(store.Maintain(ctx, generation.Identity, time.Time{}), qt.IsNil)
	release, err := store.RetireGenerationObjects(ctx, generation.Identity,
		embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
	c.Assert(err, qt.IsNil)
	registered, readErr = store.Generation(ctx, generation.Identity)
	c.Assert(readErr, qt.IsNil)
	c.Assert(release.RetiredAt, qt.Equals, registered.RetiredAt)
	c.Assert(release.RetiredAt.After(databaseNow.UTC()), qt.IsTrue)
	completed, readErr := store.Run(ctx, run.ID)
	c.Assert(readErr, qt.IsNil)
	c.Assert(completed.UpdatedAt, qt.Equals, release.RetiredAt)
}

func assertRetirementRollsBackDDLAndRunState(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	const table = "retire_rollback_articles"
	generation := embedstore.Generation{
		Identity: "gen-retire-rollback", SpecDigest: "spec-retire-rollback",
		Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: table, TargetColumn: "embedding_rollback",
		SourceSchema: "public", SourceTable: "articles", CreatedAt: liveAt,
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE public.retire_rollback_articles (
		id BIGINT PRIMARY KEY, embedding_rollback TEXT,
		embedding_rollback_generation TEXT)`)
	c.Assert(err, qt.IsNil)
	indexName := embedpg.GenerationIndexName(generation)
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX %q ON public.retire_rollback_articles (embedding_rollback)`, indexName))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE VIEW public.retire_rollback_dependency AS
		SELECT embedding_rollback FROM public.retire_rollback_articles`)
	c.Assert(err, qt.IsNil)
	_, err = store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	run := liveRun("retire-rollback-run")
	run.GenerationIdentity = generation.Identity
	run.Phase = embedrun.PhaseCutOver
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)

	_, err = store.RetireGenerationObjects(
		ctx, generation.Identity, embedstore.Pointer{}, 0,
		embedpg.RetirementDestruction{IndexExists: true, DropColumns: true})
	c.Assert(err, qt.IsNotNil)
	registered, readErr := store.Generation(ctx, generation.Identity)
	c.Assert(readErr, qt.IsNil)
	c.Assert(registered.Retired(), qt.IsFalse)
	stored, readErr := store.Run(ctx, run.ID)
	c.Assert(readErr, qt.IsNil)
	c.Assert(stored, qt.DeepEquals, run)
	indexExists, readErr := embedpg.GenerationIndexExists(ctx, db, generation)
	c.Assert(readErr, qt.IsNil)
	c.Assert(indexExists, qt.IsTrue)
	var columnExists bool
	readErr = db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)`,
		table, generation.TargetColumn).Scan(&columnExists)
	c.Assert(readErr, qt.IsNil)
	c.Assert(columnExists, qt.IsTrue)
}

func assertRetirementRecountsAfterAnInFlightTargetWrite(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	const table = "retire_write_articles"
	generation := embedstore.Generation{
		Identity: "gen-retire-write", SpecDigest: "spec-retire-write",
		Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: table, TargetColumn: "embedding_write",
		SourceSchema: "public", SourceTable: "articles", CreatedAt: liveAt,
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE public.retire_write_articles (
		id BIGINT PRIMARY KEY, embedding_write TEXT,
		embedding_write_generation TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	run := liveRun("retire-write-run")
	run.GenerationIdentity = generation.Identity
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)

	writer, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = writer.ExecContext(ctx, `INSERT INTO public.retire_write_articles (
		id, embedding_write, embedding_write_generation) VALUES (1, 'vector', $1)`,
		generation.Identity)
	c.Assert(err, qt.IsNil)
	retireResult := make(chan error, 1)
	go func() {
		_, callErr := store.RetireGenerationObjects(
			ctx, generation.Identity, embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
		retireResult <- callErr
	}()
	waitForBlockedQuery(c, ctx, db, "LOCK TABLE")
	select {
	case callErr := <-retireResult:
		c.Assert(false, qt.IsTrue,
			qt.Commentf("retirement completed before the target writer: %v", callErr))
	default:
	}
	c.Assert(writer.Commit(), qt.IsNil)
	c.Assert(<-retireResult, qt.ErrorIs, embedstore.ErrConflict)
	registered, readErr := store.Generation(ctx, generation.Identity)
	c.Assert(readErr, qt.IsNil)
	c.Assert(registered.Retired(), qt.IsFalse)
	stored, readErr := store.Run(ctx, run.ID)
	c.Assert(readErr, qt.IsNil)
	c.Assert(stored, qt.DeepEquals, run)
	rows, readErr := embedpg.CountGenerationRows(ctx, db, generation)
	c.Assert(readErr, qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}

func assertRetirementKeepsAnOutboxForAnOrphanReader(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	const table = "retire_orphan_articles"
	generation := embedstore.Generation{
		Identity: "gen-retire-orphan", SpecDigest: "spec-retire-orphan",
		Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: table, TargetColumn: "embedding_orphan",
		SourceSchema: "public", SourceTable: "orphan_source_articles",
		ConsistencyMode: string(embedcatchup.ModeOutbox), CreatedAt: liveAt,
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE public.retire_orphan_articles (
		id BIGINT PRIMARY KEY, embedding_orphan TEXT,
		embedding_orphan_generation TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE TABLE public.orphan_source_articles (id BIGINT PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	_, err = store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	orphan := liveRun("orphan-outbox-reader")
	orphan.GenerationIdentity = "gen-missing-outbox-reader"
	orphan.Source = embedpg.SourceIdentity(generation.SourceSchema, generation.SourceTable)
	orphan.SnapshotWatermark = ""
	orphan.CatchUpWatermark = ""
	c.Assert(store.CreateRun(ctx, orphan), qt.IsNil)
	_, err = db.ExecContext(ctx, `UPDATE `+embedstore.RunTable+`
		SET source = $2, snapshot_watermark = '100' WHERE id = $1`,
		orphan.ID, orphan.Source)
	c.Assert(err, qt.IsNil)

	release, err := store.RetireGenerationObjects(
		ctx, generation.Identity, embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
	c.Assert(err, qt.IsNil)
	c.Assert(release.Watched, qt.IsTrue)
	c.Assert(release.Removed, qt.IsFalse)
	c.Assert(release.Remaining, qt.Equals, 1)
	stored, readErr := store.Run(ctx, orphan.ID)
	c.Assert(readErr, qt.IsNil)
	c.Assert(stored.Terminal(), qt.IsFalse)
}

func assertCrossedRetirementsLockPhysicalRelationsInOneOrder(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	const tableA, tableB = "cross_retire_a", "cross_retire_b"
	_, err := db.ExecContext(ctx, `CREATE TABLE public.cross_retire_a (
		id BIGINT PRIMARY KEY, embedding_a TEXT, embedding_a_generation TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE TABLE public.cross_retire_b (
		id BIGINT PRIMARY KEY, embedding_b TEXT, embedding_b_generation TEXT)`)
	c.Assert(err, qt.IsNil)
	first := embedstore.Generation{
		Identity: "gen-cross-retire-a", SpecDigest: "spec-cross-retire-a",
		Reproducibility: "full", Dimension: 8,
		TargetTable: tableA, TargetColumn: "embedding_a",
		SourceSchema: "public", SourceTable: tableB,
		ConsistencyMode: string(embedcatchup.ModeOutbox), CreatedAt: liveAt,
	}
	second := embedstore.Generation{
		Identity: "gen-cross-retire-b", SpecDigest: "spec-cross-retire-b",
		Reproducibility: "full", Dimension: 8,
		TargetTable: tableB, TargetColumn: "embedding_b",
		SourceSchema: "public", SourceTable: tableA,
		ConsistencyMode: string(embedcatchup.ModeOutbox), CreatedAt: liveAt,
	}
	for _, generation := range []embedstore.Generation{first, second} {
		_, err = store.RegisterGeneration(ctx, generation)
		c.Assert(err, qt.IsNil)
		run := liveRun("run-" + generation.Identity)
		run.GenerationIdentity = generation.Identity
		run.Source = embedpg.SourceIdentity(generation.SourceSchema, generation.SourceTable)
		run.Phase = embedrun.PhaseCutOver
		c.Assert(store.CreateRun(ctx, run), qt.IsNil)
		reader := generation
		reader.Identity += "-reader"
		reader.SpecDigest += "-reader"
		_, err = store.RegisterGeneration(ctx, reader)
		c.Assert(err, qt.IsNil)
	}

	blocker, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = blocker.ExecContext(ctx,
		`LOCK TABLE public.cross_retire_a IN ACCESS EXCLUSIVE MODE`)
	c.Assert(err, qt.IsNil)
	results := make(chan error, 2)
	go func() {
		_, callErr := store.RetireGenerationObjects(
			ctx, first.Identity, embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
		results <- callErr
	}()
	waitForBlockedQuery(c, ctx, db, `LOCK TABLE "public"."cross_retire_a"`)
	go func() {
		_, callErr := store.RetireGenerationObjects(
			ctx, second.Identity, embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
		results <- callErr
	}()
	// Both fixed transactions resolve the omitted and explicit spellings to
	// the same physical OID and queue on A. The old authored-name sort let the
	// second hold B while waiting here, forming A->B/B->A after release.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		c.Assert(db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			  AND query LIKE '%LOCK TABLE "public"."cross_retire_a"%'`).Scan(&waiting), qt.IsNil)
		if waiting >= 2 {
			break
		}
		c.Assert(time.Now().Before(deadline), qt.IsTrue)
		time.Sleep(10 * time.Millisecond)
	}
	c.Assert(blocker.Commit(), qt.IsNil)
	c.Assert(<-results, qt.IsNil)
	c.Assert(<-results, qt.IsNil)
}

func assertPointerMoveAndRetirementCannotCross(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	const table = "move_retire_articles"
	oldIdentity := "gen-move-retire-old"
	newIdentity := "gen-move-retire-new"
	registerLiveGeneration(c, ctx, store, oldIdentity, table)
	registerLiveGeneration(c, ctx, store, newIdentity, table)
	initial := embedstore.Pointer{
		TargetSchema: "public", TargetTable: table, Active: oldIdentity, CutOverAt: liveAt,
	}
	c.Assert(store.MovePointer(ctx, initial, ""), qt.IsNil)

	blocker, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = blocker.ExecContext(ctx, `SELECT active_generation FROM `+embedstore.PointerTable+`
		WHERE target_schema = 'public' AND target_table = $1 FOR UPDATE`, table)
	c.Assert(err, qt.IsNil)
	moveResult := make(chan error, 1)
	retireResult := make(chan error, 1)
	go func() {
		moveResult <- store.MovePointer(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: table, Active: newIdentity,
			Previous: oldIdentity, CutOverAt: liveAt.Add(time.Minute),
		}, oldIdentity)
	}()
	waitForBlockedQuery(c, ctx, db, "INSERT INTO "+embedstore.PointerTable)
	go func() {
		_, callErr := store.RetireGenerationObjects(
			ctx, newIdentity, initial, 0, embedpg.RetirementDestruction{})
		retireResult <- callErr
	}()
	waitForBlockedQuery(c, ctx, db, "pg_advisory_xact_lock")
	c.Assert(blocker.Commit(), qt.IsNil)
	c.Assert(<-moveResult, qt.IsNil)
	c.Assert(<-retireResult, qt.ErrorIs, embedstore.ErrConflict)
	pointer, err := store.Pointer(ctx, "public", table)
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, newIdentity)
	destination, err := store.Generation(ctx, newIdentity)
	c.Assert(err, qt.IsNil)
	c.Assert(destination.Retired(), qt.IsFalse)
}

// assertPointerIsCompareAndSet is the store's half of the cutover rule.
func assertPointerIsCompareAndSet(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	schema, table := "public", "articles"
	for _, identity := range []string{"gen-1", "gen-2", "gen-3", "gen-9"} {
		registerLiveGeneration(c, ctx, store, identity, table)
	}
	// Before anything is there: a move naming a generation the table does not
	// read is refused rather than quietly becoming the first cutover.
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-1", Previous: "gen-9", CutOverAt: liveAt,
	}, ""), qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-1", CutOverAt: liveAt,
	}, "gen-0"), qt.ErrorIs, embedstore.ErrConflict)
	_, err := store.Pointer(ctx, schema, table)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)

	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-1", CutOverAt: liveAt, CutOverBy: "an operator",
		PlanDigest: "plan-1",
	}, ""), qt.IsNil)

	err = store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-3", Previous: "gen-2", CutOverAt: liveAt,
	}, "gen-2")

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err, qt.ErrorMatches, `.*public.articles reads gen-1 and this move expected gen-2.*`)
	current, readErr := store.Pointer(ctx, schema, table)
	c.Assert(readErr, qt.IsNil)
	c.Assert(current.Active, qt.Equals, "gen-1")
	c.Assert(current.PlanDigest, qt.Equals, "plan-1")
	// The historical pointer must name the state the CAS actually displaced.
	// Otherwise A to B could maintain unrelated C while silently abandoning A.
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-2", Previous: "gen-3", CutOverAt: liveAt,
	}, "gen-1"), qt.ErrorIs, embedstore.ErrConflict)
	current, readErr = store.Pointer(ctx, schema, table)
	c.Assert(readErr, qt.IsNil)
	c.Assert(current.Active, qt.Equals, "gen-1")
	c.Assert(current.Previous, qt.Equals, "")
	// The control: the move that names what is actually there succeeds.
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-2", Previous: "gen-1", CutOverAt: liveAt,
	}, "gen-1"), qt.IsNil)
	current, readErr = store.Pointer(ctx, schema, table)
	c.Assert(readErr, qt.IsNil)
	c.Assert(current.Active, qt.Equals, "gen-2")
	c.Assert(current.Previous, qt.Equals, "gen-1")
	// And a first move onto a table that already has a pointer is refused: an
	// empty expectation is a claim that nothing is there.
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-9", CutOverAt: liveAt,
	}, ""), qt.ErrorIs, embedstore.ErrConflict)
	registerLiveGeneration(c, ctx, store, "gen-wrong-target", "some_other_table")
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: schema, TargetTable: table, Active: "gen-wrong-target", CutOverAt: liveAt,
	}, "gen-2"), qt.ErrorIs, embedstore.ErrConflict)
	current, readErr = store.Pointer(ctx, schema, table)
	c.Assert(readErr, qt.IsNil)
	c.Assert(current.Active, qt.Equals, "gen-2")
}

func assertRollbackIsAtomicAndRevalidatesMaintenance(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	active := "gen-rollback-active"
	destination := "gen-rollback-destination"
	registerLiveGeneration(c, ctx, store, active, "rollback_articles")
	registerLiveGeneration(c, ctx, store, destination, "rollback_articles")
	live := liveRun("rollback-live")
	live.GenerationIdentity = active
	live.Phase = embedrun.PhaseCutOver
	abandoned := live
	abandoned.ID = "rollback-abandoned"
	c.Assert(store.CreateRun(ctx, live), qt.IsNil)
	c.Assert(store.CreateRun(ctx, abandoned), qt.IsNil)
	_, err := store.AbandonRun(ctx, abandoned.ID, "duplicate")
	c.Assert(err, qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "rollback_articles",
		Active: destination, CutOverAt: liveAt,
	}, ""), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "rollback_articles",
		Active: active, Previous: destination, CutOverAt: liveAt,
	}, destination), qt.IsNil)
	var rollbackStart time.Time
	c.Assert(db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&rollbackStart), qt.IsNil)
	rollbackStart = rollbackStart.UTC()
	maintainedUntil := rollbackStart.Add(time.Hour)
	c.Assert(store.Maintain(ctx, destination, maintainedUntil), qt.IsNil)

	rolledBackAt, err := store.MovePointerWithRollback(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "rollback_articles",
		Active: destination, Previous: active,
	}, active, maintainedUntil, rollbackStart.Add(30*time.Minute))
	c.Assert(err, qt.IsNil)
	c.Assert(rolledBackAt.Before(maintainedUntil), qt.IsTrue)
	rolledBack, err := store.Run(ctx, live.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(rolledBack.Phase, qt.Equals, embedrun.PhaseRolledBack)
	c.Assert(rolledBack.FencingToken, qt.Equals, live.FencingToken+1)
	c.Assert(rolledBack.LeaseOwner, qt.Equals, "")
	c.Assert(rolledBack.LeaseExpires.IsZero(), qt.IsTrue)
	terminal, err := store.Run(ctx, abandoned.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(terminal.Status, qt.Equals, embedrun.StatusAbandoned)
	c.Assert(terminal.Phase, qt.Equals, embedrun.PhaseCutOver)
	pointer, err := store.Pointer(ctx, "public", "rollback_articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.CutOverAt, qt.Equals, rolledBackAt)

	// Clear the window while holding the same lifecycle lock. The rollback is
	// known to be waiting on that lock before the clear commits, so this proves
	// it revalidates the approved deadline instead of using the earlier read.
	approvedUntil := rollbackStart.Add(2 * time.Hour)
	c.Assert(store.Maintain(ctx, active, approvedUntil), qt.IsNil)
	blocker, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = blocker.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"ptah:inference:generation:"+active)
	c.Assert(err, qt.IsNil)
	_, err = blocker.ExecContext(ctx, `UPDATE `+embedstore.GenerationTable+`
		SET maintained_until = NULL WHERE identity = $1`, active)
	c.Assert(err, qt.IsNil)
	rollbackResult := make(chan error, 1)
	go func() {
		_, callErr := store.MovePointerWithRollback(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: "rollback_articles",
			Active: active, Previous: destination,
		}, destination, approvedUntil, rollbackStart.Add(3*time.Hour))
		rollbackResult <- callErr
	}()
	waitForBlockedQuery(c, ctx, db, "pg_advisory_xact_lock")
	c.Assert(blocker.Commit(), qt.IsNil)
	c.Assert(<-rollbackResult, qt.ErrorIs, embedstore.ErrConflict)
	pointer, err = store.Pointer(ctx, "public", "rollback_articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, destination)
}

func assertRollbackCannotOutwaitItsDeadlines(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	assertBlockedRollbackExpires(c, ctx, db, store,
		"maintenance", time.Second, time.Hour, time.Second)
	assertBlockedRollbackExpires(c, ctx, db, store,
		"policy", time.Hour, time.Second, time.Second)
}

func assertBlockedRollbackExpires(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
	suffix string, maintainFor, policyFor, waitFor time.Duration,
) {
	c.Helper()
	table := "rollback_expiry_" + suffix
	active := "gen-rollback-expiry-active-" + suffix
	destination := "gen-rollback-expiry-destination-" + suffix
	registerLiveGeneration(c, ctx, store, active, table)
	registerLiveGeneration(c, ctx, store, destination, table)
	run := liveRun("rollback-expiry-run-" + suffix)
	run.GenerationIdentity = active
	run.Phase = embedrun.PhaseCutOver
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: table, Active: destination, CutOverAt: liveAt,
	}, ""), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: table, Active: active,
		Previous: destination, CutOverAt: liveAt,
	}, destination), qt.IsNil)

	var startedAt time.Time
	c.Assert(db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&startedAt), qt.IsNil)
	startedAt = startedAt.UTC()
	maintainedUntil := startedAt.Add(maintainFor)
	eligibilityNotAfter := startedAt.Add(policyFor)
	c.Assert(store.Maintain(ctx, destination, maintainedUntil), qt.IsNil)
	before, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)

	blocker, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = blocker.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"ptah:inference:generation:"+destination)
	c.Assert(err, qt.IsNil)
	type result struct {
		movedAt time.Time
		err     error
	}
	rollbackResult := make(chan result, 1)
	go func() {
		movedAt, callErr := store.MovePointerWithRollback(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: table, Active: destination, Previous: active,
		}, active, maintainedUntil, eligibilityNotAfter)
		rollbackResult <- result{movedAt: movedAt, err: callErr}
	}()
	waitForBlockedQuery(c, ctx, db, "pg_advisory_xact_lock")
	waitUntilDatabaseTime(c, ctx, db, startedAt.Add(waitFor))
	c.Assert(blocker.Commit(), qt.IsNil)
	resultAfterWait := <-rollbackResult
	c.Assert(resultAfterWait.err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(resultAfterWait.movedAt.IsZero(), qt.IsTrue)
	pointer, err := store.Pointer(ctx, "public", table)
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, active)
	c.Assert(pointer.Previous, qt.Equals, destination)
	stored, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(stored, qt.DeepEquals, before)
}

func waitUntilDatabaseTime(c *qt.C, ctx context.Context, db *sql.DB, after time.Time) {
	c.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var reached bool
		err := db.QueryRowContext(ctx, `SELECT clock_timestamp() > $1`, after.UTC()).Scan(&reached)
		c.Assert(err, qt.IsNil)
		if reached {
			return
		}
		c.Assert(time.Now().Before(deadline), qt.IsTrue,
			qt.Commentf("database clock did not pass %s", after.UTC().Format(time.RFC3339Nano)))
		time.Sleep(10 * time.Millisecond)
	}
}

// assertEventTrailIsOrdered keeps a run's history readable as one.
func assertEventTrailIsOrdered(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	run := liveRun("events")
	registerLiveGeneration(c, ctx, store, run.GenerationIdentity, "events_articles")
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	kinds := []embedrun.EventKind{
		embedrun.EventClaimed, embedrun.EventCheckpoint, embedrun.EventCheckpoint, embedrun.EventPaused,
	}
	for index, kind := range kinds {
		c.Assert(store.AppendEvent(ctx, embedrun.Event{
			RunID: "events", Kind: kind, At: liveAt.Add(time.Duration(index) * time.Minute),
			Actor: "worker-a", FencingToken: 7, FromPhase: embedrun.PhaseBackfilling,
			Detail: fmt.Sprintf("event %d", index),
			Counts: embedrun.Progress{RowsScanned: int64(index)},
		}), qt.IsNil)
	}

	events, err := store.Events(ctx, "events")

	c.Assert(err, qt.IsNil)
	c.Assert(events, qt.HasLen, 4)
	c.Assert(detailsOfEvents(events), qt.DeepEquals,
		[]string{"event 0", "event 1", "event 2", "event 3"})
	c.Assert(events[0].Kind, qt.Equals, embedrun.EventClaimed)
	c.Assert(events[3].Kind, qt.Equals, embedrun.EventPaused)
	c.Assert(events[2].Counts.RowsScanned, qt.Equals, int64(2))
	c.Assert(events[1].At, qt.Equals, liveAt.Add(time.Minute))
}

// assertAbsenceIsNotEmptiness keeps a missing row from reading as an empty one.
func assertAbsenceIsNotEmptiness(
	c *qt.C, ctx context.Context, db *sql.DB, store *embedpg.Store,
) {
	c.Helper()
	_, err := store.Run(ctx, "nothing")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	_, err = store.Generation(ctx, "nothing")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	_, err = store.Pointer(ctx, "public", "nothing")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	_, err = store.Events(ctx, "nothing")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	c.Assert(store.SaveRun(ctx, liveRun("nothing")), qt.ErrorIs, embedstore.ErrNotFound)
	missingGeneration := liveRun("missing-generation-abandon")
	missingGeneration.SnapshotWatermark = ""
	missingGeneration.CatchUpWatermark = ""
	c.Assert(store.CreateRun(ctx, missingGeneration), qt.IsNil)
	_, err = store.AbandonRun(ctx, missingGeneration.ID, "obsolete")
	c.Assert(err, qt.IsNil)
	abandoned, readErr := store.Run(ctx, missingGeneration.ID)
	c.Assert(readErr, qt.IsNil)
	c.Assert(abandoned.Status, qt.Equals, embedrun.StatusAbandoned)
	protected := liveRun("missing-generation-active")
	protected.SnapshotWatermark = ""
	protected.CatchUpWatermark = ""
	c.Assert(store.CreateRun(ctx, protected), qt.IsNil)
	_, err = db.ExecContext(ctx, `INSERT INTO `+embedstore.PointerTable+` (
		target_schema, target_table, active_generation, cut_over_at)
		VALUES ('public', 'missing_generation_articles', $1, $2)`,
		protected.GenerationIdentity, liveAt)
	c.Assert(err, qt.IsNil)
	_, err = store.AbandonRun(ctx, protected.ID, "obsolete")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
	_, retireErr := store.RetireGenerationObjects(
		ctx, "nothing", embedstore.Pointer{}, 0, embedpg.RetirementDestruction{})
	c.Assert(retireErr, qt.ErrorIs, embedstore.ErrNotFound)

	// A destruction naming both removals is refused before the generation is
	// even looked up, which is why this asks about a generation that does not
	// exist: the answer is the contradiction rather than the absence, so the
	// check demonstrably runs first. Reaching the DDL with both would drop the
	// relation and then ask to drop columns from it, and the operator would
	// read a missing-relation error for a plan that contradicted itself.
	_, bothErr := store.RetireGenerationObjects(
		ctx, "nothing", embedstore.Pointer{}, 0,
		embedpg.RetirementDestruction{DropColumns: true, DropTable: true})
	c.Assert(bothErr, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(bothErr, qt.ErrorMatches,
		`.*a retirement drops the generation's columns or its table, not both`)
}

// detailsOfEvents lists what a trail said.
func detailsOfEvents(events []embedrun.Event) []string {
	details := make([]string, 0, len(events))
	for _, event := range events {
		details = append(details, event.Detail)
	}
	return details
}

// TestEmbedPGStoreMaintainNeverShortensAWindowE2E is stokaro/ptah#2647.
//
// `Maintain` wrote the deadline it was given, so a shorter renewal moved it
// earlier — and `catchup --maintain-for 1h` after a `cutover --stabilize-for
// 24h`, which is the rollback guide's own recipe, took twenty-three hours of
// rollback eligibility away from a flag documented as extending the window.
//
// It runs live because the rule is a GREATEST inside an UPDATE. A test against
// the in-memory store would agree with a SQL statement PostgreSQL rejects, and
// the NULL case — a generation nothing has ever kept current — is exactly where
// GREATEST answers differently from what the fix needs.
func TestEmbedPGStoreMaintainNeverShortensAWindowE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_maintain_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)

	_, err = store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: "maintain-1", SpecDigest: "spec-1", Dimension: 4,
		TargetTable: "articles", TargetColumn: "embedding", CreatedAt: liveAt,
	})
	c.Assert(err, qt.IsNil)

	far := liveAt.Add(24 * time.Hour)
	near := liveAt.Add(time.Hour)

	// From nothing, any deadline wins: a NULL is "nobody is keeping it", not a
	// deadline in the past.
	c.Assert(store.Maintain(ctx, "maintain-1", near), qt.IsNil)
	c.Assert(storedWindow(c, ctx, store), qt.Equals, near)

	// A longer renewal extends.
	c.Assert(store.Maintain(ctx, "maintain-1", far), qt.IsNil)
	c.Assert(storedWindow(c, ctx, store), qt.Equals, far)

	// A shorter one does not shorten. This is the defect.
	c.Assert(store.Maintain(ctx, "maintain-1", near), qt.IsNil)
	c.Assert(storedWindow(c, ctx, store), qt.Equals, far)

	// And clearing still clears, which is what stops a generation being
	// reported as a way back the moment nobody is feeding it.
	c.Assert(store.Maintain(ctx, "maintain-1", time.Time{}), qt.IsNil)
	c.Assert(storedWindow(c, ctx, store).IsZero(), qt.IsTrue)
}

// storedWindow reads the window back through the store.
func storedWindow(c *qt.C, ctx context.Context, store *embedpg.Store) time.Time {
	c.Helper()
	generation, err := store.Generation(ctx, "maintain-1")
	c.Assert(err, qt.IsNil)
	return generation.MaintainedUntil.UTC()
}
