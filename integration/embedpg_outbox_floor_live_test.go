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

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedspec"
	"ptah.run/internal/embedstore"
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

	outbox, err := embedpg.NewOutbox(db, outboxSpec())
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %q (xact BIGINT NOT NULL)`, outbox.TableName()))
	c.Assert(err, qt.IsNil)
	store := &floorPruneStore{Store: embedpg.NewStore(db), outbox: outbox}
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)

	assertNoReaderIsNotAFloor(c, ctx, store)
	assertFloorIsTheEarliestReader(c, ctx, store)
	assertAnotherSourceDoesNotLowerTheFloor(c, ctx, store)
	assertARetiredGenerationDoesNotLowerTheFloor(c, ctx, db, store)
	assertAnAbandonedRunDoesNotLowerTheFloor(c, ctx, store)
	assertACompleteRunDoesNotLowerTheFloor(c, ctx, db, store)
	assertAnUncaughtRunReadsFromItsBoundary(c, ctx, store)
	assertARunWithNoPositionIsSkipped(c, ctx, store)
	assertASameNamedTableInAnotherSchemaIsAnotherSource(c, ctx, store)
	assertARunRecordedBeforeTheIdentityStillCounts(c, ctx, store)
}

// floorPruneStore measures the floor through the production operation that
// consumes it. The event table is empty in this membership test, so pruning
// changes no fixture state; it binds these assertions to the same locked query
// and DELETE path the CLI uses instead of keeping a test-only exported reader.
type floorPruneStore struct {
	*embedpg.Store
	outbox *embedpg.Outbox
}

func (s *floorPruneStore) OutboxFloor(
	ctx context.Context, _, _ string,
) (embedpg.OutboxFloorResult, bool, error) {
	floor, found, _, err := s.PruneOutbox(ctx, s.outbox)
	return floor, found, err
}

// TestPruneOutbox_SerializesANewReaderWithTheDeleteLive is the membership race
// that makes the floor and DELETE one operation. A prepare can add a run behind
// the current floor. If it lands after the floor query but before a separate
// delete, that run loses events it still owes.
//
// The source lifecycle lock is held while the behind run is created. The prune
// must be visibly queued on that advisory lock; after release it must reread
// membership and preserve both events. Without the lock the prune finishes
// first from the ahead run and this test observes an empty outbox.
func TestPruneOutbox_SerializesANewReaderWithTheDeleteLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := loadTargetSpec(c, table)
	store := embedpg.NewStore(db)
	generation, installer := prepareRecords(spec, "prune-installer", time.Now().UTC())
	installed, err := store.PrepareRun(
		ctx, spec, generation, installer, embedcatchup.ModeOutbox)
	c.Assert(err, qt.IsNil)
	c.Assert(installed.Created, qt.IsTrue)
	_, err = store.AbandonRun(
		ctx, installer.ID, "the installation run is no longer a reader")
	c.Assert(err, qt.IsNil)

	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES (2, 'two', 'second', '1')`, table))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s VALUES (3, 'three', 'third', '1')`, table))
	c.Assert(err, qt.IsNil)
	now := time.Now().UTC()
	// Deliberately beyond any server horizon this test can reach. It makes the
	// joining run the new floor, so a prune that read membership before prepare
	// committed would delete too far and cannot accidentally satisfy the test.
	c.Assert(store.CreateRun(ctx, readerRun(
		spec, "prune-ahead", ^uint64(0), now)), qt.IsNil)

	// Queue the real prepare before prune behind the same source lock. This
	// creates the precise ordering being asserted without exposing a lock-only
	// callback API that could run its action through another pool connection.
	lockConn, err := db.Conn(ctx)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = lockConn.Close() })
	lockName := "ptah:inference:source:" +
		embedpg.SourceIdentity(spec.Source.Schema, spec.Source.Table)
	_, err = lockConn.ExecContext(ctx,
		`SELECT pg_advisory_lock(hashtextextended($1, 0))`, lockName)
	c.Assert(err, qt.IsNil)

	joiningGeneration, joiningRun := prepareRecords(spec, "prune-joining", now)
	prepared := make(chan struct {
		result embedpg.PrepareResult
		err    error
	}, 1)
	go func() {
		result, prepareErr := store.PrepareRun(
			ctx, spec, joiningGeneration, joiningRun, embedcatchup.ModeOutbox)
		prepared <- struct {
			result embedpg.PrepareResult
			err    error
		}{result: result, err: prepareErr}
	}()
	waitForAdvisoryWaiters(c, ctx, db, 1)

	type pruneResult struct {
		floor   embedpg.OutboxFloorResult
		found   bool
		removed int64
		err     error
	}
	pruned := make(chan pruneResult, 1)
	go func() {
		floor, found, removed, pruneErr := store.PruneOutbox(ctx, outbox)
		pruned <- pruneResult{floor: floor, found: found, removed: removed, err: pruneErr}
	}()
	waitForAdvisoryWaiters(c, ctx, db, 2)
	var unlocked bool
	c.Assert(lockConn.QueryRowContext(ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockName).Scan(&unlocked), qt.IsNil)
	c.Assert(unlocked, qt.IsTrue)

	prepareResult := <-prepared
	c.Assert(prepareResult.err, qt.IsNil)
	c.Assert(prepareResult.result.Created, qt.IsTrue)
	result := <-pruned
	c.Assert(result.err, qt.IsNil)
	c.Assert(result.found, qt.IsTrue)
	preparedBoundary, ok, err := embedcatchup.ResumeFrom(
		"", prepareResult.result.Run.SnapshotWatermark)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(result.floor.Position, qt.Equals, preparedBoundary)
	c.Assert(result.floor.Holders, qt.DeepEquals, []embedpg.OutboxFloorHolder{{
		RunID: "prune-joining", Generation: spec.Identity().Digest,
	}})
}

// TestPrepareRun_UsesOnePinnedConnectionLive guards the session-lock shape.
// A lock-only transaction on one pool connection followed by DDL through the
// pool needs a second connection and deadlocks at this limit. Prepare instead
// runs its source and generation locks, filter probe, DDL, and row transactions
// on one pinned session.
func TestPrepareRun_UsesOnePinnedConnectionLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	db.SetMaxOpenConns(1)
	spec := loadTargetSpec(c, table)
	store := embedpg.NewStore(db)
	generation, run := prepareRecords(spec, "one-connection-prepare", time.Now().UTC())

	prepared, err := store.PrepareRun(
		ctx, spec, generation, run, embedcatchup.ModeOutbox)

	c.Assert(err, qt.IsNil)
	c.Assert(prepared.Created, qt.IsTrue)
	c.Assert(prepared.Run.Phase, qt.Equals, embedrun.PhaseBoundaryCaptured)
	c.Assert(prepared.Run.SnapshotWatermark, qt.Not(qt.Equals), "")
	durablePrepared, err := store.Run(ctx, prepared.Run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(prepared.Run, qt.DeepEquals, durablePrepared)
	advanced := prepared.Run
	c.Assert(advanced.Reach(advanced.FencingToken, embedrun.PhaseBackfilling), qt.IsNil)
	c.Assert(store.SaveRun(ctx, advanced), qt.IsNil)
	durableAdvanced, err := store.Run(ctx, advanced.ID)
	c.Assert(err, qt.IsNil)
	retried, err := store.PrepareRun(
		ctx, spec, generation, run, embedcatchup.ModeOutbox)
	c.Assert(err, qt.IsNil)
	c.Assert(retried.Created, qt.IsFalse)
	c.Assert(retried.Run, qt.DeepEquals, durableAdvanced)

	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)
	disabledTrigger := outbox.TriggerNames()[0]
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s DISABLE TRIGGER %q", table, disabledTrigger))
	c.Assert(err, qt.IsNil)
	_, err = store.PrepareRun(
		ctx, spec, generation, run, embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err.Error(), qt.Contains, "disabled for ordinary writes")
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ENABLE TRIGGER %q", table, disabledTrigger))
	c.Assert(err, qt.IsNil)
	reenabled, err := store.PrepareRun(
		ctx, spec, generation, run, embedcatchup.ModeOutbox)
	c.Assert(err, qt.IsNil)
	c.Assert(reenabled.Created, qt.IsFalse)
	c.Assert(reenabled.Run, qt.DeepEquals, durableAdvanced)
}

// TestPrepareRun_RefusesIncompleteIdempotentRowsLive proves that sharing a run
// identifier is not evidence that prepare committed. CreateRun intentionally
// accepts an unpositioned row before registration, and imported or damaged
// rows may claim a later phase. None may turn missing target, registry, source,
// or boundary state into exit-zero idempotency.
func TestPrepareRun_RefusesIncompleteIdempotentRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := loadTargetSpec(c, table)
	store := embedpg.NewStore(db)
	generation, desired := prepareRecords(spec, "incomplete-pre-boundary", time.Now().UTC())

	c.Assert(store.CreateRun(ctx, desired), qt.IsNil)
	_, err := store.PrepareRun(
		ctx, spec, generation, desired, embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedrun.ErrPhase)

	missingRegistry := desired
	missingRegistry.ID = "incomplete-no-registry"
	c.Assert(store.CreateRun(ctx, missingRegistry), qt.IsNil)
	setIncompletePreparedRow(c, ctx, db, missingRegistry.ID,
		embedrun.PhaseBoundaryCaptured, "10", missingRegistry.Source)
	_, err = store.PrepareRun(
		ctx, spec, generation, missingRegistry, embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)

	_, err = store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	mismatchedTarget := desired
	mismatchedTarget.ID = "incomplete-registry-target"
	_, err = db.ExecContext(ctx, `UPDATE `+embedstore.GenerationTable+`
		SET target_column = 'another_embedding' WHERE identity = $1`, generation.Identity)
	c.Assert(err, qt.IsNil)
	_, err = store.PrepareRun(
		ctx, spec, generation, mismatchedTarget, embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err.Error(), qt.Contains, "target column")
	assertNoPreparedArtifacts(c, ctx, db, store, spec, mismatchedTarget.ID)
	_, err = db.ExecContext(ctx, `UPDATE `+embedstore.GenerationTable+`
		SET target_column = $2 WHERE identity = $1`, generation.Identity, generation.TargetColumn)
	c.Assert(err, qt.IsNil)

	mismatchedMode := desired
	mismatchedMode.ID = "incomplete-registry-mode"
	_, err = db.ExecContext(ctx, `UPDATE `+embedstore.GenerationTable+`
		SET consistency_mode = 'immutable' WHERE identity = $1`, generation.Identity)
	c.Assert(err, qt.IsNil)
	_, err = store.PrepareRun(
		ctx, spec, generation, mismatchedMode, embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err.Error(), qt.Contains, "consistency mode")
	assertNoPreparedArtifacts(c, ctx, db, store, spec, mismatchedMode.ID)
	_, err = db.ExecContext(ctx, `UPDATE `+embedstore.GenerationTable+`
		SET consistency_mode = $2 WHERE identity = $1`,
		generation.Identity, generation.ConsistencyMode)
	c.Assert(err, qt.IsNil)

	wrongSource := desired
	wrongSource.ID = "incomplete-wrong-source"
	wrongSource.Source = "the-wrong-source"
	c.Assert(store.CreateRun(ctx, wrongSource), qt.IsNil)
	setIncompletePreparedRow(c, ctx, db, wrongSource.ID,
		embedrun.PhaseBoundaryCaptured, "11", wrongSource.Source)
	requestedWrongSource := wrongSource
	requestedWrongSource.Source = desired.Source
	_, err = store.PrepareRun(
		ctx, spec, generation, requestedWrongSource, embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err.Error(), qt.Contains, "records source")

	missingTarget := desired
	missingTarget.ID = "incomplete-missing-target"
	c.Assert(store.CreateRun(ctx, missingTarget), qt.IsNil)
	setIncompletePreparedRow(c, ctx, db, missingTarget.ID,
		embedrun.PhaseBoundaryCaptured, "12", missingTarget.Source)
	_, err = store.PrepareRun(
		ctx, spec, generation, missingTarget, embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err.Error(), qt.Contains, "prepared target column")
	c.Assert(embeddingColumns(c, ctx, db, table), qt.DeepEquals, map[string]string{})
}

func setIncompletePreparedRow(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	id string,
	phase embedrun.Phase,
	snapshot, source string,
) {
	c.Helper()
	_, err := db.ExecContext(ctx, `UPDATE `+embedstore.RunTable+`
		SET phase = $2, snapshot_watermark = $3, source = $4 WHERE id = $1`,
		id, string(phase), snapshot, source)
	c.Assert(err, qt.IsNil)
}

func assertNoPreparedArtifacts(
	c *qt.C,
	ctx context.Context,
	db *sql.DB,
	store *embedpg.Store,
	spec embedgen.Spec,
	runID string,
) {
	c.Helper()
	_, err := store.Run(ctx, runID)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	c.Assert(embeddingColumns(c, ctx, db, spec.Target.Table), qt.DeepEquals, map[string]string{})
	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)
	installed, err := outbox.Installed(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(installed, qt.IsFalse)
	var tableExists bool
	c.Assert(db.QueryRowContext(ctx,
		`SELECT to_regclass($1) IS NOT NULL`, outbox.TableName()).Scan(&tableExists), qt.IsNil)
	c.Assert(tableExists, qt.IsFalse)
}

// TestPrepareRun_PreservesEverySharedOutboxContractLive proves that preparing
// a second generation does not replace the first generation's update
// predicate. Both generations share the source-level outbox, but one embeds
// title and the other embeds body; the trigger therefore has to watch their
// union.
func TestPrepareRun_PreservesEverySharedOutboxContractLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	store := embedpg.NewStore(db)

	firstConfig := defaultCLISpec("http://127.0.0.1:9/v1")
	firstConfig.sourceTable, firstConfig.targetTable = table, table
	firstConfig.inputFields = []string{"title"}
	firstConfig.column = "embedding_title"
	firstLoaded, err := embedspec.Load(writeCLISpecFrom(c, firstConfig))
	c.Assert(err, qt.IsNil)
	firstGeneration, firstRun := prepareLoadedRecords(
		firstLoaded, "shared-title", time.Now().UTC())
	_, err = store.PrepareRun(ctx, firstLoaded.Spec,
		firstGeneration, firstRun, embedcatchup.ModeOutbox)
	c.Assert(err, qt.IsNil)

	secondConfig := firstConfig
	secondConfig.inputFields = []string{"body"}
	secondConfig.column = "embedding_body"
	secondLoaded, err := embedspec.Load(writeCLISpecFrom(c, secondConfig))
	c.Assert(err, qt.IsNil)
	secondGeneration, secondRun := prepareLoadedRecords(
		secondLoaded, "shared-body", time.Now().UTC())
	_, err = store.PrepareRun(ctx, secondLoaded.Spec,
		secondGeneration, secondRun, embedcatchup.ModeOutbox)
	c.Assert(err, qt.IsNil)
	incompatibleConfig := secondConfig
	incompatibleConfig.keyFields = []string{"id", "title"}
	incompatibleConfig.column = "embedding_incompatible"
	incompatibleLoaded, err := embedspec.Load(writeCLISpecFrom(c, incompatibleConfig))
	c.Assert(err, qt.IsNil)
	incompatibleGeneration, incompatibleRun := prepareLoadedRecords(
		incompatibleLoaded, "shared-incompatible", time.Now().UTC())
	_, err = store.PrepareRun(ctx, incompatibleLoaded.Spec,
		incompatibleGeneration, incompatibleRun, embedcatchup.ModeOutbox)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "same ordered key fields")
	c.Assert(embeddingColumns(c, ctx, db, table)["embedding_incompatible"], qt.Equals, "",
		qt.Commentf("an incompatible contract must be refused before target DDL"))

	outbox, err := embedpg.NewOutbox(db, firstLoaded.Spec)
	c.Assert(err, qt.IsNil)
	start, err := outbox.Horizon(ctx)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET title = 'changed' WHERE id = 1", table))
	c.Assert(err, qt.IsNil)
	events, _, err := outbox.Since(ctx, embedcatchup.AtTransaction(start), 100)
	c.Assert(err, qt.IsNil)
	c.Assert(events, qt.HasLen, 1)
	c.Assert(events[0].Key, qt.DeepEquals, []string{"1"})
	c.Assert(events[0].Operation, qt.Equals, embedcatchup.OperationUpdate)

	start, err = outbox.Horizon(ctx)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET body = 'changed for body generation' WHERE id = 1", table))
	c.Assert(err, qt.IsNil)
	events, _, err = outbox.Since(ctx, embedcatchup.AtTransaction(start), 100)
	c.Assert(err, qt.IsNil)
	c.Assert(events, qt.HasLen, 1,
		qt.Commentf("a field watched only by the second generation must remain in the union"))
	c.Assert(events[0].Key, qt.DeepEquals, []string{"1"})
	c.Assert(events[0].Operation, qt.Equals, embedcatchup.OperationUpdate)
}

func prepareRecords(
	spec embedgen.Spec, id string, at time.Time,
) (embedstore.Generation, embedrun.Run) {
	identity := spec.Identity().Digest
	generation := embedstore.Generation{
		Identity: identity, SpecDigest: identity, Name: spec.Name,
		Dimension:    spec.Model.ReportedDimension,
		TargetSchema: spec.Target.Schema, TargetTable: spec.Target.Table,
		TargetColumn: spec.Target.Column, SourceSchema: spec.Source.Schema,
		SourceTable: spec.Source.Table, ConsistencyMode: string(embedcatchup.ModeOutbox),
		CreatedAt: at,
	}
	run := embedrun.Run{
		ID: id, SpecDigest: identity, GenerationIdentity: identity,
		Environment: "test", Source: embedpg.SourceIdentity(spec.Source.Schema, spec.Source.Table),
		Target:          spec.Target.Table + "." + spec.Target.Column,
		ProviderProfile: spec.Model.Provider, PtahVersion: "test",
		Phase: embedrun.PhasePrepared, Status: embedrun.StatusRunning,
		CreatedAt: at, UpdatedAt: at,
	}
	run.Claim("prepare-test", time.Minute)
	return generation, run
}

func prepareLoadedRecords(
	loaded embedspec.Loaded, id string, at time.Time,
) (embedstore.Generation, embedrun.Run) {
	generation, run := prepareRecords(loaded.Spec, id, at)
	generation.SpecDigest = loaded.Digest
	generation.SpecDocument = string(loaded.Document)
	return generation, run
}

func readerRun(
	spec embedgen.Spec, id string, watermark uint64, at time.Time,
) embedrun.Run {
	identity := spec.Identity().Digest
	return embedrun.Run{
		ID: id, SpecDigest: identity, GenerationIdentity: identity,
		Environment: "test", Source: embedpg.SourceIdentity(spec.Source.Schema, spec.Source.Table),
		Target:          spec.Target.Table + "." + spec.Target.Column,
		ProviderProfile: spec.Model.Provider, PtahVersion: "test",
		Phase: embedrun.PhasePrepared, Status: embedrun.StatusRunning,
		SnapshotWatermark: fmt.Sprint(watermark), CreatedAt: at, UpdatedAt: at,
	}
}

// waitForAdvisoryWaiters observes lifecycle operations waiting on the held
// source lock.
// The database is private to this test, so no other advisory lock can satisfy
// the query.
func waitForAdvisoryWaiters(c *qt.C, ctx context.Context, db *sql.DB, want int) {
	c.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_locks
			WHERE locktype = 'advisory' AND database = (
				SELECT oid FROM pg_database WHERE datname = current_database())
			AND NOT granted`).Scan(&waiting)
		c.Assert(err, qt.IsNil)
		if waiting >= want {
			return
		}
		if time.Now().After(deadline) {
			c.Fatalf("found %d lifecycle waiters, want at least %d", waiting, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// assertACompleteRunDoesNotLowerTheFloor keeps the SQL membership predicate
// tied to Run.Terminal rather than to abandonment alone. A complete row is no
// longer a feeder even when a damaged or imported registry lacks a matching
// retired generation row.
func assertACompleteRunDoesNotLowerTheFloor(
	c *qt.C, ctx context.Context, db *sql.DB, store *floorPruneStore,
) {
	c.Helper()
	seedReader(c, ctx, store, "gen-complete", "run-complete", "articles", "5", floorAt)
	// This is deliberately a damaged/imported-registry fixture, not a supported
	// state transition: a complete row whose generation registry row is absent
	// makes the status predicate the only reason it leaves the reader set.
	_, err := db.ExecContext(ctx, `UPDATE `+embedstore.RunTable+`
		SET phase = $2, status = $3 WHERE id = $1`, "run-complete",
		string(embedrun.PhaseRetired), string(embedrun.StatusComplete))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `DELETE FROM `+embedstore.GenerationTable+` WHERE identity = $1`,
		"gen-complete")
	c.Assert(err, qt.IsNil)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
}

// assertASameNamedTableInAnotherSchemaIsAnotherSource is stokaro/ptah#2724.
//
// An outbox is keyed on the qualified pair -- Outbox.TableName digests the
// schema and the table -- while a run recorded the bare table name. So
// `public.docs` and `archive.docs` were two outboxes and one source string, and
// each of these two runs was counted as a reader of the other's outbox.
//
// The direction was safe, which is why #2690 recorded it rather than widening
// the column under time pressure: over-including a reader only lowers the floor.
// It is still an answer about the wrong table, and it stops being conservative
// the moment anything reads this to decide something less forgiving.
func assertASameNamedTableInAnotherSchemaIsAnotherSource(
	c *qt.C, ctx context.Context, store *floorPruneStore,
) {
	c.Helper()
	before, ok, err := store.OutboxFloor(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)

	// Deliberately EARLIER than anything reading public.articles, so a run
	// counted against the wrong source would pull the floor down to it and the
	// comparison below would say so.
	seedReaderIn(c, ctx, store, "archive", "gen-archive", "run-archive", "articles", "3", floorAt)

	after, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(after, qt.DeepEquals, before)
	c.Assert(after.Position, qt.Not(qt.Equals), embedcatchup.Cursor{Transaction: 3})
}

// assertARunRecordedBeforeTheIdentityStillCounts is the safety half.
//
// A run created before stokaro/ptah#2724 holds a bare table name. Excluding it
// would raise the floor and prune events it still owes, which is the one
// direction this query may not be wrong in -- so the bare name is matched too,
// deliberately, and this is what says so.
func assertARunRecordedBeforeTheIdentityStillCounts(
	c *qt.C, ctx context.Context, store *floorPruneStore,
) {
	c.Helper()
	// Earlier than every reader seeded above, so a run this query stopped
	// counting would leave the floor where it was and this would fail.
	seedLegacyReader(c, ctx, store, "gen-legacy", "run-legacy", "articles", "2", floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 2})
	c.Assert(floor.Holders, qt.DeepEquals, []embedpg.OutboxFloorHolder{{
		RunID: "run-legacy", Generation: "gen-legacy",
	}})
}

// assertNoReaderIsNotAFloor is the control the others need.
//
// An empty reader set has to report absence rather than the zero cursor. Zero
// as a floor is not a conservative answer, it is the whole table: every event
// ever captured sits above it.
func assertNoReaderIsNotAFloor(c *qt.C, ctx context.Context, store *floorPruneStore) {
	c.Helper()
	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsFalse)
	c.Assert(floor, qt.DeepEquals, embedpg.OutboxFloorResult{})
}

// assertFloorIsTheEarliestReader is the property the whole change rests on.
//
// Two live generations over one source, at different positions. The floor is
// the one that has read LESS, because the events between them are still owed by
// somebody.
func assertFloorIsTheEarliestReader(c *qt.C, ctx context.Context, store *floorPruneStore) {
	c.Helper()
	seedReader(c, ctx, store, "gen-ahead", "run-ahead", "articles", "4446", floorAt)
	seedReader(c, ctx, store, "gen-behind", "run-behind", "articles", "1200", floorAt)
	seedReader(c, ctx, store, "gen-behind-too", "run-behind-too", "articles", "1200", floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
	c.Assert(floor.Holders, qt.DeepEquals, []embedpg.OutboxFloorHolder{
		{RunID: "run-behind", Generation: "gen-behind"},
		{RunID: "run-behind-too", Generation: "gen-behind-too"},
	})
}

// assertAnotherSourceDoesNotLowerTheFloor keys the answer to the source table.
//
// Without this a query that ignored its argument would satisfy every other
// assertion here, and in production one busy migration would pin the floor of
// every other outbox on the server.
func assertAnotherSourceDoesNotLowerTheFloor(
	c *qt.C, ctx context.Context, store *floorPruneStore,
) {
	c.Helper()
	seedReader(c, ctx, store, "gen-elsewhere", "run-elsewhere", "invoices", "7", floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
}

// assertARetiredGenerationDoesNotLowerTheFloor is what lets an outbox ever
// shrink.
//
// A retired generation reads nothing, so it stops holding events. Retirement
// remains the destructive lever for the whole generation; abandonment below is
// the non-destructive lever for one run.
func assertARetiredGenerationDoesNotLowerTheFloor(
	c *qt.C, ctx context.Context, db *sql.DB, store *floorPruneStore,
) {
	c.Helper()
	seedReader(c, ctx, store, "gen-retired", "run-retired", "articles", "3", floorAt)
	_, err := db.ExecContext(ctx, `UPDATE `+embedstore.GenerationTable+`
		SET retired_at = $2 WHERE identity = $1`, "gen-retired", floorAt)
	c.Assert(err, qt.IsNil)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
}

// assertAnAbandonedRunDoesNotLowerTheFloor is stokaro/ptah#2723's
// non-destructive release. The generation and its vectors remain registered;
// the terminal run alone stops claiming outbox history.
func assertAnAbandonedRunDoesNotLowerTheFloor(
	c *qt.C, ctx context.Context, store *floorPruneStore,
) {
	c.Helper()
	seedReader(c, ctx, store, "gen-abandoned", "run-abandoned", "articles", "4", floorAt)
	_, err := store.AbandonRun(
		ctx, "run-abandoned", "the migration was superseded")
	c.Assert(err, qt.IsNil)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 1200})
	registered, err := store.Generation(ctx, "gen-abandoned")
	c.Assert(err, qt.IsNil)
	c.Assert(registered.Retired(), qt.IsFalse)
}

// assertAnUncaughtRunReadsFromItsBoundary counts a prepared run as a reader.
//
// A generation that has been prepared and backfilled but never caught up owes
// every change since its snapshot boundary. Reading only the catch-up watermark
// would leave it out of the reader set and delete the whole backlog it has yet
// to process -- and the other assertions here cannot see that, because a query
// selecting only caught-up runs still answers them correctly.
func assertAnUncaughtRunReadsFromItsBoundary(
	c *qt.C, ctx context.Context, store *floorPruneStore,
) {
	c.Helper()
	seedRun(c, ctx, store, "public", "gen-fresh", "run-fresh", "articles", embedrun.Run{
		Source:            embedpg.SourceIdentity("public", "articles"),
		SnapshotWatermark: "44",
	}, floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 44})
}

// assertARunWithNoPositionIsSkipped keeps a run that watches nothing out of the
// reader set.
//
// prepare writes both watermarks empty for a mode that records no changes. Such
// a run is not a reader sitting at zero; treating it as one would take the
// floor to zero and authorize deleting the whole table -- the one mistake with
// no recovery, which is why it is asserted rather than reasoned about.
func assertARunWithNoPositionIsSkipped(c *qt.C, ctx context.Context, store *floorPruneStore) {
	c.Helper()
	seedRun(c, ctx, store, "public", "gen-immutable", "run-immutable", "articles", embedrun.Run{
		Source: embedpg.SourceIdentity("public", "articles"),
	}, floorAt)

	floor, ok, err := store.OutboxFloor(ctx, "public", "articles")

	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(floor.Position, qt.Equals, embedcatchup.Cursor{Transaction: 44})
}

// seedReader registers a live generation and a run that has caught up to a
// position.
// seedReaderIn seeds a reader of a source in a named schema, and seedLegacyReader
// one recorded the way a Ptah before stokaro/ptah#2724 recorded it.
//
// Three seeders rather than a flag, because what varies is the value written
// into the column under test, and a test that asked a helper to decide it would
// be asserting about whatever the helper chose.
func seedReaderIn(
	c *qt.C, ctx context.Context, store *floorPruneStore,
	schema, generation, runID, source, watermark string, at time.Time,
) {
	c.Helper()
	seedReaderWithSource(c, ctx, store, schema, generation, runID, source,
		embedpg.SourceIdentity(schema, source), watermark, at)
}

func seedLegacyReader(
	c *qt.C, ctx context.Context, store *floorPruneStore,
	generation, runID, source, watermark string, at time.Time,
) {
	c.Helper()
	seedReaderWithSource(c, ctx, store, "public", generation, runID, source,
		source, watermark, at)
}

func seedReader(
	c *qt.C, ctx context.Context, store *floorPruneStore,
	generation, runID, source, watermark string, at time.Time,
) {
	c.Helper()
	seedReaderWithSource(c, ctx, store, "public", generation, runID, source,
		embedpg.SourceIdentity("public", source), watermark, at)
}

func seedReaderWithSource(
	c *qt.C, ctx context.Context, store *floorPruneStore,
	sourceSchema, generation, runID, source, recordedSource, watermark string, at time.Time,
) {
	c.Helper()
	seedRun(c, ctx, store, sourceSchema, generation, runID, source, embedrun.Run{
		Source: recordedSource, SnapshotWatermark: "1", CatchUpWatermark: watermark,
	}, at)
}

// seedRun registers a generation and the run over it, taking the watermarks and
// the recorded source from the caller.
//
// The source comes off `positioned` rather than from the `source` argument,
// because what a run records is the thing under test: a current one records the
// identity and one created before stokaro/ptah#2724 recorded the bare name.
func seedRun(
	c *qt.C, ctx context.Context, store *floorPruneStore,
	sourceSchema, generation, runID, source string, positioned embedrun.Run, at time.Time,
) {
	c.Helper()
	_, err := store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: generation, SpecDigest: generation, Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: source, TargetColumn: "embedding", CreatedAt: at,
		SourceSchema: sourceSchema, SourceTable: source,
	})
	c.Assert(err, qt.IsNil)
	run := positioned
	run.ID = runID
	run.GenerationIdentity = generation
	run.SpecDigest = generation
	run.Phase = embedrun.PhasePrepared
	run.Status = embedrun.StatusRunning
	run.CreatedAt = at
	run.UpdatedAt = at
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
}
