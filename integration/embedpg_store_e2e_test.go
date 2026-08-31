//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
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
	assertAClaimWritesTheLeaseAndNothingElse(c, ctx, store)
	assertRegistrationIsIdempotent(c, ctx, store)
	assertRetirementIsTerminal(c, ctx, store)
	assertPointerIsCompareAndSet(c, ctx, store)
	assertEventTrailIsOrdered(c, ctx, store)
	assertAbsenceIsNotEmptiness(c, ctx, store)
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
		Environment: "production", Source: "public.articles", Target: "public.articles.embedding",
		ProviderProfile: "local", ResolvedModel: "fake-model@1",
		PtahVersion: "test", PolicyDigest: "policy-1",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", LeaseExpires: liveAt.Add(time.Minute), FencingToken: 7,
		SnapshotWatermark: "lsn-1", CatchUpWatermark: "lsn-2",
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

// assertRetirementIsTerminal keeps the record of when a corpus was destroyed.
func assertRetirementIsTerminal(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	_, err := store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: "gen-retire", SpecDigest: "spec-1", Reproducibility: "full", Dimension: 8,
		TargetSchema: "public", TargetTable: "articles", TargetColumn: "embedding_old", CreatedAt: liveAt,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(store.RetireGeneration(ctx, "gen-retire", liveAt), qt.IsNil)

	err = store.RetireGeneration(ctx, "gen-retire", liveAt.Add(time.Hour))

	c.Assert(err, qt.ErrorIs, embedstore.ErrRetired)
	generation, readErr := store.Generation(ctx, "gen-retire")
	c.Assert(readErr, qt.IsNil)
	c.Assert(generation.RetiredAt, qt.Equals, liveAt)
	c.Assert(generation.Retired(), qt.IsTrue)
}

// assertPointerIsCompareAndSet is the store's half of the cutover rule.
func assertPointerIsCompareAndSet(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	schema, table := "public", "articles"
	// Before anything is there: a move naming a generation the table does not
	// read is refused rather than quietly becoming the first cutover.
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
}

// assertEventTrailIsOrdered keeps a run's history readable as one.
func assertEventTrailIsOrdered(c *qt.C, ctx context.Context, store *embedpg.Store) {
	c.Helper()
	run := liveRun("events")
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
func assertAbsenceIsNotEmptiness(c *qt.C, ctx context.Context, store *embedpg.Store) {
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
	c.Assert(store.RetireGeneration(ctx, "nothing", liveAt), qt.ErrorIs, embedstore.ErrNotFound)
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
