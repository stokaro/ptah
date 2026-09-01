//go:build integration

package integration_test

// Live PostgreSQL coverage for what a write does to a row that already holds
// one.
//
// embedrun.ResolveWrite has always held these rules -- a write never crosses
// generations, a stale answer does not win, a tombstone survives a late update
// -- and until stokaro/ptah#2391 nothing called it. The write path rendered an
// unconditional UPDATE with no generation and no version predicate, so every
// one of them was in force only in that function's own unit tests.
//
// These are live because the resolution now happens against what the target
// holds, read and locked inside the committing transaction. A fake cannot
// measure that.

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrun"
)

// TestCommit_AWriteNeverCrossesGenerationsLive is the rule an operator's
// corpus depends on.
//
// Before this, a second generation's backfill overwrote the first row by row,
// silently, and the cutover that followed had nothing to go back to.
func TestCommit_AWriteNeverCrossesGenerationsLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-1", Version: "1",
		Vector: []float32{1, 2, 3, 4}, Kind: embedrun.WriteUpsert,
	})

	err := commitReturningError(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-2", InputHash: "hash-2", Version: "2",
		Vector: []float32{9, 9, 9, 9}, Kind: embedrun.WriteUpsert,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "the row belongs to generation gen-1")
	// And the first generation's vector is still what it was. A refusal that
	// left the row half-written would be the same defect wearing an error.
	vector, generation := storedVector(c, ctx, db, table, "embedding")
	c.Assert(vector, qt.Equals, "[1,2,3,4]")
	c.Assert(generation, qt.Equals, "gen-1")
}

// TestCommit_AStaleAnswerDoesNotWinLive is what makes at-least-once delivery
// safe.
//
// A provider result computed from a version the row has moved past arrives
// late. It is not an error -- retries produce these -- and it must not replace
// the newer vector that already landed.
func TestCommit_AStaleAnswerDoesNotWinLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-9", Version: "9",
		Vector: []float32{9, 9, 9, 9}, Kind: embedrun.WriteUpsert,
	})
	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-7", Version: "7",
		Vector: []float32{7, 7, 7, 7}, Kind: embedrun.WriteUpsert,
	})

	vector, _ := storedVector(c, ctx, db, table, "embedding")
	c.Assert(vector, qt.Equals, "[9,9,9,9]")
}

// TestCommit_ANewerAnswerStillWinsLive is the control the test above needs.
//
// "The second write did not land" is satisfied by a write path that stopped
// writing altogether. This is the same shape with the versions the other way
// round, and it must land.
func TestCommit_ANewerAnswerStillWinsLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-7", Version: "7",
		Vector: []float32{7, 7, 7, 7}, Kind: embedrun.WriteUpsert,
	})
	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-9", Version: "9",
		Vector: []float32{9, 9, 9, 9}, Kind: embedrun.WriteUpsert,
	})

	vector, _ := storedVector(c, ctx, db, table, "embedding")
	c.Assert(vector, qt.Equals, "[9,9,9,9]")
}

// TestCommit_ATombstoneSurvivesALateUpdateLive keeps a deleted row deleted.
//
// A row the source dropped, then reached by an embedding that was already in
// flight, must not come back.
func TestCommit_ATombstoneSurvivesALateUpdateLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-8", Version: "8",
		Kind: embedrun.WriteTombstone,
	})
	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-7", Version: "7",
		Vector: []float32{7, 7, 7, 7}, Kind: embedrun.WriteUpsert,
	})

	var state string
	c.Assert(db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT embedding_state FROM %s WHERE id = 1", table)).Scan(&state), qt.IsNil)
	c.Assert(state, qt.Equals, string(embedrun.WriteTombstone))
}

// TestCommit_ANewerSourceVersionRevivesATombstonedRowLive is that rule's
// control.
//
// A tombstone is terminal until the source says otherwise with a NEWER
// version. Without this row, a write path that ignored every write after a
// tombstone would pass the test above.
func TestCommit_ANewerSourceVersionRevivesATombstonedRowLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-8", Version: "8",
		Kind: embedrun.WriteTombstone,
	})
	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-9", Version: "9",
		Vector: []float32{9, 9, 9, 9}, Kind: embedrun.WriteUpsert,
	})

	vector, _ := storedVector(c, ctx, db, table, "embedding")
	c.Assert(vector, qt.Equals, "[9,9,9,9]")
}

// TestEnsureTarget_RefusesAColumnAnotherGenerationHoldsLive moves the refusal
// to where it costs nothing.
//
// The write path refuses one row at a time, in the middle of a backfill, after
// the provider was called for that batch. A column that is somebody else's is a
// specification to edit, and prepare is when an operator can still edit it.
func TestEnsureTarget_RefusesAColumnAnotherGenerationHoldsLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)
	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "somebody-elses-generation",
		InputHash: "hash-1", Version: "1",
		Vector: []float32{1, 2, 3, 4}, Kind: embedrun.WriteUpsert,
	})

	err := embedpg.EnsureTarget(ctx, db, spec)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `column "embedding" on `+table)
	c.Assert(err.Error(), qt.Contains, "Give this one its own target.column")
}

// TestEnsureTarget_TwoGenerationsInTwoColumnsCoexistLive is Decision 6, which
// nothing in this repository had ever built.
//
// Every rollback test until now registered a second generation as a bookkeeping
// row with no vectors behind it and moved the pointer by hand. This puts two
// real corpora on one table and requires both to survive, which is the property
// the whole cutover-and-rollback design rests on.
func TestEnsureTarget_TwoGenerationsInTwoColumnsCoexistLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)

	first := loadTargetSpec(c, table)
	second := loadTargetSpec(c, table)
	second.Target.Column = "embedding_v2"
	second.Model.Identifier = "another-model"

	c.Assert(embedpg.EnsureTarget(ctx, db, first), qt.IsNil)
	commit(c, ctx, db, first, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: first.Identity().Digest,
		InputHash: "hash-1", Version: "1",
		Vector: []float32{1, 1, 1, 1}, Kind: embedrun.WriteUpsert,
	})
	c.Assert(embedpg.EnsureTarget(ctx, db, second), qt.IsNil)
	commit(c, ctx, db, second, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: second.Identity().Digest,
		InputHash: "hash-2", Version: "1",
		Vector: []float32{2, 2, 2, 2}, Kind: embedrun.WriteUpsert,
	})

	firstVector, firstGeneration := storedVector(c, ctx, db, table, "embedding")
	secondVector, secondGeneration := storedVector(c, ctx, db, table, "embedding_v2")

	c.Assert(firstVector, qt.Equals, "[1,1,1,1]")
	c.Assert(secondVector, qt.Equals, "[2,2,2,2]")
	c.Assert(firstGeneration, qt.Equals, first.Identity().Digest)
	c.Assert(secondGeneration, qt.Equals, second.Identity().Digest)
	c.Assert(firstGeneration, qt.Not(qt.Equals), secondGeneration)
}

// commit applies one write and requires it to succeed.
func commit(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, write embedrun.TargetWrite,
) {
	c.Helper()
	c.Assert(commitReturningError(c, ctx, db, spec, write), qt.IsNil)
}

// commitReturningError applies one write and hands back whatever happened.
func commitReturningError(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, write embedrun.TargetWrite,
) error {
	c.Helper()
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)

	// Commit writes the run's progress in the same transaction as the vectors,
	// so the run has to be there. One per write, named for what the write
	// carries: these tests deliver the same key more than once and a shared run
	// row would make the second commit lose the fencing check rather than the
	// resolution this is about.
	run := embedrun.Run{
		ID: write.Generation + ":" + write.InputHash, SpecDigest: "spec",
		GenerationIdentity: write.Generation,
		Phase:              embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
	}
	c.Assert(embedpg.NewStore(db).CreateRun(ctx, run), qt.IsNil)
	return target.Commit(ctx, []embedrun.TargetWrite{write}, run)
}

// storedVector reads one row's vector and the generation it is tagged with.
func storedVector(
	c *qt.C, ctx context.Context, db *sql.DB, table, column string,
) (string, string) {
	c.Helper()
	var vector, generation sql.NullString
	// #nosec G201 -- a column name this test chose.
	query := fmt.Sprintf("SELECT %s::text, %s_generation FROM %s WHERE id = 1",
		column, column, table)
	c.Assert(db.QueryRowContext(ctx, query).Scan(&vector, &generation), qt.IsNil)
	return vector.String, generation.String
}

// TestCommit_ATombstoneSurvivesAnUnorderedUpdateLive is the fixture the
// tombstone rule needs to be measurable at all.
//
// With a strictly older update, the stale-version rule already refuses it, so
// removing the tombstone rule changes nothing and no sweep can see it -- two
// answers, neither measurable. At the SAME version the two rules disagree: a
// tombstone is terminal until the source says otherwise with a NEWER version,
// so an equal one loses, and without the rule the differing input hash carries
// the write through and the deleted row comes back.
func TestCommit_ATombstoneSurvivesAnUnorderedUpdateLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-a", Version: "8",
		Kind: embedrun.WriteTombstone,
	})
	commit(c, ctx, db, spec, embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", InputHash: "hash-b", Version: "8",
		Vector: []float32{7, 7, 7, 7}, Kind: embedrun.WriteUpsert,
	})

	var state string
	c.Assert(db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT embedding_state FROM %s WHERE id = 1", table)).Scan(&state), qt.IsNil)
	c.Assert(state, qt.Equals, string(embedrun.WriteTombstone))
}

// TestCommit_ConcurrentWritesForOneKeyAreOrderedLive is what the row lock is
// for.
//
// Two workers embedding the same row is exactly what the resolution exists to
// order. Without FOR UPDATE both read the same prior state, both decide they
// win, and the row ends up holding whichever transaction happened to commit
// second rather than whichever answer is newer.
//
// Repeated, because the defect is a race: with the lock the answer is always
// the newer version, so a run that reports the older one is a real failure and
// never a flake. The repetition only shortens how long a broken build can hide.
func TestCommit_ConcurrentWritesForOneKeyAreOrderedLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := counterVersionedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	for attempt := range 12 {
		t.Run(fmt.Sprintf("attempt %d", attempt), func(t *testing.T) {
			c := qt.New(t)
			resetRow(c, ctx, db, table)

			newer := embedrun.TargetWrite{
				Key: []string{"1"}, Generation: "gen-1",
				InputHash: fmt.Sprintf("hash-9-%d", attempt), Version: "9",
				Vector: []float32{9, 9, 9, 9}, Kind: embedrun.WriteUpsert,
			}
			older := embedrun.TargetWrite{
				Key: []string{"1"}, Generation: "gen-1",
				InputHash: fmt.Sprintf("hash-7-%d", attempt), Version: "7",
				Vector: []float32{7, 7, 7, 7}, Kind: embedrun.WriteUpsert,
			}

			var wait sync.WaitGroup
			wait.Add(2)
			errs := make([]error, 2)
			for index, write := range []embedrun.TargetWrite{newer, older} {
				go func() {
					defer wait.Done()
					errs[index] = commitReturningError(c, ctx, db, spec, write)
				}()
			}
			wait.Wait()

			c.Assert(errs[0], qt.IsNil)
			c.Assert(errs[1], qt.IsNil)
			vector, _ := storedVector(c, ctx, db, table, "embedding")
			c.Assert(vector, qt.Equals, "[9,9,9,9]",
				qt.Commentf("the older answer won, so the two writes were not ordered"))
		})
	}
}

// resetRow clears the generation's columns between attempts.
func resetRow(c *qt.C, ctx context.Context, db *sql.DB, table string) {
	c.Helper()
	// #nosec G201 -- a table name this test created.
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET embedding = NULL, embedding_generation = NULL,
			embedding_input_hash = NULL, embedding_source_version = NULL,
			embedding_state = NULL`, table))
	c.Assert(err, qt.IsNil)
}

// counterVersionedSpec declares the ordering these tests' versions actually
// have.
//
// Every write below carries a counter -- "7", "9", "10" -- and the shared
// fixture declares `version_strategy: updated_at`. That went unnoticed while
// one comparison served every strategy: ordering opaque strings by length then
// lexicographically happens to order small integers correctly, so a fixture
// whose values did not match its own declaration still passed.
//
// It stopped passing when the strategy started deciding the order
// (stokaro/ptah#2635), and the fixture was the thing that was wrong: a
// timestamp strategy cannot read "9", so nothing was comparable and the stale
// answer won. Declaring `monotonic` is what these values are.
func counterVersionedSpec(c *qt.C, table string) embedgen.Spec {
	c.Helper()
	spec := loadTargetSpec(c, table)
	spec.Source.VersionStrategy = embedgen.VersionMonotonic
	spec.Source.VersionField = "updated_at"
	return spec
}
