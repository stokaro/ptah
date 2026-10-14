package embedstore_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// A store built from a snapshot holds exactly it, and answers its own
// snapshot back in key order, so a store that persists rows elsewhere can
// decide on them and see what the decision changed.
func TestNewMemoryFrom_HoldsTheSnapshot(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	second := aRun()
	second.ID = "run-0"
	snapshot := embedstore.Snapshot{
		Generations: []embedstore.Generation{aGeneration()},
		Runs:        []embedrun.Run{aRun(), second},
		Pointers: []embedstore.Pointer{{
			TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
		}},
	}

	store := embedstore.NewMemoryFrom(snapshot, nil)

	run, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(run, qt.DeepEquals, aRun())
	pointer, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, "gen-1")
	read := store.Snapshot()
	c.Assert(read.Generations, qt.DeepEquals, snapshot.Generations)
	c.Assert(read.Runs, qt.DeepEquals, []embedrun.Run{second, aRun()})
	c.Assert(read.Pointers, qt.DeepEquals, snapshot.Pointers)
}

// The snapshot a decision answers with does not share the store's slices, so
// a caller writing it back cannot rewrite what the store holds.
func TestMemory_SnapshotDoesNotShareTheStoresCursor(t *testing.T) {
	c := qt.New(t)
	store := embedstore.NewMemoryFrom(embedstore.Snapshot{Runs: []embedrun.Run{aRun()}}, nil)

	read := store.Snapshot()
	read.Runs[0].Cursor[0] = "changed"

	run, err := store.Run(context.Background(), "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(run.Cursor, qt.DeepEquals, aRun().Cursor)
}

// ReachPhase moves a run forward and leaves a phase it has passed alone.
func TestMemory_ReachPhase(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)

	c.Assert(store.ReachPhase(ctx, "run-1", embedrun.PhaseBackfilled), qt.IsNil)
	c.Assert(store.ReachPhase(ctx, "run-1", embedrun.PhaseBackfilling), qt.IsNil)

	run, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(run.Phase, qt.Equals, embedrun.PhaseBackfilled)
	c.Assert(store.ReachPhase(ctx, "run-1", embedrun.PhaseCutOver), qt.ErrorIs, embedrun.ErrPhase)
	c.Assert(store.ReachPhase(ctx, "no-such-run", embedrun.PhaseBackfilled), qt.ErrorIs, embedstore.ErrNotFound)
}

// Retirement completes every run of the generation, records the phase where
// the lifecycle declares it, and stamps the registry row with the instant it
// sampled.
func TestMemory_RetireGeneration_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemoryWithClock(func() time.Time { return at })
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	cutOver := aRun()
	cutOver.Phase = embedrun.PhaseCutOver
	early := aRun()
	early.ID = "run-early"
	c.Assert(store.CreateRun(ctx, cutOver), qt.IsNil)
	c.Assert(store.CreateRun(ctx, early), qt.IsNil)

	retiredAt, err := store.RetireGeneration(ctx, "gen-1", embedstore.Pointer{})

	c.Assert(err, qt.IsNil)
	c.Assert(retiredAt, qt.Equals, at)
	generation, err := store.Generation(ctx, "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(generation.RetiredAt, qt.Equals, at)
	retired, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(retired.Phase, qt.Equals, embedrun.PhaseRetired)
	c.Assert(retired.Status, qt.Equals, embedrun.StatusComplete)
	c.Assert(retired.FencingToken, qt.Equals, aRun().FencingToken+1)
	c.Assert(retired.LeaseOwner, qt.Equals, "")
	stopped, err := store.Run(ctx, "run-early")
	c.Assert(err, qt.IsNil)
	c.Assert(stopped.Phase, qt.Equals, embedrun.PhaseBackfilling)
	c.Assert(stopped.Status, qt.Equals, embedrun.StatusComplete)
}

// Retirement refuses a generation that is gone already, one queries read, one
// whose pointer moved since the approval, and one still maintained, and
// changes nothing when it refuses.
func TestMemory_RetireGeneration_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		retiredAt  time.Time
		maintained time.Time
		pointers   []embedstore.Pointer
		expected   embedstore.Pointer
		wantErr    error
		wantText   string
	}{
		{name: "retired already", retiredAt: at.Add(-time.Hour), wantErr: embedstore.ErrRetired,
			wantText: `the generation was retired: generation gen-1 was retired at 2026-08-27T11:00:00Z`},
		{name: "active",
			pointers: []embedstore.Pointer{{TargetSchema: "public", TargetTable: "articles", Active: "gen-1"}},
			expected: embedstore.Pointer{TargetSchema: "public", TargetTable: "articles", Active: "gen-1"},
			wantErr:  embedstore.ErrConflict,
			wantText: `the state changed underneath this write: generation gen-1 is active for public.articles`},
		{name: "pointer moved",
			pointers: []embedstore.Pointer{{
				TargetSchema: "public", TargetTable: "articles", Active: "gen-2", Previous: "gen-1"}},
			wantErr: embedstore.ErrConflict,
			wantText: `the state changed underneath this write: public.articles pointer changed from active , ` +
				`previous  to active gen-2, previous gen-1`},
		{name: "maintained", maintained: at.Add(time.Hour), wantErr: embedstore.ErrConflict,
			wantText: `the state changed underneath this write: generation gen-1 is maintained until ` +
				`2026-08-27T13:00:00Z`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generation := aGeneration()
			generation.RetiredAt, generation.MaintainedUntil = test.retiredAt, test.maintained
			store := embedstore.NewMemoryFrom(embedstore.Snapshot{
				Generations: []embedstore.Generation{generation},
				Runs:        []embedrun.Run{aRun()},
				Pointers:    test.pointers,
			}, func() time.Time { return at })
			before := store.Snapshot()

			retiredAt, err := store.RetireGeneration(context.Background(), "gen-1", test.expected)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(err, qt.ErrorMatches, test.wantText)
			c.Assert(retiredAt.IsZero(), qt.IsTrue)
			c.Assert(store.Snapshot(), qt.DeepEquals, before)
		})
	}
}
