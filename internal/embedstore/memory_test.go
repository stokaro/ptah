package embedstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// at is a fixed instant, so a test says what it means rather than what the
// clock happened to answer.
var at = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

// aRun is a run in mid-backfill.
func aRun() embedrun.Run {
	return embedrun.Run{
		ID:                 "run-1",
		SpecDigest:         "spec-1",
		GenerationIdentity: "gen-1",
		Environment:        "production",
		Source:             embedstore.SourceIdentity("public", "articles"),
		Phase:              embedrun.PhaseBackfilling,
		Status:             embedrun.StatusRunning,
		LeaseOwner:         "worker-a",
		LeaseExpires:       at.Add(time.Minute),
		FencingToken:       7,
		Cursor:             []string{"2026-01-01", "1000"},
		CreatedAt:          at,
		UpdatedAt:          at,
	}
}

// aGeneration is a registry row.
func aGeneration() embedstore.Generation {
	return embedstore.Generation{
		Identity: "gen-1", SpecDigest: "spec-1", Name: "articles v2",
		Reproducibility: "full", Dimension: 1024,
		TargetSchema: "public", TargetTable: "articles", TargetColumn: "embedding_v2",
		SourceSchema: "public", SourceTable: "articles",
		CreatedAt: at,
	}
}

// TestMemory_ARunSurvivesASaveAndReload is the control.
func TestMemory_ARunSurvivesASaveAndReload(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)

	loaded, err := store.Run(ctx, "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(loaded, qt.DeepEquals, aRun())
}

// TestMemory_AStaleFencingTokenIsRefused is the rule the token exists for.
//
// The worker's lease was taken over while its request was in flight. It is
// still running, it still believes it owns the run, and the store is the only
// place that knows otherwise -- a lease check in the worker cannot see this,
// because the worker checked before the takeover happened.
func TestMemory_AStaleFencingTokenIsRefused(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	takenOver := aRun()
	takenOver.FencingToken = 8
	takenOver.LeaseOwner = "worker-b"
	c.Assert(store.SaveRun(ctx, takenOver), qt.IsNil)

	err := store.SaveRun(ctx, aRun())

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err, qt.ErrorMatches, `.*run run-1 is fenced at token 8 and this write carries 7.*`)
	current, _ := store.Run(ctx, "run-1")
	c.Assert(current.LeaseOwner, qt.Equals, "worker-b")
}

// TestMemory_TheCurrentTokenHolderStillWrites is the control for the row above.
//
// A store that refused every write would satisfy it and stop every backfill
// checkpointing.
func TestMemory_TheCurrentTokenHolderStillWrites(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	advanced := aRun()
	advanced.Progress.RowsEmbedded = 5000

	c.Assert(store.SaveRun(ctx, advanced), qt.IsNil)

	current, _ := store.Run(ctx, "run-1")
	c.Assert(current.Progress.RowsEmbedded, qt.Equals, int64(5000))
}

func TestMemory_SaveRunPreservesResumeAndProgressMonotonicity(t *testing.T) {
	base := aRun()
	base.SnapshotWatermark = "100"
	base.CatchUpWatermark = "100:5"
	base.Progress = embedrun.Progress{
		RowsScanned: 20, RowsEmbedded: 18, RowsSkipped: 2,
		BatchesCommitted: 4, ProviderPromptTokens: 30,
		ProviderTotalTokens: 40, ProviderUsageBatches: 3, RetryCount: 2,
	}
	tests := []struct {
		name   string
		mutate func(*embedrun.Run)
	}{
		{name: "source changes", mutate: func(run *embedrun.Run) { run.Source = "other" }},
		{name: "snapshot changes", mutate: func(run *embedrun.Run) { run.SnapshotWatermark = "101" }},
		{name: "position disappears", mutate: func(run *embedrun.Run) {
			run.SnapshotWatermark, run.CatchUpWatermark = "", ""
		}},
		{name: "catch-up moves backward", mutate: func(run *embedrun.Run) {
			run.CatchUpWatermark = "100:4"
		}},
		{name: "rows move backward", mutate: func(run *embedrun.Run) {
			run.Progress.RowsEmbedded--
		}},
		{name: "retry count moves backward in one batch", mutate: func(run *embedrun.Run) {
			run.Progress.RetryCount--
		}},
		{name: "cursor changes without a batch", mutate: func(run *embedrun.Run) {
			run.Cursor = []string{"stale"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			store := embedstore.NewMemoryWithClock(func() time.Time { return at })
			generation := aGeneration()
			_, err := store.RegisterGeneration(ctx, generation)
			c.Assert(err, qt.IsNil)
			c.Assert(store.CreateRun(ctx, base), qt.IsNil)
			offered := base
			test.mutate(&offered)

			c.Assert(store.SaveRun(ctx, offered), qt.ErrorIs, embedstore.ErrConflict)
			stored, err := store.Run(ctx, base.ID)
			c.Assert(err, qt.IsNil)
			c.Assert(stored, qt.DeepEquals, base)
		})
	}

	// The retry count is per batch, so a committed next batch legitimately
	// resets it while every cumulative counter and the cursor advance.
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, base), qt.IsNil)
	next := base
	next.Progress.BatchesCommitted++
	next.Progress.RetryCount = 0
	next.Cursor = []string{"next"}
	c.Assert(store.SaveRun(ctx, next), qt.IsNil)
}

func TestMemory_SaveRunCannotChangePositionMembership(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	unpositioned := aRun()
	c.Assert(store.CreateRun(ctx, unpositioned), qt.IsNil)
	offered := unpositioned
	offered.SnapshotWatermark = "100"
	c.Assert(store.SaveRun(ctx, offered), qt.ErrorIs, embedstore.ErrConflict)
}

func TestMemory_SaveRunCannotEnterOrLeavePointerAuthoritativePhases(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	run := aRun()
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	offered := run
	offered.Phase = embedrun.PhaseCutOver
	c.Assert(store.SaveRun(ctx, offered), qt.ErrorIs, embedrun.ErrPhase)

	cutOver := run
	cutOver.ID = "already-cut-over"
	cutOver.Phase = embedrun.PhaseCutOver
	c.Assert(store.CreateRun(ctx, cutOver), qt.IsNil)
	checkpoint := cutOver
	checkpoint.Progress.RowsEmbedded++
	c.Assert(store.SaveRun(ctx, checkpoint), qt.IsNil)
	for _, phase := range []embedrun.Phase{embedrun.PhaseVerified, embedrun.PhaseRolledBack} {
		stale := checkpoint
		stale.Phase = phase
		c.Assert(store.SaveRun(ctx, stale), qt.ErrorIs, embedrun.ErrPhase)
	}
}

// TestMemory_ASaveCannotMoveARunBetweenGenerations keeps a checkpoint from
// changing live-feeder membership without either generation's lifecycle lock.
func TestMemory_ASaveCannotMoveARunBetweenGenerations(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	moved := aRun()
	moved.GenerationIdentity = "gen-2"

	c.Assert(store.SaveRun(ctx, moved), qt.ErrorIs, embedrun.ErrGeneration)
	stored, err := store.Run(ctx, moved.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(stored.GenerationIdentity, qt.Equals, aRun().GenerationIdentity)
}

func TestMemory_ASaveCannotCompleteARun(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	run := aRun()
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
}

func TestMemory_RetiredPhaseCannotRemainNonterminal(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	run := aRun()
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	write := run
	write.Phase = embedrun.PhaseRetired

	c.Assert(store.SaveRun(ctx, write), qt.ErrorIs, embedrun.ErrPhase)
	stored, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(stored, qt.DeepEquals, run)
	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: "retired-running", GenerationIdentity: "gen-1",
		Phase: embedrun.PhaseRetired, Status: embedrun.StatusRunning,
	}), qt.ErrorIs, embedrun.ErrPhase)
	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: "already-abandoned", GenerationIdentity: "gen-1",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusAbandoned,
	}), qt.ErrorIs, embedrun.ErrTerminal)
	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: "already-complete", GenerationIdentity: "gen-1",
		Phase: embedrun.PhaseRetired, Status: embedrun.StatusComplete,
	}), qt.ErrorIs, embedrun.ErrTerminal)
}

// TestMemory_TheStoredCursorDoesNotShareTheCallersArray is the aliasing bug a
// value-typed struct hides.
//
// Run is copied by assignment and its cursor is not. A worker appending to its
// own cursor would rewrite what the store believes it checkpointed, and on
// resume that reads as a backfill that skipped rows.
func TestMemory_TheStoredCursorDoesNotShareTheCallersArray(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	run := aRun()
	run.Cursor = make([]string, 2, 8)
	copy(run.Cursor, []string{"2026-01-01", "1000"})
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)

	run.Cursor[1] = "9999"

	loaded, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(loaded.Cursor, qt.DeepEquals, []string{"2026-01-01", "1000"})
}

// TestMemory_ALoadedRunDoesNotShareTheStoresArray is the same aliasing on the
// way out.
func TestMemory_ALoadedRunDoesNotShareTheStoresArray(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	loaded, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)

	loaded.Cursor[1] = "9999"

	again, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(again.Cursor, qt.DeepEquals, []string{"2026-01-01", "1000"})
}

// TestMemory_RegisteringOneIdentityTwiceKeepsTheFirstRow is what a content
// address means for a registry.
//
// Two registrations of one identity are the same registration. A second
// overwriting the first would let a display name -- which is deliberately
// outside the identity -- rewrite when the generation was created.
func TestMemory_RegisteringOneIdentityTwiceKeepsTheFirstRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	first, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	renamed := aGeneration()
	renamed.Name = "something else"
	renamed.CreatedAt = at.Add(24 * time.Hour)

	second, err := store.RegisterGeneration(ctx, renamed)

	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.DeepEquals, first)
	c.Assert(second.Name, qt.Equals, "articles v2")
}

// TestMemory_ACreateRefusesAKnownRetiredGeneration preserves creation before
// registration while preventing a retired registry row from gaining a new
// claimable feeder.
func TestMemory_ACreateRefusesAKnownRetiredGeneration(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	retired := aGeneration()
	retired.RetiredAt = at
	_, err := store.RegisterGeneration(ctx, retired)
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, aRun()), qt.ErrorIs, embedstore.ErrRetired)

	missing := aRun()
	missing.ID = "run-imported"
	missing.GenerationIdentity = "gen-imported"
	c.Assert(store.CreateRun(ctx, missing), qt.IsNil)
}

// TestMemory_MovingAPointerIsCompareAndSet is the store's half of the cutover
// rule.
//
// The decision layer refuses a plan whose pointer moved. This is what stops the
// same race being reintroduced between deciding and writing, which is the
// window the decision cannot cover.
func TestMemory_MovingAPointerIsCompareAndSet(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
	}, ""), qt.IsNil)

	err = store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-3", Previous: "gen-2", CutOverAt: at,
	}, "gen-2")

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err, qt.ErrorMatches, `.*public.articles reads gen-1 and this move expected gen-2.*`)
	current, _ := store.Pointer(ctx, "public", "articles")
	c.Assert(current.Active, qt.Equals, "gen-1")
}

// TestMemory_APointerRecordsTheStateItsCompareAndSetDisplaced keeps the
// historical pointer and maintenance target tied to the same state the move
// compared. A caller cannot move A to B while recording unrelated C as the
// previous generation.
func TestMemory_APointerRecordsTheStateItsCompareAndSetDisplaced(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	for _, identity := range []string{"gen-1", "gen-2", "gen-3"} {
		generation := aGeneration()
		generation.Identity = identity
		_, err := store.RegisterGeneration(ctx, generation)
		c.Assert(err, qt.IsNil)
	}

	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-1",
		Previous: "gen-3", CutOverAt: at,
	}, ""), qt.ErrorIs, embedstore.ErrConflict)
	_, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)

	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
	}, ""), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-2",
		Previous: "gen-3", CutOverAt: at.Add(time.Minute),
	}, "gen-1"), qt.ErrorIs, embedstore.ErrConflict)
	current, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(current.Active, qt.Equals, "gen-1")
	c.Assert(current.Previous, qt.Equals, "")

	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-2",
		Previous: "gen-1", CutOverAt: at.Add(time.Minute),
	}, "gen-1"), qt.IsNil)
	current, err = store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(current.Active, qt.Equals, "gen-2")
	c.Assert(current.Previous, qt.Equals, "gen-1")
}

// TestMemory_AFirstCutoverExpectsNoPointer pins the other end of the same rule.
func TestMemory_AFirstCutoverExpectsNoPointer(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		wantErr  bool
	}{
		{name: "expecting nothing", expected: "", wantErr: false},
		{name: "expecting a generation that is not there", expected: "gen-0", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			store := embedstore.NewMemory()
			_, err := store.RegisterGeneration(ctx, aGeneration())
			c.Assert(err, qt.IsNil)

			err = store.MovePointer(ctx, embedstore.Pointer{
				TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
			}, test.expected)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("%v", err))
		})
	}
}

func TestMemory_ARetiredGenerationCannotBecomeActive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	retired := aGeneration()
	retired.RetiredAt = at
	_, err := store.RegisterGeneration(ctx, retired)
	c.Assert(err, qt.IsNil)

	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: retired.Identity, CutOverAt: at,
	}, ""), qt.ErrorIs, embedstore.ErrRetired)
}

func TestMemory_APointerCannotNameAGenerationForAnotherTarget(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)

	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "other_articles",
		Active: "gen-1", CutOverAt: at,
	}, ""), qt.ErrorIs, embedstore.ErrConflict)
	_, err = store.Pointer(ctx, "public", "other_articles")
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
}

func TestMemory_ReregisteringARetiredGenerationIsRefused(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	retired := aGeneration()
	retired.RetiredAt = at
	_, err := store.RegisterGeneration(ctx, retired)
	c.Assert(err, qt.IsNil)

	_, err = store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.ErrorIs, embedstore.ErrRetired)
}

func TestMemory_RegisterGenerationCannotHideALiveOrTerminalOnlyRun(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	run := aRun()
	run.GenerationIdentity = "gen-import"
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	retired := aGeneration()
	retired.Identity = run.GenerationIdentity
	retired.RetiredAt = at

	_, err := store.RegisterGeneration(ctx, retired)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	_, err = store.Generation(ctx, retired.Identity)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	_, err = store.AbandonRun(ctx, run.ID, "obsolete")
	c.Assert(err, qt.IsNil)
	maintained := aGeneration()
	maintained.Identity = run.GenerationIdentity
	maintained.MaintainedUntil = at.Add(time.Hour)

	_, err = store.RegisterGeneration(ctx, maintained)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
	_, err = store.Generation(ctx, maintained.Identity)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	retiredAndMaintained := aGeneration()
	retiredAndMaintained.Identity = "gen-retired-maintained"
	retiredAndMaintained.RetiredAt = at
	retiredAndMaintained.MaintainedUntil = at.Add(time.Hour)
	_, err = store.RegisterGeneration(ctx, retiredAndMaintained)
	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
}

// TestMemory_TheEventTrailIsOrdered keeps a run's history readable as one.
func TestMemory_TheEventTrailIsOrdered(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	c.Assert(store.AppendEvent(ctx, embedrun.Event{RunID: "run-1", Kind: "started", At: at}), qt.IsNil)
	c.Assert(store.AppendEvent(ctx, embedrun.Event{
		RunID: "run-1", Kind: "checkpointed", At: at.Add(time.Minute),
	}), qt.IsNil)

	events, err := store.Events(ctx, "run-1")

	c.Assert(err, qt.IsNil)
	c.Assert(events, qt.HasLen, 2)
	c.Assert(string(events[0].Kind), qt.Equals, "started")
	c.Assert(string(events[1].Kind), qt.Equals, "checkpointed")
}

// TestMemory_AbsenceIsItsOwnAnswer keeps a missing row from reading as an empty
// one.
//
// A resumed worker asking for a run that is not there and getting a zero Run
// would start a backfill from the beginning, over a target that already holds
// half a corpus.
func TestMemory_AbsenceIsItsOwnAnswer(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *embedstore.Memory) error
	}{
		{
			name: "a run", call: func(ctx context.Context, s *embedstore.Memory) error {
				_, err := s.Run(ctx, "nothing")
				return err
			},
		},
		{
			name: "a generation", call: func(ctx context.Context, s *embedstore.Memory) error {
				_, err := s.Generation(ctx, "nothing")
				return err
			},
		},
		{
			name: "a pointer", call: func(ctx context.Context, s *embedstore.Memory) error {
				_, err := s.Pointer(ctx, "public", "nothing")
				return err
			},
		},
		{
			name: "events for a run that does not exist",
			call: func(ctx context.Context, s *embedstore.Memory) error {
				_, err := s.Events(ctx, "nothing")
				return err
			},
		},
		{
			name: "an event appended to one", call: func(ctx context.Context, s *embedstore.Memory) error {
				return s.AppendEvent(ctx, embedrun.Event{RunID: "nothing"})
			},
		},
		{
			name: "saving one", call: func(ctx context.Context, s *embedstore.Memory) error {
				return s.SaveRun(ctx, aRun())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.call(context.Background(), embedstore.NewMemory())

			c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
		})
	}
}

// TestMemory_ARunIsNotCreatedTwice keeps a restarted worker from resetting one.
func TestMemory_ARunIsNotCreatedTwice(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)

	err := store.CreateRun(ctx, aRun())

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
}

// TestMemory_SatisfiesTheStoreContract keeps the implementation and the
// interface from drifting, and reaches every method through the interface so
// that a signature change is a compile error here rather than at the first
// caller.
func TestMemory_SatisfiesTheStoreContract(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	var store embedstore.Store = embedstore.NewMemory()

	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	c.Assert(store.SaveRun(ctx, aRun()), qt.IsNil)
	c.Assert(store.AppendEvent(ctx, embedrun.Event{RunID: "run-1", Kind: "started", At: at}), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
	}, ""), qt.IsNil)

	generation, err := store.Generation(ctx, "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(generation.Identity, qt.Equals, "gen-1")
	run, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(run.ID, qt.Equals, "run-1")
	events, err := store.Events(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(events, qt.HasLen, 1)
	pointer, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, "gen-1")
}

// TestMemory_AReadEventTrailDoesNotShareTheStoresSlice is the third aliasing
// case, and the one a run's history is worst placed to survive.
//
// A reader rendering the trail -- redacting a detail, folding a phase name --
// would be editing the audit log it was asked to display, and the record that
// changed is the record of what happened.
func TestMemory_AReadEventTrailDoesNotShareTheStoresSlice(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	c.Assert(store.AppendEvent(ctx, embedrun.Event{
		RunID: "run-1", Kind: "paused", At: at, Detail: "the provider was unreachable",
	}), qt.IsNil)
	events, err := store.Events(ctx, "run-1")
	c.Assert(err, qt.IsNil)

	events[0].Detail = "redacted"
	events[0].Kind = "started"

	again, err := store.Events(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(again[0].Detail, qt.Equals, "the provider was unreachable")
	c.Assert(string(again[0].Kind), qt.Equals, "paused")
}

// TestMemory_AVerificationIsRecordedOnlyByAVerification is what a rollback
// rests on.
//
// A generation somebody asserts is fine is not a generation anybody measured,
// and the eligibility check reads this timestamp rather than a claim.
func TestMemory_AVerificationIsRecordedOnlyByAVerification(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)

	before, err := store.Generation(ctx, "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(before.VerifiedAt.IsZero(), qt.IsTrue)

	c.Assert(store.RecordVerification(ctx, "gen-1", at), qt.IsNil)

	after, err := store.Generation(ctx, "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(after.VerifiedAt, qt.Equals, at)
}

// TestMemory_MaintenanceIsAWindowAndNotAFlag is the difference between a
// generation whose tables exist and one you can go back to.
//
// Something has to be keeping it current, and a promise to do so until Tuesday
// stops being true on Wednesday without anybody writing anything.
func TestMemory_MaintenanceIsAWindowAndNotAFlag(t *testing.T) {
	tests := []struct {
		name       string
		until      time.Time
		now        time.Time
		maintained bool
	}{
		{name: "inside the window", until: at.Add(time.Hour), now: at, maintained: true},
		{name: "at its end", until: at, now: at, maintained: true},
		{name: "past it", until: at, now: at.Add(time.Second), maintained: false},
		{name: "never opened", until: time.Time{}, now: at, maintained: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			store := embedstore.NewMemory()
			_, err := store.RegisterGeneration(ctx, aGeneration())
			c.Assert(err, qt.IsNil)
			c.Assert(store.Maintain(ctx, "gen-1", test.until), qt.IsNil)

			generation, err := store.Generation(ctx, "gen-1")

			c.Assert(err, qt.IsNil)
			c.Assert(generation.Maintained(test.now), qt.Equals, test.maintained)
		})
	}
}

// TestMemory_ARetiredGenerationRecordsNeither keeps a destroyed generation from
// looking like a way back.
//
// The vectors are gone. A verification recorded against it, or a maintenance
// window opened over it, would make the eligibility check answer yes about
// something that no longer exists.
func TestMemory_ARetiredGenerationRecordsNeither(t *testing.T) {
	tests := []struct {
		name string
		call func(context.Context, *embedstore.Memory) error
	}{
		{
			name: "a verification", call: func(ctx context.Context, s *embedstore.Memory) error {
				return s.RecordVerification(ctx, "gen-1", at)
			},
		},
		{
			name: "a maintenance window", call: func(ctx context.Context, s *embedstore.Memory) error {
				return s.Maintain(ctx, "gen-1", at.Add(time.Hour))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			store := embedstore.NewMemory()
			retired := aGeneration()
			retired.RetiredAt = at
			_, err := store.RegisterGeneration(ctx, retired)
			c.Assert(err, qt.IsNil)

			c.Assert(test.call(ctx, store), qt.ErrorIs, embedstore.ErrRetired)
		})
	}
}

// TestMemory_RecordingAgainstAGenerationThatIsNotThereIsItsOwnAnswer keeps a
// typo from silently doing nothing.
func TestMemory_RecordingAgainstAGenerationThatIsNotThereIsItsOwnAnswer(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()

	c.Assert(store.RecordVerification(ctx, "nothing", at), qt.ErrorIs, embedstore.ErrNotFound)
	c.Assert(store.Maintain(ctx, "nothing", at), qt.ErrorIs, embedstore.ErrNotFound)
}

// TestMemory_MaintainNeverShortensAWindow mirrors the SQL store's rule.
//
// Two implementations of one interface must answer the same question the same
// way, or a unit test passing against the fake says nothing about the product.
// The rule itself is stokaro/ptah#2647: a renewal that moved the deadline
// earlier took rollback eligibility away from a flag documented as extending
// the window.
func TestMemory_MaintainNeverShortensAWindow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, embedstore.Generation{
		Identity: "gen-1", SpecDigest: "spec-1", Dimension: 4,
		TargetTable: "articles", TargetColumn: "embedding",
	})
	c.Assert(err, qt.IsNil)

	far := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	near := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	c.Assert(store.Maintain(ctx, "gen-1", far), qt.IsNil)
	c.Assert(windowOf(c, ctx, store), qt.Equals, far)

	c.Assert(store.Maintain(ctx, "gen-1", near), qt.IsNil)
	c.Assert(windowOf(c, ctx, store), qt.Equals, far)

	// Clearing still clears.
	c.Assert(store.Maintain(ctx, "gen-1", time.Time{}), qt.IsNil)
	c.Assert(windowOf(c, ctx, store).IsZero(), qt.IsTrue)

	// And from nothing, any deadline wins.
	c.Assert(store.Maintain(ctx, "gen-1", near), qt.IsNil)
	c.Assert(windowOf(c, ctx, store), qt.Equals, near)
}

// windowOf reads a generation's maintenance window back.
func windowOf(c *qt.C, ctx context.Context, store *embedstore.Memory) time.Time {
	c.Helper()
	generation, err := store.Generation(ctx, "gen-1")
	c.Assert(err, qt.IsNil)
	return generation.MaintainedUntil.UTC()
}

// TestMemory_AbandonmentFencesWithoutRewritingProgress is the store contract
// issue #2723 needs: ending a run and fencing its worker are one write, while
// the resume position remains available for inspection.
func TestMemory_AbandonmentFencesWithoutRewritingProgress(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemoryWithClock(func() time.Time { return at.Add(time.Minute) })
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	before := aRun()
	c.Assert(store.CreateRun(ctx, before), qt.IsNil)

	abandoned, err := store.AbandonRun(ctx, before.ID, "superseded by run-2")

	c.Assert(err, qt.IsNil)
	c.Assert(abandoned.Status, qt.Equals, embedrun.StatusAbandoned)
	c.Assert(abandoned.FencingToken, qt.Equals, before.FencingToken+1)
	c.Assert(abandoned.LeaseOwner, qt.Equals, "")
	c.Assert(abandoned.LeaseExpires.IsZero(), qt.IsTrue)
	c.Assert(abandoned.Cursor, qt.DeepEquals, before.Cursor)
	c.Assert(abandoned.Progress, qt.DeepEquals, before.Progress)
	c.Assert(abandoned.SnapshotWatermark, qt.Equals, before.SnapshotWatermark)
	c.Assert(abandoned.FailureDetail, qt.Equals, "superseded by run-2")
	c.Assert(abandoned.UpdatedAt, qt.Equals, at.Add(time.Minute))

	// A retry is idempotent: it neither advances the fence again nor rewrites
	// the operator's original reason.
	again, err := store.AbandonRun(ctx, before.ID, "a later retry")
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.DeepEquals, abandoned)

	// The worker that held the pre-abandonment copy cannot put it back.
	c.Assert(store.SaveRun(ctx, before), qt.ErrorIs, embedstore.ErrConflict)
}

// TestMemory_SaveRunCannotBypassAbandonmentPolicy keeps generic persistence
// from becoming a second, unchecked abandonment path.
func TestMemory_SaveRunCannotBypassAbandonmentPolicy(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	run := aRun()
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	c.Assert(run.Abandon(run.FencingToken, "bypass"), qt.IsNil)

	c.Assert(store.SaveRun(ctx, run), qt.ErrorIs, embedstore.ErrConflict)
	stored, err := store.Run(ctx, run.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Status, qt.Equals, embedrun.StatusRunning)
}

// TestMemory_AProtectedGenerationKeepsOneLiveFeeder covers both promises that
// make a generation unsafe to stop feeding: active queries and maintenance.
func TestMemory_AProtectedGenerationKeepsOneLiveFeeder(t *testing.T) {
	tests := []struct {
		name    string
		protect func(context.Context, *embedstore.Memory) error
	}{
		{
			name: "active",
			protect: func(ctx context.Context, store *embedstore.Memory) error {
				return store.MovePointer(ctx, embedstore.Pointer{
					TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
				}, "")
			},
		},
		{
			name: "maintained",
			protect: func(ctx context.Context, store *embedstore.Memory) error {
				return store.Maintain(ctx, "gen-1", at.Add(time.Hour))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			store := embedstore.NewMemoryWithClock(func() time.Time { return at })
			_, err := store.RegisterGeneration(ctx, aGeneration())
			c.Assert(err, qt.IsNil)
			c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
			c.Assert(test.protect(ctx, store), qt.IsNil)

			_, err = store.AbandonRun(ctx, "run-1", "obsolete")
			c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)

			other := aRun()
			other.ID = "run-2"
			other.FencingToken = 1
			c.Assert(store.CreateRun(ctx, other), qt.IsNil)
			_, err = store.AbandonRun(ctx, "run-1", "run-2 is the feeder")
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestMemory_OutboxAbandonmentNeedsAnotherPositionedFeeder(t *testing.T) {
	tests := []struct {
		name            string
		siblingSnapshot string
		wantErr         error
	}{
		{name: "positioned sibling", siblingSnapshot: "100"},
		{name: "unpositioned sibling", wantErr: embedstore.ErrNoLiveRun},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertMemoryOutboxAbandonment(t, test.siblingSnapshot, test.wantErr)
		})
	}
}

func assertMemoryOutboxAbandonment(t *testing.T, siblingSnapshot string, wantErr error) {
	t.Helper()
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	generation := aGeneration()
	generation.ConsistencyMode = "outbox"
	_, err := store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	current := aRun()
	current.SnapshotWatermark = "100"
	c.Assert(store.CreateRun(ctx, current), qt.IsNil)
	sibling := current
	sibling.ID = "run-sibling"
	sibling.SnapshotWatermark = siblingSnapshot
	c.Assert(store.CreateRun(ctx, sibling), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: generation.Identity,
	}, ""), qt.IsNil)

	_, err = store.AbandonRun(ctx, current.ID, "superseded")
	if wantErr == nil {
		c.Assert(err, qt.IsNil)
		return
	}
	c.Assert(err, qt.ErrorIs, wantErr)
}

// TestMemory_ATerminalOnlyGenerationCannotBecomeProtected closes the other
// side of the invariant. A generation with no run history remains valid for
// imported state, while one with only terminal runs cannot be made active or
// maintained.
func TestMemory_ATerminalOnlyGenerationCannotBecomeProtected(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	_, err = store.AbandonRun(ctx, "run-1", "obsolete")
	c.Assert(err, qt.IsNil)

	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
	}, ""), qt.ErrorIs, embedstore.ErrNoLiveRun)
	c.Assert(store.Maintain(ctx, "gen-1", at.Add(time.Hour)), qt.ErrorIs, embedstore.ErrNoLiveRun)

	// A registered or externally imported generation has no run rows. Existing
	// callers rely on being able to point at and maintain one.
	untracked := aGeneration()
	untracked.Identity = "gen-imported"
	untracked.TargetTable = "imported"
	_, err = store.RegisterGeneration(ctx, untracked)
	c.Assert(err, qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "imported", Active: "gen-imported", CutOverAt: at,
	}, ""), qt.IsNil)
	c.Assert(store.Maintain(ctx, "gen-imported", at.Add(time.Hour)), qt.IsNil)
}

// TestMemory_ConcurrentAbandonmentAdvancesTheFenceOnce proves the operation is
// idempotent at the atomic boundary rather than in a caller's check-then-write.
func TestMemory_ConcurrentAbandonmentAdvancesTheFenceOnce(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)

	var wg sync.WaitGroup
	abandonErrors := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			_, callErr := store.AbandonRun(ctx, "run-1", "obsolete")
			abandonErrors <- callErr
		})
	}
	wg.Wait()
	close(abandonErrors)
	for err := range abandonErrors {
		c.Assert(err, qt.IsNil)
	}
	stored, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Status, qt.Equals, embedrun.StatusAbandoned)
	c.Assert(stored.FencingToken, qt.Equals, int64(8))
}

// TestMemory_CutoverAndMaintenanceAreAtomicAgainstAbandonment forces both
// orders of the race to have the same safe outcome. Before the move the old
// generation is active; after it, the same transaction maintains it. There is
// no instant at which abandoning its last feeder is legal.
func TestMemory_CutoverAndMaintenanceAreAtomicAgainstAbandonment(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	cutOverAt := at.Add(2 * time.Hour)
	store := embedstore.NewMemoryWithClock(func() time.Time { return cutOverAt })
	old := aGeneration()
	_, err := store.RegisterGeneration(ctx, old)
	c.Assert(err, qt.IsNil)
	newGeneration := old
	newGeneration.Identity = "gen-2"
	_, err = store.RegisterGeneration(ctx, newGeneration)
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)
	newRun := aRun()
	newRun.ID = "run-2"
	newRun.GenerationIdentity = "gen-2"
	newRun.Phase = embedrun.PhaseVerified
	c.Assert(store.CreateRun(ctx, newRun), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
	}, ""), qt.IsNil)

	start := make(chan struct{})
	type moveCall struct {
		move embedstore.CutoverMove
		err  error
	}
	moveResult := make(chan moveCall, 1)
	abandonResult := make(chan error, 1)
	go func() {
		<-start
		move, callErr := store.MovePointerWithMaintenance(ctx, embedstore.Pointer{
			TargetSchema: "public", TargetTable: "articles", Active: "gen-2",
			Previous: "gen-1", CutOverAt: at.Add(time.Minute),
		}, "gen-1", "run-2", time.Hour)
		moveResult <- moveCall{move: move, err: callErr}
	}()
	go func() {
		<-start
		_, callErr := store.AbandonRun(ctx, "run-1", "obsolete")
		abandonResult <- callErr
	}()
	close(start)

	move := <-moveResult
	c.Assert(move.err, qt.IsNil)
	c.Assert(move.move.CutOverAt, qt.Equals, cutOverAt)
	c.Assert(move.move.PreviousMaintainedUntil, qt.Equals, cutOverAt.Add(time.Hour))
	c.Assert(<-abandonResult, qt.ErrorIs, embedstore.ErrNoLiveRun)
	pointer, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, "gen-2")
	previous, err := store.Generation(ctx, "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(previous.MaintainedUntil, qt.Equals, cutOverAt.Add(time.Hour))
	c.Assert(pointer.CutOverAt, qt.Equals, cutOverAt)
	cutOverRun, err := store.Run(ctx, "run-2")
	c.Assert(err, qt.IsNil)
	c.Assert(cutOverRun.Phase, qt.Equals, embedrun.PhaseCutOver)
	c.Assert(cutOverRun.FencingToken, qt.Equals, newRun.FencingToken+1)
	oldRun, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(oldRun.Terminal(), qt.IsFalse)
}

func TestMemory_CutoverReturnsTheCommittedMaintenanceWindow(t *testing.T) {
	tests := []struct {
		name           string
		stabilizeFor   time.Duration
		wantMaintained time.Time
	}{
		{name: "positive duration preserves a later window", stabilizeFor: time.Hour,
			wantMaintained: at.Add(24 * time.Hour)},
		{name: "zero clears an earlier window", stabilizeFor: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			moveAt := at.Add(2 * time.Hour)
			store := embedstore.NewMemoryWithClock(func() time.Time { return moveAt })
			old := aGeneration()
			_, err := store.RegisterGeneration(ctx, old)
			c.Assert(err, qt.IsNil)
			newGeneration := old
			newGeneration.Identity = "gen-window-new"
			_, err = store.RegisterGeneration(ctx, newGeneration)
			c.Assert(err, qt.IsNil)
			oldRun := aRun()
			c.Assert(store.CreateRun(ctx, oldRun), qt.IsNil)
			newRun := aRun()
			newRun.ID = "run-window-new"
			newRun.GenerationIdentity = newGeneration.Identity
			newRun.Phase = embedrun.PhaseVerified
			c.Assert(store.CreateRun(ctx, newRun), qt.IsNil)
			later := at.Add(24 * time.Hour)
			c.Assert(store.Maintain(ctx, old.Identity, later), qt.IsNil)
			c.Assert(store.MovePointer(ctx, embedstore.Pointer{
				TargetSchema: "public", TargetTable: "articles", Active: old.Identity,
				CutOverAt: at,
			}, ""), qt.IsNil)

			move, err := store.MovePointerWithMaintenance(ctx, embedstore.Pointer{
				TargetSchema: "public", TargetTable: "articles", Active: newGeneration.Identity,
				Previous: old.Identity,
			}, old.Identity, newRun.ID, test.stabilizeFor)
			c.Assert(err, qt.IsNil)
			c.Assert(move.CutOverAt, qt.Equals, moveAt)
			c.Assert(move.PreviousMaintainedUntil, qt.Equals, test.wantMaintained)
			stored, err := store.Generation(ctx, old.Identity)
			c.Assert(err, qt.IsNil)
			c.Assert(stored.MaintainedUntil, qt.Equals, test.wantMaintained)
		})
	}
}

func TestMemory_OutboxLifecycleRequiresAUsableLiveFeeder(t *testing.T) {
	tests := []struct {
		name             string
		configure        func(*embedrun.Run)
		wantCreateErr    error
		wantLifecycleErr error
	}{
		{name: "positioned", configure: func(run *embedrun.Run) {
			run.SnapshotWatermark = "100"
		}},
		{name: "unpositioned", configure: func(*embedrun.Run) {}, wantLifecycleErr: embedstore.ErrNoLiveRun},
		{name: "wrong source", configure: func(run *embedrun.Run) {
			run.Source = "wrong"
			run.SnapshotWatermark = "100"
		}, wantCreateErr: embedstore.ErrConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertMemoryOutboxLifecycle(t, test.configure, test.wantCreateErr, test.wantLifecycleErr)
		})
	}

	// Importing a maintenance window over existing but unpositioned outbox
	// history is the same invalid protected state and is refused at registration.
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	run := aRun()
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	generation := aGeneration()
	generation.ConsistencyMode = "outbox"
	generation.MaintainedUntil = at.Add(time.Hour)
	_, err := store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.ErrorIs, embedstore.ErrNoLiveRun)
}

func assertMemoryOutboxLifecycle(
	t *testing.T,
	configure func(*embedrun.Run),
	wantCreateErr, wantLifecycleErr error,
) {
	t.Helper()
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	generation := aGeneration()
	generation.ConsistencyMode = "outbox"
	_, err := store.RegisterGeneration(ctx, generation)
	c.Assert(err, qt.IsNil)
	run := aRun()
	configure(&run)
	createErr := store.CreateRun(ctx, run)
	if wantCreateErr != nil {
		c.Assert(createErr, qt.ErrorIs, wantCreateErr)
		return
	}
	c.Assert(createErr, qt.IsNil)
	moveErr := store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: generation.Identity,
	}, "")
	maintainErr := store.Maintain(ctx, generation.Identity, at.Add(time.Hour))
	if wantLifecycleErr == nil {
		c.Assert(moveErr, qt.IsNil)
		c.Assert(maintainErr, qt.IsNil)
		return
	}
	c.Assert(moveErr, qt.ErrorIs, wantLifecycleErr)
	c.Assert(maintainErr, qt.ErrorIs, wantLifecycleErr)
}

// TestMemory_CutoverRequiresTheExactAuthorizingRun keeps a live sibling from
// substituting for the run whose verification authorized the pointer move.
// Run counts alone cannot answer that question: run-2 keeps the generation
// live, but it does not make an abandoned run-1's approval valid.
func TestMemory_CutoverRequiresTheExactAuthorizingRun(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	old := aGeneration()
	_, err := store.RegisterGeneration(ctx, old)
	c.Assert(err, qt.IsNil)
	newGeneration := old
	newGeneration.Identity = "gen-2"
	_, err = store.RegisterGeneration(ctx, newGeneration)
	c.Assert(err, qt.IsNil)

	first := aRun()
	first.ID = "run-new-1"
	first.GenerationIdentity = newGeneration.Identity
	first.Phase = embedrun.PhaseVerified
	second := first
	second.ID = "run-new-2"
	c.Assert(store.CreateRun(ctx, first), qt.IsNil)
	c.Assert(store.CreateRun(ctx, second), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: old.Identity, CutOverAt: at,
	}, ""), qt.IsNil)
	_, err = store.AbandonRun(ctx, first.ID, "use the second attempt")
	c.Assert(err, qt.IsNil)

	pointer := embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: newGeneration.Identity,
		Previous: old.Identity, CutOverAt: at.Add(time.Minute),
	}
	_, err = store.MovePointerWithMaintenance(ctx, pointer, old.Identity, first.ID, 0)
	c.Assert(err, qt.ErrorIs, embedrun.ErrTerminal)
	current, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(current.Active, qt.Equals, old.Identity)

	_, err = store.MovePointerWithMaintenance(ctx, pointer, old.Identity, second.ID, 0)
	c.Assert(err, qt.IsNil)
	current, err = store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(current.Active, qt.Equals, newGeneration.Identity)
	cutOver, err := store.Run(ctx, second.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(cutOver.Phase, qt.Equals, embedrun.PhaseCutOver)
	c.Assert(cutOver.FencingToken, qt.Equals, second.FencingToken+1)
	c.Assert(cutOver.LeaseOwner, qt.Equals, "")
	c.Assert(cutOver.LeaseExpires.IsZero(), qt.IsTrue)
}

// TestMemory_RollbackSkipsAnAbandonedDuplicateAndFencesTheLiveRun keeps a
// terminal duplicate from turning a safe pointer rollback into a partial
// failure. The abandoned run's high-water phase remains truthful; the live run
// records and is fenced by the rollback that actually happened.
func TestMemory_RollbackSkipsAnAbandonedDuplicateAndFencesTheLiveRun(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	moveAt := at.Add(time.Minute)
	store := embedstore.NewMemoryWithClock(func() time.Time { return moveAt })
	active := aGeneration()
	_, err := store.RegisterGeneration(ctx, active)
	c.Assert(err, qt.IsNil)
	previous := active
	previous.Identity = "gen-previous"
	_, err = store.RegisterGeneration(ctx, previous)
	c.Assert(err, qt.IsNil)
	previous.MaintainedUntil = at.Add(time.Hour)
	c.Assert(store.Maintain(ctx, previous.Identity, previous.MaintainedUntil), qt.IsNil)

	abandoned := aRun()
	abandoned.ID = "run-abandoned-cutover"
	abandoned.Phase = embedrun.PhaseCutOver
	live := abandoned
	live.ID = "run-live-cutover"
	c.Assert(store.CreateRun(ctx, abandoned), qt.IsNil)
	c.Assert(store.CreateRun(ctx, live), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: previous.Identity, CutOverAt: at,
	}, ""), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: active.Identity,
		Previous: previous.Identity, CutOverAt: at,
	}, previous.Identity), qt.IsNil)
	_, err = store.AbandonRun(ctx, abandoned.ID, "duplicate attempt")
	c.Assert(err, qt.IsNil)
	abandonedBefore, err := store.Run(ctx, abandoned.ID)
	c.Assert(err, qt.IsNil)

	rolledBackAt, err := store.MovePointerWithRollback(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: previous.Identity,
		Previous: active.Identity,
	}, active.Identity, previous.MaintainedUntil, at.Add(2*time.Hour))
	c.Assert(err, qt.IsNil)
	c.Assert(rolledBackAt, qt.Equals, moveAt)

	storedAbandoned, err := store.Run(ctx, abandoned.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(storedAbandoned, qt.DeepEquals, abandonedBefore)
	storedLive, err := store.Run(ctx, live.ID)
	c.Assert(err, qt.IsNil)
	c.Assert(storedLive.Phase, qt.Equals, embedrun.PhaseRolledBack)
	c.Assert(storedLive.FencingToken, qt.Equals, live.FencingToken+1)
	c.Assert(storedLive.LeaseOwner, qt.Equals, "")
	c.Assert(storedLive.LeaseExpires.IsZero(), qt.IsTrue)
	pointer, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, previous.Identity)
	c.Assert(pointer.CutOverAt, qt.Equals, moveAt)
}

func TestMemory_RollbackRevalidatesTheMaintenanceWindow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemoryWithClock(func() time.Time { return at.Add(time.Minute) })
	active := aGeneration()
	_, err := store.RegisterGeneration(ctx, active)
	c.Assert(err, qt.IsNil)
	destination := active
	destination.Identity = "gen-destination"
	_, err = store.RegisterGeneration(ctx, destination)
	c.Assert(err, qt.IsNil)
	approvedUntil := at.Add(time.Hour)
	c.Assert(store.Maintain(ctx, destination.Identity, approvedUntil), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: destination.Identity, CutOverAt: at,
	}, ""), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: active.Identity,
		Previous: destination.Identity, CutOverAt: at,
	}, destination.Identity), qt.IsNil)
	c.Assert(store.Maintain(ctx, destination.Identity, time.Time{}), qt.IsNil)

	_, err = store.MovePointerWithRollback(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: destination.Identity,
		Previous: active.Identity,
	}, active.Identity, approvedUntil, at.Add(2*time.Hour))

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	pointer, readErr := store.Pointer(ctx, "public", "articles")
	c.Assert(readErr, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, active.Identity)
}

func TestMemory_RollbackCannotOutwaitItsDeadlines(t *testing.T) {
	tests := []struct {
		name                string
		now                 time.Time
		maintainedUntil     time.Time
		eligibilityNotAfter time.Time
	}{
		{
			name: "maintenance expired while waiting", now: at.Add(2 * time.Hour),
			maintainedUntil: at.Add(time.Hour), eligibilityNotAfter: at.Add(3 * time.Hour),
		},
		{
			name: "policy expired while waiting", now: at.Add(2 * time.Hour),
			maintainedUntil: at.Add(3 * time.Hour), eligibilityNotAfter: at.Add(time.Hour),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			store := embedstore.NewMemoryWithClock(func() time.Time { return test.now })
			active := aGeneration()
			_, err := store.RegisterGeneration(ctx, active)
			c.Assert(err, qt.IsNil)
			destination := active
			destination.Identity = "gen-expiry-destination"
			_, err = store.RegisterGeneration(ctx, destination)
			c.Assert(err, qt.IsNil)
			c.Assert(store.Maintain(ctx, destination.Identity, test.maintainedUntil), qt.IsNil)
			run := aRun()
			run.Phase = embedrun.PhaseCutOver
			c.Assert(store.CreateRun(ctx, run), qt.IsNil)
			c.Assert(store.MovePointer(ctx, embedstore.Pointer{
				TargetSchema: "public", TargetTable: "articles", Active: destination.Identity,
				CutOverAt: at,
			}, ""), qt.IsNil)
			c.Assert(store.MovePointer(ctx, embedstore.Pointer{
				TargetSchema: "public", TargetTable: "articles", Active: active.Identity,
				Previous: destination.Identity, CutOverAt: at,
			}, destination.Identity), qt.IsNil)

			movedAt, err := store.MovePointerWithRollback(ctx, embedstore.Pointer{
				TargetSchema: "public", TargetTable: "articles", Active: destination.Identity,
				Previous: active.Identity,
			}, active.Identity, test.maintainedUntil, test.eligibilityNotAfter)

			c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
			c.Assert(movedAt.IsZero(), qt.IsTrue)
			pointer, readErr := store.Pointer(ctx, "public", "articles")
			c.Assert(readErr, qt.IsNil)
			c.Assert(pointer.Active, qt.Equals, active.Identity)
			stored, readErr := store.Run(ctx, run.ID)
			c.Assert(readErr, qt.IsNil)
			c.Assert(stored, qt.DeepEquals, run)
		})
	}
}

func TestMemory_RollbackAcceptsTheExactDeadline(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	deadline := at.Add(time.Hour)
	store := embedstore.NewMemoryWithClock(func() time.Time { return deadline })
	active := aGeneration()
	_, err := store.RegisterGeneration(ctx, active)
	c.Assert(err, qt.IsNil)
	destination := active
	destination.Identity = "gen-exact-deadline-destination"
	_, err = store.RegisterGeneration(ctx, destination)
	c.Assert(err, qt.IsNil)
	c.Assert(store.Maintain(ctx, destination.Identity, deadline), qt.IsNil)
	run := aRun()
	run.Phase = embedrun.PhaseCutOver
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: destination.Identity,
		CutOverAt: at,
	}, ""), qt.IsNil)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: active.Identity,
		Previous: destination.Identity, CutOverAt: at,
	}, destination.Identity), qt.IsNil)

	movedAt, err := store.MovePointerWithRollback(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: destination.Identity,
		Previous: active.Identity,
	}, active.Identity, deadline, deadline)

	c.Assert(err, qt.IsNil)
	c.Assert(movedAt, qt.Equals, deadline)
	pointer, err := store.Pointer(ctx, "public", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(pointer.Active, qt.Equals, destination.Identity)
	c.Assert(pointer.CutOverAt, qt.Equals, deadline)
}

func TestMemory_AbandonRunSupportsAMissingGenerationRegistryRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)

	_, err := store.AbandonRun(ctx, aRun().ID, "obsolete")
	c.Assert(err, qt.IsNil)
	stored, readErr := store.Run(ctx, aRun().ID)
	c.Assert(readErr, qt.IsNil)
	c.Assert(stored.Status, qt.Equals, embedrun.StatusAbandoned)
}

// TestMemory_MaintenanceAndAbandonmentHaveNoPartialOutcome covers the same
// lock when no pointer protects the generation. Whichever operation wins, the
// loser observes that state and refuses; a terminal maintained generation is
// impossible.
func TestMemory_MaintenanceAndAbandonmentHaveNoPartialOutcome(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemoryWithClock(func() time.Time { return at })
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, aRun()), qt.IsNil)

	start := make(chan struct{})
	maintainResult := make(chan error, 1)
	abandonResult := make(chan error, 1)
	go func() {
		<-start
		maintainResult <- store.Maintain(ctx, "gen-1", at.Add(time.Hour))
	}()
	go func() {
		<-start
		_, callErr := store.AbandonRun(ctx, "run-1", "obsolete")
		abandonResult <- callErr
	}()
	close(start)

	maintainErr := <-maintainResult
	abandonErr := <-abandonResult
	maintained := maintainErr == nil
	abandoned := abandonErr == nil
	c.Assert(maintained, qt.Not(qt.Equals), abandoned,
		qt.Commentf("maintain=%v abandon=%v", maintainErr, abandonErr))
	c.Assert(errors.Is(maintainErr, embedstore.ErrNoLiveRun), qt.Equals, abandoned)
	c.Assert(errors.Is(abandonErr, embedstore.ErrNoLiveRun), qt.Equals, maintained)
	generation, err := store.Generation(ctx, "gen-1")
	c.Assert(err, qt.IsNil)
	run, err := store.Run(ctx, "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(generation.MaintainedUntil.IsZero() || !run.Terminal(), qt.IsTrue)
}
