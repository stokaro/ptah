package embedstore_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
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

// TestMemory_RetirementIsTerminal keeps the record of when a corpus was
// destroyed.
//
// Retiring twice is not idempotent bookkeeping: the second call moves the
// timestamp, and that timestamp is the whole value of the row that remains.
func TestMemory_RetirementIsTerminal(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := embedstore.NewMemory()
	_, err := store.RegisterGeneration(ctx, aGeneration())
	c.Assert(err, qt.IsNil)
	c.Assert(store.RetireGeneration(ctx, "gen-1", at), qt.IsNil)

	err = store.RetireGeneration(ctx, "gen-1", at.Add(time.Hour))

	c.Assert(err, qt.ErrorIs, embedstore.ErrRetired)
	generation, _ := store.Generation(ctx, "gen-1")
	c.Assert(generation.RetiredAt, qt.Equals, at)
	c.Assert(generation.Retired(), qt.IsTrue)
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
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
	}, ""), qt.IsNil)

	err := store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "public", TargetTable: "articles", Active: "gen-3", Previous: "gen-2", CutOverAt: at,
	}, "gen-2")

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err, qt.ErrorMatches, `.*public.articles reads gen-1 and this move expected gen-2.*`)
	current, _ := store.Pointer(ctx, "public", "articles")
	c.Assert(current.Active, qt.Equals, "gen-1")
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

			err := store.MovePointer(ctx, embedstore.Pointer{
				TargetSchema: "public", TargetTable: "articles", Active: "gen-1", CutOverAt: at,
			}, test.expected)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("%v", err))
		})
	}
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
		{
			name: "retiring a generation that does not exist",
			call: func(ctx context.Context, s *embedstore.Memory) error {
				return s.RetireGeneration(ctx, "nothing", at)
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
	c.Assert(store.RetireGeneration(ctx, "gen-1", at), qt.IsNil)
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
		{name: "at its end", until: at, now: at, maintained: false},
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
			_, err := store.RegisterGeneration(ctx, aGeneration())
			c.Assert(err, qt.IsNil)
			c.Assert(store.RetireGeneration(ctx, "gen-1", at), qt.IsNil)

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
