package embedrpc_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedrpc"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// fakeStore is a provider's run-state store held in memory. It answers the
// first conflicts mutations with CONFLICT, applies the rest, and stores each
// written record through store, which stands for an engine's own precision. A
// call a test does not make reaches the nil interface and panics.
type fakeStore struct {
	provider.EmbeddingStore

	records   *protocol.EmbeddingRecords
	reads     int
	conflicts int
	refusal   *protocol.EmbeddingRefusal
	mutations []*protocol.MutateEmbeddingStateRequest
	appended  []*protocol.AppendEmbeddingEventRequest
	store     func(*protocol.EmbeddingRecords) *protocol.EmbeddingRecords
}

// ReadEmbeddingState answers the records the scope selects, with a token
// that names the read.
func (f *fakeStore) ReadEmbeddingState(_ context.Context, request *protocol.ReadEmbeddingStateRequest) (*protocol.EmbeddingState, error) {
	f.reads++
	selected := &protocol.EmbeddingRecords{}
	switch scope := request.GetScope().GetScope().(type) {
	case *protocol.EmbeddingStateScope_Run:
		for _, run := range f.records.GetRuns() {
			if run.GetId() == scope.Run {
				selected.Runs = append(selected.Runs, run)
			}
		}
	case *protocol.EmbeddingStateScope_Generation:
		for _, generation := range f.records.GetGenerations() {
			if generation.GetIdentity() == scope.Generation {
				selected.Generations = append(selected.Generations, generation)
			}
		}
	case *protocol.EmbeddingStateScope_RunsOfGeneration:
		for _, run := range f.records.GetRuns() {
			if run.GetGenerationIdentity() == scope.RunsOfGeneration {
				selected.Runs = append(selected.Runs, run)
			}
		}
	default:
		selected = proto.CloneOf(f.records)
	}
	return &protocol.EmbeddingState{Scope: request.GetScope(), Exists: true, Records: selected, Token: "token-" + strconv.Itoa(f.reads)}, nil
}

// MutateEmbeddingState answers CONFLICT while conflicts last, then writes.
func (f *fakeStore) MutateEmbeddingState(_ context.Context, request *protocol.MutateEmbeddingStateRequest) (*protocol.EmbeddingWriteResult, error) {
	f.mutations = append(f.mutations, request)
	if f.conflicts > 0 {
		f.conflicts--
		result := &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_CONFLICT}
		return result, provider.CheckEmbeddingWrite(result)
	}
	written := proto.CloneOf(request.GetRecords())
	if f.store != nil {
		written = f.store(written)
	}
	f.records = merge(f.records, written)
	return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_APPLIED, Written: written}, nil
}

// AppendEmbeddingEvent answers the refusal it holds, or the next sequence.
func (f *fakeStore) AppendEmbeddingEvent(_ context.Context, request *protocol.AppendEmbeddingEventRequest) (*protocol.EmbeddingWriteResult, error) {
	f.appended = append(f.appended, request)
	if f.refusal != nil {
		result := &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_REFUSED, Refusal: f.refusal}
		return result, provider.CheckEmbeddingWrite(result)
	}
	return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_APPLIED, Sequence: int64(len(f.appended))}, nil
}

// merge replaces records by key and adds the new ones.
func merge(records, written *protocol.EmbeddingRecords) *protocol.EmbeddingRecords {
	merged := proto.CloneOf(records)
	if merged == nil {
		merged = &protocol.EmbeddingRecords{}
	}
	for _, generation := range written.GetGenerations() {
		merged.Generations = replace(merged.Generations, generation, func(a, b *protocol.EmbeddingGeneration) bool { return a.GetIdentity() == b.GetIdentity() })
	}
	for _, run := range written.GetRuns() {
		merged.Runs = replace(merged.Runs, run, func(a, b *protocol.EmbeddingRun) bool { return a.GetId() == b.GetId() })
	}
	for _, pointer := range written.GetPointers() {
		merged.Pointers = replace(merged.Pointers, pointer, func(a, b *protocol.EmbeddingPointer) bool {
			return a.GetTargetSchema() == b.GetTargetSchema() && a.GetTargetTable() == b.GetTargetTable()
		})
	}
	return merged
}

func replace[T any](list []T, record T, same func(T, T) bool) []T {
	for i, existing := range list {
		if same(existing, record) {
			list[i] = record
			return list
		}
	}
	return append(list, record)
}

func backendOver(store provider.EmbeddingStore) *embedrpc.Backend {
	return embedrpc.New(embedrpc.Session{Store: store, Engine: "fixture", ConsistencyModes: []string{"immutable"}, ID: "session"}, nil)
}

var created = time.Date(2026, 10, 24, 9, 30, 0, 123456789, time.UTC)

func generation() embedstore.Generation {
	return embedstore.Generation{Identity: "gen-1", SpecDigest: "spec", Name: "articles", Dimension: 4, TargetTable: "articles",
		TargetColumn: "embedding", SourceTable: "articles", ConsistencyMode: "immutable", CreatedAt: created}
}

func pendingRun() embedrun.Run {
	return embedrun.Run{ID: "run-1", SpecDigest: "spec", GenerationIdentity: "gen-1", Source: embedstore.SourceIdentity("", "articles"),
		Target: "articles.embedding", Phase: embedrun.PhaseBoundaryCaptured, Status: embedrun.StatusRunning, FencingToken: 3,
		Cursor: []string{"7"}, Progress: embedrun.Progress{RowsScanned: 7, RowsEmbedded: 6, RowsSkipped: 1, BatchesCommitted: 2, RetryCount: 1},
		CreatedAt: created, UpdatedAt: created}
}

// seeded is a store that holds the generation and the run as the backend
// wrote them.
func seeded(c *qt.C) *fakeStore {
	c.Helper()
	store := &fakeStore{}
	backend := backendOver(store)
	_, err := backend.Store().RegisterGeneration(c.Context(), generation())
	c.Assert(err, qt.IsNil)
	c.Assert(backend.Store().CreateRun(c.Context(), pendingRun()), qt.IsNil)
	store.mutations = nil
	return store
}

// TestStore_ReadsBackWhatItWrote is the conversion both ways: a generation
// and a run written through the service read back equal, nanoseconds
// included.
func TestStore_ReadsBackWhatItWrote(t *testing.T) {
	c := qt.New(t)
	backend := backendOver(seeded(c))

	gotGeneration, err := backend.Store().Generation(c.Context(), "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(gotGeneration, qt.DeepEquals, generation())
	gotRun, err := backend.Store().Run(c.Context(), "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(gotRun, qt.DeepEquals, pendingRun())
}

// TestStore_DecidesAgainAfterAConflict is the compare-and-write: a decision
// whose token went stale is read and decided again, and each mutation carries
// the token of the read it was decided on.
func TestStore_DecidesAgainAfterAConflict(t *testing.T) {
	c := qt.New(t)
	store := seeded(c)
	store.conflicts = 2
	backend := backendOver(store)
	reads := store.reads

	c.Assert(backend.Store().RecordVerification(c.Context(), "gen-1", created.Add(time.Hour)), qt.IsNil)

	c.Assert(store.mutations, qt.HasLen, 3)
	tokens := []string{store.mutations[0].GetExpectedToken(), store.mutations[1].GetExpectedToken(), store.mutations[2].GetExpectedToken()}
	c.Assert(tokens, qt.DeepEquals, []string{"token-" + strconv.Itoa(reads+1), "token-" + strconv.Itoa(reads+2), "token-" + strconv.Itoa(reads+3)})
	verified, err := backend.Store().Generation(c.Context(), "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(verified.VerifiedAt, qt.Equals, created.Add(time.Hour))
}

// TestStore_WritesOnlyWhatTheDecisionChanged sends the one record a decision
// changed, and nothing at all for a decision that changed nothing.
func TestStore_WritesOnlyWhatTheDecisionChanged(t *testing.T) {
	c := qt.New(t)
	store := seeded(c)
	store.records.Pointers = []*protocol.EmbeddingPointer{{TargetTable: "articles", ActiveGeneration: "gen-0"}}
	backend := backendOver(store)

	c.Assert(backend.Store().RecordVerification(c.Context(), "gen-1", created.Add(time.Hour)), qt.IsNil)
	c.Assert(store.mutations, qt.HasLen, 1)
	c.Assert(store.mutations[0].GetRecords().GetGenerations(), qt.HasLen, 1)
	c.Assert(store.mutations[0].GetRecords().GetRuns(), qt.HasLen, 0)
	c.Assert(store.mutations[0].GetRecords().GetPointers(), qt.HasLen, 0)

	registered, err := backend.Store().RegisterGeneration(c.Context(), generation())
	c.Assert(err, qt.IsNil)
	c.Assert(registered.Identity, qt.Equals, "gen-1")
	c.Assert(store.mutations, qt.HasLen, 1)
}

// TestStore_AnswersWhatTheEngineStored is a provider that keeps times to the
// microsecond: the run a claim returns is the run as stored, not the run the
// decision built, so a later comparison of the two agrees.
func TestStore_AnswersWhatTheEngineStored(t *testing.T) {
	c := qt.New(t)
	store := seeded(c)
	store.store = func(records *protocol.EmbeddingRecords) *protocol.EmbeddingRecords {
		for _, run := range records.GetRuns() {
			run.LeaseExpires.Nanos -= run.GetLeaseExpires().GetNanos() % 1000
		}
		return records
	}
	backend := backendOver(store)

	claimed, token, err := backend.Store().ClaimRun(c.Context(), "run-1", "worker", created.Add(time.Minute))
	c.Assert(err, qt.IsNil)
	c.Assert(token, qt.Equals, int64(4))
	c.Assert(claimed.LeaseExpires, qt.Equals, created.Add(time.Minute).Truncate(time.Microsecond))
	read, err := backend.Store().Run(c.Context(), "run-1")
	c.Assert(err, qt.IsNil)
	c.Assert(read, qt.DeepEquals, claimed)
}

// TestStore_AnEventOfARunThatIsNotThere is the provider's ABSENT refusal read
// as the store's not-found.
func TestStore_AnEventOfARunThatIsNotThere(t *testing.T) {
	c := qt.New(t)
	store := &fakeStore{refusal: &protocol.EmbeddingRefusal{Reason: protocol.EmbeddingRefusal_ABSENT, Message: "run run-9 is not recorded"}}

	err := backendOver(store).Store().AppendEvent(c.Context(), embedrun.Event{RunID: "run-9", Kind: "claimed", At: created})

	c.Assert(err, qt.ErrorIs, embedstore.ErrNotFound)
	c.Assert(err, qt.ErrorMatches, `not found: run run-9`)
}

// TestStore_AnEventRefusedForAnotherReason keeps the provider's line and the
// SDK's error.
func TestStore_AnEventRefusedForAnotherReason(t *testing.T) {
	c := qt.New(t)
	store := &fakeStore{refusal: &protocol.EmbeddingRefusal{Reason: protocol.EmbeddingRefusal_INVALID, Message: "the event is too long"}}

	err := backendOver(store).Store().AppendEvent(c.Context(), embedrun.Event{RunID: "run-1", Kind: "claimed", At: created})

	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(err, qt.ErrorMatches, `append an event to run run-1: the event is too long: provider embedding write was refused: INVALID`)
	c.Assert(err, qt.Not(qt.ErrorIs), embedstore.ErrNotFound)
}

// TestBackend_CheckMode_HappyPath accepts a mode the engine declares, and no
// mode at all, which installs nothing.
func TestBackend_CheckMode_HappyPath(t *testing.T) {
	for _, mode := range []embedcatchup.Mode{embedcatchup.ModeImmutable, embedcatchup.ModeNone} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(backendOver(&fakeStore{}).CheckMode(mode), qt.IsNil)
		})
	}
}

// TestBackend_CheckMode_FailurePath refuses a mode the engine does not
// declare, naming the ones it does.
func TestBackend_CheckMode_FailurePath(t *testing.T) {
	c := qt.New(t)
	err := backendOver(&fakeStore{}).CheckMode(embedcatchup.ModeOutbox)
	c.Assert(err, qt.ErrorIs, embedrpc.ErrUnsupported)
	c.Assert(err, qt.ErrorMatches, `.*engine "fixture" builds generations in consistency mode immutable, and the specification selects "outbox".*`)
}

// pagedEvents answers a run's events a page at a time, or a page that is not
// the last one and holds nothing.
type pagedEvents struct {
	provider.EmbeddingStore

	pages []*protocol.EmbeddingEventPage
	asked []int64
}

func (p *pagedEvents) ReadEmbeddingEvents(_ context.Context, request *protocol.ReadEmbeddingEventsRequest) (*protocol.EmbeddingEventPage, error) {
	p.asked = append(p.asked, request.GetAfterSequence())
	page := p.pages[0]
	p.pages = p.pages[1:]
	return page, nil
}

func event(sequence int64, kind string) *protocol.EmbeddingEvent {
	return &protocol.EmbeddingEvent{RunId: "run-1", Sequence: sequence, Kind: kind}
}

// TestStore_EventsReadsEveryPage asks for each page after the last sequence
// the one before it held, until the provider says the history is complete.
func TestStore_EventsReadsEveryPage(t *testing.T) {
	c := qt.New(t)
	store := &pagedEvents{pages: []*protocol.EmbeddingEventPage{
		{RunId: "run-1", Exists: true, Events: []*protocol.EmbeddingEvent{event(1, "prepared"), event(2, "claimed")}},
		{RunId: "run-1", Exists: true, Events: []*protocol.EmbeddingEvent{event(3, "committed")}, Complete: true},
	}}

	events, err := backendOver(store).Store().Events(c.Context(), "run-1")

	c.Assert(err, qt.IsNil)
	kinds := make([]string, len(events))
	for i, read := range events {
		kinds[i] = string(read.Kind)
	}
	c.Assert(kinds, qt.DeepEquals, []string{"prepared", "claimed", "committed"})
	c.Assert(store.asked, qt.DeepEquals, []int64{0, 2})
}

// TestStore_EventsFailurePath refuses a run that is not there and a page that
// is not the last one and holds nothing, which would otherwise be asked for
// again forever.
func TestStore_EventsFailurePath(t *testing.T) {
	tests := []struct {
		name string
		page *protocol.EmbeddingEventPage
		want string
	}{
		{name: "a run that is not there", page: &protocol.EmbeddingEventPage{RunId: "run-1", Complete: true}, want: `not found: run run-1`},
		{name: "an empty page that is not the last", page: &protocol.EmbeddingEventPage{RunId: "run-1", Exists: true},
			want: `read the events of run run-1: a page that is not the last one holds no event`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			events, err := backendOver(&pagedEvents{pages: []*protocol.EmbeddingEventPage{test.page}}).Store().Events(c.Context(), "run-1")
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(events, qt.IsNil)
		})
	}
}

// TestStore_RunsForGenerationNewestFirst orders a generation's runs newest
// first and by identifier after that, the order the other stores give.
func TestStore_RunsForGenerationNewestFirst(t *testing.T) {
	c := qt.New(t)
	older, newer, tied := pendingRun(), pendingRun(), pendingRun()
	older.ID, newer.ID, tied.ID = "run-a", "run-c", "run-b"
	newer.CreatedAt, tied.CreatedAt = created.Add(time.Hour), created.Add(time.Hour)
	store := &fakeStore{records: &protocol.EmbeddingRecords{}}
	backend := backendOver(store)
	_, err := backend.Store().RegisterGeneration(c.Context(), generation())
	c.Assert(err, qt.IsNil)
	for _, run := range []embedrun.Run{older, newer, tied} {
		c.Assert(backend.Store().CreateRun(c.Context(), run), qt.IsNil)
	}

	runs, err := backend.Store().RunsForGeneration(c.Context(), "gen-1")

	c.Assert(err, qt.IsNil)
	ids := make([]string, len(runs))
	for i, run := range runs {
		ids[i] = run.ID
	}
	c.Assert(ids, qt.DeepEquals, []string{"run-b", "run-c", "run-a"})
}

// TestStore_DecidesAboutOneRunOverThatRun reads and writes a decision about
// one run's own record over that run alone, so it conflicts with no work on
// another run.
func TestStore_DecidesAboutOneRunOverThatRun(t *testing.T) {
	c := qt.New(t)
	store := seeded(c)
	backend := backendOver(store)
	saved := pendingRun()
	saved.Phase = embedrun.PhaseBackfilling

	c.Assert(backend.Store().SaveRun(c.Context(), saved), qt.IsNil)
	_, _, err := backend.Store().ClaimRun(c.Context(), "run-1", "worker", created.Add(time.Minute))
	c.Assert(err, qt.IsNil)
	c.Assert(backend.Store().ReachPhase(c.Context(), "run-1", embedrun.PhaseBackfilled), qt.IsNil)

	c.Assert(store.mutations, qt.HasLen, 3)
	for _, mutation := range store.mutations {
		c.Assert(mutation.GetScope().GetRun(), qt.Equals, "run-1")
	}
}

// microseconds stores every time of the records to the microsecond, as an
// engine that keeps no finer precision does.
func microseconds(records *protocol.EmbeddingRecords) *protocol.EmbeddingRecords {
	truncate := func(at *protocol.TimestampValue) {
		if at != nil {
			at.Nanos -= at.GetNanos() % 1000
		}
	}
	for _, generation := range records.GetGenerations() {
		for _, at := range []*protocol.TimestampValue{generation.GetCreatedAt(), generation.GetRetiredAt(), generation.GetVerifiedAt(), generation.GetMaintainedUntil()} {
			truncate(at)
		}
	}
	for _, run := range records.GetRuns() {
		for _, at := range []*protocol.TimestampValue{run.GetLeaseExpires(), run.GetCreatedAt(), run.GetUpdatedAt()} {
			truncate(at)
		}
	}
	for _, pointer := range records.GetPointers() {
		truncate(pointer.GetCutOverAt())
	}
	return records
}

// TestStore_PointerMovesAnswerWhatTheEngineStored cuts over with a
// stabilization window and rolls back on an engine that keeps microseconds:
// each move answers the instants as stored, which is what a later move
// compares them with.
func TestStore_PointerMovesAnswerWhatTheEngineStored(t *testing.T) {
	c := qt.New(t)
	store := &fakeStore{store: microseconds}
	backend := backendOver(store)
	for _, identity := range []string{"gen-1", "gen-2"} {
		registered := generation()
		registered.Identity = identity
		_, err := backend.Store().RegisterGeneration(c.Context(), registered)
		c.Assert(err, qt.IsNil)
	}
	for _, run := range []embedrun.Run{
		{ID: "run-1", GenerationIdentity: "gen-1", Phase: embedrun.PhaseCutOver, Status: embedrun.StatusRunning},
		{ID: "run-2", GenerationIdentity: "gen-2", Phase: embedrun.PhaseVerified, Status: embedrun.StatusRunning},
	} {
		c.Assert(backend.Store().CreateRun(c.Context(), run), qt.IsNil)
	}
	store.records.Pointers = []*protocol.EmbeddingPointer{{TargetTable: "articles", ActiveGeneration: "gen-1"}}

	move, err := backend.Store().MovePointerWithMaintenance(c.Context(),
		embedstore.Pointer{TargetTable: "articles", Active: "gen-2", Previous: "gen-1"}, "gen-1", "run-2", time.Hour)
	c.Assert(err, qt.IsNil)
	cutOver, err := backend.Store().Pointer(c.Context(), "", "articles")
	c.Assert(err, qt.IsNil)
	previous, err := backend.Store().Generation(c.Context(), "gen-1")
	c.Assert(err, qt.IsNil)
	c.Assert(move.CutOverAt, qt.Equals, cutOver.CutOverAt)
	c.Assert(move.PreviousMaintainedUntil, qt.Equals, previous.MaintainedUntil)
	c.Assert(move.PreviousMaintainedUntil.IsZero(), qt.IsFalse)

	at, err := backend.Store().MovePointerWithRollback(c.Context(),
		embedstore.Pointer{TargetTable: "articles", Active: "gen-1", Previous: "gen-2"}, "gen-2", previous.MaintainedUntil, time.Time{})
	c.Assert(err, qt.IsNil)
	rolledBack, err := backend.Store().Pointer(c.Context(), "", "articles")
	c.Assert(err, qt.IsNil)
	c.Assert(rolledBack.Active, qt.Equals, "gen-1")
	c.Assert(at, qt.Equals, rolledBack.CutOverAt)
	c.Assert(at.IsZero(), qt.IsFalse)
}
