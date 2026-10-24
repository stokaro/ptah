package embedrpc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// maxDecisions bounds how often a lifecycle operation reads and decides again
// after a CONFLICT. Each attempt reads everything it decides on again, so
// deciding again is correct; the bound is what turns contention that never
// clears into an error rather than a command that never returns.
const maxDecisions = 12

// errContended is the last conflict of an operation that ran out of
// decisions.
var errContended = errors.New("another writer kept changing the records this decision read")

// Store is [embedbackend.Store] over the provider's run-state store.
//
// An operation about one run reads that run alone, so a batch checkpoint does
// not conflict with work on any other run. Every other operation reads every
// generation, run and pointer, because the rules it applies ask about the
// generation's other runs and about every pointer that might name it.
type Store struct {
	backend *Backend
}

var _ embedbackend.Store = (*Store)(nil)

// clock is what a decision samples time from. A provider returns each record
// it wrote as a later read returns it, and what a decision answers is taken
// from those records, so a time the engine keeps to the microsecond comes back
// as stored.
func clock() time.Time { return time.Now().UTC() }

func everything() *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Everything{Everything: &protocol.EmbeddingStateScope_AllRecords{}}}
}

func oneRun(id string) *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Run{Run: id}}
}

func oneGeneration(identity string) *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Generation{Generation: identity}}
}

func runsOf(identity string) *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_RunsOfGeneration{RunsOfGeneration: identity}}
}

func onePointer(schema, table string) *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Pointer{
		Pointer: &protocol.EmbeddingPointerKey{TargetSchema: schema, TargetTable: table}}}
}

// read reads one scope as one snapshot. A store that does not exist reads as
// one that holds nothing.
func (s *Store) read(ctx context.Context, scope *protocol.EmbeddingStateScope) (*protocol.EmbeddingState, error) {
	state, err := s.backend.session.Store.ReadEmbeddingState(ctx, &protocol.ReadEmbeddingStateRequest{
		SessionId: s.backend.session.ID, Scope: scope, MaxRecords: provider.MaxEmbeddingRecords,
	})
	if err != nil {
		return nil, fmt.Errorf("read the embedding state on %s: %w", s.backend.engineName(), err)
	}
	return state, nil
}

// decide runs decision on a store built from the records scope reads and
// writes back every record the decision changed, with the token the read
// returned. It returns the state after the decision, each written record as
// the provider stored it.
func (s *Store) decide(
	ctx context.Context, scope *protocol.EmbeddingStateScope, what string,
	decision func(*embedstore.Memory) error,
) (embedstore.Snapshot, error) {
	for attempt := range maxDecisions {
		if attempt > 0 {
			if err := pause(ctx, attempt); err != nil {
				return embedstore.Snapshot{}, fmt.Errorf("%s: %w", what, err)
			}
		}
		state, err := s.read(ctx, scope)
		if err != nil {
			return embedstore.Snapshot{}, err
		}
		memory := embedstore.NewMemoryFrom(snapshotOf(state.GetRecords()), clock)
		if err := decision(memory); err != nil {
			return embedstore.Snapshot{}, err
		}
		after := memory.Snapshot()
		changed := changedRecords(state.GetRecords(), after)
		if recordCount(changed) == 0 {
			return after, nil
		}
		result, err := s.backend.session.Store.MutateEmbeddingState(ctx, &protocol.MutateEmbeddingStateRequest{
			SessionId: s.backend.session.ID, RequestId: requestID(), Scope: scope, ExpectedToken: state.GetToken(), Records: changed,
		})
		if errors.Is(err, provider.ErrEmbeddingConflict) {
			continue
		}
		if err != nil {
			return embedstore.Snapshot{}, writeError(what, result, err)
		}
		return stored(after, result.GetWritten()), nil
	}
	return embedstore.Snapshot{}, fmt.Errorf("%s: %w: %w after %d decisions", what, embedstore.ErrConflict, errContended, maxDecisions)
}

// pause waits before attempt, longer each time and with jitter, so two
// writers that conflicted do not meet again at the same instant.
func pause(ctx context.Context, attempt int) error {
	base := min(10*time.Millisecond<<attempt, time.Second)
	// #nosec G404 -- jitter, not a secret.
	timer := time.NewTimer(base/2 + time.Duration(rand.Int64N(int64(base))))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func recordCount(records *protocol.EmbeddingRecords) int {
	return len(records.GetGenerations()) + len(records.GetRuns()) + len(records.GetPointers())
}

// changedRecords lists the records of after that differ from the ones read,
// in the SDK's key order. A decision never deletes a record, so a record
// missing from after is one the scope did not read.
func changedRecords(before *protocol.EmbeddingRecords, after embedstore.Snapshot) *protocol.EmbeddingRecords {
	changed := &protocol.EmbeddingRecords{}
	for _, generation := range after.Generations {
		record := generationRecord(generation)
		if !slices.ContainsFunc(before.GetGenerations(), func(read *protocol.EmbeddingGeneration) bool { return proto.Equal(read, record) }) {
			changed.Generations = append(changed.Generations, record)
		}
	}
	for _, run := range after.Runs {
		record := runRecord(run)
		if !slices.ContainsFunc(before.GetRuns(), func(read *protocol.EmbeddingRun) bool { return proto.Equal(read, record) }) {
			changed.Runs = append(changed.Runs, record)
		}
	}
	for _, pointer := range after.Pointers {
		record := pointerRecord(pointer)
		if !slices.ContainsFunc(before.GetPointers(), func(read *protocol.EmbeddingPointer) bool { return proto.Equal(read, record) }) {
			changed.Pointers = append(changed.Pointers, record)
		}
	}
	slices.SortFunc(changed.Generations, func(a, b *protocol.EmbeddingGeneration) int { return cmp.Compare(a.GetIdentity(), b.GetIdentity()) })
	slices.SortFunc(changed.Runs, func(a, b *protocol.EmbeddingRun) int { return cmp.Compare(a.GetId(), b.GetId()) })
	slices.SortFunc(changed.Pointers, func(a, b *protocol.EmbeddingPointer) int {
		return cmp.Or(cmp.Compare(a.GetTargetSchema(), b.GetTargetSchema()), cmp.Compare(a.GetTargetTable(), b.GetTargetTable()))
	})
	return changed
}

// stored is after with each record the provider wrote replaced by the record
// as it stored it.
func stored(after embedstore.Snapshot, written *protocol.EmbeddingRecords) embedstore.Snapshot {
	for _, record := range written.GetGenerations() {
		for i := range after.Generations {
			if after.Generations[i].Identity == record.GetIdentity() {
				after.Generations[i] = generationOf(record)
			}
		}
	}
	for _, record := range written.GetRuns() {
		for i := range after.Runs {
			if after.Runs[i].ID == record.GetId() {
				after.Runs[i] = runOf(record)
			}
		}
	}
	for _, record := range written.GetPointers() {
		for i := range after.Pointers {
			if after.Pointers[i].TargetSchema == record.GetTargetSchema() && after.Pointers[i].TargetTable == record.GetTargetTable() {
				after.Pointers[i] = pointerOf(record)
			}
		}
	}
	return after
}

// generationIn finds a generation in a snapshot.
func generationIn(snapshot embedstore.Snapshot, identity string) (embedstore.Generation, error) {
	for _, generation := range snapshot.Generations {
		if generation.Identity == identity {
			return generation, nil
		}
	}
	return embedstore.Generation{}, fmt.Errorf("%w: generation %s", embedstore.ErrNotFound, identity)
}

// runIn finds a run in a snapshot.
func runIn(snapshot embedstore.Snapshot, id string) (embedrun.Run, error) {
	for _, run := range snapshot.Runs {
		if run.ID == id {
			return run, nil
		}
	}
	return embedrun.Run{}, fmt.Errorf("%w: run %s", embedstore.ErrNotFound, id)
}

// pointerIn finds a pointer in a snapshot, the zero pointer when there is
// none.
func pointerIn(snapshot embedstore.Snapshot, schema, table string) embedstore.Pointer {
	for _, pointer := range snapshot.Pointers {
		if pointer.TargetSchema == schema && pointer.TargetTable == table {
			return pointer
		}
	}
	return embedstore.Pointer{}
}

// RegisterGeneration records a generation, or returns the existing record
// unchanged.
func (s *Store) RegisterGeneration(ctx context.Context, generation embedstore.Generation) (embedstore.Generation, error) {
	after, err := s.decide(ctx, everything(), "register generation "+generation.Identity, func(memory *embedstore.Memory) error {
		_, err := memory.RegisterGeneration(ctx, generation)
		return err
	})
	if err != nil {
		return embedstore.Generation{}, err
	}
	return generationIn(after, generation.Identity)
}

// Generation reads one back.
func (s *Store) Generation(ctx context.Context, identity string) (embedstore.Generation, error) {
	state, err := s.read(ctx, oneGeneration(identity))
	if err != nil {
		return embedstore.Generation{}, err
	}
	return generationIn(snapshotOf(state.GetRecords()), identity)
}

// RecordVerification records that a verification passed over a generation.
func (s *Store) RecordVerification(ctx context.Context, identity string, at time.Time) error {
	_, err := s.decide(ctx, everything(), "record the verification of generation "+identity, func(memory *embedstore.Memory) error {
		return memory.RecordVerification(ctx, identity, at)
	})
	return err
}

// Maintain records how long something will keep a generation current.
func (s *Store) Maintain(ctx context.Context, identity string, until time.Time) error {
	_, err := s.decide(ctx, everything(), "maintain generation "+identity, func(memory *embedstore.Memory) error {
		return memory.Maintain(ctx, identity, until)
	})
	return err
}

// CreateRun records a new run.
func (s *Store) CreateRun(ctx context.Context, run embedrun.Run) error {
	_, err := s.decide(ctx, everything(), "create run "+run.ID, func(memory *embedstore.Memory) error {
		return memory.CreateRun(ctx, run)
	})
	return err
}

// Run reads one back.
func (s *Store) Run(ctx context.Context, id string) (embedrun.Run, error) {
	state, err := s.read(ctx, oneRun(id))
	if err != nil {
		return embedrun.Run{}, err
	}
	return runIn(snapshotOf(state.GetRecords()), id)
}

// RunsForGeneration reads every run that built one generation, newest first
// and by identifier after that, which is the order the other stores give.
func (s *Store) RunsForGeneration(ctx context.Context, identity string) ([]embedrun.Run, error) {
	state, err := s.read(ctx, runsOf(identity))
	if err != nil {
		return nil, err
	}
	runs := make([]embedrun.Run, 0, len(state.GetRecords().GetRuns()))
	runs = append(runs, snapshotOf(state.GetRecords()).Runs...)
	slices.SortFunc(runs, func(a, b embedrun.Run) int { return cmp.Or(b.CreatedAt.Compare(a.CreatedAt), cmp.Compare(a.ID, b.ID)) })
	return runs, nil
}

// SaveRun writes a non-terminal run's state, refusing a stale fencing token.
func (s *Store) SaveRun(ctx context.Context, run embedrun.Run) error {
	_, err := s.decide(ctx, oneRun(run.ID), "save run "+run.ID, func(memory *embedstore.Memory) error {
		return memory.SaveRun(ctx, run)
	})
	return err
}

// ClaimRun takes a run for a worker and returns it as the store now holds it.
func (s *Store) ClaimRun(ctx context.Context, id, worker string, leaseExpires time.Time) (embedrun.Run, int64, error) {
	after, err := s.decide(ctx, oneRun(id), "claim run "+id, func(memory *embedstore.Memory) error {
		_, _, err := memory.ClaimRun(ctx, id, worker, leaseExpires)
		return err
	})
	if err != nil {
		return embedrun.Run{}, 0, err
	}
	run, err := runIn(after, id)
	if err != nil {
		return embedrun.Run{}, 0, err
	}
	return run, run.FencingToken, nil
}

// AbandonRun permanently ends one run without destroying its generation.
func (s *Store) AbandonRun(ctx context.Context, id, reason string) (embedrun.Run, error) {
	after, err := s.decide(ctx, everything(), "abandon run "+id, func(memory *embedstore.Memory) error {
		_, err := memory.AbandonRun(ctx, id, reason)
		return err
	})
	if err != nil {
		return embedrun.Run{}, err
	}
	return runIn(after, id)
}

// ReachPhase records that a run has got as far as a phase.
func (s *Store) ReachPhase(ctx context.Context, runID string, to embedrun.Phase) error {
	_, err := s.decide(ctx, oneRun(runID), "record phase "+string(to)+" on run "+runID, func(memory *embedstore.Memory) error {
		return memory.ReachPhase(ctx, runID, to)
	})
	return err
}

// AppendEvent records what happened. The provider gives the event the run's
// next sequence.
func (s *Store) AppendEvent(ctx context.Context, event embedrun.Event) error {
	result, err := s.backend.session.Store.AppendEmbeddingEvent(ctx, &protocol.AppendEmbeddingEventRequest{
		SessionId: s.backend.session.ID, RequestId: requestID(), Event: eventRecord(event),
	})
	if written, confirmed := errors.AsType[*provider.EmbeddingWriteError](err); confirmed && written.Reason == protocol.EmbeddingRefusal_ABSENT {
		return fmt.Errorf("%w: run %s", embedstore.ErrNotFound, event.RunID)
	}
	return writeError("append an event to run "+event.RunID, result, err)
}

// Events reads a run's history in order, a page at a time.
func (s *Store) Events(ctx context.Context, runID string) ([]embedrun.Event, error) {
	events := make([]embedrun.Event, 0)
	after := int64(0)
	for {
		page, err := s.backend.session.Store.ReadEmbeddingEvents(ctx, &protocol.ReadEmbeddingEventsRequest{
			SessionId: s.backend.session.ID, RunId: runID, AfterSequence: after, MaxEvents: provider.MaxEmbeddingEvents,
		})
		if err != nil {
			return nil, fmt.Errorf("read the events of run %s: %w", runID, err)
		}
		if !page.GetExists() {
			return nil, fmt.Errorf("%w: run %s", embedstore.ErrNotFound, runID)
		}
		for _, record := range page.GetEvents() {
			events = append(events, eventOf(record))
			after = record.GetSequence()
		}
		if page.GetComplete() {
			return events, nil
		}
		if len(page.GetEvents()) == 0 {
			return nil, fmt.Errorf("read the events of run %s: a page that is not the last one holds no event", runID)
		}
	}
}

// Pointer reads which generation a target's queries currently read.
func (s *Store) Pointer(ctx context.Context, targetSchema, targetTable string) (embedstore.Pointer, error) {
	state, err := s.read(ctx, onePointer(targetSchema, targetTable))
	if err != nil {
		return embedstore.Pointer{}, err
	}
	if len(state.GetRecords().GetPointers()) == 0 {
		return embedstore.Pointer{}, fmt.Errorf("%w: no pointer for %s", embedstore.ErrNotFound, embedstore.QualifiedName(targetSchema, targetTable))
	}
	return pointerOf(state.GetRecords().GetPointers()[0]), nil
}

// MovePointer moves it, refusing when it is not where the caller thinks.
func (s *Store) MovePointer(ctx context.Context, pointer embedstore.Pointer, expectedActive string) error {
	_, err := s.decide(ctx, everything(), "move the pointer of "+embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable),
		func(memory *embedstore.Memory) error {
			return memory.MovePointer(ctx, pointer, expectedActive)
		})
	return err
}

// MovePointerWithMaintenance moves the pointer, opens the previous
// generation's maintenance window and records the cutover on its run, in one
// compare-and-write.
func (s *Store) MovePointerWithMaintenance(
	ctx context.Context, pointer embedstore.Pointer, expectedActive, requiredRunID string, stabilizeFor time.Duration,
) (embedstore.CutoverMove, error) {
	after, err := s.decide(ctx, everything(), "cut over "+embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable),
		func(memory *embedstore.Memory) error {
			_, err := memory.MovePointerWithMaintenance(ctx, pointer, expectedActive, requiredRunID, stabilizeFor)
			return err
		})
	if err != nil {
		return embedstore.CutoverMove{}, err
	}
	// The answer is read off the records as stored. The decision set the
	// previous generation's window to the one it answers, so the stored window
	// is that answer at the engine's precision.
	move := embedstore.CutoverMove{CutOverAt: pointerIn(after, pointer.TargetSchema, pointer.TargetTable).CutOverAt}
	if pointer.Previous == "" {
		return move, nil
	}
	previous, err := generationIn(after, pointer.Previous)
	if err != nil {
		return embedstore.CutoverMove{}, err
	}
	move.PreviousMaintainedUntil = previous.MaintainedUntil
	return move, nil
}

// MovePointerWithRollback moves the pointer back and records the rollback on
// the displaced generation's runs, in one compare-and-write.
func (s *Store) MovePointerWithRollback(
	ctx context.Context, pointer embedstore.Pointer, expectedActive string, expectedMaintainedUntil, eligibilityNotAfter time.Time,
) (time.Time, error) {
	after, err := s.decide(ctx, everything(), "roll back "+embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable),
		func(memory *embedstore.Memory) error {
			_, err := memory.MovePointerWithRollback(ctx, pointer, expectedActive, expectedMaintainedUntil, eligibilityNotAfter)
			return err
		})
	if err != nil {
		return time.Time{}, err
	}
	return pointerIn(after, pointer.TargetSchema, pointer.TargetTable).CutOverAt, nil
}
