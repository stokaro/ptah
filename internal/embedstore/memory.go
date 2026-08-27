package embedstore

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.5x5.cz/ptah/internal/embedrun"
)

// Memory is a Store held in memory.
//
// It exists so the rules above can be tested without a database, and so a
// caller can be written against the contract before the PostgreSQL
// implementation lands. It is not a fallback: nothing here survives a restart,
// which is the one thing a run-state store is for, so a caller that reached for
// it in production would have a resumable migration that cannot resume.
type Memory struct {
	mu          sync.Mutex
	generations map[string]Generation
	runs        map[string]embedrun.Run
	events      map[string][]embedrun.Event
	pointers    map[string]Pointer
}

// NewMemory returns an empty store.
func NewMemory() *Memory {
	return &Memory{
		generations: make(map[string]Generation),
		runs:        make(map[string]embedrun.Run),
		events:      make(map[string][]embedrun.Event),
		pointers:    make(map[string]Pointer),
	}
}

// RegisterGeneration records a generation, or returns the existing row.
func (m *Memory) RegisterGeneration(_ context.Context, generation Generation) (Generation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, found := m.generations[generation.Identity]; found {
		return existing, nil
	}
	m.generations[generation.Identity] = generation
	return generation, nil
}

// Generation reads one back.
func (m *Memory) Generation(_ context.Context, identity string) (Generation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation, found := m.generations[identity]
	if !found {
		return Generation{}, fmt.Errorf("%w: generation %s", ErrNotFound, identity)
	}
	return generation, nil
}

// RetireGeneration marks one destroyed.
func (m *Memory) RetireGeneration(_ context.Context, identity string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation, found := m.generations[identity]
	if !found {
		return fmt.Errorf("%w: generation %s", ErrNotFound, identity)
	}
	if generation.Retired() {
		// Retiring twice is not idempotent bookkeeping: the second call would
		// move the timestamp, and the record of when a corpus was destroyed is
		// the whole value of the row that remains.
		return fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, identity, generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	generation.RetiredAt = at
	m.generations[identity] = generation
	return nil
}

// CreateRun records a new run.
func (m *Memory) CreateRun(_ context.Context, run embedrun.Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, found := m.runs[run.ID]; found {
		return fmt.Errorf("%w: run %s already exists", ErrConflict, run.ID)
	}
	m.runs[run.ID] = copyRun(run)
	return nil
}

// Run reads one back.
func (m *Memory) Run(_ context.Context, id string) (embedrun.Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, found := m.runs[id]
	if !found {
		return embedrun.Run{}, fmt.Errorf("%w: run %s", ErrNotFound, id)
	}
	return copyRun(run), nil
}

// SaveRun writes a run's state, refusing a stale fencing token.
func (m *Memory) SaveRun(_ context.Context, run embedrun.Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, found := m.runs[run.ID]
	if !found {
		return fmt.Errorf("%w: run %s", ErrNotFound, run.ID)
	}
	if run.FencingToken < stored.FencingToken {
		// The worker's lease was taken over while its request was in flight.
		// It is still running, it still believes it owns the run, and this is
		// the only place that knows otherwise.
		return fmt.Errorf("%w: run %s is fenced at token %d and this write carries %d",
			ErrConflict, run.ID, stored.FencingToken, run.FencingToken)
	}
	m.runs[run.ID] = copyRun(run)
	return nil
}

// AppendEvent records what happened.
func (m *Memory) AppendEvent(_ context.Context, event embedrun.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, found := m.runs[event.RunID]; !found {
		return fmt.Errorf("%w: run %s", ErrNotFound, event.RunID)
	}
	m.events[event.RunID] = append(m.events[event.RunID], event)
	return nil
}

// Events reads a run's history in order.
func (m *Memory) Events(_ context.Context, runID string) ([]embedrun.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, found := m.runs[runID]; !found {
		return nil, fmt.Errorf("%w: run %s", ErrNotFound, runID)
	}
	return slices.Clone(m.events[runID]), nil
}

// Pointer reads which generation a target's queries currently read.
func (m *Memory) Pointer(_ context.Context, targetTable string) (Pointer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pointer, found := m.pointers[targetTable]
	if !found {
		return Pointer{}, fmt.Errorf("%w: no pointer for %s", ErrNotFound, targetTable)
	}
	return pointer, nil
}

// MovePointer moves it, refusing when it is not where the caller thinks.
func (m *Memory) MovePointer(_ context.Context, pointer Pointer, expectedActive string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.pointers[pointer.TargetTable]
	switch {
	case !found && expectedActive != "":
		return fmt.Errorf("%w: %s has no pointer and this move expected %s",
			ErrConflict, pointer.TargetTable, expectedActive)
	case found && current.Active != expectedActive:
		return fmt.Errorf("%w: %s reads %s and this move expected %s",
			ErrConflict, pointer.TargetTable, current.Active, expectedActive)
	}
	m.pointers[pointer.TargetTable] = pointer
	return nil
}

// copyRun copies a run's slice-valued fields.
//
// Without it the caller and the store share the cursor's backing array, and a
// worker appending to its own cursor rewrites what the store believes it
// checkpointed -- which reads, on resume, as a backfill that skipped rows.
func copyRun(run embedrun.Run) embedrun.Run {
	run.Cursor = slices.Clone(run.Cursor)
	return run
}
