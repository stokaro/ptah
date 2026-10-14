package embedstore

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"ptah.run/internal/embedrun"
)

// Snapshot is every registry, run and pointer row a lifecycle decision reads.
//
// It exists so a store that keeps its rows in a database without the locks the
// PostgreSQL store takes can still decide with these rules rather than with a
// copy of them. Such a store reads the rows inside one serializable
// transaction, decides on a [Memory] built from them, and writes back the rows
// the decision changed; the database refuses the commit when another
// transaction changed a row this one read, and the operation runs again on
// what is there now. The audit trail is not part of it: no rule reads events.
type Snapshot struct {
	Generations []Generation
	Runs        []embedrun.Run
	Pointers    []Pointer
}

// NewMemoryFrom returns a store holding snapshot, whose lifecycle operations
// sample time from now. A nil now reads the system clock.
func NewMemoryFrom(snapshot Snapshot, now func() time.Time) *Memory {
	memory := NewMemoryWithClock(now)
	for _, generation := range snapshot.Generations {
		memory.generations[generation.Identity] = generation
	}
	for _, run := range snapshot.Runs {
		memory.runs[run.ID] = copyRun(run)
	}
	for _, pointer := range snapshot.Pointers {
		memory.pointers[pointerKey(pointer.TargetSchema, pointer.TargetTable)] = pointer
	}
	return memory
}

// Snapshot returns the rows the store holds, each list in key order, so two
// snapshots of one state compare equal.
func (m *Memory) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := Snapshot{
		Generations: make([]Generation, 0, len(m.generations)),
		Runs:        make([]embedrun.Run, 0, len(m.runs)),
		Pointers:    make([]Pointer, 0, len(m.pointers)),
	}
	for _, identity := range slices.Sorted(maps.Keys(m.generations)) {
		snapshot.Generations = append(snapshot.Generations, m.generations[identity])
	}
	for _, id := range slices.Sorted(maps.Keys(m.runs)) {
		snapshot.Runs = append(snapshot.Runs, copyRun(m.runs[id]))
	}
	for _, key := range slices.Sorted(maps.Keys(m.pointers)) {
		snapshot.Pointers = append(snapshot.Pointers, m.pointers[key])
	}
	return snapshot
}

// ReachPhase records that a run has got as far as a phase.
//
// Read, decide, save, and the decision is [embedrun.Run.Reach], which leaves a
// phase already at or past the target alone. The stored token is the one
// offered, because the caller is an operator running a command rather than a
// worker holding a lease; the fence exists to stop a worker the run has moved
// past from committing. It mirrors the PostgreSQL store's ReachPhase.
func (m *Memory) ReachPhase(ctx context.Context, runID string, to embedrun.Phase) error {
	run, err := m.Run(ctx, runID)
	if err != nil {
		return err
	}
	before := run.Phase
	if err := run.Reach(run.FencingToken, to); err != nil {
		return err
	}
	if run.Phase == before {
		return nil
	}
	return m.SaveRun(ctx, run)
}

// RetireGeneration records that a generation was destroyed: it completes every
// run that built it and stamps the registry row, and returns the instant it
// recorded.
//
// It decides under the same rules the PostgreSQL store applies before its DDL.
// A retired generation is refused. A generation queries read anywhere is
// refused, because a generation identity is global while a pointer is per
// target, and a malformed historical pointer must not authorize destroying
// objects it still serves. The pointer of the generation's own target must
// still be the one the retirement was approved against, where a zero expected
// pointer means the approval saw none. A generation still maintained is
// refused, because something has promised to keep it as a way back.
//
// Every run is fenced and completed because its generation no longer exists. A
// phase that leads to [embedrun.PhaseRetired] records it; an earlier run keeps
// its truthful high-water phase while becoming terminal.
func (m *Memory) RetireGeneration(
	_ context.Context, identity string, expectedPointer Pointer,
) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation, found := m.generations[identity]
	if !found {
		return time.Time{}, fmt.Errorf("%w: generation %s", ErrNotFound, identity)
	}
	if generation.Retired() {
		return time.Time{}, fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, identity, generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	if err := m.validateRetirementPointer(generation, expectedPointer); err != nil {
		return time.Time{}, err
	}
	retiredAt := m.now().UTC()
	if generation.Maintained(retiredAt) {
		return time.Time{}, fmt.Errorf("%w: generation %s is maintained until %s",
			ErrConflict, identity, generation.MaintainedUntil.UTC().Format(time.RFC3339))
	}
	for _, id := range slices.Sorted(maps.Keys(m.runs)) {
		run := m.runs[id]
		if run.GenerationIdentity != identity {
			continue
		}
		run.FencingToken++
		if run.Phase.LeadsTo(embedrun.PhaseRetired) {
			run.Phase = embedrun.PhaseRetired
		}
		run.Status = embedrun.StatusComplete
		run.LeaseOwner = ""
		run.LeaseExpires = time.Time{}
		run.RollbackEligible = false
		run.UpdatedAt = retiredAt
		m.runs[id] = copyRun(run)
	}
	generation.RetiredAt = retiredAt
	m.generations[identity] = generation
	return retiredAt, nil
}

// validateRetirementPointer compares the pointers that decide whether a
// generation is active or a rollback dependency with the ones the retirement
// was approved against. The caller holds m.mu.
func (m *Memory) validateRetirementPointer(generation Generation, expected Pointer) error {
	target := QualifiedName(generation.TargetSchema, generation.TargetTable)
	if (expected.TargetSchema != "" || expected.TargetTable != "") &&
		(expected.TargetSchema != generation.TargetSchema ||
			expected.TargetTable != generation.TargetTable) {
		return fmt.Errorf("%w: retirement approval names pointer %s, generation %s targets %s",
			ErrConflict, QualifiedName(expected.TargetSchema, expected.TargetTable),
			generation.Identity, target)
	}
	active := slices.SortedFunc(maps.Values(m.pointers), func(a, b Pointer) int {
		return cmp.Or(cmp.Compare(a.TargetSchema, b.TargetSchema), cmp.Compare(a.TargetTable, b.TargetTable))
	})
	for _, pointer := range active {
		if pointer.Active == generation.Identity {
			return fmt.Errorf("%w: generation %s is active for %s",
				ErrConflict, generation.Identity,
				QualifiedName(pointer.TargetSchema, pointer.TargetTable))
		}
	}
	current, found := m.pointers[pointerKey(generation.TargetSchema, generation.TargetTable)]
	if !found {
		if expected.Active == "" && expected.Previous == "" {
			return nil
		}
		return fmt.Errorf("%w: %s has no pointer; approval expected active %s and previous %s",
			ErrConflict, target, expected.Active, expected.Previous)
	}
	if current.Active != expected.Active || current.Previous != expected.Previous {
		return fmt.Errorf("%w: %s pointer changed from active %s, previous %s to active %s, previous %s",
			ErrConflict, target, expected.Active, expected.Previous, current.Active, current.Previous)
	}
	return nil
}
