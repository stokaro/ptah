package embedstore

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedrun"
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
	now         func() time.Time
	generations map[string]Generation
	runs        map[string]embedrun.Run
	events      map[string][]embedrun.Event
	pointers    map[string]Pointer
}

// NewMemory returns an empty store.
func NewMemory() *Memory {
	return NewMemoryWithClock(time.Now)
}

// NewMemoryWithClock returns an empty store whose lifecycle operations sample
// time from now. It exists so a test can exercise expiry after lock acquisition
// without sleeping or making a caller-supplied timestamp authoritative.
func NewMemoryWithClock(now func() time.Time) *Memory {
	if now == nil {
		now = time.Now
	}
	return &Memory{
		now:         now,
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
		if existing.Retired() {
			return Generation{}, fmt.Errorf("%w: generation %s was retired at %s",
				ErrRetired, existing.Identity, existing.RetiredAt.UTC().Format(time.RFC3339))
		}
		return existing, nil
	}
	if generation.Retired() && generation.MaintainedUntil.After(generation.RetiredAt) {
		return Generation{}, fmt.Errorf(
			"%w: generation %s cannot be registered as both retired and maintained",
			ErrConflict, generation.Identity)
	}
	if generation.Retired() && m.hasActivePointer(generation.Identity) {
		return Generation{}, fmt.Errorf(
			"%w: cannot register active generation %s as retired",
			ErrConflict, generation.Identity)
	}
	if generation.Retired() && m.hasNonterminalRun(generation.Identity) {
		return Generation{}, fmt.Errorf(
			"%w: cannot register generation %s as retired while a nonterminal run still reads it",
			ErrConflict, generation.Identity)
	}
	if !generation.Retired() && !generation.MaintainedUntil.IsZero() {
		if err := m.refuseTerminalOnlyGeneration(generation); err != nil {
			return Generation{}, fmt.Errorf("register maintained generation %s: %w",
				generation.Identity, err)
		}
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

// RecordVerification records that a verification passed over a generation.
func (m *Memory) RecordVerification(_ context.Context, identity string, at time.Time) error {
	return m.updateGeneration(identity, func(generation *Generation) {
		generation.VerifiedAt = at
	})
}

// Maintain records how long something will keep a generation current.
func (m *Memory) Maintain(_ context.Context, identity string, until time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation, found := m.generations[identity]
	if !found {
		return fmt.Errorf("%w: generation %s", ErrNotFound, identity)
	}
	if generation.Retired() {
		return fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, identity, generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	if !until.IsZero() {
		if err := m.refuseTerminalOnlyGeneration(generation); err != nil {
			return fmt.Errorf("maintain generation %s: %w", identity, err)
		}
	}
	// Never earlier, mirroring the SQL store's GREATEST. A zero clears,
	// which is what stops a generation being reported as a way back
	// (stokaro/ptah#2647).
	if until.IsZero() || until.After(generation.MaintainedUntil) {
		generation.MaintainedUntil = until
	}
	m.generations[identity] = generation
	return nil
}

// updateGeneration applies a change to a generation that is there and not
// retired.
//
// Retired is refused rather than ignored: recording a verification against a
// destroyed generation would make it look like a way back, and the vectors are
// gone.
func (m *Memory) updateGeneration(identity string, change func(*Generation)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	generation, found := m.generations[identity]
	if !found {
		return fmt.Errorf("%w: generation %s", ErrNotFound, identity)
	}
	if generation.Retired() {
		return fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, identity, generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	change(&generation)
	m.generations[identity] = generation
	return nil
}

// CreateRun records a new run.
func (m *Memory) CreateRun(_ context.Context, run embedrun.Run) error {
	if run.Phase == embedrun.PhaseRetired && run.Status != embedrun.StatusComplete {
		return fmt.Errorf("create run %s: %w: phase %s requires status %s",
			run.ID, embedrun.ErrPhase, embedrun.PhaseRetired, embedrun.StatusComplete)
	}
	if run.Terminal() {
		return fmt.Errorf("create run %s: %w: run is already %s",
			run.ID, embedrun.ErrTerminal, run.Status)
	}
	if err := validateResumePosition(run); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	generation, generationFound := m.generations[run.GenerationIdentity]
	if run.SnapshotWatermark != "" || run.CatchUpWatermark != "" {
		if !generationFound {
			return fmt.Errorf("create positioned run %s: %w: generation %s must be registered",
				run.ID, ErrNotFound, run.GenerationIdentity)
		}
		if err := validateRunSource(run, generation); err != nil {
			return err
		}
	}
	if generationFound && generation.Retired() {
		return fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, generation.Identity, generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	if _, found := m.runs[run.ID]; found {
		return fmt.Errorf("%w: run %s already exists", ErrConflict, run.ID)
	}
	m.runs[run.ID] = copyRun(run)
	return nil
}

func validateRunSource(run embedrun.Run, generation Generation) error {
	if generation.SourceTable == "" {
		return fmt.Errorf("%w: generation %s does not record a source for positioned run %s",
			ErrConflict, generation.Identity, run.ID)
	}
	canonical := SourceIdentity(generation.SourceSchema, generation.SourceTable)
	if run.Source != canonical && run.Source != generation.SourceTable {
		return fmt.Errorf("%w: run %s records source %s, generation %s uses %s",
			ErrConflict, run.ID, run.Source, generation.Identity,
			QualifiedName(generation.SourceSchema, generation.SourceTable))
	}
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

// RunsForGeneration reads every run that built one generation, newest first.
//
// The map has no order, so the sort is what makes this answer the same twice.
// It sorts by CreatedAt descending and by ID after it, matching the SQL store:
// a fake whose order differs from the real one lets a test pass against a
// caller that depends on an order the product does not give it.
func (m *Memory) RunsForGeneration(_ context.Context, identity string) ([]embedrun.Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runs := make([]embedrun.Run, 0)
	for _, run := range m.runs {
		if run.GenerationIdentity == identity {
			runs = append(runs, copyRun(run))
		}
	}
	slices.SortFunc(runs, func(a, b embedrun.Run) int {
		if order := b.CreatedAt.Compare(a.CreatedAt); order != 0 {
			return order
		}
		return strings.Compare(a.ID, b.ID)
	})
	return runs, nil
}

// SaveRun writes a run's state, refusing a stale fencing token.
func (m *Memory) SaveRun(_ context.Context, run embedrun.Run) error {
	if run.Phase == embedrun.PhaseRetired && run.Status != embedrun.StatusComplete {
		return fmt.Errorf("save run %s: %w: phase %s requires status %s",
			run.ID, embedrun.ErrPhase, embedrun.PhaseRetired, embedrun.StatusComplete)
	}
	if run.Status == embedrun.StatusComplete {
		return fmt.Errorf("save run %s: %w: terminal state is owned by generation retirement",
			run.ID, embedrun.ErrTerminal)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, found := m.runs[run.ID]
	if !found {
		return fmt.Errorf("%w: run %s", ErrNotFound, run.ID)
	}
	if run.Status == embedrun.StatusAbandoned {
		return fmt.Errorf("%w: run %s must be abandoned through AbandonRun",
			ErrConflict, run.ID)
	}
	if run.GenerationIdentity != stored.GenerationIdentity {
		return fmt.Errorf("save run %s: %w: stored generation is %s, write names %s",
			run.ID, embedrun.ErrGeneration, stored.GenerationIdentity, run.GenerationIdentity)
	}
	if run.FencingToken < stored.FencingToken {
		// The worker's lease was taken over while its request was in flight.
		// It is still running, it still believes it owns the run, and this is
		// the only place that knows otherwise.
		return fmt.Errorf("%w: run %s is fenced at token %d and this write carries %d",
			ErrConflict, run.ID, stored.FencingToken, run.FencingToken)
	}
	if run.Source != stored.Source {
		return fmt.Errorf("save run %s: %w: source is immutable", run.ID, ErrConflict)
	}
	if run.SnapshotWatermark != stored.SnapshotWatermark {
		return fmt.Errorf("save run %s: %w: snapshot boundary is immutable",
			run.ID, ErrConflict)
	}
	if err := validateResumeAdvance(stored, run); err != nil {
		return err
	}
	if err := validateProgressAdvance(stored, run); err != nil {
		return err
	}
	if (pointerAuthoritativePhase(stored.Phase) || pointerAuthoritativePhase(run.Phase)) &&
		stored.Phase != run.Phase {
		return fmt.Errorf("save run %s: %w: phase %s may only be entered with its pointer move",
			run.ID, embedrun.ErrPhase, run.Phase)
	}
	if stored.Phase != run.Phase {
		advanced := stored
		if err := advanced.Reach(stored.FencingToken, run.Phase); err != nil ||
			advanced.Phase != run.Phase {
			return fmt.Errorf("save run %s: %w: phase cannot move from %s to %s",
				run.ID, embedrun.ErrPhase, stored.Phase, run.Phase)
		}
	}
	if stored.Terminal() {
		return fmt.Errorf("save run %s: %w: run %s is %s",
			run.ID, embedrun.ErrTerminal, run.ID, stored.Status)
	}
	m.runs[run.ID] = copyRun(run)
	return nil
}

func pointerAuthoritativePhase(phase embedrun.Phase) bool {
	return phase == embedrun.PhaseCutOver || phase == embedrun.PhaseRolledBack
}

func validateResumePosition(run embedrun.Run) error {
	_, _, err := embedcatchup.ResumeFrom(run.CatchUpWatermark, run.SnapshotWatermark)
	if err != nil {
		return fmt.Errorf("run %s resume position: %w", run.ID, err)
	}
	return nil
}

func validateResumeAdvance(stored, offered embedrun.Run) error {
	if err := validateResumePosition(offered); err != nil {
		return err
	}
	before, beforeOK, err := embedcatchup.ResumeFrom(
		stored.CatchUpWatermark, stored.SnapshotWatermark)
	if err != nil {
		return fmt.Errorf("stored run %s resume position: %w", stored.ID, err)
	}
	after, afterOK, err := embedcatchup.ResumeFrom(
		offered.CatchUpWatermark, offered.SnapshotWatermark)
	if err != nil {
		return fmt.Errorf("run %s resume position: %w", offered.ID, err)
	}
	if beforeOK != afterOK || beforeOK && after.Before(before) {
		return fmt.Errorf("save run %s: %w: resume position cannot move backward",
			offered.ID, ErrConflict)
	}
	return nil
}

func validateProgressAdvance(stored, offered embedrun.Run) error {
	before, after := stored.Progress, offered.Progress
	regressed := after.RowsScanned < before.RowsScanned ||
		after.RowsEmbedded < before.RowsEmbedded ||
		after.RowsSkipped < before.RowsSkipped ||
		after.RowsDeleted < before.RowsDeleted ||
		after.BatchesCommitted < before.BatchesCommitted ||
		after.ProviderPromptTokens < before.ProviderPromptTokens ||
		after.ProviderTotalTokens < before.ProviderTotalTokens ||
		after.ProviderUsageBatches < before.ProviderUsageBatches
	if regressed {
		return fmt.Errorf("save run %s: %w: committed progress cannot move backward",
			offered.ID, ErrConflict)
	}
	if after.BatchesCommitted == before.BatchesCommitted &&
		after.RetryCount < before.RetryCount {
		return fmt.Errorf("save run %s: %w: retry count cannot move backward within a batch",
			offered.ID, ErrConflict)
	}
	if !slices.Equal(stored.Cursor, offered.Cursor) &&
		after.BatchesCommitted <= before.BatchesCommitted {
		return fmt.Errorf("save run %s: %w: cursor change requires a new committed batch",
			offered.ID, ErrConflict)
	}
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
func (m *Memory) Pointer(_ context.Context, targetSchema, targetTable string) (Pointer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := pointerKey(targetSchema, targetTable)
	pointer, found := m.pointers[key]
	if !found {
		return Pointer{}, fmt.Errorf("%w: no pointer for %s", ErrNotFound,
			QualifiedName(targetSchema, targetTable))
	}
	return pointer, nil
}

// pointerKey addresses a pointer by both parts of its target.
//
// The schema is part of the key because two same-named tables in two schemas
// are two targets: keyed on the table alone they shared one pointer, so a
// cutover in one schema moved the other schema's readers (stokaro/ptah#2629).
// The separator is a character no unquoted PostgreSQL identifier holds, so two
// distinct targets cannot fold onto one key.
func pointerKey(targetSchema, targetTable string) string {
	return targetSchema + "\x00" + targetTable
}

// MovePointer moves it, refusing when it is not where the caller thinks.
func (m *Memory) MovePointer(_ context.Context, pointer Pointer, expectedActive string) error {
	return m.movePointer(pointer, expectedActive, "", 0, false, "", time.Time{}, time.Time{}, nil)
}

// MovePointerWithMaintenance moves a pointer and opens the previous
// generation's maintenance window under the same lock.
func (m *Memory) MovePointerWithMaintenance(
	_ context.Context, pointer Pointer, expectedActive, requiredRunID string,
	stabilizeFor time.Duration,
) (CutoverMove, error) {
	var move CutoverMove
	err := m.movePointer(
		pointer, expectedActive, requiredRunID, stabilizeFor, true, "",
		time.Time{}, time.Time{}, &move)
	if err != nil {
		return CutoverMove{}, err
	}
	return move, nil
}

// MovePointerWithRollback moves a pointer and records the displaced
// generation's rollback under the same lock.
func (m *Memory) MovePointerWithRollback(
	_ context.Context, pointer Pointer, expectedActive string,
	expectedMaintainedUntil, eligibilityNotAfter time.Time,
) (time.Time, error) {
	var move CutoverMove
	err := m.movePointer(
		pointer, expectedActive, "", 0, false, expectedActive,
		expectedMaintainedUntil, eligibilityNotAfter, &move)
	if err != nil {
		return time.Time{}, err
	}
	return move.CutOverAt, nil
}

type pointerMovePlan struct {
	pointer                  Pointer
	expectedActive           string
	requiredRunID            string
	stabilizeFor             time.Duration
	managePrevious           bool
	rolledBackGeneration     string
	expectedMaintainedUntil  time.Time
	eligibilityNotAfter      time.Time
	recordCommittedTimestamp bool
}

type preparedPointerMove struct {
	pointer        Pointer
	authorizingRun embedrun.Run
	previous       Generation
	rolledBackRuns map[string]embedrun.Run
	move           CutoverMove
}

func (m *Memory) movePointer(
	pointer Pointer, expectedActive, requiredRunID string, stabilizeFor time.Duration,
	managePrevious bool, rolledBackGeneration string,
	expectedMaintainedUntil, eligibilityNotAfter time.Time,
	committed *CutoverMove,
) error {
	plan := pointerMovePlan{
		pointer:                  pointer,
		expectedActive:           expectedActive,
		requiredRunID:            requiredRunID,
		stabilizeFor:             stabilizeFor,
		managePrevious:           managePrevious,
		rolledBackGeneration:     rolledBackGeneration,
		expectedMaintainedUntil:  expectedMaintainedUntil,
		eligibilityNotAfter:      eligibilityNotAfter,
		recordCommittedTimestamp: committed != nil,
	}
	if pointer.Previous != expectedActive {
		return fmt.Errorf("%w: pointer previous generation %q does not match expected active generation %q",
			ErrConflict, pointer.Previous, expectedActive)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	prepared, err := m.preparePointerMove(plan)
	if err != nil {
		return err
	}
	m.commitPointerMove(plan, prepared)
	if committed != nil {
		*committed = prepared.move
	}
	return nil
}

// preparePointerMove validates and derives every row before the caller commits
// any of them. The caller holds m.mu.
func (m *Memory) preparePointerMove(plan pointerMovePlan) (preparedPointerMove, error) {
	if err := m.validateCurrentPointerForMove(plan); err != nil {
		return preparedPointerMove{}, err
	}
	destination, err := m.destinationForPointerMove(plan)
	if err != nil {
		return preparedPointerMove{}, err
	}
	authorizing, err := m.authorizingRunForPointerMove(plan)
	if err != nil {
		return preparedPointerMove{}, err
	}
	previous, err := m.previousGenerationForPointerMove(plan)
	if err != nil {
		return preparedPointerMove{}, err
	}
	rolledBackRuns, err := m.runsForPointerRollback(plan)
	if err != nil {
		return preparedPointerMove{}, err
	}
	pointer, move, err := m.timePointerMove(plan, destination, previous)
	if err != nil {
		return preparedPointerMove{}, err
	}
	return preparedPointerMove{
		pointer:        pointer,
		authorizingRun: authorizing,
		previous:       previous,
		rolledBackRuns: rolledBackRuns,
		move:           move,
	}, nil
}

func (m *Memory) validateCurrentPointerForMove(plan pointerMovePlan) error {
	current, found := m.pointers[pointerKey(plan.pointer.TargetSchema, plan.pointer.TargetTable)]
	switch {
	case !found && plan.expectedActive != "":
		return fmt.Errorf("%w: %s has no pointer and this move expected %s",
			ErrConflict, QualifiedName(plan.pointer.TargetSchema, plan.pointer.TargetTable), plan.expectedActive)
	case found && current.Active != plan.expectedActive:
		return fmt.Errorf("%w: %s reads %s and this move expected %s",
			ErrConflict, QualifiedName(plan.pointer.TargetSchema, plan.pointer.TargetTable),
			current.Active, plan.expectedActive)
	}
	if plan.rolledBackGeneration != "" &&
		(current.Previous == "" || current.Previous != plan.pointer.Active) {
		return fmt.Errorf("%w: rollback destination generation %s is not the pointer's previous generation %s",
			ErrConflict, plan.pointer.Active, current.Previous)
	}
	return nil
}

func (m *Memory) destinationForPointerMove(plan pointerMovePlan) (Generation, error) {
	destination, found := m.generations[plan.pointer.Active]
	if !found {
		return Generation{}, fmt.Errorf("%w: generation %s", ErrNotFound, plan.pointer.Active)
	}
	if destination.Retired() {
		return Generation{}, fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, destination.Identity, destination.RetiredAt.UTC().Format(time.RFC3339))
	}
	if destination.TargetSchema != plan.pointer.TargetSchema ||
		destination.TargetTable != plan.pointer.TargetTable {
		return Generation{}, fmt.Errorf("%w: generation %s targets %s, not pointer target %s",
			ErrConflict, destination.Identity,
			QualifiedName(destination.TargetSchema, destination.TargetTable),
			QualifiedName(plan.pointer.TargetSchema, plan.pointer.TargetTable))
	}
	if err := m.refuseTerminalOnlyGeneration(destination); err != nil {
		return Generation{}, fmt.Errorf("move pointer for %s to generation %s: %w",
			QualifiedName(plan.pointer.TargetSchema, plan.pointer.TargetTable), plan.pointer.Active, err)
	}
	if plan.rolledBackGeneration != "" &&
		!destination.MaintainedUntil.Equal(plan.expectedMaintainedUntil) {
		return Generation{}, fmt.Errorf("%w: rollback destination generation %s maintenance changed "+
			"from %s to %s",
			ErrConflict, destination.Identity,
			plan.expectedMaintainedUntil.UTC().Format(time.RFC3339),
			destination.MaintainedUntil.UTC().Format(time.RFC3339))
	}
	return destination, nil
}

func (m *Memory) authorizingRunForPointerMove(plan pointerMovePlan) (embedrun.Run, error) {
	if plan.requiredRunID == "" {
		return embedrun.Run{}, nil
	}
	authorizing, found := m.runs[plan.requiredRunID]
	if !found {
		return embedrun.Run{}, fmt.Errorf("%w: run %s", ErrNotFound, plan.requiredRunID)
	}
	if authorizing.GenerationIdentity != plan.pointer.Active {
		return embedrun.Run{}, fmt.Errorf("%w: run %s authorizes generation %s, not %s",
			ErrConflict, plan.requiredRunID, authorizing.GenerationIdentity, plan.pointer.Active)
	}
	if authorizing.Terminal() {
		return embedrun.Run{}, fmt.Errorf("%w: run %s is %s",
			embedrun.ErrTerminal, plan.requiredRunID, authorizing.Status)
	}
	authorizing.FencingToken++
	if err := authorizing.Reach(authorizing.FencingToken, embedrun.PhaseCutOver); err != nil {
		return embedrun.Run{}, fmt.Errorf("record cutover on run %s: %w", plan.requiredRunID, err)
	}
	authorizing.LeaseOwner = ""
	authorizing.LeaseExpires = time.Time{}
	return authorizing, nil
}

func (m *Memory) previousGenerationForPointerMove(plan pointerMovePlan) (Generation, error) {
	if plan.pointer.Previous == "" {
		return Generation{}, nil
	}
	previous, found := m.generations[plan.pointer.Previous]
	if !found {
		return Generation{}, fmt.Errorf("%w: generation %s", ErrNotFound, plan.pointer.Previous)
	}
	if previous.Retired() {
		return Generation{}, fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, previous.Identity, previous.RetiredAt.UTC().Format(time.RFC3339))
	}
	if previous.TargetSchema != plan.pointer.TargetSchema ||
		previous.TargetTable != plan.pointer.TargetTable {
		return Generation{}, fmt.Errorf("%w: previous generation %s targets %s, not pointer target %s",
			ErrConflict, previous.Identity,
			QualifiedName(previous.TargetSchema, previous.TargetTable),
			QualifiedName(plan.pointer.TargetSchema, plan.pointer.TargetTable))
	}
	if plan.managePrevious && plan.stabilizeFor > 0 {
		if err := m.refuseTerminalOnlyGeneration(previous); err != nil {
			return Generation{}, fmt.Errorf("maintain previous generation %s while moving pointer for %s: %w",
				plan.pointer.Previous,
				QualifiedName(plan.pointer.TargetSchema, plan.pointer.TargetTable), err)
		}
	}
	return previous, nil
}

func (m *Memory) runsForPointerRollback(plan pointerMovePlan) (map[string]embedrun.Run, error) {
	rolledBackRuns := make(map[string]embedrun.Run)
	if plan.rolledBackGeneration == "" {
		return rolledBackRuns, nil
	}
	if plan.rolledBackGeneration != plan.expectedActive {
		return nil, fmt.Errorf("%w: rollback generation %s is not expected active generation %s",
			ErrConflict, plan.rolledBackGeneration, plan.expectedActive)
	}
	for id, run := range m.runs {
		if run.GenerationIdentity != plan.rolledBackGeneration ||
			!run.Phase.LeadsTo(embedrun.PhaseRolledBack) || run.Terminal() {
			continue
		}
		run.FencingToken++
		if err := run.Reach(run.FencingToken, embedrun.PhaseRolledBack); err != nil {
			return nil, fmt.Errorf("record rollback on run %s: %w", id, err)
		}
		run.LeaseOwner = ""
		run.LeaseExpires = time.Time{}
		rolledBackRuns[id] = run
	}
	return rolledBackRuns, nil
}

func (m *Memory) timePointerMove(
	plan pointerMovePlan, destination, previous Generation,
) (Pointer, CutoverMove, error) {
	if !plan.recordCommittedTimestamp {
		return plan.pointer, CutoverMove{}, nil
	}
	sampledAt := m.now().UTC()
	if plan.rolledBackGeneration != "" && !destination.Maintained(sampledAt) {
		return Pointer{}, CutoverMove{}, fmt.Errorf(
			"%w: rollback destination generation %s maintenance expired at %s before %s",
			ErrConflict, destination.Identity,
			destination.MaintainedUntil.UTC().Format(time.RFC3339),
			sampledAt.Format(time.RFC3339))
	}
	if plan.rolledBackGeneration != "" &&
		!plan.eligibilityNotAfter.IsZero() && sampledAt.After(plan.eligibilityNotAfter) {
		return Pointer{}, CutoverMove{}, fmt.Errorf("%w: rollback eligibility expired at %s before %s",
			ErrConflict, plan.eligibilityNotAfter.UTC().Format(time.RFC3339),
			sampledAt.Format(time.RFC3339))
	}
	pointer := plan.pointer
	pointer.CutOverAt = sampledAt
	move := CutoverMove{CutOverAt: sampledAt}
	if plan.managePrevious && pointer.Previous != "" && plan.stabilizeFor > 0 {
		move.PreviousMaintainedUntil = sampledAt.Add(plan.stabilizeFor)
		if previous.MaintainedUntil.After(move.PreviousMaintainedUntil) {
			move.PreviousMaintainedUntil = previous.MaintainedUntil
		}
	}
	return pointer, move, nil
}

func (m *Memory) commitPointerMove(plan pointerMovePlan, prepared preparedPointerMove) {
	pointer := prepared.pointer
	m.pointers[pointerKey(pointer.TargetSchema, pointer.TargetTable)] = pointer
	if plan.requiredRunID != "" {
		m.runs[plan.requiredRunID] = copyRun(prepared.authorizingRun)
	}
	if plan.managePrevious && pointer.Previous != "" {
		// A zero stabilization duration is an explicit request to leave no
		// rollback path. It clears an older window instead of silently keeping
		// one while the caller reports that none exists. A positive duration is
		// a renewal and therefore preserves any later existing deadline.
		previous := prepared.previous
		previous.MaintainedUntil = prepared.move.PreviousMaintainedUntil
		m.generations[pointer.Previous] = previous
	}
	for id, run := range prepared.rolledBackRuns {
		m.runs[id] = copyRun(run)
	}
}

// refuseTerminalOnlyGeneration refuses a generation with run history but no
// run that can still receive source changes. The caller holds m.mu.
func (m *Memory) refuseTerminalOnlyGeneration(generation Generation) error {
	hasRuns := false
	for _, run := range m.runs {
		if run.GenerationIdentity != generation.Identity {
			continue
		}
		hasRuns = true
		if usableGenerationFeeder(run, generation) {
			return nil
		}
	}
	if hasRuns {
		return fmt.Errorf("%w: generation %s has run history, but no usable live feeder",
			ErrNoLiveRun, generation.Identity)
	}
	return nil
}

func usableGenerationFeeder(run embedrun.Run, generation Generation) bool {
	if run.Terminal() {
		return false
	}
	if generation.ConsistencyMode != string(embedcatchup.ModeOutbox) {
		return true
	}
	if generation.SourceTable == "" {
		return false
	}
	canonical := SourceIdentity(generation.SourceSchema, generation.SourceTable)
	if run.Source != canonical && run.Source != generation.SourceTable {
		return false
	}
	_, positioned, err := embedcatchup.ResumeFrom(
		run.CatchUpWatermark, run.SnapshotWatermark)
	return err == nil && positioned
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

// ClaimRun takes a run for a worker, writing the lease and nothing else.
//
// It mirrors the SQL store's claim rather than reusing [embedrun.Run.Claim] on
// a copy the caller holds: the point of the contract is that a claim reads and
// writes under one lock and returns what the store then holds, so a fake that
// claimed a caller's stale copy would agree with the defect
// (stokaro/ptah#2636).
func (m *Memory) ClaimRun(
	_ context.Context, id, worker string, leaseExpires time.Time,
) (embedrun.Run, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, found := m.runs[id]
	if !found {
		return embedrun.Run{}, 0, fmt.Errorf("%w: run %s", ErrNotFound, id)
	}
	if stored.Terminal() {
		return embedrun.Run{}, 0, fmt.Errorf(
			"%w: run %s is %s", embedrun.ErrTerminal, id, stored.Status)
	}
	stored.FencingToken++
	stored.LeaseOwner = worker
	stored.LeaseExpires = leaseExpires.UTC()
	stored.UpdatedAt = time.Now().UTC()
	m.runs[id] = copyRun(stored)
	return copyRun(stored), stored.FencingToken, nil
}

// AbandonRun permanently ends a run without destroying its generation.
func (m *Memory) AbandonRun(
	_ context.Context, id, reason string,
) (embedrun.Run, error) {
	if reason == "" {
		return embedrun.Run{}, fmt.Errorf(
			"abandon run %s: %w: an abandonment without a reason cannot be acted on",
			id, embedrun.ErrCheckpoint)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	when := m.now().UTC()
	stored, found := m.runs[id]
	if !found {
		return embedrun.Run{}, fmt.Errorf("%w: run %s", ErrNotFound, id)
	}
	if stored.Status == embedrun.StatusAbandoned {
		return copyRun(stored), nil
	}
	if stored.Status == embedrun.StatusComplete {
		return embedrun.Run{}, fmt.Errorf(
			"abandon run %s: %w: run %s is complete", id, embedrun.ErrTerminal, id)
	}
	generation, generationFound := m.generations[stored.GenerationIdentity]
	if generationFound && generation.Retired() {
		return embedrun.Run{}, fmt.Errorf("%w: generation %s was retired at %s",
			ErrRetired, generation.Identity, generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	if !generationFound {
		// Missing registry rows are deliberately supported for imported or
		// damaged histories. They carry no maintenance window, but a historical
		// pointer may still name the identity and must keep one feeder alive.
		generation.Identity = stored.GenerationIdentity
	}
	feederRequirement, err := abandonmentFeederRequirement(generation, stored)
	if err != nil {
		return embedrun.Run{}, fmt.Errorf("abandon run %s: %w", id, err)
	}
	if protected, why := m.generationProtected(generation, when); protected &&
		!m.hasOtherFeeder(generation, stored, id, feederRequirement) {
		return embedrun.Run{}, fmt.Errorf(
			"abandon run %s: %w: generation %s is %s and no other usable live feeder remains",
			id, ErrNoLiveRun, generation.Identity, why)
	}

	stored.FencingToken++
	if err := stored.Abandon(stored.FencingToken, reason); err != nil {
		return embedrun.Run{}, fmt.Errorf("abandon run %s: %w", id, err)
	}
	stored.UpdatedAt = when.UTC()
	m.runs[id] = copyRun(stored)
	return copyRun(stored), nil
}

// generationProtected reports why a generation must keep a live feeder. The
// caller holds m.mu.
func (m *Memory) generationProtected(generation Generation, now time.Time) (bool, string) {
	for _, pointer := range m.pointers {
		if pointer.Active == generation.Identity {
			return true, "active for " + QualifiedName(pointer.TargetSchema, pointer.TargetTable)
		}
	}
	if generation.Maintained(now) {
		return true, "maintained until " + generation.MaintainedUntil.UTC().Format(time.RFC3339)
	}
	return false, ""
}

type feederPositionRequirement uint8

const (
	feederMayBeUnpositioned feederPositionRequirement = iota
	feederMustBePositioned
)

// hasOtherFeeder reports whether another run can keep feeding a generation.
// The caller holds m.mu.
func (m *Memory) hasOtherFeeder(
	generation Generation, current embedrun.Run, excluding string,
	positionRequirement feederPositionRequirement,
) bool {
	for _, run := range m.runs {
		if run.ID == excluding || run.GenerationIdentity != generation.Identity || run.Terminal() ||
			!sameGenerationSource(run.Source, current.Source, generation) {
			continue
		}
		if positionRequirement == feederMayBeUnpositioned {
			return true
		}
		_, positioned, err := embedcatchup.ResumeFrom(
			run.CatchUpWatermark, run.SnapshotWatermark)
		if err == nil && positioned {
			return true
		}
	}
	return false
}

func abandonmentFeederRequirement(
	generation Generation, current embedrun.Run,
) (feederPositionRequirement, error) {
	if generation.ConsistencyMode != "" {
		if generation.ConsistencyMode == string(embedcatchup.ModeOutbox) {
			return feederMustBePositioned, nil
		}
		return feederMayBeUnpositioned, nil
	}
	_, positioned, err := embedcatchup.ResumeFrom(
		current.CatchUpWatermark, current.SnapshotWatermark)
	if err != nil {
		return feederMayBeUnpositioned,
			fmt.Errorf("current run %s has an invalid resume position: %w", current.ID, err)
	}
	if positioned {
		return feederMustBePositioned, nil
	}
	return feederMayBeUnpositioned, nil
}

func sameGenerationSource(candidate, current string, generation Generation) bool {
	if generation.SourceTable == "" {
		return candidate == current
	}
	canonical := SourceIdentity(generation.SourceSchema, generation.SourceTable)
	candidateMatches := candidate == canonical || candidate == generation.SourceTable
	currentMatches := current == canonical || current == generation.SourceTable
	return candidateMatches && currentMatches
}

// hasNonterminalRun reports whether any run can still receive source changes.
// The caller holds m.mu.
func (m *Memory) hasNonterminalRun(identity string) bool {
	for _, run := range m.runs {
		if run.GenerationIdentity == identity && !run.Terminal() {
			return true
		}
	}
	return false
}

// hasActivePointer reports whether queries currently read an identity. The
// caller holds m.mu.
func (m *Memory) hasActivePointer(identity string) bool {
	for _, pointer := range m.pointers {
		if pointer.Active == identity {
			return true
		}
	}
	return false
}
