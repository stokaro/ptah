package embedydb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// Store is [embedstore.Store] over YDB row tables.
//
// Every operation that changes state is a decision made by an
// [embedstore.Memory] over the rows it reads, inside one serializable
// transaction (see [Backend.inTransaction]). The decision code is the
// in-memory store's, so a YDB run and an in-memory one are refused for the
// same reasons in the same words, and nothing here restates a rule.
//
// An operation that touches one run reads that run alone, which keeps a
// batch checkpoint from conflicting with work on any other run. Every other
// operation reads the whole registry, every run and every pointer, because
// the rules it applies ask about the generation's other runs and about every
// pointer that might name it.
type Store struct {
	backend *Backend
}

// clock is what a decision samples time from: the system clock, to the
// microsecond YDB stores.
func clock() time.Time {
	return storedInstant(time.Now())
}

// decide runs decision on a store built from the rows scope reads and writes
// back every row the decision changed, in one transaction. It returns the
// state after the decision, as written.
func (s *Store) decide(
	ctx context.Context, scope readScope, what string,
	decision func(*embedstore.Memory) error,
) (embedstore.Snapshot, error) {
	var after embedstore.Snapshot
	err := s.backend.inTransaction(ctx, what, func(tx *sql.Tx) error {
		before, err := s.load(ctx, tx, scope)
		if err != nil {
			return err
		}
		memory := embedstore.NewMemoryFrom(before, clock)
		if err := decision(memory); err != nil {
			return err
		}
		after = stored(memory.Snapshot())
		return s.write(ctx, tx, before, after)
	})
	return after, err
}

// readScope says which rows a decision reads.
type readScope struct {
	// run, when set, limits the read to that one run: the decisions about a
	// single run's own row read nothing else.
	run string
}

// everything is the scope of a decision that asks about other rows.
var everything = readScope{}

// oneRun is the scope of a decision about a single run's own row.
func oneRun(id string) readScope {
	return readScope{run: id}
}

// load reads the rows scope names.
func (s *Store) load(ctx context.Context, tx *sql.Tx, scope readScope) (embedstore.Snapshot, error) {
	var snapshot embedstore.Snapshot
	if scope.run != "" {
		runs, err := readRuns(ctx, tx, "WHERE "+quote("id")+" = $p1", scope.run)
		if err != nil {
			return embedstore.Snapshot{}, err
		}
		snapshot.Runs = runs
		return snapshot, nil
	}
	generations, err := readGenerations(ctx, tx, "")
	if err != nil {
		return embedstore.Snapshot{}, err
	}
	runs, err := readRuns(ctx, tx, "")
	if err != nil {
		return embedstore.Snapshot{}, err
	}
	pointers, err := readPointers(ctx, tx, "")
	if err != nil {
		return embedstore.Snapshot{}, err
	}
	return embedstore.Snapshot{Generations: generations, Runs: runs, Pointers: pointers}, nil
}

// write upserts every row of after that differs from before. A decision never
// deletes a row, so a row missing from after is one the scope did not read.
func (s *Store) write(ctx context.Context, tx *sql.Tx, before, after embedstore.Snapshot) error {
	beforeGenerations := make(map[string]embedstore.Generation, len(before.Generations))
	for _, generation := range before.Generations {
		beforeGenerations[generation.Identity] = generation
	}
	for _, generation := range after.Generations {
		if previous, found := beforeGenerations[generation.Identity]; found && reflect.DeepEqual(previous, generation) {
			continue
		}
		if err := upsert(ctx, tx, generationTable, generationValues(generation)); err != nil {
			return err
		}
	}
	beforeRuns := make(map[string]embedrun.Run, len(before.Runs))
	for _, run := range before.Runs {
		beforeRuns[run.ID] = run
	}
	for _, run := range after.Runs {
		if previous, found := beforeRuns[run.ID]; found && reflect.DeepEqual(previous, run) {
			continue
		}
		values, err := runValues(run)
		if err != nil {
			return err
		}
		if err := upsert(ctx, tx, runTable, values); err != nil {
			return err
		}
	}
	beforePointers := make(map[string]embedstore.Pointer, len(before.Pointers))
	for _, pointer := range before.Pointers {
		beforePointers[pointer.TargetSchema+"\x00"+pointer.TargetTable] = pointer
	}
	for _, pointer := range after.Pointers {
		if previous, found := beforePointers[pointer.TargetSchema+"\x00"+pointer.TargetTable]; found &&
			reflect.DeepEqual(previous, pointer) {
			continue
		}
		if err := upsert(ctx, tx, pointerTable, pointerValues(pointer)); err != nil {
			return err
		}
	}
	return nil
}

// upsert writes one whole row.
func upsert(ctx context.Context, tx *sql.Tx, into table, values map[string]any) error {
	statement, arguments, err := into.upsert(values)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("write %s: %w", into.name, err)
	}
	return nil
}

// stored is a snapshot with every time as YDB stores it, so what a decision
// returns is what a later read returns.
func stored(snapshot embedstore.Snapshot) embedstore.Snapshot {
	for index := range snapshot.Generations {
		generation := &snapshot.Generations[index]
		generation.CreatedAt = storedInstant(generation.CreatedAt)
		generation.RetiredAt = storedInstant(generation.RetiredAt)
		generation.VerifiedAt = storedInstant(generation.VerifiedAt)
		generation.MaintainedUntil = storedInstant(generation.MaintainedUntil)
	}
	for index := range snapshot.Runs {
		run := &snapshot.Runs[index]
		run.LeaseExpires = storedInstant(run.LeaseExpires)
		run.CreatedAt = storedInstant(run.CreatedAt)
		run.UpdatedAt = storedInstant(run.UpdatedAt)
	}
	for index := range snapshot.Pointers {
		snapshot.Pointers[index].CutOverAt = storedInstant(snapshot.Pointers[index].CutOverAt)
	}
	return snapshot
}

// queryer is what a read runs on: the pool, or the transaction a decision
// holds.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// readRows reads a table's rows matching where, decoded by decode.
func readRows[T any](
	ctx context.Context, db queryer, from table, where string,
	decode func(scanned) (T, error), args ...any,
) ([]T, error) {
	// #nosec G201 -- the table and its columns are this package's own names,
	// and where is one of its literal predicates; values are parameters.
	query := fmt.Sprintf("SELECT %s FROM %s %s", from.selectList(), quote(from.name), where)
	rows, err := db.QueryContext(ctx, strings.TrimSpace(query), args...)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", from.name, err)
	}
	defer func() { _ = rows.Close() }()
	var decoded []T
	for rows.Next() {
		row, err := from.scan(rows)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", from.name, err)
		}
		value, err := decode(row)
		if err != nil {
			return nil, err
		}
		decoded = append(decoded, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", from.name, err)
	}
	return decoded, nil
}

func readGenerations(ctx context.Context, db queryer, where string, args ...any) ([]embedstore.Generation, error) {
	return readRows(ctx, db, generationTable, where, func(row scanned) (embedstore.Generation, error) {
		return scannedGeneration(row), nil
	}, args...)
}

func readRuns(ctx context.Context, db queryer, where string, args ...any) ([]embedrun.Run, error) {
	return readRows(ctx, db, runTable, where, scannedRun, args...)
}

func readPointers(ctx context.Context, db queryer, where string, args ...any) ([]embedstore.Pointer, error) {
	return readRows(ctx, db, pointerTable, where, func(row scanned) (embedstore.Pointer, error) {
		return scannedPointer(row), nil
	}, args...)
}

// RegisterGeneration records a generation, or returns the existing row
// unchanged.
func (s *Store) RegisterGeneration(
	ctx context.Context, generation embedstore.Generation,
) (embedstore.Generation, error) {
	var registered embedstore.Generation
	_, err := s.decide(ctx, everything, "register generation "+generation.Identity,
		func(memory *embedstore.Memory) error {
			var err error
			registered, err = memory.RegisterGeneration(ctx, generation)
			return err
		})
	if err != nil {
		return embedstore.Generation{}, err
	}
	return storedGeneration(registered), nil
}

// storedGeneration is a generation with its times as YDB stores them.
func storedGeneration(generation embedstore.Generation) embedstore.Generation {
	return stored(embedstore.Snapshot{Generations: []embedstore.Generation{generation}}).Generations[0]
}

// Generation reads one back.
func (s *Store) Generation(ctx context.Context, identity string) (embedstore.Generation, error) {
	generations, err := readGenerations(ctx, s.backend.db, "WHERE "+quote("identity")+" = $p1", identity)
	if err != nil {
		return embedstore.Generation{}, err
	}
	if len(generations) == 0 {
		return embedstore.Generation{}, fmt.Errorf("%w: generation %s", embedstore.ErrNotFound, identity)
	}
	return generations[0], nil
}

// RecordVerification records that a verification passed over a generation.
func (s *Store) RecordVerification(ctx context.Context, identity string, at time.Time) error {
	_, err := s.decide(ctx, everything, "record the verification of generation "+identity,
		func(memory *embedstore.Memory) error {
			return memory.RecordVerification(ctx, identity, at)
		})
	return err
}

// Maintain records how long something will keep a generation current.
func (s *Store) Maintain(ctx context.Context, identity string, until time.Time) error {
	_, err := s.decide(ctx, everything, "maintain generation "+identity,
		func(memory *embedstore.Memory) error {
			return memory.Maintain(ctx, identity, until)
		})
	return err
}

// CreateRun records a new run.
func (s *Store) CreateRun(ctx context.Context, run embedrun.Run) error {
	_, err := s.decide(ctx, everything, "create run "+run.ID,
		func(memory *embedstore.Memory) error {
			return memory.CreateRun(ctx, run)
		})
	return err
}

// Run reads one back.
func (s *Store) Run(ctx context.Context, id string) (embedrun.Run, error) {
	runs, err := readRuns(ctx, s.backend.db, "WHERE "+quote("id")+" = $p1", id)
	if err != nil {
		return embedrun.Run{}, err
	}
	if len(runs) == 0 {
		return embedrun.Run{}, fmt.Errorf("%w: run %s", embedstore.ErrNotFound, id)
	}
	return runs[0], nil
}

// RunsForGeneration reads every run that built one generation, newest first
// and by identifier after that, which is the order the other stores give.
func (s *Store) RunsForGeneration(ctx context.Context, identity string) ([]embedrun.Run, error) {
	runs, err := readRuns(ctx, s.backend.db,
		"WHERE "+quote("generation_identity")+" = $p1 ORDER BY "+quote("created_at")+" DESC, "+quote("id"),
		identity)
	if err != nil {
		return nil, err
	}
	if runs == nil {
		return make([]embedrun.Run, 0), nil
	}
	return runs, nil
}

// SaveRun writes a non-terminal run's state, refusing a stale fencing token.
func (s *Store) SaveRun(ctx context.Context, run embedrun.Run) error {
	_, err := s.decide(ctx, oneRun(run.ID), "save run "+run.ID,
		func(memory *embedstore.Memory) error {
			return memory.SaveRun(ctx, run)
		})
	return err
}

// ClaimRun takes a run for a worker, writing the lease and nothing else, and
// returns the run as the store now holds it.
func (s *Store) ClaimRun(
	ctx context.Context, id, worker string, leaseExpires time.Time,
) (embedrun.Run, int64, error) {
	after, err := s.decide(ctx, oneRun(id), "claim run "+id,
		func(memory *embedstore.Memory) error {
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
	after, err := s.decide(ctx, everything, "abandon run "+id,
		func(memory *embedstore.Memory) error {
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
	_, err := s.decide(ctx, oneRun(runID), "record phase "+string(to)+" on run "+runID,
		func(memory *embedstore.Memory) error {
			return memory.ReachPhase(ctx, runID, to)
		})
	return err
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

// AppendEvent records what happened.
//
// The sequence is the run's last one plus one, read in the same transaction
// as the insert, so two workers appending at once conflict and one of them
// runs again rather than both taking one position.
func (s *Store) AppendEvent(ctx context.Context, event embedrun.Event) error {
	return s.backend.inTransaction(ctx, "append an event to run "+event.RunID, func(tx *sql.Tx) error {
		runs, err := readRuns(ctx, tx, "WHERE "+quote("id")+" = $p1", event.RunID)
		if err != nil {
			return err
		}
		if len(runs) == 0 {
			return fmt.Errorf("%w: run %s", embedstore.ErrNotFound, event.RunID)
		}
		var last sql.NullInt64
		// #nosec G201 -- the table and its columns are this package's own names.
		query := fmt.Sprintf("SELECT MAX(%s) FROM %s WHERE %s = $p1",
			quote("sequence"), quote(eventTable.name), quote("run_id"))
		if err := tx.QueryRowContext(ctx, query, event.RunID).Scan(&last); err != nil {
			return fmt.Errorf("read the event sequence of run %s: %w", event.RunID, err)
		}
		return upsert(ctx, tx, eventTable, eventValues(event, last.Int64+1))
	})
}

// Events reads a run's history in order.
func (s *Store) Events(ctx context.Context, runID string) ([]embedrun.Event, error) {
	if _, err := s.Run(ctx, runID); err != nil {
		return nil, err
	}
	events, err := readRows(ctx, s.backend.db, eventTable,
		"WHERE "+quote("run_id")+" = $p1 ORDER BY "+quote("sequence"),
		func(row scanned) (embedrun.Event, error) { return scannedEvent(row), nil }, runID)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// Pointer reads which generation a target's queries currently read.
func (s *Store) Pointer(ctx context.Context, targetSchema, targetTable string) (embedstore.Pointer, error) {
	pointers, err := readPointers(ctx, s.backend.db,
		"WHERE "+quote("target_schema")+" = $p1 AND "+quote("target_table")+" = $p2",
		targetSchema, targetTable)
	if err != nil {
		return embedstore.Pointer{}, err
	}
	if len(pointers) == 0 {
		return embedstore.Pointer{}, fmt.Errorf("%w: no pointer for %s",
			embedstore.ErrNotFound, embedstore.QualifiedName(targetSchema, targetTable))
	}
	return pointers[0], nil
}

// MovePointer moves it, refusing when it is not where the caller thinks.
func (s *Store) MovePointer(ctx context.Context, pointer embedstore.Pointer, expectedActive string) error {
	_, err := s.decide(ctx, everything, "move the pointer of "+
		embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable),
		func(memory *embedstore.Memory) error {
			return memory.MovePointer(ctx, pointer, expectedActive)
		})
	return err
}

// MovePointerWithMaintenance moves the pointer, opens the previous
// generation's maintenance window and records the cutover on its run, in one
// transaction.
func (s *Store) MovePointerWithMaintenance(
	ctx context.Context, pointer embedstore.Pointer, expectedActive, requiredRunID string,
	stabilizeFor time.Duration,
) (embedstore.CutoverMove, error) {
	var move embedstore.CutoverMove
	_, err := s.decide(ctx, everything, "cut over "+
		embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable),
		func(memory *embedstore.Memory) error {
			var err error
			move, err = memory.MovePointerWithMaintenance(ctx, pointer, expectedActive, requiredRunID, stabilizeFor)
			return err
		})
	if err != nil {
		return embedstore.CutoverMove{}, err
	}
	return embedstore.CutoverMove{
		CutOverAt:               storedInstant(move.CutOverAt),
		PreviousMaintainedUntil: storedInstant(move.PreviousMaintainedUntil),
	}, nil
}

// MovePointerWithRollback moves the pointer back and records the rollback on
// the displaced generation's runs, in one transaction.
func (s *Store) MovePointerWithRollback(
	ctx context.Context, pointer embedstore.Pointer, expectedActive string,
	expectedMaintainedUntil, eligibilityNotAfter time.Time,
) (time.Time, error) {
	var at time.Time
	_, err := s.decide(ctx, everything, "roll back "+
		embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable),
		func(memory *embedstore.Memory) error {
			var err error
			at, err = memory.MovePointerWithRollback(ctx, pointer, expectedActive,
				expectedMaintainedUntil, eligibilityNotAfter)
			return err
		})
	if err != nil {
		return time.Time{}, err
	}
	return storedInstant(at), nil
}
