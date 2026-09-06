package embedpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// RetirementDestruction is what an approved retirement destroys.
//
// One value rather than three booleans in a parameter list, because they are
// one decision: which storage a generation's vectors are in decides whether
// removing them is a DROP COLUMN or a DROP TABLE, and a caller that could pass
// both has a state to get wrong. The approval binds to these facts through
// [embedcutover.RetirementPlan], so what an operator signed and what the store
// executes are the same three answers.
type RetirementDestruction struct {
	// IndexExists is the index state the retirement was approved against, and
	// is re-measured under the lifecycle lock before any DDL runs.
	IndexExists bool
	// DropColumns removes the vector column and its metadata columns from a
	// relation the application keeps, which is what retiring a
	// LayoutSourceColumns generation destroys.
	DropColumns bool
	// DropTable removes the relation itself, which is what retiring a
	// LayoutOwnTable generation destroys. [RetireTable] refuses unless Ptah
	// created that relation for this generation.
	DropTable bool
}

// validate refuses a destruction that names both removals.
//
// They are alternatives rather than options: a generation's vectors live in
// one place, and a caller asking for both has decided the layout twice. The
// second answer would run against a relation the first one had already
// dropped, so the failure would surface as a missing relation rather than as
// the contradiction it is.
func (d RetirementDestruction) validate() error {
	if d.DropColumns && d.DropTable {
		return fmt.Errorf(
			"%w: a retirement drops the generation's columns or its table, not both",
			embedstore.ErrConflict)
	}
	return nil
}

// RetireGenerationObjects destroys one generation and records that outcome in
// the registry and every run in the same PostgreSQL transaction.
//
// expectedPointer is the pointer snapshot against which the retirement was
// approved. Its active and previous identities are re-read while the
// generation lifecycle lock is held. A concurrent cutover therefore either
// commits first and makes this operation refuse before DDL, or follows this
// operation and sees a retired destination. The active generation is never a
// valid retirement target.
//
// Every run is fenced and completed because its generation no longer exists.
// A phase that directly leads to PhaseRetired records that phase; an earlier
// run keeps its truthful high-water phase while becoming terminal.
func (s *Store) RetireGenerationObjects(
	ctx context.Context,
	identity string,
	expectedPointer embedstore.Pointer,
	expectedRows int,
	destruction RetirementDestruction,
) (OutboxRelease, error) {
	if err := destruction.validate(); err != nil {
		return OutboxRelease{}, err
	}
	initial, err := s.Generation(ctx, identity)
	if err != nil {
		return OutboxRelease{}, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return OutboxRelease{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := lifecycleLock(ctx, tx, "source",
		SourceIdentity(initial.SourceSchema, initial.SourceTable)); err != nil {
		return OutboxRelease{}, err
	}
	if err := lockGenerations(ctx, tx, identity); err != nil {
		return OutboxRelease{}, err
	}
	generation, retiredAt, err := validateRetirementEligibility(
		ctx, tx, initial, expectedPointer)
	if err != nil {
		return OutboxRelease{}, err
	}
	if err := terminalizeGenerationRuns(ctx, tx, identity, retiredAt); err != nil {
		return OutboxRelease{}, err
	}
	if err := lockGenerationRelations(ctx, tx, generation); err != nil {
		return OutboxRelease{}, err
	}
	facts := retirementFacts{rows: expectedRows, indexExists: destruction.IndexExists}
	if err := validateRetirementArtifacts(ctx, tx, generation, facts); err != nil {
		return OutboxRelease{}, err
	}
	release, err := releaseOutboxForRetirement(ctx, tx, s.db, generation)
	if err != nil {
		return OutboxRelease{}, err
	}
	if err := RetireIndex(ctx, tx, generation); err != nil {
		return OutboxRelease{}, err
	}
	if destruction.DropTable {
		if err := RetireTable(ctx, tx, generation); err != nil {
			return OutboxRelease{}, err
		}
	}
	if destruction.DropColumns {
		if err := RetireColumns(ctx, tx, generation); err != nil {
			return OutboxRelease{}, err
		}
	}
	if err := markGenerationRetired(ctx, tx, identity, retiredAt); err != nil {
		return OutboxRelease{}, err
	}
	if err := tx.Commit(); err != nil {
		return OutboxRelease{}, fmt.Errorf("retire generation %s: %w", identity, err)
	}
	release.RetiredAt = retiredAt
	return release, nil
}

func validateRetirementEligibility(
	ctx context.Context,
	tx *sql.Tx,
	initial embedstore.Generation,
	expectedPointer embedstore.Pointer,
) (embedstore.Generation, time.Time, error) {
	identity := initial.Identity
	generation, err := readGeneration(ctx, tx, identity)
	if err != nil {
		return embedstore.Generation{}, time.Time{}, err
	}
	if generation.Retired() {
		return embedstore.Generation{}, time.Time{}, fmt.Errorf(
			"%w: generation %s was retired at %s",
			embedstore.ErrRetired, identity,
			generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	if generation.SourceSchema != initial.SourceSchema ||
		generation.SourceTable != initial.SourceTable ||
		generation.ConsistencyMode != initial.ConsistencyMode {
		return embedstore.Generation{}, time.Time{}, fmt.Errorf(
			"%w: generation %s source changed while retirement acquired its lifecycle locks",
			embedstore.ErrConflict, identity)
	}
	if err := revalidateRetirementPointer(ctx, tx, generation, expectedPointer); err != nil {
		return embedstore.Generation{}, time.Time{}, err
	}
	var retiredAt time.Time
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&retiredAt); err != nil {
		return embedstore.Generation{}, time.Time{}, fmt.Errorf(
			"sample retirement time for generation %s: %w", identity, err)
	}
	retiredAt = retiredAt.UTC()
	if generation.Maintained(retiredAt) {
		return embedstore.Generation{}, time.Time{}, fmt.Errorf(
			"%w: generation %s is maintained until %s",
			embedstore.ErrConflict, identity,
			generation.MaintainedUntil.UTC().Format(time.RFC3339))
	}
	return generation, retiredAt, nil
}

type retirementFacts struct {
	rows        int
	indexExists bool
}

func validateRetirementArtifacts(
	ctx context.Context,
	tx *sql.Tx,
	generation embedstore.Generation,
	expected retirementFacts,
) error {
	rows, err := CountGenerationRows(ctx, tx, generation)
	if err != nil {
		return err
	}
	if rows != expected.rows {
		return fmt.Errorf(
			"%w: generation %s row count changed from approved %d to %d",
			embedstore.ErrConflict, generation.Identity, expected.rows, rows)
	}
	indexExists, err := GenerationIndexExists(ctx, tx, generation)
	if err != nil {
		return err
	}
	if indexExists != expected.indexExists {
		return fmt.Errorf(
			"%w: generation %s index state changed from approved %t to %t",
			embedstore.ErrConflict, generation.Identity, expected.indexExists, indexExists)
	}
	return nil
}

func markGenerationRetired(
	ctx context.Context, tx *sql.Tx, identity string, retiredAt time.Time,
) error {
	const retire = `UPDATE ` + embedstore.GenerationTable + `
		SET retired_at = $2 WHERE identity = $1 AND retired_at IS NULL`
	result, err := tx.ExecContext(ctx, retire, identity, retiredAt)
	if err != nil {
		return fmt.Errorf("retire generation %s: %w", identity, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("retire generation %s: %w", identity, err)
	}
	if changed != 1 {
		return fmt.Errorf("retire generation %s: %w", identity, embedstore.ErrConflict)
	}
	return nil
}

// lockGenerationRelations makes the approved row count, index state, and
// shared source capture stable through the destructive DDL. Relations are
// locked in physical-OID order because crossed specifications can use each
// other's source as their target. Resolving first is essential: an authored
// empty schema and an explicit `public` schema can name the same relation, and
// sorting the authored strings would give different orders for the same pair.
func lockGenerationRelations(
	ctx context.Context, tx *sql.Tx, generation embedstore.Generation,
) error {
	type authoredRelation struct {
		schema string
		table  string
	}
	authored := []authoredRelation{{generation.TargetSchema, generation.TargetTable}}
	if generation.ConsistencyMode == string(embedcatchup.ModeOutbox) &&
		generation.SourceTable != "" &&
		(generation.SourceSchema != generation.TargetSchema ||
			generation.SourceTable != generation.TargetTable) {
		authored = append(authored,
			authoredRelation{generation.SourceSchema, generation.SourceTable})
	}
	type relation struct {
		oid    uint32
		schema string
		table  string
	}
	const resolve = `SELECT c.oid, n.nspname, c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = to_regclass($1)`
	byOID := make(map[uint32]relation, len(authored))
	for _, name := range authored {
		var resolved relation
		authoredName := qualify(name.schema, name.table)
		if err := tx.QueryRowContext(ctx, resolve, authoredName).Scan(
			&resolved.oid, &resolved.schema, &resolved.table); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("lock relation %s of generation %s for retirement: %w",
					authoredName, generation.Identity, embedstore.ErrNotFound)
			}
			return fmt.Errorf("resolve relation %s of generation %s for retirement: %w",
				authoredName, generation.Identity, err)
		}
		byOID[resolved.oid] = resolved
	}
	relations := make([]relation, 0, len(byOID))
	for _, relation := range byOID {
		relations = append(relations, relation)
	}
	slices.SortFunc(relations, func(a, b relation) int {
		if a.oid < b.oid {
			return -1
		}
		if a.oid > b.oid {
			return 1
		}
		return 0
	})
	for _, relation := range relations {
		// #nosec G201 -- both identifiers come from the generation registry and
		// go through qualify/quoteIdentifier.
		statement := fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE",
			qualify(relation.schema, relation.table))
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("lock relation %s of generation %s for retirement: %w",
				embedstore.QualifiedName(relation.schema, relation.table),
				generation.Identity, err)
		}
	}
	return nil
}

// revalidateRetirementPointer locks the target pointer row and compares the
// two identities that decide whether a generation is active or a rollback
// dependency. A zero snapshot means the approval observed no pointer row.
func revalidateRetirementPointer(
	ctx context.Context,
	tx *sql.Tx,
	generation embedstore.Generation,
	expected embedstore.Pointer,
) error {
	target := embedstore.QualifiedName(generation.TargetSchema, generation.TargetTable)
	if (expected.TargetSchema != "" || expected.TargetTable != "") &&
		(expected.TargetSchema != generation.TargetSchema ||
			expected.TargetTable != generation.TargetTable) {
		return fmt.Errorf("%w: retirement approval names pointer %s, generation %s targets %s",
			embedstore.ErrConflict,
			embedstore.QualifiedName(expected.TargetSchema, expected.TargetTable),
			generation.Identity, target)
	}

	// A generation identity is global, while the pointer table is keyed by
	// target. Reject every active reference rather than only the row the
	// generation says it belongs to. This keeps a malformed historical pointer
	// from authorizing destruction of objects it still serves.
	const activeQuery = `SELECT target_schema, target_table
		FROM ` + embedstore.PointerTable + `
		WHERE active_generation = $1
		ORDER BY target_schema, target_table
		LIMIT 1 FOR UPDATE`
	var activeSchema, activeTable string
	err := tx.QueryRowContext(ctx, activeQuery, generation.Identity).Scan(
		&activeSchema, &activeTable)
	if err == nil {
		return fmt.Errorf("%w: generation %s is active for %s",
			embedstore.ErrConflict, generation.Identity,
			embedstore.QualifiedName(activeSchema, activeTable))
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read active pointers for retirement of generation %s: %w",
			generation.Identity, err)
	}

	const query = `SELECT active_generation, COALESCE(previous_generation, '')
		FROM ` + embedstore.PointerTable + `
		WHERE target_schema = $1 AND target_table = $2
		FOR UPDATE`
	var active, previous string
	err = tx.QueryRowContext(
		ctx, query, generation.TargetSchema, generation.TargetTable,
	).Scan(&active, &previous)
	if errors.Is(err, sql.ErrNoRows) {
		if expected.Active == "" && expected.Previous == "" {
			return nil
		}
		return fmt.Errorf("%w: %s has no pointer; approval expected active %s and previous %s",
			embedstore.ErrConflict, target, expected.Active, expected.Previous)
	}
	if err != nil {
		return fmt.Errorf("read pointer for retirement of generation %s: %w",
			generation.Identity, err)
	}
	if active != expected.Active || previous != expected.Previous {
		return fmt.Errorf("%w: %s pointer changed from active %s, previous %s to active %s, previous %s",
			embedstore.ErrConflict, target,
			expected.Active, expected.Previous, active, previous)
	}
	return nil
}

func releaseOutboxForRetirement(
	ctx context.Context,
	tx *sql.Tx,
	db *sql.DB,
	generation embedstore.Generation,
) (OutboxRelease, error) {
	source := embedstore.QualifiedName(generation.SourceSchema, generation.SourceTable)
	if generation.ConsistencyMode != string(embedcatchup.ModeOutbox) {
		return OutboxRelease{Source: source}, nil
	}
	const countRegistered = `SELECT count(*) FROM ` + embedstore.GenerationTable + `
		WHERE source_schema = $1 AND source_table = $2 AND consistency_mode = $3
			AND identity <> $4 AND retired_at IS NULL`
	var remaining int
	if err := tx.QueryRowContext(ctx, countRegistered,
		generation.SourceSchema, generation.SourceTable,
		string(embedcatchup.ModeOutbox), generation.Identity).Scan(&remaining); err != nil {
		return OutboxRelease{}, fmt.Errorf("count live outbox readers of %s: %w", source, err)
	}
	// A missing generation row is a supported conservative history: pruning
	// deliberately keeps its events. It must also keep the shared outbox here,
	// or retirement can remove the capture mechanism while that same live run
	// still owes events. Count identities rather than runs so retries over one
	// orphan generation have the same unit as the registry count above.
	const countOrphans = `SELECT count(DISTINCT r.generation_identity)
		FROM ` + embedstore.RunTable + ` r
		WHERE (r.source = $1 OR r.source = $2)
		  AND r.generation_identity <> $3
		  AND r.status NOT IN ($4, $5)
		  AND (NULLIF(r.catch_up_watermark, '') IS NOT NULL
		       OR NULLIF(r.snapshot_watermark, '') IS NOT NULL)
		  AND NOT EXISTS (
		        SELECT 1 FROM ` + embedstore.GenerationTable + ` g
		        WHERE g.identity = r.generation_identity)`
	var orphanReaders int
	if err := tx.QueryRowContext(ctx, countOrphans,
		SourceIdentity(generation.SourceSchema, generation.SourceTable),
		generation.SourceTable, generation.Identity,
		string(embedrun.StatusComplete), string(embedrun.StatusAbandoned)).Scan(&orphanReaders); err != nil {
		return OutboxRelease{}, fmt.Errorf("count orphan outbox readers of %s: %w", source, err)
	}
	remaining += orphanReaders
	release := OutboxRelease{Watched: true, Source: source, Remaining: remaining}
	if remaining > 0 {
		return release, nil
	}
	recorded, err := RecordedSpec(generation,
		"watched by an outbox this retirement would have to remove")
	if err != nil {
		return OutboxRelease{}, err
	}
	physical, err := WithResolvedRelations(ctx, db, recorded.Spec)
	if err != nil {
		return OutboxRelease{}, err
	}
	outbox, err := NewOutbox(db, physical)
	if err != nil {
		return OutboxRelease{}, err
	}
	statements := append([]string(nil), outbox.dropTriggers()...)
	statements = append(statements,
		fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", quoteIdentifier(outbox.FunctionName())),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(outbox.TableName())))
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return OutboxRelease{}, fmt.Errorf("remove the outbox on %s: %w", source, err)
		}
	}
	release.Removed = true
	return release, nil
}

func terminalizeGenerationRuns(
	ctx context.Context, tx *sql.Tx, identity string, at time.Time,
) error {
	rows, err := tx.QueryContext(ctx, selectRunsForGenerationForUpdateSQL, identity)
	if err != nil {
		return fmt.Errorf("read runs for retirement of generation %s: %w", identity, err)
	}
	runs := make([]embedrun.Run, 0)
	for rows.Next() {
		run, scanErr := scanRun(rows, identity)
		if scanErr != nil {
			_ = rows.Close()
			return scanErr
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read runs for retirement of generation %s: %w", identity, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("read runs for retirement of generation %s: %w", identity, err)
	}

	for _, run := range runs {
		run.FencingToken++
		if run.Phase.LeadsTo(embedrun.PhaseRetired) {
			run.Phase = embedrun.PhaseRetired
		}
		run.Status = embedrun.StatusComplete
		run.LeaseOwner = ""
		run.LeaseExpires = time.Time{}
		run.RollbackEligible = false
		run.UpdatedAt = at.UTC()
		cursor, err := encodeCursor(run.Cursor)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, retireGenerationRunSQL, runArguments(run, cursor)...)
		if err != nil {
			return fmt.Errorf("complete run %s while retiring generation %s: %w",
				run.ID, identity, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("complete run %s while retiring generation %s: %w",
				run.ID, identity, err)
		}
		if changed != 1 {
			return fmt.Errorf("complete run %s while retiring generation %s: %w",
				run.ID, identity, embedstore.ErrConflict)
		}
	}
	return nil
}
