package embedpg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// Store is embedstore.Store over PostgreSQL.
type Store struct {
	db *sql.DB
}

// NewStore returns a store over an open database.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// EnsureSchema creates the store's tables if they are not there.
//
// It is idempotent because a worker starting is the normal time to call it, and
// several of them start at once.
func (s *Store) EnsureSchema(ctx context.Context) error {
	statements, err := SchemaSQL()
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create store schema: %w", err)
		}
	}
	return nil
}

// RegisterGeneration records a generation, or returns the existing row.
//
// ON CONFLICT DO NOTHING followed by a read, rather than an upsert: a
// generation is a content address, so two registrations of one identity are the
// same registration, and the row that is already there is the one that counts.
func (s *Store) RegisterGeneration(
	ctx context.Context, generation embedstore.Generation,
) (embedstore.Generation, error) {
	const query = `INSERT INTO ` + embedstore.GenerationTable + ` (
		identity, spec_digest, name, reproducibility, reproducibility_reason,
		resolved_model, dimension, target_table, target_column, created_at, retired_at,
		verified_at, maintained_until)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (identity) DO NOTHING`
	if _, err := s.db.ExecContext(ctx, query,
		generation.Identity, generation.SpecDigest, generation.Name,
		generation.Reproducibility, nullable(generation.ReproducibilityReason),
		nullable(generation.ResolvedModel), generation.Dimension,
		generation.TargetTable, generation.TargetColumn,
		generation.CreatedAt.UTC(), nullableTime(generation.RetiredAt),
		nullableTime(generation.VerifiedAt), nullableTime(generation.MaintainedUntil),
	); err != nil {
		return embedstore.Generation{}, fmt.Errorf("register generation: %w", err)
	}
	return s.Generation(ctx, generation.Identity)
}

// Generation reads one back.
func (s *Store) Generation(ctx context.Context, identity string) (embedstore.Generation, error) {
	const query = `SELECT identity, spec_digest, COALESCE(name,''), reproducibility,
		COALESCE(reproducibility_reason,''), COALESCE(resolved_model,''), dimension,
		target_table, target_column, created_at, retired_at, verified_at, maintained_until
		FROM ` + embedstore.GenerationTable + ` WHERE identity = $1`
	var generation embedstore.Generation
	var retired, verified, maintained sql.NullTime
	err := s.db.QueryRowContext(ctx, query, identity).Scan(
		&generation.Identity, &generation.SpecDigest, &generation.Name,
		&generation.Reproducibility, &generation.ReproducibilityReason, &generation.ResolvedModel,
		&generation.Dimension, &generation.TargetTable, &generation.TargetColumn,
		&generation.CreatedAt, &retired, &verified, &maintained)
	if errors.Is(err, sql.ErrNoRows) {
		return embedstore.Generation{}, fmt.Errorf("%w: generation %s", embedstore.ErrNotFound, identity)
	}
	if err != nil {
		return embedstore.Generation{}, fmt.Errorf("read generation %s: %w", identity, err)
	}
	if retired.Valid {
		generation.RetiredAt = retired.Time.UTC()
	}
	if verified.Valid {
		generation.VerifiedAt = verified.Time.UTC()
	}
	if maintained.Valid {
		generation.MaintainedUntil = maintained.Time.UTC()
	}
	generation.CreatedAt = generation.CreatedAt.UTC()
	return generation, nil
}

// RetireGeneration marks one destroyed, which is terminal.
//
// The WHERE clause is what makes it terminal rather than a check-then-write: a
// second retirement would otherwise move the timestamp, and when a corpus was
// destroyed is the whole value of the row that remains.
func (s *Store) RetireGeneration(ctx context.Context, identity string, at time.Time) error {
	const query = `UPDATE ` + embedstore.GenerationTable + `
		SET retired_at = $2 WHERE identity = $1 AND retired_at IS NULL`
	result, err := s.db.ExecContext(ctx, query, identity, at.UTC())
	if err != nil {
		return fmt.Errorf("retire generation %s: %w", identity, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("retire generation %s: %w", identity, err)
	}
	if changed == 1 {
		return nil
	}
	return s.explainRetirementRefusal(ctx, identity)
}

// explainRetirementRefusal says which of the two reasons applied.
//
// A caller told only that nothing changed cannot tell a generation that is
// already gone from one that was never there, and those have different fixes.
func (s *Store) explainRetirementRefusal(ctx context.Context, identity string) error {
	generation, err := s.Generation(ctx, identity)
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: generation %s was retired at %s",
		embedstore.ErrRetired, identity, generation.RetiredAt.Format(time.RFC3339))
}

// RecordVerification records that a verification passed over a generation.
func (s *Store) RecordVerification(ctx context.Context, identity string, at time.Time) error {
	return s.updateGeneration(ctx, identity, "verified_at", nullableTime(at))
}

// Maintain records how long something will keep a generation current.
func (s *Store) Maintain(ctx context.Context, identity string, until time.Time) error {
	return s.updateGeneration(ctx, identity, "maintained_until", nullableTime(until))
}

// updateGeneration writes one column of a generation that is there and not
// retired.
//
// The retired clause is in the WHERE rather than in a check before it, for the
// reason every other rule in this file is: between a check and a write is where
// the thing being checked changes.
func (s *Store) updateGeneration(ctx context.Context, identity, column string, value any) error {
	// #nosec G201 -- the column is one of two literals chosen by the two
	// methods above; nothing a caller supplies reaches it.
	query := fmt.Sprintf(`UPDATE %s SET %s = $2 WHERE identity = $1 AND retired_at IS NULL`,
		embedstore.GenerationTable, column)
	result, err := s.db.ExecContext(ctx, query, identity, value)
	if err != nil {
		return fmt.Errorf("record %s for generation %s: %w", column, identity, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record %s for generation %s: %w", column, identity, err)
	}
	if changed == 1 {
		return nil
	}
	return s.explainRetirementRefusal(ctx, identity)
}

// CreateRun records a new run.
func (s *Store) CreateRun(ctx context.Context, run embedrun.Run) error {
	cursor, err := encodeCursor(run.Cursor)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, insertRunSQL, runArguments(run, cursor)...)
	if err != nil {
		return fmt.Errorf("create run %s: %w", run.ID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create run %s: %w", run.ID, err)
	}
	if changed == 0 {
		return fmt.Errorf("%w: run %s already exists", embedstore.ErrConflict, run.ID)
	}
	return nil
}

// Run reads one back.
func (s *Store) Run(ctx context.Context, id string) (embedrun.Run, error) {
	return scanRun(s.db.QueryRowContext(ctx, selectRunSQL, id), id)
}

// SaveRun writes a run's state, refusing a stale fencing token.
//
// The refusal is a WHERE clause rather than a read followed by a write, because
// between those two a takeover is exactly what happens.
func (s *Store) SaveRun(ctx context.Context, run embedrun.Run) error {
	cursor, err := encodeCursor(run.Cursor)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, updateRunSQL, runArguments(run, cursor)...)
	if err != nil {
		return fmt.Errorf("save run %s: %w", run.ID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("save run %s: %w", run.ID, err)
	}
	if changed == 1 {
		return nil
	}
	return s.explainSaveRefusal(ctx, run)
}

// explainSaveRefusal says whether the run is missing or fenced.
func (s *Store) explainSaveRefusal(ctx context.Context, run embedrun.Run) error {
	stored, err := s.Run(ctx, run.ID)
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: run %s is fenced at token %d and this write carries %d",
		embedstore.ErrConflict, run.ID, stored.FencingToken, run.FencingToken)
}

// AppendEvent records what happened.
//
// The sequence is chosen inside the INSERT so that two workers appending at
// once cannot both read the same maximum. What they can do is collide on the
// primary key, and the loser gets a duplicate-key error rather than a silently
// reordered history.
func (s *Store) AppendEvent(ctx context.Context, event embedrun.Event) error {
	const query = `INSERT INTO ` + embedstore.EventTable + ` (
		sequence, run_id, kind, at, actor, fencing_token, from_phase, to_phase, detail,
		rows_scanned, rows_embedded, rows_skipped, rows_deleted, batches_committed)
		SELECT COALESCE(MAX(sequence), 0) + 1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		FROM ` + embedstore.EventTable + ` WHERE run_id = $1`
	if _, err := s.db.ExecContext(ctx, query,
		event.RunID, string(event.Kind), event.At.UTC(), nullable(event.Actor), event.FencingToken,
		nullable(string(event.FromPhase)), nullable(string(event.ToPhase)), nullable(event.Detail),
		event.Counts.RowsScanned, event.Counts.RowsEmbedded, event.Counts.RowsSkipped,
		event.Counts.RowsDeleted, event.Counts.BatchesCommitted,
	); err != nil {
		return fmt.Errorf("append event for run %s: %w", event.RunID, err)
	}
	return nil
}

// Events reads a run's history in order.
func (s *Store) Events(ctx context.Context, runID string) ([]embedrun.Event, error) {
	if _, err := s.Run(ctx, runID); err != nil {
		return nil, err
	}
	const query = `SELECT run_id, kind, at, COALESCE(actor,''), fencing_token,
		COALESCE(from_phase,''), COALESCE(to_phase,''), COALESCE(detail,''),
		rows_scanned, rows_embedded, rows_skipped, rows_deleted, batches_committed
		FROM ` + embedstore.EventTable + ` WHERE run_id = $1 ORDER BY sequence`
	rows, err := s.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("read events for run %s: %w", runID, err)
	}
	defer rows.Close()

	var events []embedrun.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read events for run %s: %w", runID, err)
	}
	return events, nil
}

// Pointer reads which generation a target's queries currently read.
func (s *Store) Pointer(ctx context.Context, targetTable string) (embedstore.Pointer, error) {
	const query = `SELECT target_table, active_generation, COALESCE(previous_generation,''),
		cut_over_at, COALESCE(cut_over_by,''), COALESCE(plan_digest,'')
		FROM ` + embedstore.PointerTable + ` WHERE target_table = $1`
	var pointer embedstore.Pointer
	err := s.db.QueryRowContext(ctx, query, targetTable).Scan(
		&pointer.TargetTable, &pointer.Active, &pointer.Previous,
		&pointer.CutOverAt, &pointer.CutOverBy, &pointer.PlanDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return embedstore.Pointer{}, fmt.Errorf("%w: no pointer for %s", embedstore.ErrNotFound, targetTable)
	}
	if err != nil {
		return embedstore.Pointer{}, fmt.Errorf("read pointer for %s: %w", targetTable, err)
	}
	pointer.CutOverAt = pointer.CutOverAt.UTC()
	return pointer, nil
}

// MovePointer moves it, refusing when it is not where the caller thinks.
//
// One statement, so the comparison and the write are the same instant. Two --
// read the pointer, then write it -- is the shape that reintroduces the race
// the cutover decision already refused.
func (s *Store) MovePointer(ctx context.Context, pointer embedstore.Pointer, expectedActive string) error {
	// The SELECT's condition has to let the row through in BOTH cases this
	// statement covers, which is why it is not simply `$7 = ''`: that guard
	// alone made every move onto an existing pointer produce no row to conflict
	// with, so ON CONFLICT never fired and a correct cutover was refused as a
	// conflict. Measured against PostgreSQL 18, not reasoned about -- the
	// in-memory store agreed with the wrong SQL.
	const query = `INSERT INTO ` + embedstore.PointerTable + ` (
		target_table, active_generation, previous_generation, cut_over_at, cut_over_by, plan_digest)
		SELECT $1, $2, $3, $4, $5, $6
		WHERE $7 = '' OR EXISTS (
			SELECT 1 FROM ` + embedstore.PointerTable + `
			WHERE target_table = $1 AND active_generation = $7)
		ON CONFLICT (target_table) DO UPDATE SET
			active_generation = EXCLUDED.active_generation,
			previous_generation = EXCLUDED.previous_generation,
			cut_over_at = EXCLUDED.cut_over_at,
			cut_over_by = EXCLUDED.cut_over_by,
			plan_digest = EXCLUDED.plan_digest
		WHERE ` + embedstore.PointerTable + `.active_generation = $7`
	result, err := s.db.ExecContext(ctx, query,
		pointer.TargetTable, pointer.Active, nullable(pointer.Previous), pointer.CutOverAt.UTC(),
		nullable(pointer.CutOverBy), nullable(pointer.PlanDigest), expectedActive)
	if err != nil {
		return fmt.Errorf("move pointer for %s: %w", pointer.TargetTable, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("move pointer for %s: %w", pointer.TargetTable, err)
	}
	if changed == 1 {
		return nil
	}
	return s.explainPointerRefusal(ctx, pointer.TargetTable, expectedActive)
}

// explainPointerRefusal says what the pointer actually reads.
func (s *Store) explainPointerRefusal(ctx context.Context, targetTable, expectedActive string) error {
	current, err := s.Pointer(ctx, targetTable)
	if errors.Is(err, embedstore.ErrNotFound) {
		return fmt.Errorf("%w: %s has no pointer and this move expected %s",
			embedstore.ErrConflict, targetTable, expectedActive)
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: %s reads %s and this move expected %s",
		embedstore.ErrConflict, targetTable, current.Active, expectedActive)
}

// nullable turns an empty string into a SQL NULL.
//
// Empty and absent are the same thing for every optional column here, and
// storing one of them as ” would make a later NOT NULL migration lie about
// what is present.
func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// nullableTime turns a zero time into a SQL NULL.
func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

// encodeCursor renders a keyset cursor for one column.
//
// JSON rather than a join, because a key component may contain any character a
// separator could use, and a cursor that decoded to a different key silently
// resumes somewhere else.
func encodeCursor(cursor []string) (any, error) {
	if len(cursor) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return nil, fmt.Errorf("encode cursor: %w", err)
	}
	return string(encoded), nil
}

// decodeCursor reads one back.
func decodeCursor(encoded sql.NullString) ([]string, error) {
	if !encoded.Valid || encoded.String == "" {
		return nil, nil
	}
	var cursor []string
	if err := json.Unmarshal([]byte(encoded.String), &cursor); err != nil {
		return nil, fmt.Errorf("decode cursor %q: %w", encoded.String, err)
	}
	return cursor, nil
}
