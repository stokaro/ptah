package embedpg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// Store is embedstore.Store over PostgreSQL.
type Store struct {
	db *sql.DB
}

// transactionStarter is the shared transaction surface of a database pool and
// a pinned connection. Lifecycle operations use the latter when PostgreSQL
// session advisory locks must bridge several transactions and DDL statements.
type transactionStarter interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type runTransactionSource interface {
	transactionStarter
	queryRower
}

// NewStore returns a store over an open database.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// lifecycleLock takes a transaction-scoped PostgreSQL advisory lock for one
// lifecycle identity. The prefix gives runs and generations disjoint keys;
// hash collisions only serialize unrelated work and cannot weaken a rule.
//
// Every operation that can create or terminalize a run takes the generation
// lock. Ordinary claims and checkpoints stay on their one-statement hot path:
// they do not change membership in the live-feeder set, and AbandonRun's
// targeted token increment fences them through the run row itself. An
// operation over several generations sorts them before locking, which keeps
// two concurrent pointer moves from deadlocking each other.
func lifecycleLock(ctx context.Context, tx *sql.Tx, kind, identity string) error {
	const statement = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`
	if _, err := tx.ExecContext(ctx, statement, lifecycleLockName(kind, identity)); err != nil {
		return fmt.Errorf("lock inference %s %s: %w", kind, identity, err)
	}
	return nil
}

// lockGenerations takes generation locks in stable order, ignoring empty and
// duplicate identities.
func lockGenerations(ctx context.Context, tx *sql.Tx, identities ...string) error {
	slices.Sort(identities)
	previous := ""
	for _, identity := range identities {
		if identity == "" || identity == previous {
			continue
		}
		if err := lifecycleLock(ctx, tx, "generation", identity); err != nil {
			return err
		}
		previous = identity
	}
	return nil
}

// begin starts a lifecycle transaction and installs a safe rollback for every
// return path. A successful caller still commits explicitly.
func (s *Store) begin(ctx context.Context) (*sql.Tx, error) {
	return beginStoreTransaction(ctx, s.db)
}

func beginStoreTransaction(ctx context.Context, source transactionStarter) (*sql.Tx, error) {
	tx, err := source.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin inference store transaction: %w", err)
	}
	return tx, nil
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
	return registerGeneration(ctx, s.db, generation)
}

func registerGeneration(
	ctx context.Context,
	source transactionStarter,
	generation embedstore.Generation,
) (embedstore.Generation, error) {
	tx, err := beginStoreTransaction(ctx, source)
	if err != nil {
		return embedstore.Generation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if generation.SourceTable != "" {
		if err := lifecycleLock(
			ctx, tx, "source",
			embedstore.SourceIdentity(generation.SourceSchema, generation.SourceTable)); err != nil {
			return embedstore.Generation{}, err
		}
	}
	if err := lockGenerations(ctx, tx, generation.Identity); err != nil {
		return embedstore.Generation{}, err
	}
	const query = `INSERT INTO ` + embedstore.GenerationTable + ` (
		identity, spec_digest, spec_document, name, reproducibility, reproducibility_reason,
		resolved_model, dimension, target_schema, target_table, target_column,
		source_schema, source_table, consistency_mode,
		created_at, retired_at, verified_at, maintained_until)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		ON CONFLICT (identity) DO NOTHING`
	result, err := tx.ExecContext(ctx, query,
		generation.Identity, generation.SpecDigest, generation.SpecDocument, generation.Name,
		generation.Reproducibility, nullable(generation.ReproducibilityReason),
		nullable(generation.ResolvedModel), generation.Dimension,
		generation.TargetSchema, generation.TargetTable, generation.TargetColumn,
		generation.SourceSchema, generation.SourceTable, generation.ConsistencyMode,
		generation.CreatedAt.UTC(), nullableTime(generation.RetiredAt),
		nullableTime(generation.VerifiedAt), nullableTime(generation.MaintainedUntil),
	)
	if err != nil {
		return embedstore.Generation{}, fmt.Errorf("register generation: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return embedstore.Generation{}, fmt.Errorf("register generation: %w", err)
	}
	registered, err := readGeneration(ctx, tx, generation.Identity)
	if err != nil {
		return embedstore.Generation{}, err
	}
	if inserted == 1 {
		if err := validateInsertedGenerationState(ctx, tx, generation); err != nil {
			return embedstore.Generation{}, err
		}
	}
	if inserted == 0 && registered.Retired() {
		return embedstore.Generation{}, fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, registered.Identity,
			registered.RetiredAt.UTC().Format(time.RFC3339))
	}
	if err := tx.Commit(); err != nil {
		return embedstore.Generation{}, fmt.Errorf("register generation: %w", err)
	}
	return registered, nil
}

func validateInsertedGenerationState(
	ctx context.Context, tx *sql.Tx, generation embedstore.Generation,
) error {
	if !generation.Retired() && generation.MaintainedUntil.IsZero() {
		return nil
	}
	if generation.Retired() {
		if err := validateInsertedRetirement(ctx, tx, generation); err != nil {
			return err
		}
	}
	counts, err := generationRunCounts(ctx, tx, generation)
	if err != nil {
		return err
	}
	if generation.Retired() && counts.nonterminal > 0 {
		return fmt.Errorf(
			"%w: cannot register generation %s as retired while a nonterminal run still reads it",
			embedstore.ErrConflict, generation.Identity)
	}
	if !generation.Retired() && counts.total > 0 && counts.live == 0 {
		return fmt.Errorf(
			"register maintained generation %s: %w: generation %s has run history, "+
				"but no usable live feeder",
			generation.Identity, embedstore.ErrNoLiveRun, generation.Identity)
	}
	return nil
}

func validateInsertedRetirement(
	ctx context.Context, tx *sql.Tx, generation embedstore.Generation,
) error {
	if generation.MaintainedUntil.After(generation.RetiredAt) {
		return fmt.Errorf(
			"%w: generation %s cannot be registered as both retired and maintained",
			embedstore.ErrConflict, generation.Identity)
	}
	const active = `SELECT EXISTS (SELECT 1 FROM ` + embedstore.PointerTable + `
		WHERE active_generation = $1)`
	var isActive bool
	if err := tx.QueryRowContext(ctx, active, generation.Identity).Scan(&isActive); err != nil {
		return fmt.Errorf(
			"read active pointer for generation %s: %w", generation.Identity, err)
	}
	if isActive {
		return fmt.Errorf(
			"%w: cannot register active generation %s as retired",
			embedstore.ErrConflict, generation.Identity)
	}
	return nil
}

// Generation reads one back.
func (s *Store) Generation(ctx context.Context, identity string) (embedstore.Generation, error) {
	return readGeneration(ctx, s.db, identity)
}

// queryRower is the shared surface of a database and a transaction needed by
// the row readers below.
type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// readGeneration reads a generation through either the database or the
// transaction holding the lifecycle lock for it.
func readGeneration(
	ctx context.Context, source queryRower, identity string,
) (embedstore.Generation, error) {
	const statement = `SELECT identity, spec_digest, spec_document, COALESCE(name,''), reproducibility,
		COALESCE(reproducibility_reason,''), COALESCE(resolved_model,''), dimension,
		target_schema, target_table, target_column, source_schema, source_table,
		consistency_mode, created_at, retired_at,
		verified_at, maintained_until
		FROM ` + embedstore.GenerationTable + ` WHERE identity = $1`
	var generation embedstore.Generation
	var retired, verified, maintained sql.NullTime
	err := source.QueryRowContext(ctx, statement, identity).Scan(
		&generation.Identity, &generation.SpecDigest, &generation.SpecDocument, &generation.Name,
		&generation.Reproducibility, &generation.ReproducibilityReason, &generation.ResolvedModel,
		&generation.Dimension, &generation.TargetSchema, &generation.TargetTable,
		&generation.TargetColumn, &generation.SourceSchema, &generation.SourceTable,
		&generation.ConsistencyMode, &generation.CreatedAt, &retired, &verified, &maintained)
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
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockGenerations(ctx, tx, identity); err != nil {
		return err
	}
	generation, err := readGeneration(ctx, tx, identity)
	if err != nil {
		return err
	}
	if generation.Retired() {
		return fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, identity, generation.RetiredAt.Format(time.RFC3339))
	}
	if !until.IsZero() {
		counts, err := generationRunCounts(ctx, tx, generation)
		if err != nil {
			return fmt.Errorf("maintain generation %s: %w", identity, err)
		}
		if counts.total > 0 && counts.live == 0 {
			return fmt.Errorf(
				"maintain generation %s: %w: generation %s has run history, but no usable live feeder",
				identity, embedstore.ErrNoLiveRun, identity)
		}
	}

	if until.IsZero() {
		const query = `UPDATE ` + embedstore.GenerationTable + `
			SET maintained_until = NULL WHERE identity = $1 AND retired_at IS NULL`
		result, err := tx.ExecContext(ctx, query, identity)
		if err != nil {
			return fmt.Errorf("record maintained_until for generation %s: %w", identity, err)
		}
		return s.finishGenerationUpdate(ctx, tx, result, identity, "maintained_until")
	}
	// GREATEST, so maintenance never moves the deadline earlier. Written as a
	// plain assignment it made `--maintain-for 1h` after a
	// `--stabilize-for 24h` take twenty-three hours of rollback eligibility
	// away, from a flag documented as extending the window
	// (stokaro/ptah#2647). The comparison is in the UPDATE rather than in a
	// read before it, for the reason the retired clause is: between a read and
	// a write is where the thing being read changes.
	//
	// No COALESCE around the stored value, and its absence is measured rather
	// than assumed: PostgreSQL's GREATEST ignores NULL operands and answers
	// NULL only when every one of them is NULL, so a generation nothing has
	// ever kept current takes the new deadline outright. A COALESCE here was
	// written first and no fixture could tell it from this, which is the sign
	// it was doing nothing.
	const query = `UPDATE ` + embedstore.GenerationTable + `
		SET maintained_until = GREATEST(maintained_until, $2)
		WHERE identity = $1 AND retired_at IS NULL`
	result, err := tx.ExecContext(ctx, query, identity, until.UTC())
	if err != nil {
		return fmt.Errorf("record maintained_until for generation %s: %w", identity, err)
	}
	return s.finishGenerationUpdate(ctx, tx, result, identity, "maintained_until")
}

// finishGenerationUpdate explains a missing row without leaving the lifecycle
// lock, or commits the one-row change.
func (s *Store) finishGenerationUpdate(
	ctx context.Context, tx *sql.Tx, result sql.Result, identity, column string,
) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record %s for generation %s: %w", column, identity, err)
	}
	if changed == 0 {
		generation, readErr := readGeneration(ctx, tx, identity)
		if readErr != nil {
			return readErr
		}
		return fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, identity, generation.RetiredAt.Format(time.RFC3339))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("record %s for generation %s: %w", column, identity, err)
	}
	return nil
}

// generationRunCounts returns all runs, the non-terminal subset, and the
// usable live feeder subset. The caller holds the generation lifecycle lock,
// so no Store operation can move a run into or out of those sets before the
// protected write commits.
type generationRunCensus struct {
	total       int
	nonterminal int
	live        int
}

func generationRunCounts(
	ctx context.Context, tx *sql.Tx, generation embedstore.Generation,
) (generationRunCensus, error) {
	const query = `SELECT source, COALESCE(catch_up_watermark, ''),
			COALESCE(snapshot_watermark, ''), status
		FROM ` + embedstore.RunTable + ` WHERE generation_identity = $1`
	rows, err := tx.QueryContext(ctx, query, generation.Identity)
	if err != nil {
		return generationRunCensus{}, fmt.Errorf("count runs for generation %s: %w", generation.Identity, err)
	}
	defer rows.Close()
	var counts generationRunCensus
	for rows.Next() {
		var source, catchUp, snapshot string
		var status embedrun.Status
		if err := rows.Scan(&source, &catchUp, &snapshot, &status); err != nil {
			return generationRunCensus{}, fmt.Errorf("count runs for generation %s: %w", generation.Identity, err)
		}
		counts.total++
		if status == embedrun.StatusComplete || status == embedrun.StatusAbandoned {
			continue
		}
		counts.nonterminal++
		if generation.ConsistencyMode != string(embedcatchup.ModeOutbox) {
			counts.live++
			continue
		}
		canonical := embedstore.SourceIdentity(generation.SourceSchema, generation.SourceTable)
		if generation.SourceTable == "" || source != canonical && source != generation.SourceTable {
			continue
		}
		_, positioned, parseErr := embedcatchup.ResumeFrom(catchUp, snapshot)
		if parseErr == nil && positioned {
			counts.live++
		}
	}
	if err := rows.Err(); err != nil {
		return generationRunCensus{}, fmt.Errorf("count runs for generation %s: %w", generation.Identity, err)
	}
	return counts, nil
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
	return s.oneGenerationChanged(ctx, result, identity, column)
}

// oneGenerationChanged turns "no row" into the reason there was none.
//
// Shared by the column writers and by [Store.Maintain], which writes its own
// statement: a generation that is absent and one that is retired are different
// answers, and both look like zero rows affected from here.
func (s *Store) oneGenerationChanged(
	ctx context.Context, result sql.Result, identity, column string,
) error {
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
	return createRunRecord(ctx, s.db, run)
}

func createRunRecord(
	ctx context.Context,
	source runTransactionSource,
	run embedrun.Run,
) error {
	state, err := prepareRunCreation(ctx, source, run)
	if err != nil {
		return err
	}
	tx, err := beginStoreTransaction(ctx, source)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if state.positioned {
		if err := lifecycleLock(ctx, tx, "source", state.sourceIdentity); err != nil {
			return err
		}
	}
	if err := lockGenerations(ctx, tx, run.GenerationIdentity); err != nil {
		return err
	}
	if err := validateRunCreationGeneration(ctx, tx, run, state); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, insertRunSQL, runArguments(run, state.cursor)...)
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
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("create run %s: %w", run.ID, err)
	}
	return nil
}

type runCreationState struct {
	cursor         any
	positioned     bool
	sourceIdentity string
}

func prepareRunCreation(
	ctx context.Context, source runTransactionSource, run embedrun.Run,
) (runCreationState, error) {
	if run.Phase == embedrun.PhaseRetired && run.Status != embedrun.StatusComplete {
		return runCreationState{}, fmt.Errorf("create run %s: %w: phase %s requires status %s",
			run.ID, embedrun.ErrPhase, embedrun.PhaseRetired, embedrun.StatusComplete)
	}
	if run.Terminal() {
		return runCreationState{}, fmt.Errorf("create run %s: %w: run is already %s",
			run.ID, embedrun.ErrTerminal, run.Status)
	}
	if err := validateRunResume(run); err != nil {
		return runCreationState{}, err
	}
	cursor, err := encodeCursor(run.Cursor)
	if err != nil {
		return runCreationState{}, err
	}
	state := runCreationState{
		cursor:     cursor,
		positioned: run.SnapshotWatermark != "" || run.CatchUpWatermark != "",
	}
	if state.positioned {
		generation, readErr := readGeneration(ctx, source, run.GenerationIdentity)
		if readErr != nil {
			if errors.Is(readErr, embedstore.ErrNotFound) {
				return runCreationState{}, fmt.Errorf("create positioned run %s: %w: generation %s must be registered",
					run.ID, embedstore.ErrNotFound, run.GenerationIdentity)
			}
			return runCreationState{}, readErr
		}
		state.sourceIdentity, err = validateRunSource(run, generation)
		if err != nil {
			return runCreationState{}, err
		}
	}
	return state, nil
}

func validateRunCreationGeneration(
	ctx context.Context, tx *sql.Tx, run embedrun.Run, state runCreationState,
) error {
	generation, err := readGeneration(ctx, tx, run.GenerationIdentity)
	if state.positioned {
		if err != nil {
			return err
		}
		lockedSourceIdentity, validateErr := validateRunSource(run, generation)
		if validateErr != nil {
			return validateErr
		}
		if lockedSourceIdentity != state.sourceIdentity {
			return fmt.Errorf("%w: generation %s source changed while its lifecycle lock was acquired",
				embedstore.ErrConflict, generation.Identity)
		}
	}
	if err == nil && generation.Retired() {
		return fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, generation.Identity,
			generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	if err != nil && !errors.Is(err, embedstore.ErrNotFound) {
		return err
	}
	return nil
}

func validateRunSource(
	run embedrun.Run, generation embedstore.Generation,
) (string, error) {
	if generation.SourceTable == "" {
		return "", fmt.Errorf(
			"%w: generation %s does not record a source for positioned run %s",
			embedstore.ErrConflict, generation.Identity, run.ID)
	}
	canonical := embedstore.SourceIdentity(generation.SourceSchema, generation.SourceTable)
	if run.Source != canonical && run.Source != generation.SourceTable {
		return "", fmt.Errorf("%w: run %s records source %s, generation %s uses %s",
			embedstore.ErrConflict, run.ID, run.Source, generation.Identity,
			embedstore.QualifiedName(generation.SourceSchema, generation.SourceTable))
	}
	return canonical, nil
}

// Run reads one back.
func (s *Store) Run(ctx context.Context, id string) (embedrun.Run, error) {
	return scanRun(s.db.QueryRowContext(ctx, selectRunSQL, id), id)
}

// RunsForGeneration reads every run that built one generation, newest first.
//
// See [embedstore.Store.RunsForGeneration]. An empty slice is the answer for a
// generation nothing built, and it is not an error: `retire` is reachable for a
// generation registered by a run this store never saw.
func (s *Store) RunsForGeneration(ctx context.Context, identity string) ([]embedrun.Run, error) {
	rows, err := s.db.QueryContext(ctx, selectRunsForGenerationSQL, identity)
	if err != nil {
		return nil, fmt.Errorf("read the runs for generation %s: %w", identity, err)
	}
	defer rows.Close()

	runs := make([]embedrun.Run, 0)
	for rows.Next() {
		run, scanErr := scanRun(rows, identity)
		if scanErr != nil {
			return nil, scanErr
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the runs for generation %s: %w", identity, err)
	}
	return runs, nil
}

// SaveRun writes a run's state, refusing a stale fencing token.
//
// The refusal is a WHERE clause rather than a read followed by a write, because
// between those two a takeover is exactly what happens.
func (s *Store) SaveRun(ctx context.Context, run embedrun.Run) error {
	if run.Phase == embedrun.PhaseRetired && run.Status != embedrun.StatusComplete {
		return fmt.Errorf("save run %s: %w: phase %s requires status %s",
			run.ID, embedrun.ErrPhase, embedrun.PhaseRetired, embedrun.StatusComplete)
	}
	if run.Status == embedrun.StatusComplete {
		return fmt.Errorf("save run %s: %w: terminal state is owned by generation retirement",
			run.ID, embedrun.ErrTerminal)
	}
	if err := validateRunResume(run); err != nil {
		return err
	}
	cursor, err := encodeCursor(run.Cursor)
	if err != nil {
		return err
	}
	if run.Status == embedrun.StatusAbandoned {
		return fmt.Errorf("%w: run %s must be abandoned through AbandonRun",
			embedstore.ErrConflict, run.ID)
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
	if stored.GenerationIdentity != run.GenerationIdentity {
		return fmt.Errorf("save run %s: %w: stored generation is %s, write names %s",
			run.ID, embedrun.ErrGeneration, stored.GenerationIdentity, run.GenerationIdentity)
	}
	if run.FencingToken < stored.FencingToken {
		return fmt.Errorf("%w: run %s is fenced at token %d and this write carries %d",
			embedstore.ErrConflict, run.ID, stored.FencingToken, run.FencingToken)
	}
	if stored.Terminal() {
		return fmt.Errorf("save run %s: %w: run %s is %s",
			run.ID, embedrun.ErrTerminal, run.ID, stored.Status)
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
func (s *Store) Pointer(
	ctx context.Context, targetSchema, targetTable string,
) (embedstore.Pointer, error) {
	const query = `SELECT target_schema, target_table, active_generation,
		COALESCE(previous_generation,''),
		cut_over_at, COALESCE(cut_over_by,''), COALESCE(plan_digest,'')
		FROM ` + embedstore.PointerTable + `
		WHERE target_schema = $1 AND target_table = $2`
	var pointer embedstore.Pointer
	err := s.db.QueryRowContext(ctx, query, targetSchema, targetTable).Scan(
		&pointer.TargetSchema, &pointer.TargetTable, &pointer.Active, &pointer.Previous,
		&pointer.CutOverAt, &pointer.CutOverBy, &pointer.PlanDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return embedstore.Pointer{}, fmt.Errorf("%w: no pointer for %s",
			embedstore.ErrNotFound, embedstore.QualifiedName(targetSchema, targetTable))
	}
	if err != nil {
		return embedstore.Pointer{}, fmt.Errorf("read pointer for %s: %w",
			embedstore.QualifiedName(targetSchema, targetTable), err)
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
	return s.movePointer(
		ctx, pointer, expectedActive, "", 0, false, "", time.Time{}, time.Time{}, nil)
}

// MovePointerWithMaintenance moves a pointer and opens the previous
// generation's maintenance window in the same transaction.
func (s *Store) MovePointerWithMaintenance(
	ctx context.Context, pointer embedstore.Pointer, expectedActive, requiredRunID string,
	stabilizeFor time.Duration,
) (embedstore.CutoverMove, error) {
	var move embedstore.CutoverMove
	err := s.movePointer(
		ctx, pointer, expectedActive, requiredRunID, stabilizeFor, true, "",
		time.Time{}, time.Time{}, &move)
	if err != nil {
		return embedstore.CutoverMove{}, err
	}
	return move, nil
}

// MovePointerWithRollback moves the pointer and records the displaced
// generation's rollback in the same transaction.
func (s *Store) MovePointerWithRollback(
	ctx context.Context, pointer embedstore.Pointer, expectedActive string,
	expectedMaintainedUntil, eligibilityNotAfter time.Time,
) (time.Time, error) {
	var move embedstore.CutoverMove
	err := s.movePointer(
		ctx, pointer, expectedActive, "", 0, false, expectedActive,
		expectedMaintainedUntil, eligibilityNotAfter, &move)
	if err != nil {
		return time.Time{}, err
	}
	return move.CutOverAt, nil
}

func (s *Store) movePointer(
	ctx context.Context, pointer embedstore.Pointer, expectedActive, requiredRunID string,
	stabilizeFor time.Duration, managePrevious bool, rolledBackGeneration string,
	expectedMaintainedUntil, eligibilityNotAfter time.Time,
	committed *embedstore.CutoverMove,
) error {
	operation := pointerMoveOperation{
		pointer:                 pointer,
		expectedActive:          expectedActive,
		requiredRunID:           requiredRunID,
		stabilizeFor:            stabilizeFor,
		managePrevious:          managePrevious,
		rolledBackGeneration:    rolledBackGeneration,
		expectedMaintainedUntil: expectedMaintainedUntil,
		eligibilityNotAfter:     eligibilityNotAfter,
		committed:               committed,
	}
	if pointer.Previous != expectedActive {
		return fmt.Errorf("%w: pointer previous generation %q does not match expected active generation %q",
			embedstore.ErrConflict, pointer.Previous, expectedActive)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockGenerations(ctx, tx, pointer.Active, expectedActive, pointer.Previous); err != nil {
		return err
	}
	if err := lockPointerForMove(ctx, tx, operation); err != nil {
		return err
	}
	destination, err := validatePointerMoveDestination(ctx, tx, operation)
	if err != nil {
		return err
	}
	authorizing, err := authorizePointerMove(ctx, tx, operation)
	if err != nil {
		return err
	}
	previous, err := validatePointerMovePrevious(ctx, tx, operation)
	if err != nil {
		return err
	}
	rolledBackRuns, err := lockRollbackRuns(ctx, tx, operation)
	if err != nil {
		return err
	}
	pointer, move, err := preparePointerMove(ctx, tx, operation, destination, previous)
	if err != nil {
		return err
	}
	changed, target, err := writePointerMove(ctx, tx, pointer, expectedActive)
	if err != nil {
		return err
	}
	if changed != 1 {
		return explainPointerMoveConflict(ctx, tx, pointer, expectedActive, target)
	}
	if err := commitPointerMove(
		ctx, tx, operation, pointer, target, &move, authorizing, rolledBackRuns); err != nil {
		return err
	}
	return nil
}

type pointerMoveOperation struct {
	pointer                 embedstore.Pointer
	expectedActive          string
	requiredRunID           string
	stabilizeFor            time.Duration
	managePrevious          bool
	rolledBackGeneration    string
	expectedMaintainedUntil time.Time
	eligibilityNotAfter     time.Time
	committed               *embedstore.CutoverMove
}

func lockPointerForMove(
	ctx context.Context, tx *sql.Tx, operation pointerMoveOperation,
) error {
	if operation.committed == nil || operation.expectedActive == "" {
		return nil
	}
	lockedPrevious, err := lockExpectedPointer(
		ctx, tx, operation.pointer, operation.expectedActive)
	if err != nil {
		return err
	}
	if operation.rolledBackGeneration == "" {
		return nil
	}
	if lockedPrevious == "" || lockedPrevious != operation.pointer.Active {
		return fmt.Errorf(
			"%w: rollback destination generation %s is not the pointer's previous generation %s",
			embedstore.ErrConflict, operation.pointer.Active, lockedPrevious)
	}
	return nil
}

func validatePointerMoveDestination(
	ctx context.Context, tx *sql.Tx, operation pointerMoveOperation,
) (embedstore.Generation, error) {
	pointer := operation.pointer
	destination, err := readGeneration(ctx, tx, pointer.Active)
	if err != nil {
		return embedstore.Generation{}, err
	}
	if destination.Retired() {
		return embedstore.Generation{}, fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, destination.Identity,
			destination.RetiredAt.UTC().Format(time.RFC3339))
	}
	if destination.TargetSchema != pointer.TargetSchema ||
		destination.TargetTable != pointer.TargetTable {
		return embedstore.Generation{}, fmt.Errorf("%w: generation %s targets %s, not pointer target %s",
			embedstore.ErrConflict, destination.Identity,
			embedstore.QualifiedName(destination.TargetSchema, destination.TargetTable),
			embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable))
	}
	counts, err := generationRunCounts(ctx, tx, destination)
	if err != nil {
		return embedstore.Generation{}, fmt.Errorf("move pointer for %s: %w",
			embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable), err)
	}
	if counts.total > 0 && counts.live == 0 {
		return embedstore.Generation{}, fmt.Errorf("move pointer for %s to generation %s: %w: "+
			"generation %s has run history, but no usable live feeder",
			embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable), pointer.Active,
			embedstore.ErrNoLiveRun, pointer.Active)
	}
	if operation.rolledBackGeneration != "" &&
		!destination.MaintainedUntil.Equal(operation.expectedMaintainedUntil) {
		return embedstore.Generation{}, fmt.Errorf(
			"%w: rollback destination generation %s maintenance changed from %s to %s",
			embedstore.ErrConflict, destination.Identity,
			operation.expectedMaintainedUntil.UTC().Format(time.RFC3339),
			destination.MaintainedUntil.UTC().Format(time.RFC3339))
	}
	return destination, nil
}

func authorizePointerMove(
	ctx context.Context, tx *sql.Tx, operation pointerMoveOperation,
) (embedrun.Run, error) {
	if operation.requiredRunID == "" {
		return embedrun.Run{}, nil
	}
	run, err := scanRun(
		tx.QueryRowContext(ctx, selectRunForUpdateSQL, operation.requiredRunID),
		operation.requiredRunID)
	if err != nil {
		return embedrun.Run{}, err
	}
	if run.GenerationIdentity != operation.pointer.Active {
		return embedrun.Run{}, fmt.Errorf("%w: run %s authorizes generation %s, not %s",
			embedstore.ErrConflict, operation.requiredRunID,
			run.GenerationIdentity, operation.pointer.Active)
	}
	if run.Terminal() {
		return embedrun.Run{}, fmt.Errorf("%w: run %s is %s",
			embedrun.ErrTerminal, operation.requiredRunID, run.Status)
	}
	run.FencingToken++
	if err := run.Reach(run.FencingToken, embedrun.PhaseCutOver); err != nil {
		return embedrun.Run{}, fmt.Errorf(
			"record cutover on run %s: %w", operation.requiredRunID, err)
	}
	run.LeaseOwner = ""
	run.LeaseExpires = time.Time{}
	return run, nil
}

func validatePointerMovePrevious(
	ctx context.Context, tx *sql.Tx, operation pointerMoveOperation,
) (embedstore.Generation, error) {
	pointer := operation.pointer
	if pointer.Previous == "" {
		return embedstore.Generation{}, nil
	}
	previous, err := readGeneration(ctx, tx, pointer.Previous)
	if err != nil {
		return embedstore.Generation{}, err
	}
	if previous.Retired() {
		return embedstore.Generation{}, fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, previous.Identity,
			previous.RetiredAt.UTC().Format(time.RFC3339))
	}
	if previous.TargetSchema != pointer.TargetSchema || previous.TargetTable != pointer.TargetTable {
		return embedstore.Generation{}, fmt.Errorf(
			"%w: previous generation %s targets %s, not pointer target %s",
			embedstore.ErrConflict, previous.Identity,
			embedstore.QualifiedName(previous.TargetSchema, previous.TargetTable),
			embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable))
	}
	if !operation.managePrevious || operation.stabilizeFor <= 0 {
		return previous, nil
	}
	counts, err := generationRunCounts(ctx, tx, previous)
	if err != nil {
		return embedstore.Generation{}, fmt.Errorf(
			"maintain previous generation %s while moving pointer for %s: %w",
			pointer.Previous,
			embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable), err)
	}
	if counts.total > 0 && counts.live == 0 {
		return embedstore.Generation{}, fmt.Errorf(
			"maintain previous generation %s while moving pointer for %s: %w: "+
				"generation %s has run history, but no usable live feeder",
			pointer.Previous,
			embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable),
			embedstore.ErrNoLiveRun, pointer.Previous)
	}
	return previous, nil
}

func lockRollbackRuns(
	ctx context.Context, tx *sql.Tx, operation pointerMoveOperation,
) ([]embedrun.Run, error) {
	identity := operation.rolledBackGeneration
	if identity == "" {
		return nil, nil
	}
	if identity != operation.expectedActive {
		return nil, fmt.Errorf("%w: rollback generation %s is not expected active generation %s",
			embedstore.ErrConflict, identity, operation.expectedActive)
	}
	rows, err := tx.QueryContext(ctx, selectRunsForGenerationForUpdateSQL, identity)
	if err != nil {
		return nil, fmt.Errorf("read runs for rollback generation %s: %w", identity, err)
	}
	var runs []embedrun.Run
	for rows.Next() {
		run, scanErr := scanRun(rows, identity)
		if scanErr != nil {
			_ = rows.Close()
			return nil, scanErr
		}
		if !run.Phase.LeadsTo(embedrun.PhaseRolledBack) || run.Terminal() {
			continue
		}
		run.FencingToken++
		if err := run.Reach(run.FencingToken, embedrun.PhaseRolledBack); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("record rollback on run %s: %w", run.ID, err)
		}
		run.LeaseOwner = ""
		run.LeaseExpires = time.Time{}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read runs for rollback generation %s: %w", identity, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("read runs for rollback generation %s: %w", identity, err)
	}
	return runs, nil
}

func preparePointerMove(
	ctx context.Context,
	tx *sql.Tx,
	operation pointerMoveOperation,
	destination, previous embedstore.Generation,
) (embedstore.Pointer, embedstore.CutoverMove, error) {
	pointer := operation.pointer
	var move embedstore.CutoverMove
	if operation.committed == nil {
		return pointer, move, nil
	}
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&move.CutOverAt); err != nil {
		return embedstore.Pointer{}, embedstore.CutoverMove{}, fmt.Errorf(
			"sample pointer move time: %w", err)
	}
	move.CutOverAt = move.CutOverAt.UTC()
	if operation.rolledBackGeneration != "" && !destination.Maintained(move.CutOverAt) {
		return embedstore.Pointer{}, embedstore.CutoverMove{}, fmt.Errorf(
			"%w: rollback destination generation %s maintenance expired at %s before %s",
			embedstore.ErrConflict, destination.Identity,
			destination.MaintainedUntil.UTC().Format(time.RFC3339),
			move.CutOverAt.Format(time.RFC3339))
	}
	if operation.rolledBackGeneration != "" &&
		!operation.eligibilityNotAfter.IsZero() &&
		move.CutOverAt.After(operation.eligibilityNotAfter) {
		return embedstore.Pointer{}, embedstore.CutoverMove{}, fmt.Errorf(
			"%w: rollback eligibility expired at %s before %s",
			embedstore.ErrConflict,
			operation.eligibilityNotAfter.UTC().Format(time.RFC3339),
			move.CutOverAt.Format(time.RFC3339))
	}
	pointer.CutOverAt = move.CutOverAt
	if operation.managePrevious && pointer.Previous != "" && operation.stabilizeFor > 0 {
		move.PreviousMaintainedUntil = move.CutOverAt.Add(operation.stabilizeFor)
		if previous.MaintainedUntil.After(move.PreviousMaintainedUntil) {
			move.PreviousMaintainedUntil = previous.MaintainedUntil
		}
	}
	return pointer, move, nil
}

// writePointerMove covers both a first pointer and a compare-and-swap update.
// The SELECT's condition must let the row through in both cases: a guard of
// only an empty expected-active guard made every move onto an existing pointer produce no row to
// conflict with, so ON CONFLICT never fired and a correct cutover was refused
// as a conflict. This was measured against PostgreSQL 18; the in-memory store
// agreed with the wrong SQL.
func writePointerMove(
	ctx context.Context,
	tx *sql.Tx,
	pointer embedstore.Pointer,
	expectedActive string,
) (int64, string, error) {
	const query = `INSERT INTO ` + embedstore.PointerTable + ` (
		target_schema, target_table, active_generation, previous_generation,
		cut_over_at, cut_over_by, plan_digest)
		SELECT $1, $2, $3, $4, $5, $6, $7
		WHERE $8 = '' OR EXISTS (
			SELECT 1 FROM ` + embedstore.PointerTable + `
			WHERE target_schema = $1 AND target_table = $2 AND active_generation = $8)
		ON CONFLICT (target_schema, target_table) DO UPDATE SET
			active_generation = EXCLUDED.active_generation,
			previous_generation = EXCLUDED.previous_generation,
			cut_over_at = EXCLUDED.cut_over_at,
			cut_over_by = EXCLUDED.cut_over_by,
			plan_digest = EXCLUDED.plan_digest
		WHERE ` + embedstore.PointerTable + `.active_generation = $8`
	result, err := tx.ExecContext(ctx, query,
		pointer.TargetSchema, pointer.TargetTable, pointer.Active, nullable(pointer.Previous),
		pointer.CutOverAt.UTC(), nullable(pointer.CutOverBy), nullable(pointer.PlanDigest),
		expectedActive)
	target := embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable)
	if err != nil {
		return 0, target, fmt.Errorf("move pointer for %s: %w", target, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, target, fmt.Errorf("move pointer for %s: %w", target, err)
	}
	return changed, target, nil
}

func commitPointerMove(
	ctx context.Context,
	tx *sql.Tx,
	operation pointerMoveOperation,
	pointer embedstore.Pointer,
	target string,
	move *embedstore.CutoverMove,
	authorizing embedrun.Run,
	rolledBackRuns []embedrun.Run,
) error {
	if err := updatePreviousMaintenance(ctx, tx, operation, pointer, target, move); err != nil {
		return err
	}
	if operation.requiredRunID != "" {
		if err := recordPointerRun(ctx, tx, authorizing, "cutover"); err != nil {
			return err
		}
	}
	for _, run := range rolledBackRuns {
		if err := recordPointerRun(ctx, tx, run, "rollback"); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("move pointer for %s: %w", target, err)
	}
	if operation.committed != nil {
		*operation.committed = *move
	}
	return nil
}

func updatePreviousMaintenance(
	ctx context.Context,
	tx *sql.Tx,
	operation pointerMoveOperation,
	pointer embedstore.Pointer,
	target string,
	move *embedstore.CutoverMove,
) error {
	if !operation.managePrevious || pointer.Previous == "" {
		return nil
	}
	var maintained sql.NullTime
	var err error
	if operation.stabilizeFor > 0 {
		const maintain = `UPDATE ` + embedstore.GenerationTable + `
			SET maintained_until = GREATEST(maintained_until, $2)
			WHERE identity = $1 AND retired_at IS NULL
			RETURNING maintained_until`
		err = tx.QueryRowContext(
			ctx, maintain, pointer.Previous,
			move.PreviousMaintainedUntil.UTC()).Scan(&maintained)
	} else {
		const clearMaintenance = `UPDATE ` + embedstore.GenerationTable + `
			SET maintained_until = NULL
			WHERE identity = $1 AND retired_at IS NULL
			RETURNING maintained_until`
		err = tx.QueryRowContext(ctx, clearMaintenance, pointer.Previous).Scan(&maintained)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("maintain previous generation %s while moving pointer for %s: "+
			"the generation changed while its lifecycle lock was held", pointer.Previous, target)
	}
	if err != nil {
		return fmt.Errorf("maintain previous generation %s while moving pointer for %s: %w",
			pointer.Previous, target, err)
	}
	if !maintained.Valid {
		move.PreviousMaintainedUntil = time.Time{}
		return nil
	}
	move.PreviousMaintainedUntil = maintained.Time.UTC()
	return nil
}

func recordPointerRun(
	ctx context.Context, tx *sql.Tx, run embedrun.Run, action string,
) error {
	cursor, err := encodeCursor(run.Cursor)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(
		ctx, updateRunLifecyclePhaseSQL, runArguments(run, cursor)...)
	if err != nil {
		return fmt.Errorf("record %s on run %s: %w", action, run.ID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record %s on run %s: %w", action, run.ID, err)
	}
	if updated != 1 {
		return fmt.Errorf("record %s on run %s: %w", action, run.ID, embedstore.ErrConflict)
	}
	return nil
}

func explainPointerMoveConflict(
	ctx context.Context,
	tx *sql.Tx,
	pointer embedstore.Pointer,
	expectedActive, target string,
) error {
	const currentQuery = `SELECT active_generation FROM ` + embedstore.PointerTable + `
		WHERE target_schema = $1 AND target_table = $2`
	var active string
	err := tx.QueryRowContext(ctx, currentQuery, pointer.TargetSchema, pointer.TargetTable).Scan(&active)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s has no pointer and this move expected %s",
			embedstore.ErrConflict, target, expectedActive)
	}
	if err != nil {
		return fmt.Errorf("read pointer for %s: %w", target, err)
	}
	return fmt.Errorf("%w: %s reads %s and this move expected %s",
		embedstore.ErrConflict, target, active, expectedActive)
}

// lockExpectedPointer removes the last wait between sampling a rollback's
// clock and changing the pointer. Generation lifecycle locks serialize Ptah
// operations, while this row lock also covers a transaction that already held
// the pointer row before entering that protocol.
func lockExpectedPointer(
	ctx context.Context, tx *sql.Tx, pointer embedstore.Pointer, expectedActive string,
) (string, error) {
	const query = `SELECT active_generation, COALESCE(previous_generation, '') FROM ` + embedstore.PointerTable + `
		WHERE target_schema = $1 AND target_table = $2 FOR UPDATE`
	target := embedstore.QualifiedName(pointer.TargetSchema, pointer.TargetTable)
	var active, previous string
	err := tx.QueryRowContext(ctx, query, pointer.TargetSchema, pointer.TargetTable).Scan(
		&active, &previous)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s has no pointer and this move expected %s",
			embedstore.ErrConflict, target, expectedActive)
	}
	if err != nil {
		return "", fmt.Errorf("lock pointer for %s: %w", target, err)
	}
	if active != expectedActive {
		return "", fmt.Errorf("%w: %s reads %s and this move expected %s",
			embedstore.ErrConflict, target, active, expectedActive)
	}
	return previous, nil
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

// ReachPhase records that a run has got as far as a phase.
//
// Read, decide, save — and the decision is embedrun.Run.Reach, which leaves a
// phase already at or past the target alone. That is why this is one call
// rather than a read and a write at every verb: the caller says how far it got,
// not what the phase should now be, and a verb re-run out of order tells the
// run nothing rather than dragging it backwards.
//
// The stored token is the one offered, because the caller here is an operator
// running a command rather than a worker holding a lease. The fence exists to
// stop a worker the run has moved past from committing; a person naming a run
// on the command line is not that, and the guards on what they asked for --
// the plan digest a cutover binds to, the freshness a rollback needs -- are the
// ones that decide whether the work happens at all.
func (s *Store) ReachPhase(ctx context.Context, runID string, to embedrun.Phase) error {
	run, err := s.Run(ctx, runID)
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
	return s.SaveRun(ctx, run)
}

// ClaimRun takes a run for a worker, writing the lease and nothing else.
//
// See [embedstore.Store.ClaimRun] for why the row is not rewritten and why the
// token comes from the store. The whole claim is one statement, so there is no
// window between deciding the token and writing it.
func (s *Store) ClaimRun(
	ctx context.Context, id, worker string, leaseExpires time.Time,
) (embedrun.Run, int64, error) {
	row := s.db.QueryRowContext(ctx, claimRunSQL,
		id, worker, leaseExpires.UTC(), time.Now().UTC(),
		string(embedrun.StatusComplete), string(embedrun.StatusAbandoned))
	run, err := scanRun(row, id)
	if err != nil {
		// The conditional update deliberately has the same empty result for a
		// missing row and a terminal one. Read once to distinguish them for the
		// operator; terminal state never becomes claimable again, so this cannot
		// race back to a different answer.
		if errors.Is(err, embedstore.ErrNotFound) {
			stored, readErr := s.Run(ctx, id)
			if readErr == nil && stored.Terminal() {
				return embedrun.Run{}, 0, fmt.Errorf(
					"claim run %s: %w: run %s is %s",
					id, embedrun.ErrTerminal, id, stored.Status)
			}
		}
		return embedrun.Run{}, 0, fmt.Errorf("claim run %s: %w", id, err)
	}
	return run, run.FencingToken, nil
}

// AbandonRun permanently ends a run without destroying its generation.
//
// The targeted UPDATE's token increment fences claims and saves through the
// run row. The generation lock makes the last-feeder decision one instant with
// retirement, maintenance, pointer moves, sibling creation and sibling
// terminal transitions.
func (s *Store) AbandonRun(
	ctx context.Context, id, reason string,
) (embedrun.Run, error) {
	if reason == "" {
		return embedrun.Run{}, fmt.Errorf(
			"abandon run %s: %w: an abandonment without a reason cannot be acted on",
			id, embedrun.ErrCheckpoint)
	}
	for {
		initial, err := s.Run(ctx, id)
		if err != nil {
			return embedrun.Run{}, err
		}
		run, retry, err := s.abandonRunInGeneration(ctx, initial, reason)
		if !retry {
			return run, err
		}
	}
}

func (s *Store) abandonRunInGeneration(
	ctx context.Context, initial embedrun.Run, reason string,
) (embedrun.Run, bool, error) {
	id := initial.ID
	tx, err := s.begin(ctx)
	if err != nil {
		return embedrun.Run{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockGenerations(ctx, tx, initial.GenerationIdentity); err != nil {
		return embedrun.Run{}, false, err
	}
	stored, err := scanRun(tx.QueryRowContext(ctx, selectRunForUpdateSQL, id), id)
	if err != nil {
		return embedrun.Run{}, false, err
	}
	if stored.GenerationIdentity != initial.GenerationIdentity {
		return embedrun.Run{}, true, nil
	}
	if stored.Status == embedrun.StatusAbandoned {
		return stored, false, nil
	}
	if stored.Status == embedrun.StatusComplete {
		return embedrun.Run{}, false, fmt.Errorf(
			"abandon run %s: %w: run %s is complete", id, embedrun.ErrTerminal, id)
	}
	generation, err := readGeneration(ctx, tx, stored.GenerationIdentity)
	generationFound := !errors.Is(err, embedstore.ErrNotFound)
	if !generationFound {
		// Missing registry rows are deliberately kept in pruning's reader set.
		// They have no maintenance window, but a historical pointer can still
		// make the identity active and therefore protected.
		generation.Identity = stored.GenerationIdentity
	} else if err != nil {
		return embedrun.Run{}, false, err
	}
	if generation.Retired() {
		return embedrun.Run{}, false, fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, generation.Identity,
			generation.RetiredAt.UTC().Format(time.RFC3339))
	}
	var when time.Time
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&when); err != nil {
		return embedrun.Run{}, false, fmt.Errorf("sample abandonment time for run %s: %w", id, err)
	}
	when = when.UTC()
	requirement, err := abandonmentFeederRequirement(generation, stored)
	if err != nil {
		return embedrun.Run{}, false, fmt.Errorf("abandon run %s: %w", id, err)
	}

	protected, why, err := protectedGeneration(ctx, tx, generation, when)
	if err != nil {
		return embedrun.Run{}, false, err
	}
	if protected {
		other, err := hasOtherFeeder(
			ctx, tx, generation, stored, id, requirement)
		if err != nil {
			return embedrun.Run{}, false, err
		}
		if !other {
			return embedrun.Run{}, false, fmt.Errorf(
				"abandon run %s: %w: generation %s is %s and no other usable live feeder remains",
				id, embedstore.ErrNoLiveRun, generation.Identity, why)
		}
	}

	run, err := scanRun(tx.QueryRowContext(ctx, abandonRunSQL,
		id, string(embedrun.StatusAbandoned), reason, when.UTC()), id)
	if err != nil {
		return embedrun.Run{}, false, fmt.Errorf("abandon run %s: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return embedrun.Run{}, false, fmt.Errorf("abandon run %s: %w", id, err)
	}
	return run, false, nil
}

type feederRequirement uint8

const (
	feederRequirementAny feederRequirement = iota
	feederRequirementPositioned
)

func hasOtherFeeder(
	ctx context.Context, tx *sql.Tx, generation embedstore.Generation,
	current embedrun.Run, excluding string, requirement feederRequirement,
) (bool, error) {
	const otherRuns = `SELECT source, COALESCE(catch_up_watermark, ''),
			COALESCE(snapshot_watermark, '')
		FROM ` + embedstore.RunTable + `
		WHERE generation_identity = $1 AND id <> $2 AND status NOT IN ($3, $4)`
	rows, err := tx.QueryContext(ctx, otherRuns, generation.Identity, excluding,
		string(embedrun.StatusComplete), string(embedrun.StatusAbandoned))
	if err != nil {
		return false, fmt.Errorf("read other live runs for generation %s: %w",
			generation.Identity, err)
	}
	defer rows.Close()
	for rows.Next() {
		var source, catchUp, snapshot string
		if err := rows.Scan(&source, &catchUp, &snapshot); err != nil {
			return false, fmt.Errorf("read other live runs for generation %s: %w",
				generation.Identity, err)
		}
		if !sameGenerationSource(source, current.Source, generation) {
			continue
		}
		if requirement == feederRequirementAny {
			return true, nil
		}
		_, positioned, parseErr := embedcatchup.ResumeFrom(catchUp, snapshot)
		if parseErr == nil && positioned {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read other live runs for generation %s: %w",
			generation.Identity, err)
	}
	return false, nil
}

func abandonmentFeederRequirement(
	generation embedstore.Generation, current embedrun.Run,
) (feederRequirement, error) {
	if generation.ConsistencyMode != "" {
		if generation.ConsistencyMode == string(embedcatchup.ModeOutbox) {
			return feederRequirementPositioned, nil
		}
		return feederRequirementAny, nil
	}
	_, positioned, err := embedcatchup.ResumeFrom(
		current.CatchUpWatermark, current.SnapshotWatermark)
	if err != nil {
		return feederRequirementAny, fmt.Errorf(
			"current run %s has an invalid resume position: %w", current.ID, err)
	}
	if positioned {
		return feederRequirementPositioned, nil
	}
	return feederRequirementAny, nil
}

func sameGenerationSource(
	candidate, current string, generation embedstore.Generation,
) bool {
	if generation.SourceTable == "" {
		return candidate == current
	}
	canonical := embedstore.SourceIdentity(generation.SourceSchema, generation.SourceTable)
	candidateMatches := candidate == canonical || candidate == generation.SourceTable
	currentMatches := current == canonical || current == generation.SourceTable
	return candidateMatches && currentMatches
}

// protectedGeneration reports why a generation must keep a usable live feeder.
// The caller holds its lifecycle lock.
func protectedGeneration(
	ctx context.Context, tx *sql.Tx, generation embedstore.Generation, now time.Time,
) (bool, string, error) {
	const activeQuery = `SELECT target_schema, target_table FROM ` + embedstore.PointerTable + `
		WHERE active_generation = $1 ORDER BY target_schema, target_table LIMIT 1`
	var schema, table string
	err := tx.QueryRowContext(ctx, activeQuery, generation.Identity).Scan(&schema, &table)
	switch {
	case err == nil:
		return true, "active for " + embedstore.QualifiedName(schema, table), nil
	case !errors.Is(err, sql.ErrNoRows):
		return false, "", fmt.Errorf("read active pointer for generation %s: %w",
			generation.Identity, err)
	case generation.Maintained(now):
		return true, "maintained until " + generation.MaintainedUntil.UTC().Format(time.RFC3339), nil
	default:
		return false, "", nil
	}
}

// outboxFloorSQL reads the watermarks of every run still reading a source table.
//
// A reader leaves in either of two explicit ways. Retiring its generation
// destroys the vectors and terminalizes every run over it. Abandoning one run
// keeps the vectors and releases only that run. A phase is deliberately not
// used: retirement advances only phases that lead directly to PhaseRetired,
// while earlier runs keep their truthful high-water phase and still become
// terminal.
//
// NOT EXISTS rather than a join, so a run whose generation row is missing still
// counts as a reader. Every bound here leans the same way: a reader wrongly
// included keeps events, and a reader wrongly excluded deletes events it still
// owes.
// $1 is the source identity SourceIdentity answers; $2 is the bare table name a
// run recorded before it did.
//
// Both, and that is not a compatibility layer -- it is the same lean as every
// other bound here. A run created before this change holds a bare table name,
// and excluding it would raise the floor and prune events it still owes, which
// is the one direction this query is not allowed to be wrong in. Matching it
// keeps its events; the imprecision it brings back is the one it already had
// (stokaro/ptah#2724).
//
// It costs nothing once no such run is live: a bare table name is not a digest,
// so the second predicate matches nothing a current Ptah writes.
const outboxFloorSQL = `SELECT r.id, r.generation_identity,
		COALESCE(catch_up_watermark, ''), COALESCE(snapshot_watermark, '')
	FROM ` + embedstore.RunTable + ` r
	WHERE (r.source = $1 OR r.source = $2)
	  AND r.status NOT IN ($3, $4)
	  AND NOT EXISTS (
	        SELECT 1 FROM ` + embedstore.GenerationTable + ` g
	        WHERE g.identity = r.generation_identity
	          AND g.retired_at IS NOT NULL)
	ORDER BY r.generation_identity, r.id`

// OutboxFloorHolder identifies a run at the earliest position in an outbox.
type OutboxFloorHolder struct {
	// RunID is the exact identifier an operator can pass to `inference
	// abandon` or inspect with `inference status`.
	RunID string
	// Generation is the generation that run was building.
	Generation string
}

// OutboxFloorResult is the earliest position every usable live feeder has
// passed and the positioned readers currently holding it there.
type OutboxFloorResult struct {
	Position embedcatchup.Cursor
	Holders  []OutboxFloorHolder
}
