package embedpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// MetadataSuffixes are what a generation's bookkeeping columns are called,
// relative to its vector column.
//
// They live beside the vector rather than in a side table because every one of
// them has to be written in the same statement as the vector it describes: a
// row whose vector landed and whose input hash did not is a row verification
// calls fresh forever.
const (
	// GenerationSuffix holds the generation identity the row belongs to.
	GenerationSuffix = "_generation"
	// InputHashSuffix holds the source-input hash the vector was computed from.
	InputHashSuffix = "_input_hash"
	// VersionSuffix holds the source version it was computed at.
	VersionSuffix = "_source_version"
	// StateSuffix holds whether the row is a vector, a skip or a tombstone.
	StateSuffix = "_state"
)

// MetadataSuffixes returns them in one list.
//
// One list because three places need the same answer -- creating the columns,
// dropping them, and writing them -- and a hand-written enumeration in each is
// how a fifth suffix comes to be created and never dropped. A caller that wants
// the vector column too prepends the empty string; the vector is not in here
// because it is not metadata and its type is not TEXT.
func MetadataSuffixes() []string {
	return []string{GenerationSuffix, InputHashSuffix, VersionSuffix, StateSuffix}
}

// Target is embedengine.Target over a PostgreSQL table with pgvector.
type Target struct {
	db   *sql.DB
	spec embedgen.Spec
}

// NewTarget returns a target for a specification.
func NewTarget(db *sql.DB, spec embedgen.Spec) (*Target, error) {
	if _, err := spec.TargetObjects(); err != nil {
		return nil, err
	}
	if len(spec.Source.KeyFields) == 0 {
		return nil, errors.New("the specification names no key fields, so a target row cannot be addressed")
	}
	return &Target{db: db, spec: spec}, nil
}

// Commit writes a batch's target writes and the run that checkpoints them, in
// one transaction.
//
// This method is the design's one non-negotiable made literal. Everything else
// in the lifecycle can be reconciled after the fact; this cannot, because a
// checkpoint that outlives the vectors it claims produces a resumed run that
// skips them and looks completely healthy.
func (t *Target) Commit(ctx context.Context, writes []embedrun.TargetWrite, run embedrun.Run) error {
	transaction, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	// The run first, which is the fencing check: its UPDATE carries the token
	// in its WHERE clause, so a worker whose lease was taken is refused here.
	//
	// Before the writes rather than after them, and the order is the
	// diagnostic. Both refusals roll the same transaction back, so nothing
	// lands either way -- but a fenced worker asked to resolve its writes first
	// is told whichever row-level rule its stale batch happens to trip, and the
	// answer an operator can act on is that the lease moved. It also takes the
	// run's row lock before any target row, which is the order two workers
	// should contend in.
	if err := saveRunTx(ctx, transaction, run); err != nil {
		return err
	}

	// What the target already holds for these keys, read inside the same
	// transaction and locked, so the decision below is made against what is
	// there rather than against what this worker believes. embedrun.ResolveWrite
	// is the decision; before stokaro/ptah#2391 nothing called it and every
	// write won unconditionally.
	existing, err := t.existingWrites(ctx, transaction, writes)
	if err != nil {
		return err
	}
	for _, write := range writes {
		// The strategy's own ordering. Reading a rendered timestamp as an
		// opaque string discarded fresh answers as stale (stokaro/ptah#2635).
		resolved, changed, err := embedrun.ResolveWrite(
			existing[writeKey(write.Key)], write, t.spec.Source.VersionStrategy.VersionOrder())
		if err != nil {
			return fmt.Errorf("write %v: %w", write.Key, err)
		}
		if !changed {
			continue
		}
		if err := t.applyWrite(ctx, transaction, resolved); err != nil {
			return err
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// applyWrite writes one row.
// existingWrites reads what the target holds for a batch's keys.
//
// FOR UPDATE, because two workers embedding the same row are exactly what the
// resolution exists to order: without the lock both read the same prior state,
// both decide they win, and the later commit is the one that happens to be
// second rather than the one that is newer.
//
// A row whose generation column is NULL has never been written, and is absent
// from the answer -- ResolveWrite reads a nil as "nothing here yet", which is
// the same thing said once instead of twice.
func (t *Target) existingWrites(
	ctx context.Context, transaction *sql.Tx, writes []embedrun.TargetWrite,
) (map[string]*embedrun.TargetWrite, error) {
	held := make(map[string]*embedrun.TargetWrite, len(writes))
	if len(writes) == 0 {
		return held, nil
	}
	column := t.spec.Target.Column
	keys := t.spec.Source.KeyFields

	arguments := make([]any, 0, len(writes)*len(keys))
	tuples := make([]string, 0, len(writes))
	for _, write := range writes {
		placeholders := make([]string, 0, len(keys))
		for index := range keys {
			arguments = append(arguments, write.Key[index])
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(arguments)))
		}
		tuples = append(tuples, "("+strings.Join(placeholders, ", ")+")")
	}

	selected := make([]string, 0, len(keys)+4)
	for _, key := range keys {
		selected = append(selected, quoteIdentifier(key)+"::text")
	}
	for _, suffix := range MetadataSuffixes() {
		selected = append(selected, quoteIdentifier(column+suffix))
	}
	casted := make([]string, 0, len(keys))
	for _, key := range keys {
		casted = append(casted, quoteIdentifier(key)+"::text")
	}
	// #nosec G201 -- identifiers from the specification, through quoteIdentifier.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE (%s) IN (%s) FOR UPDATE",
		strings.Join(selected, ", "), t.qualifiedTable(),
		strings.Join(casted, ", "), strings.Join(tuples, ", "))

	rows, err := transaction.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("read the target rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		key := make([]string, len(keys))
		var generation, inputHash, version, state sql.NullString
		destinations := make([]any, 0, len(keys)+4)
		for index := range key {
			destinations = append(destinations, &key[index])
		}
		destinations = append(destinations, &generation, &inputHash, &version, &state)
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("read the target rows: %w", err)
		}
		if !generation.Valid {
			continue
		}
		held[writeKey(key)] = &embedrun.TargetWrite{
			Key: key, Generation: generation.String, InputHash: inputHash.String,
			Version: version.String, Kind: embedrun.WriteKind(state.String),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the target rows: %w", err)
	}
	return held, nil
}

// writeKey renders a composite key as one map key.
//
// NUL separates the components because a PostgreSQL text value cannot contain
// one, so no pair of distinct keys can render to the same string. Joining on a
// printable separator would make ("a", "b|c") and ("a|b", "c") the same row.
func writeKey(key []string) string {
	return strings.Join(key, "\x00")
}

func (t *Target) applyWrite(ctx context.Context, transaction *sql.Tx, write embedrun.TargetWrite) error {
	query, arguments := t.upsertStatement(write)
	if _, err := transaction.ExecContext(ctx, query, arguments...); err != nil {
		return fmt.Errorf("write %v: %w", write.Key, err)
	}
	return nil
}

// upsertStatement renders one row's write.
//
// It is an UPDATE rather than an INSERT: a generation adds a column to rows
// that already exist, so the row is there and the vector is what is missing.
// Inserting would create a second row for a key the source has once.
func (t *Target) upsertStatement(write embedrun.TargetWrite) (string, []any) {
	column := t.spec.Target.Column
	arguments := []any{
		vectorLiteral(write.Vector), write.Generation, write.InputHash,
		nullable(write.Version), string(write.Kind),
	}
	assignments := []string{
		fmt.Sprintf("%s = $1::vector", quoteIdentifier(column)),
		fmt.Sprintf("%s = $2", quoteIdentifier(column+GenerationSuffix)),
		fmt.Sprintf("%s = $3", quoteIdentifier(column+InputHashSuffix)),
		fmt.Sprintf("%s = $4", quoteIdentifier(column+VersionSuffix)),
		fmt.Sprintf("%s = $5", quoteIdentifier(column+StateSuffix)),
	}
	conditions := make([]string, 0, len(t.spec.Source.KeyFields))
	for index, key := range t.spec.Source.KeyFields {
		arguments = append(arguments, write.Key[index])
		conditions = append(conditions, fmt.Sprintf("%s::text = $%d", quoteIdentifier(key), len(arguments)))
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		t.qualifiedTable(), strings.Join(assignments, ", "), strings.Join(conditions, " AND "))
	return query, arguments
}

// qualifiedTable renders the target table with its schema when it has one.
func (t *Target) qualifiedTable() string {
	return qualify(t.spec.Target.Schema, t.spec.Target.Table)
}

// vectorLiteral renders a vector the way pgvector reads one, or NULL.
//
// A skip and a tombstone have no vector, and NULL is what "there is no answer
// here" means in a column. Writing a zero vector instead would put a point at
// the origin into every distance query, and it would be the nearest neighbour
// of anything else near the origin.
func vectorLiteral(vector []float32) any {
	if len(vector) == 0 {
		return nil
	}
	components := make([]string, len(vector))
	for index, value := range vector {
		components[index] = strconv.FormatFloat(float64(value), 'g', -1, 32)
	}
	return "[" + strings.Join(components, ",") + "]"
}

// saveRunTx writes the run inside a caller's transaction, refusing a stale
// fencing token.
//
// The refusal has to happen here rather than before the transaction, because
// before it is a moment the takeover can happen in.
func saveRunTx(ctx context.Context, transaction *sql.Tx, run embedrun.Run) error {
	cursor, err := encodeCursor(run.Cursor)
	if err != nil {
		return err
	}
	result, err := transaction.ExecContext(ctx, updateRunSQL, runArguments(run, cursor)...)
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
	return fmt.Errorf("%w: run %s was not updated, so another worker holds it or it is gone",
		embedstore.ErrConflict, run.ID)
}
