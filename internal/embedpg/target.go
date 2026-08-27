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

	for _, write := range writes {
		if err := t.applyWrite(ctx, transaction, write); err != nil {
			return err
		}
	}
	if err := saveRunTx(ctx, transaction, run); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// applyWrite writes one row.
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
	if schema := strings.TrimSpace(t.spec.Target.Schema); schema != "" {
		return quoteIdentifier(schema) + "." + quoteIdentifier(t.spec.Target.Table)
	}
	return quoteIdentifier(t.spec.Target.Table)
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
