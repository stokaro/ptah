package embedpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
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
	// OrdinalSuffix holds a chunk's position in its source row's set.
	//
	// It is not in [MetadataSuffixes] and it is not TEXT: it is an integer, it
	// is NOT NULL, and it is part of the target relation's primary key, which
	// is what makes a source row able to hold more than one vector. The three
	// facts are why it is created explicitly rather than in the loop that
	// creates the others.
	OrdinalSuffix = "_chunk_ordinal"
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
	for _, set := range setsByKey(writes) {
		// The strategy's own ordering. Reading a rendered timestamp as an
		// opaque string discarded fresh answers as stale (stokaro/ptah#2635).
		//
		// One decision per SOURCE KEY, taken against the set's first member.
		// The rules ResolveWrite carries are about the source row -- its
		// generation, its version, whether it was deleted -- and every member
		// of a set shares those, so asking once is asking the question the
		// rules are about. Asking per chunk would compare a chunk to whatever
		// held its ordinal before, which a re-split moves (ADR 0017).
		_, changed, err := embedrun.ResolveWrite(
			existing[writeKey(set[0].Key)], set[0], t.spec.Source.VersionStrategy.VersionOrder())
		if err != nil {
			return fmt.Errorf("write %v: %w", set[0].Key, err)
		}
		if !changed {
			continue
		}
		if err := t.applySet(ctx, transaction, set); err != nil {
			return err
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// setsByKey groups a batch's writes into one set per source key, in the order
// the keys first appear and each set in ordinal order.
//
// The order is the batch's own rather than a sort of the keys, so a run's write
// order is what the caller produced and two runs over one batch contend on
// their rows in the same order. Within a set the ordinal decides, because the
// rows are written in that order and a set arriving shuffled would still be
// stored in the order the source implies.
func setsByKey(writes []embedrun.TargetWrite) [][]embedrun.TargetWrite {
	order := make([]string, 0, len(writes))
	byKey := make(map[string][]embedrun.TargetWrite, len(writes))
	for _, write := range writes {
		key := writeKey(write.Key)
		if _, seen := byKey[key]; !seen {
			order = append(order, key)
		}
		byKey[key] = append(byKey[key], write)
	}
	sets := make([][]embedrun.TargetWrite, 0, len(order))
	for _, key := range order {
		set := byKey[key]
		slices.SortStableFunc(set, func(a, b embedrun.TargetWrite) int {
			return a.Ordinal - b.Ordinal
		})
		sets = append(sets, set)
	}
	return sets
}

// applySet makes one source key's stored rows equal to the set it produced.
//
// Two statements and not one: the members are written, and the rows the new set
// does not have are removed. Going from four chunks to three and going from
// four to none are the same operation with different arguments, which is why
// there is no separate delete path (ADR 0017 section 3.4).
//
// It takes the set rather than the resolution's answer, and that is safe for
// one reason worth naming: ResolveWrite reports a change only when the incoming
// write wins, and the incoming write it was asked about is this set's first
// member. Every answer it gives that is NOT the incoming one -- a tombstone
// surviving a late update, the same work arriving again, a version the row has
// moved past -- reports no change, and the caller does not reach here.
func (t *Target) applySet(
	ctx context.Context, transaction *sql.Tx, set []embedrun.TargetWrite,
) error {
	for _, member := range set {
		if err := t.applyWrite(ctx, transaction, member); err != nil {
			return err
		}
	}
	return t.removeSurplus(ctx, transaction, set[0].Key, len(set))
}

// removeSurplus deletes the rows of a source key's set beyond its new length.
//
// Only under a layout whose relation is the generation's own: the other layout
// stores one vector in the source row's own columns, so there is no surplus row
// to delete and a DELETE there would remove the application's data.
//
// A set that shrank from four chunks to three leaves a fourth row that no
// source text produces any more, and verification reports it as a target row
// outside the generation's scope -- forever, because nothing else would ever
// visit it.
func (t *Target) removeSurplus(
	ctx context.Context, transaction *sql.Tx, key []string, length int,
) error {
	if !t.spec.Target.Layout.OwnsTable() {
		return nil
	}
	arguments := make([]any, 0, len(key)+1)
	conditions := make([]string, 0, len(t.spec.Source.KeyFields)+1)
	for index, field := range t.spec.Source.KeyFields {
		arguments = append(arguments, key[index])
		conditions = append(conditions,
			fmt.Sprintf("%s::text = $%d", quoteIdentifier(field), len(arguments)))
	}
	arguments = append(arguments, length)
	conditions = append(conditions, fmt.Sprintf("%s >= $%d",
		quoteIdentifier(t.spec.Target.Column+OrdinalSuffix), len(arguments)))
	// #nosec G201 -- identifiers from the specification, through quoteIdentifier.
	statement := fmt.Sprintf("DELETE FROM %s WHERE %s",
		t.qualifiedTable(), strings.Join(conditions, " AND "))
	if _, err := transaction.ExecContext(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("remove the surplus chunks of %v: %w", key, err)
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
	// Under a layout that can hold a set, only its first member is read. The
	// rules the resolution applies are about the source row, and every member
	// carries the same generation, version and hash -- so reading the rest
	// would be reading the same answer several times and then having to decide
	// which copy to believe.
	representative := ""
	if t.spec.Target.Layout.OwnsTable() {
		representative = fmt.Sprintf(" AND %s = 0",
			quoteIdentifier(column+OrdinalSuffix))
	}
	// #nosec G201 -- identifiers from the specification, through quoteIdentifier.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE (%s) IN (%s)%s FOR UPDATE",
		strings.Join(selected, ", "), t.qualifiedTable(),
		strings.Join(casted, ", "), strings.Join(tuples, ", "), representative)

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

// upsertStatement renders one row's write, in the shape the generation's
// layout needs.
//
// Under LayoutSourceColumns it is an UPDATE and not an INSERT: a generation
// adds a column to rows that already exist, so the row is there and the vector
// is what is missing, and inserting would create a second row for a key the
// source has once.
//
// Under LayoutOwnTable there is no row yet. Ptah creates that relation empty
// and the rows arrive as the generation is backfilled, so an UPDATE there
// would match nothing and a backfill would report success having written no
// vectors at all.
func (t *Target) upsertStatement(write embedrun.TargetWrite) (string, []any) {
	arguments := []any{
		vectorLiteral(write.Vector), write.Generation, write.InputHash,
		nullable(write.Version), string(write.Kind),
	}
	if t.spec.Target.Layout.OwnsTable() {
		return t.insertStatement(write, arguments)
	}
	conditions := make([]string, 0, len(t.spec.Source.KeyFields))
	for index, key := range t.spec.Source.KeyFields {
		arguments = append(arguments, write.Key[index])
		conditions = append(conditions, fmt.Sprintf("%s::text = $%d", quoteIdentifier(key), len(arguments)))
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		t.qualifiedTable(), strings.Join(t.stateAssignments(), ", "),
		strings.Join(conditions, " AND "))
	return query, arguments
}

// insertStatement renders one row's write into a relation of the generation's
// own.
//
// The key is SELECTed out of the source rather than bound as a parameter. A
// write carries its key as text, because that is the one rendering every key
// type has, and inserting it into a column the application declared BIGINT, as
// a domain, or with a collation would need Ptah to know how to cast text into
// whichever of those it is. The source column already holds the value in its
// own type, so the row's key is taken from there and no mapping exists to be
// wrong. The same `::text` comparison the UPDATE above uses is what finds it,
// so the two paths address a row identically.
//
// A source row that is gone inserts nothing, which is the right answer twice
// over: the foreign key would refuse the row anyway, and a write arriving for
// a key the source no longer has must not create storage for it. That includes
// a tombstone -- under this layout the deletion already took the target row
// with it, and the foreign key is what stops a late write from putting it
// back, so there is nothing for a tombstone to hold open.
func (t *Target) insertStatement(write embedrun.TargetWrite, arguments []any) (string, []any) {
	keys := t.spec.Source.KeyFields
	columns := make([]string, 0, len(keys)+5)
	selected := make([]string, 0, len(keys)+5)
	for _, key := range keys {
		columns = append(columns, quoteIdentifier(key))
		selected = append(selected, "source."+quoteIdentifier(key))
	}
	column := t.spec.Target.Column
	columns = append(columns,
		quoteIdentifier(column), quoteIdentifier(column+GenerationSuffix),
		quoteIdentifier(column+InputHashSuffix), quoteIdentifier(column+VersionSuffix),
		quoteIdentifier(column+StateSuffix))
	selected = append(selected, "$1::vector", "$2", "$3", "$4", "$5")

	conditions := make([]string, 0, len(keys))
	for index, key := range keys {
		arguments = append(arguments, write.Key[index])
		conditions = append(conditions,
			fmt.Sprintf("source.%s::text = $%d", quoteIdentifier(key), len(arguments)))
	}
	// The ordinal is a written column and part of the conflict target, which is
	// the whole of what lets one source row hold several vectors: without it in
	// the conflict target, the second chunk of a row updates the first.
	arguments = append(arguments, write.Ordinal)
	ordinal := quoteIdentifier(column + OrdinalSuffix)
	columns = append(columns, ordinal)
	selected = append(selected, fmt.Sprintf("$%d", len(arguments)))
	keyList := make([]string, 0, len(keys)+1)
	for _, key := range keys {
		keyList = append(keyList, quoteIdentifier(key))
	}
	keyList = append(keyList, ordinal)
	assignments := make([]string, 0, 5)
	for _, suffix := range append([]string{""}, MetadataSuffixes()...) {
		name := quoteIdentifier(column + suffix)
		assignments = append(assignments, name+" = EXCLUDED."+name)
	}
	// #nosec G201 -- identifiers from the specification, through quoteIdentifier.
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s AS source WHERE %s "+
			"ON CONFLICT (%s) DO UPDATE SET %s",
		t.qualifiedTable(), strings.Join(columns, ", "), strings.Join(selected, ", "),
		qualify(t.spec.Source.Schema, t.spec.Source.Table), strings.Join(conditions, " AND "),
		strings.Join(keyList, ", "), strings.Join(assignments, ", "))
	return query, arguments
}

// stateAssignments is what a write sets, for the UPDATE path.
func (t *Target) stateAssignments() []string {
	column := t.spec.Target.Column
	return []string{
		fmt.Sprintf("%s = $1::vector", quoteIdentifier(column)),
		fmt.Sprintf("%s = $2", quoteIdentifier(column+GenerationSuffix)),
		fmt.Sprintf("%s = $3", quoteIdentifier(column+InputHashSuffix)),
		fmt.Sprintf("%s = $4", quoteIdentifier(column+VersionSuffix)),
		fmt.Sprintf("%s = $5", quoteIdentifier(column+StateSuffix)),
	}
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
	// Batch commits are ordinary progress writes. A terminal status may only be
	// established by AbandonRun or retirement, where generation membership is
	// checked under its lifecycle lock; accepting it here would turn a stale
	// worker snapshot into an unchecked second terminalization path. A retired
	// phase with a nonterminal status is the inverse invalid state and would
	// remain claimable after its corpus was declared gone.
	if run.Terminal() {
		return fmt.Errorf("save run %s with target writes: %w: run is %s",
			run.ID, embedrun.ErrTerminal, run.Status)
	}
	if run.Phase == embedrun.PhaseRetired {
		return fmt.Errorf("save run %s with target writes: %w: phase %s requires terminal retirement",
			run.ID, embedrun.ErrPhase, run.Phase)
	}
	if err := validateRunResume(run); err != nil {
		return err
	}
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
