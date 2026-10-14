package embedydb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/embeddigest"
	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/ydbcomment"
)

// The bookkeeping columns beside a generation's vector, as suffixes of its
// column name. They are the PostgreSQL layout's names, so one specification
// describes the same columns on either engine.
const (
	generationSuffix = "_generation"
	inputHashSuffix  = "_input_hash"
	versionSuffix    = "_source_version"
	stateSuffix      = "_state"
	ordinalSuffix    = "_chunk_ordinal"
)

// metadataSuffixes lists the four Utf8 columns in the order they are created.
func metadataSuffixes() []string {
	return []string{generationSuffix, inputHashSuffix, versionSuffix, stateSuffix}
}

// targetColumns are the five columns a generation adds, with their YQL types.
func targetColumns(column string) []ydbschema.RowColumn {
	columns := []ydbschema.RowColumn{{Name: column, Type: ColumnType, Nullable: true}}
	for _, suffix := range metadataSuffixes() {
		columns = append(columns, ydbschema.RowColumn{Name: column + suffix, Type: "Utf8", Nullable: true})
	}
	return columns
}

// ensureTarget creates the generation's storage: five columns on the target
// table, or a table of the generation's own.
//
// Idempotent, because a restarted prepare runs it again. YDB has no ADD
// COLUMN IF NOT EXISTS, so the table is described and only what is missing is
// added; a column of the right name and another type is refused rather than
// written into.
func (b *Backend) ensureTarget(ctx context.Context, spec embedgen.Spec) error {
	objects, err := TargetObjects(spec)
	if err != nil {
		return err
	}
	if objects.OwnsTable {
		return b.ensureOwnedTable(ctx, spec, objects)
	}
	described, exists, err := b.describe(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("target table %s does not exist, so the generation's column has nowhere to go",
			tablePath(spec.Target.Schema, spec.Target.Table))
	}
	if _, err := keyOf(described, spec.Source.KeyFields,
		tablePath(spec.Target.Schema, spec.Target.Table)); err != nil {
		return err
	}
	var additions []string
	for _, wanted := range targetColumns(spec.Target.Column) {
		present, found := described.Column(wanted.Name)
		switch {
		case !found:
			additions = append(additions, "ADD COLUMN "+quote(wanted.Name)+" "+wanted.Type)
		case present.Type != wanted.Type:
			return fmt.Errorf("column %s on %s is %s, and a generation needs it to be %s",
				wanted.Name, tablePath(spec.Target.Schema, spec.Target.Table), present.Type, wanted.Type)
		}
	}
	if len(additions) > 0 {
		statement := "ALTER TABLE " + quoteTable(spec.Target.Schema, spec.Target.Table) + " " +
			strings.Join(additions, ", ")
		if _, err := b.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create the target columns: %w", err)
		}
		if err := b.comment(ctx, ydbcomment.Statement{
			Object: ydbcomment.Column, Path: tablePath(spec.Target.Schema, spec.Target.Table),
			Name: spec.Target.Column, Comment: objects.Column.Comment,
		}); err != nil {
			return fmt.Errorf("comment the target column: %w", err)
		}
	}
	return b.refuseAnotherGenerationsColumn(ctx, spec)
}

// comment writes a comment through Ptah's YDB comment statement, which YDB
// keeps as an attribute of the object.
func (b *Backend) comment(ctx context.Context, statement ydbcomment.Statement) error {
	text, err := statement.Text()
	if err != nil {
		return err
	}
	_, err = b.db.ExecContext(ctx, text)
	return err
}

// refuseAnotherGenerationsColumn refuses a column that already holds another
// generation's vectors, before any embedding is paid for. Two generations
// over one table need two columns, which is what makes a cutover a pointer
// move rather than a data migration.
func (b *Backend) refuseAnotherGenerationsColumn(ctx context.Context, spec embedgen.Spec) error {
	column := quote(spec.Target.Column + generationSuffix)
	// #nosec G201 -- names from the specification, through quote.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL AND %s != $p1 LIMIT 1",
		column, quoteTable(spec.Target.Schema, spec.Target.Table), column, column)
	var occupant string
	switch err := b.db.QueryRowContext(ctx, query, spec.Identity().Digest).Scan(&occupant); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("read the target generation: %w", err)
	}
	return fmt.Errorf(
		"column %q on %s holds generation %s, and this run is generation %s: a generation "+
			"writes its own column so the previous one is still there to go back to. "+
			"Give this one its own target.column in the specification",
		spec.Target.Column, spec.Target.Table, embeddigest.Short(occupant), spec.Identity().Short())
}

// ensureOwnedTable creates the table an own-table generation stores its
// vectors in, keyed by the source key and the chunk ordinal.
//
// The key columns take the source's own types, read from the source table,
// so a row's key means the same thing on both. There is no reference to the
// source: YDB has no foreign keys, so deleting a source row does not take its
// vectors with it, and verification reports such a row as one outside the
// generation's scope.
//
// The comment [embedgen.TableComment] writes is what makes the table Ptah's
// to drop at retirement. YDB runs CREATE TABLE and the comment as two scheme
// operations, so a prepare stopped between them leaves a table without one;
// such a table is adopted only when it is empty and has exactly the columns
// this generation creates, and every other table without the comment is
// refused as somebody else's.
func (b *Backend) ensureOwnedTable(ctx context.Context, spec embedgen.Spec, objects embedgen.TargetObjects) error {
	key, err := b.sourceKey(ctx, spec)
	if err != nil {
		return err
	}
	wanted := ownedColumns(key, spec.Target.Column)
	name := tablePath(spec.Target.Schema, spec.Target.Table)
	described, exists, err := b.describe(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil {
		return err
	}
	if !exists {
		if _, err := b.db.ExecContext(ctx, createOwnedTable(spec, key)); err != nil {
			return fmt.Errorf("create the target table %s: %w", name, err)
		}
	} else if found := described.Attributes[commentKey(ydbcomment.Table, "")]; found != objects.TableComment {
		if found != "" || !slices.Equal(described.Columns, wanted) {
			return fmt.Errorf(
				"%w: table %s is not Ptah's: a generation of layout %q creates its own table and drops it "+
					"at retirement, and this one carries the comment %q rather than %q",
				embedstore.ErrConflict, name, string(embedgen.LayoutOwnTable), found, objects.TableComment)
		}
		empty, err := b.isEmpty(ctx, spec.Target.Schema, spec.Target.Table)
		if err != nil {
			return err
		}
		if !empty {
			return fmt.Errorf("%w: table %s has the shape a generation creates but no comment naming one, "+
				"and it holds rows, so it is not Ptah's to adopt", embedstore.ErrConflict, name)
		}
	}
	if err := b.comment(ctx, ydbcomment.Statement{
		Object: ydbcomment.Table, Path: name, Comment: objects.TableComment,
	}); err != nil {
		return fmt.Errorf("mark the target table %s as the generation's: %w", name, err)
	}
	return nil
}

// commentKey is the attribute a comment statement for object writes.
func commentKey(object ydbcomment.Object, name string) string {
	return ydbcomment.Statement{Object: object, Name: name}.Key()
}

// ownedColumns are an owned table's columns in the order it declares them.
func ownedColumns(key []keyColumn, column string) []ydbschema.RowColumn {
	columns := make([]ydbschema.RowColumn, 0, len(key)+6)
	for _, part := range key {
		columns = append(columns, ydbschema.RowColumn{Name: part.name, Type: part.ydbType, Nullable: part.nullable})
	}
	columns = append(columns, ydbschema.RowColumn{Name: column + ordinalSuffix, Type: "Int64"})
	return append(columns, targetColumns(column)...)
}

// createOwnedTable renders the CREATE TABLE of an owned table.
func createOwnedTable(spec embedgen.Spec, key []keyColumn) string {
	definitions := make([]string, 0, len(key)+6)
	primary := make([]string, 0, len(key)+1)
	for _, column := range ownedColumns(key, spec.Target.Column) {
		definition := quote(column.Name) + " " + column.Type
		if !column.Nullable {
			definition += " NOT NULL"
		}
		definitions = append(definitions, definition)
	}
	for _, part := range key {
		primary = append(primary, quote(part.name))
	}
	primary = append(primary, quote(spec.Target.Column+ordinalSuffix))
	return fmt.Sprintf("CREATE TABLE %s (%s, PRIMARY KEY (%s))",
		quoteTable(spec.Target.Schema, spec.Target.Table), strings.Join(definitions, ", "),
		strings.Join(primary, ", "))
}

// isEmpty reports whether a table holds no rows.
func (b *Backend) isEmpty(ctx context.Context, schema, table string) (bool, error) {
	var count uint64
	// #nosec G201 -- the path is the specification's, through quoteTable.
	query := "SELECT COUNT(*) FROM " + quoteTable(schema, table)
	if err := b.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("count the rows of %s: %w", tablePath(schema, table), err)
	}
	return count == 0, nil
}

// requirePreparedTarget checks that a prepared generation's columns are still
// there, which is what an idempotent prepare retry has to establish beyond the
// recorded state.
func (b *Backend) requirePreparedTarget(ctx context.Context, spec embedgen.Spec) error {
	described, exists, err := b.describe(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil {
		return err
	}
	name := tablePath(spec.Target.Schema, spec.Target.Table)
	if !exists {
		return fmt.Errorf("%w: prepared target table %s is absent", embedstore.ErrConflict, name)
	}
	for _, column := range targetColumns(spec.Target.Column) {
		if _, found := described.Column(column.Name); !found {
			return fmt.Errorf("%w: prepared target column %s.%s is absent", embedstore.ErrConflict, name, column.Name)
		}
	}
	return nil
}

// Target is [embedengine.Target] over a YDB table.
type Target struct {
	backend *Backend
	spec    embedgen.Spec

	once sync.Once
	// key is the source key as the target table holds it, and indexed
	// whether the generation's index exists, both read on the first commit.
	key     []keyColumn
	indexed bool
	err     error
}

// NewTarget returns a target for a specification.
func (b *Backend) NewTarget(spec embedgen.Spec) (embedengine.Target, error) {
	if _, err := TargetObjects(spec); err != nil {
		return nil, err
	}
	if len(spec.Source.KeyFields) == 0 {
		return nil, errors.New("the specification names no key fields, so a target row cannot be addressed")
	}
	return &Target{backend: b, spec: spec}, nil
}

// describeTarget reads the target table's key and the generation's index once.
func (t *Target) describeTarget(ctx context.Context) error {
	t.once.Do(func() {
		described, exists, err := t.backend.describe(ctx, t.spec.Target.Schema, t.spec.Target.Table)
		switch {
		case err != nil:
			t.err = err
			return
		case !exists:
			t.err = fmt.Errorf("target table %s does not exist; run prepare first",
				tablePath(t.spec.Target.Schema, t.spec.Target.Table))
			return
		}
		key := t.spec.Source.KeyFields
		if t.spec.Target.Layout.OwnsTable() {
			key = append(slices.Clone(key), t.spec.Target.Column+ordinalSuffix)
		}
		t.key, t.err = keyOf(described, key, tablePath(t.spec.Target.Schema, t.spec.Target.Table))
		if t.err == nil && t.spec.Target.Layout.OwnsTable() {
			t.key = t.key[:len(t.spec.Source.KeyFields)]
		}
		_, t.indexed = described.Index(embedgen.IndexName(
			t.spec.Target.Table, t.spec.Target.Column, t.spec.Identity().Digest))
	})
	return t.err
}

// Commit writes a batch's target writes and the run that checkpoints them in
// one serializable transaction.
//
// The run is decided first, as the in-memory store decides a save: a worker
// whose lease was taken carries a stale token and is refused before any row
// it would write is looked at. What the target holds for the batch's keys is
// read in the same transaction, so [embedrun.ResolveWrite] decides against
// what is there; a concurrent writer of the same rows makes YDB abort one of
// the two, and that one decides again.
//
// A write into a generation whose index exists is refused on a line whose
// vector index does not take in later writes: there the index would go on
// answering as it stood at the build while the table moved on.
func (t *Target) Commit(ctx context.Context, writes []embedrun.TargetWrite, run embedrun.Run) error {
	if run.Terminal() {
		return fmt.Errorf("save run %s with target writes: %w: run is %s", run.ID, embedrun.ErrTerminal, run.Status)
	}
	if err := t.describeTarget(ctx); err != nil {
		return err
	}
	return t.backend.inTransaction(ctx, "commit a batch of run "+run.ID, func(tx *sql.Tx) error {
		before, err := t.backend.store.load(ctx, tx, oneRun(run.ID))
		if err != nil {
			return err
		}
		existing, err := t.existingWrites(ctx, tx, writes)
		if err != nil {
			return err
		}
		memory := embedstore.NewMemoryFrom(before, clock)
		if err := memory.SaveRun(ctx, run); err != nil {
			return err
		}
		changed := 0
		for _, set := range setsByKey(writes) {
			_, wins, err := embedrun.ResolveWrite(
				existing[writeKey(set[0].Key)], set[0], t.spec.Source.VersionStrategy.VersionOrder())
			if err != nil {
				return fmt.Errorf("write %v: %w", set[0].Key, err)
			}
			if !wins {
				continue
			}
			if changed == 0 && t.indexed {
				if err := t.backend.require(capability.VectorIndexMaintainedOnWrite,
					"generation "+t.spec.Identity().Short()+"'s index exists, so a write now would leave the "+
						"index answering as it stood at the build"); err != nil {
					return err
				}
			}
			changed++
			if err := t.applySet(ctx, tx, set); err != nil {
				return err
			}
		}
		return t.backend.store.write(ctx, tx, before, stored(memory.Snapshot()))
	})
}

// existingWrites reads what the target holds for a batch's keys: the
// generation, hash, version and state of each, keyed by [writeKey]. A row
// whose generation column is NULL was never written and is absent from the
// answer. Under a layout that holds a set, its first member answers for it.
func (t *Target) existingWrites(
	ctx context.Context, tx *sql.Tx, writes []embedrun.TargetWrite,
) (map[string]*embedrun.TargetWrite, error) {
	held := make(map[string]*embedrun.TargetWrite, len(writes))
	if len(writes) == 0 {
		return held, nil
	}
	var arguments []any
	seen := make(map[string]bool, len(writes))
	tuples := make([]string, 0, len(writes))
	for _, write := range writes {
		if seen[writeKey(write.Key)] {
			continue
		}
		seen[writeKey(write.Key)] = true
		casts := make([]string, len(t.key))
		for index, column := range t.key {
			arguments = append(arguments, write.Key[index])
			casts[index] = column.cast(len(arguments))
		}
		tuples = append(tuples, "("+strings.Join(casts, ", ")+")")
	}
	names := make([]string, len(t.key))
	selected := make([]string, 0, len(t.key)+4)
	for index, column := range t.key {
		names[index] = quote(column.name)
		selected = append(selected, asText("", column.name))
	}
	column := t.spec.Target.Column
	for _, suffix := range metadataSuffixes() {
		selected = append(selected, quote(column+suffix))
	}
	representative := ""
	if t.spec.Target.Layout.OwnsTable() {
		representative = " AND " + quote(column+ordinalSuffix) + " = 0"
	}
	// #nosec G201 -- names from the specification, through quote; key values
	// are parameters.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE (%s) IN (%s)%s",
		strings.Join(selected, ", "), quoteTable(t.spec.Target.Schema, t.spec.Target.Table),
		strings.Join(names, ", "), strings.Join(tuples, ", "), representative)
	rows, err := tx.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("read the target rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		key := make([]sql.NullString, len(t.key))
		var generation, inputHash, version, state sql.NullString
		destinations := make([]any, 0, len(t.key)+4)
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
		components := make([]string, len(key))
		for index, value := range key {
			components[index] = value.String
		}
		held[writeKey(components)] = &embedrun.TargetWrite{
			Key: components, Generation: generation.String, InputHash: inputHash.String,
			Version: version.String, Kind: embedrun.WriteKind(state.String),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the target rows: %w", err)
	}
	return held, nil
}

// applySet makes one source key's stored rows equal to the set it produced:
// the members are written and, under a layout that holds a set, the rows past
// its end are removed.
//
// A tombstone in a table of the generation's own removes the key's rows
// rather than writing one: the PostgreSQL layout's foreign key removes them
// when the source row goes, and YDB has none, so the write path does it.
func (t *Target) applySet(ctx context.Context, tx *sql.Tx, set []embedrun.TargetWrite) error {
	if t.spec.Target.Layout.OwnsTable() && set[0].Kind == embedrun.WriteTombstone {
		return t.removeFrom(ctx, tx, set[0].Key, 0)
	}
	for _, member := range set {
		statement, arguments, err := t.writeStatement(member)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, statement, arguments...); err != nil {
			return fmt.Errorf("write %v: %w", member.Key, err)
		}
	}
	if !t.spec.Target.Layout.OwnsTable() {
		return nil
	}
	return t.removeFrom(ctx, tx, set[0].Key, len(set))
}

// removeFrom deletes a key's stored rows from ordinal from onwards.
func (t *Target) removeFrom(ctx context.Context, tx *sql.Tx, key []string, from int) error {
	arguments := make([]any, 0, len(t.key)+1)
	for index := range t.key {
		arguments = append(arguments, key[index])
	}
	arguments = append(arguments, int64(from))
	// #nosec G201 -- names from the specification, through quote.
	statement := fmt.Sprintf("DELETE FROM %s WHERE %s AND %s >= $p%d",
		quoteTable(t.spec.Target.Schema, t.spec.Target.Table), keyMatch("", t.key, 1),
		quote(t.spec.Target.Column+ordinalSuffix), len(arguments))
	if _, err := tx.ExecContext(ctx, statement, arguments...); err != nil {
		return fmt.Errorf("remove the surplus chunks of %v: %w", key, err)
	}
	return nil
}

// writeStatement renders one row's write in the shape the layout needs.
//
// On the source's own columns it is an UPDATE by the key: the row is there and
// the vector is what is missing, and a key the source no longer has updates
// nothing rather than creating a row. In a table of the generation's own it is
// an UPSERT whose key is selected out of the source, so a write for a source
// row that is gone stores nothing, the answer the PostgreSQL layout's foreign
// key gives.
func (t *Target) writeStatement(write embedrun.TargetWrite) (string, []any, error) {
	column := t.spec.Target.Column
	arguments := []any{write.Generation, write.InputHash, optional(write.Version), string(write.Kind)}
	vector := "CAST(NULL AS String)"
	if len(write.Vector) > 0 {
		encoded, err := EncodeVector(write.Vector)
		if err != nil {
			return "", nil, fmt.Errorf("write %v: %w", write.Key, err)
		}
		arguments = append(arguments, encoded)
		vector = fmt.Sprintf("$p%d", len(arguments))
	}
	first := len(arguments) + 1
	for index := range t.key {
		arguments = append(arguments, write.Key[index])
	}
	if !t.spec.Target.Layout.OwnsTable() {
		// #nosec G201 -- names from the specification, through quote.
		statement := fmt.Sprintf("UPDATE %s SET %s = %s, %s = $p1, %s = $p2, %s = $p3, %s = $p4 WHERE %s",
			quoteTable(t.spec.Target.Schema, t.spec.Target.Table), quote(column), vector,
			quote(column+generationSuffix), quote(column+inputHashSuffix), quote(column+versionSuffix),
			quote(column+stateSuffix), keyMatch("", t.key, first))
		return statement, arguments, nil
	}
	arguments = append(arguments, int64(write.Ordinal))
	columns := make([]string, 0, len(t.key)+6)
	selected := make([]string, 0, len(t.key)+6)
	for _, part := range t.key {
		columns = append(columns, quote(part.name))
		selected = append(selected, qualified("source", part.name)+" AS "+quote(part.name))
	}
	columns = append(columns, quote(column+ordinalSuffix), quote(column), quote(column+generationSuffix),
		quote(column+inputHashSuffix), quote(column+versionSuffix), quote(column+stateSuffix))
	selected = append(selected,
		fmt.Sprintf("$p%d AS %s", len(arguments), quote(column+ordinalSuffix)),
		vector+" AS "+quote(column),
		"$p1 AS "+quote(column+generationSuffix), "$p2 AS "+quote(column+inputHashSuffix),
		"$p3 AS "+quote(column+versionSuffix), "$p4 AS "+quote(column+stateSuffix))
	// #nosec G201 -- names from the specification, through quote.
	statement := fmt.Sprintf("UPSERT INTO %s (%s) SELECT %s FROM %s AS source WHERE %s",
		quoteTable(t.spec.Target.Schema, t.spec.Target.Table), strings.Join(columns, ", "),
		strings.Join(selected, ", "), quoteTable(t.spec.Source.Schema, t.spec.Source.Table),
		keyMatch("source", t.key, first))
	return statement, arguments, nil
}

// setsByKey groups a batch's writes into one set per source key, in the order
// the keys first appear, each set in ordinal order.
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
		slices.SortStableFunc(set, func(a, b embedrun.TargetWrite) int { return a.Ordinal - b.Ordinal })
		sets = append(sets, set)
	}
	return sets
}

// writeKey renders a composite key as one map key. JSON rather than a join,
// because a Utf8 key component may hold any character a separator could, and
// two keys that render alike would be decided as one row.
func writeKey(key []string) string {
	encoded, err := json.Marshal(key)
	if err != nil {
		// A slice of strings always marshals; this branch is unreachable.
		return strings.Join(key, "\x00")
	}
	return string(encoded)
}
