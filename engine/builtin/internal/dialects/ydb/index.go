package ydb

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbcomment"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// indexClause is an index as YQL writes it after the table, in CREATE TABLE
// and in ALTER TABLE ... ADD INDEX alike:
//
//	INDEX `name` GLOBAL [UNIQUE] SYNC|ASYNC ON (`a`, `b`) [COVER (`c`)]
//	INDEX `name` GLOBAL USING vector_kmeans_tree ON ([`prefix`, ] `v`) [COVER (`c`)] WITH (...)
type indexClause struct {
	name    string
	kind    ydbindex.Kind
	unique  bool
	columns []string
	cover   []string
	// settings is what the ALTER INDEX after the index names: each setting
	// the declaration names. YDB takes no partitioning in the clause that
	// creates an index, so the settings are written by an ALTER INDEX of
	// their own after it; see [indexClause.partitioningStatement].
	settings []string
	// vector is a vector index's settings, resolved; it is written as the
	// clause's WITH (...), the one place YDB takes them.
	vector ast.VectorIndexSpec
	// fullText holds normalized text-analysis options for the WITH clause.
	fullText map[string]string
	local    map[string]string
}

func (c indexClause) String() string {
	quoted := func(names []string) string {
		out := make([]string, 0, len(names))
		for _, name := range names {
			out = append(out, quote(name))
		}
		return strings.Join(out, ", ")
	}
	clause := fmt.Sprintf("INDEX %s %s ON (%s)", quote(c.name), c.kind.Clause(c.unique), quoted(c.columns))
	if len(c.cover) > 0 {
		clause += " COVER (" + quoted(c.cover) + ")"
	}
	if c.kind == ydbindex.Vector {
		clause += " " + ydbindex.VectorClause(c.vector)
	}
	if c.kind.IsLocal() && len(c.local) > 0 {
		clause += " " + ydbindex.LocalClause(c.local)
	}
	if c.kind.IsFullText() {
		clause += " " + ydbindex.FullTextClause(c.fullText)
	}
	return clause
}

// shape is the clause as [ydbindex.ShapeRefusal] reads it.
func (c indexClause) shape() ydbindex.Shape {
	return ydbindex.Shape{Kind: c.kind, Columns: c.columns, Cover: c.cover, Dimension: c.vector.Dimension}
}

// partitioningStatement is the ALTER INDEX that gives a new index on table the
// partitioning it declares, or "" for an index that declares none. A setting
// the declaration leaves out is the one YDB gives the new index.
//
// It is a statement of its own because no clause that creates an index takes
// the settings. Measured on 25.1.4.7 and 26.2.1.14, `INDEX i GLOBAL ON (v)
// WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3)` answers `Unknown index
// setting: auto_partitioning_min_partitions_count` on 26.2 and `with:
// alternative is not implemented yet` on 25.1, inline in CREATE TABLE and in
// ADD INDEX alike, while ALTER INDEX ... SET takes it once the index exists.
func (c indexClause) partitioningStatement(table string) string {
	if len(c.settings) == 0 {
		return ""
	}
	return alterIndexSet(table, c.name, c.settings)
}

// alterIndexSet writes `ALTER TABLE t ALTER INDEX i SET (...)` with settings.
func alterIndexSet(table, index string, settings []string) string {
	return fmt.Sprintf("ALTER TABLE %s ALTER INDEX %s SET (%s);", tablePath(table), quote(index), strings.Join(settings, ", "))
}

// indexSettings writes the settings a new index's declared partitioning
// names, refusing it on a target without [capability.IndexPartitioning] and
// refusing what YDB would refuse whatever the index holds.
func (r *Renderer) indexSettings(subject string, spec *ast.IndexPartitioningSpec) ([]string, error) {
	if spec.IsZero() {
		return nil, nil
	}
	if !r.caps.Has(capability.IndexPartitioning) {
		return nil, refuseKey(capability.IndexPartitioning, subject+" declares its partitioning")
	}
	if _, err := ydbindex.Resolve(spec, ydbpartition.DefaultSettings()); err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	return ydbindex.CreateClause(spec), nil
}

// indexClauseOf reads an index node into the clause YDB takes, refusing what
// the clause cannot say. It does not know the table's key; a caller that does
// checks the key against the clause.
func (r *Renderer) indexClauseOf(index *ast.IndexNode) (indexClause, error) {
	if index == nil {
		return indexClause{}, refuseFact("an index", "the index node is nil")
	}
	subject := fmt.Sprintf("index %q", index.Name)
	if strings.TrimSpace(index.Name) == "" {
		return indexClause{}, refuseFact("an index on "+index.Table, "YDB names every index, and this one has no name")
	}
	kind, err := ydbindex.KindOf(index.Type)
	if err != nil {
		return indexClause{}, refuseFact(subject, err.Error())
	}
	if err := r.refuseIndexDeclarations(subject, index, kind); err != nil {
		return indexClause{}, err
	}
	switch {
	case kind == ydbindex.Async && !r.caps.Has(capability.AsyncIndexes):
		return indexClause{}, refuseKey(capability.AsyncIndexes, subject+" is asynchronous")
	case kind == ydbindex.Async && index.Unique:
		return indexClause{}, refuseFact(subject,
			"a unique index is synchronous on YDB (`GLOBAL UNIQUE ASYNC` answers `unique: alternative is not implemented yet`)")
	case len(index.IncludeColumns) > 0 && !r.caps.Has(capability.IndexCoveringColumns):
		return indexClause{}, refuseKey(capability.IndexCoveringColumns, subject+" covers columns")
	}
	columns, err := indexColumns(subject, index)
	if err != nil {
		return indexClause{}, err
	}
	for _, covered := range index.IncludeColumns {
		if slices.Contains(columns, covered) {
			return indexClause{}, refuseFact(subject,
				fmt.Sprintf("column %q is both a key and a covered column, which YDB refuses", covered))
		}
	}
	clause := indexClause{
		name: index.Name, kind: kind, unique: index.Unique,
		columns: columns, cover: slices.Clone(index.IncludeColumns),
	}
	if kind == ydbindex.Vector {
		clause.vector, err = r.vectorSettings(subject, index)
		return clause, err
	}
	if kind.IsLocal() {
		clause.local, err = r.localIndexSettings(subject, kind, index)
		return clause, err
	}
	if kind.IsFullText() {
		clause.fullText, err = r.fullTextSettings(subject, index)
		return clause, err
	}
	clause.settings, err = r.indexSettings(subject, index.Partitioning)
	if err != nil {
		return indexClause{}, err
	}
	return clause, nil
}

// vectorSettings resolves a vector index's settings, refusing the index on a
// target without [capability.VectorIndexes], and refusing what a vector index
// cannot be.
func (r *Renderer) vectorSettings(subject string, index *ast.IndexNode) (ast.VectorIndexSpec, error) {
	switch {
	case !r.caps.Has(capability.VectorIndexes):
		return ast.VectorIndexSpec{}, refuseKey(capability.VectorIndexes, subject+" is a vector index")
	case index.Unique:
		return ast.VectorIndexSpec{}, refuseFact(subject,
			"a vector index is not unique (`VECTOR_KMEANS_TREE index can only be GLOBAL [SYNC]`)")
	case !index.Partitioning.IsZero():
		return ast.VectorIndexSpec{}, refuseFact(subject, "a vector index keeps the partitioning YDB gives it "+
			"(`ALTER INDEX ... SET` answers `Only index with one impl table is supported`)")
	}
	if names := slices.Sorted(maps.Keys(index.StorageParams)); len(names) > 0 {
		return ast.VectorIndexSpec{}, refuseFact(subject, ydbindex.StorageParameterRefusal(names[0]))
	}
	spec, err := ydbindex.ResolveVector(index.Vector, index.Operator)
	if err != nil {
		return ast.VectorIndexSpec{}, refuseFact(subject, err.Error())
	}
	if spec.VectorType == ydbindex.BitVectorType && !r.caps.Has(capability.VectorBitType) {
		return ast.VectorIndexSpec{}, refuseKey(capability.VectorBitType, subject+" stores bit vectors")
	}
	return spec, nil
}

// refuseIndexDeclarations refuses what an index may say that a YDB index of
// kind has no clause for. A vector index's operator class and storage
// parameters are read by [Renderer.vectorSettings] instead.
func (r *Renderer) refuseIndexDeclarations(subject string, index *ast.IndexNode, kind ydbindex.Kind) error {
	vector := kind == ydbindex.Vector
	switch {
	case strings.TrimSpace(index.Condition) != "":
		return refuseFact(subject, "YDB has no partial index")
	case index.NullsDistinct != nil && !*index.NullsDistinct:
		return r.keyed(capability.UniqueNullsDistinctClause, "NULLS NOT DISTINCT",
			subject+" treats NULLs as equal; a YDB unique index treats them as distinct")
	case index.Invisible:
		return r.keyed(capability.InvisibleIndexes, "invisible index", subject+" is invisible")
	case index.Parser != "":
		return refuseFact(subject, "a FULLTEXT parser is MySQL's")
	case index.Operator != "" && !vector:
		return refuseFact(subject, "YDB has no index operator class")
	case index.Granularity != 0:
		return refuseFact(subject, "GRANULARITY is ClickHouse's")
	case len(index.StorageParams) > 0 && !vector && !kind.IsFullText() && !kind.IsLocal():
		return refuseFact(subject, "a YDB global index takes no storage parameters; its settings are its "+
			"partitioning and read replicas, declared with the auto_partitioning_* and read_replicas_settings attributes")
	case index.Vector != nil && !vector:
		return refuseFact(subject, fmt.Sprintf("it declares vector settings and is a %s index; declare type %q "+
			"for a vector index", kind, ydbindex.VectorMethod))
	}
	return nil
}

// indexColumns are the index's key columns. A YDB index key is a list of
// columns: an expression, an order, a prefix or a NULLS order has no spelling.
func indexColumns(subject string, index *ast.IndexNode) ([]string, error) {
	parts := index.EffectiveParts()
	if len(parts) == 0 {
		return nil, refuseFact(subject, "the index names no column")
	}
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		switch {
		case part.Expr != "":
			return nil, refuseFact(subject, "YDB has no expression index")
		case part.Desc:
			return nil, refuseFact(subject, "a YDB index column has no order")
		case part.Prefix != "":
			return nil, refuseFact(subject, "a YDB index column has no prefix length")
		case part.NullsOrder != "":
			return nil, refuseFact(subject, "a YDB index column has no NULLS order")
		case part.Operator != "":
			return nil, refuseFact(subject, "YDB has no index operator class")
		}
		columns = append(columns, part.Name)
	}
	return columns, nil
}

// inlineIndex reads an index declared inside CREATE TABLE, where the table's
// key and column types are known and the index is held to
// [ydbindex.ShapeRefusal].
func (r *Renderer) inlineIndex(table string, index *ast.IndexNode, keyColumns []string, columns map[string]ydbindex.Column) (indexClause, error) {
	clause, err := r.indexClauseOf(index)
	if err != nil {
		return indexClause{}, err
	}
	column := func(name string) (ydbindex.Column, bool) {
		declared, ok := columns[name]
		return declared, ok
	}
	if reason := ydbindex.ShapeRefusal(clause.shape(), keyColumns, column); reason != "" {
		return indexClause{}, refuseFact(fmt.Sprintf("index %q on %s", index.Name, tableref.Phrase(table)), reason)
	}
	return clause, nil
}

// renderIndex writes an index added to a table that exists, as ALTER TABLE
// ... ADD INDEX: YDB has no CREATE INDEX statement (`no viable alternative at
// input 'CREATE INDEX'`). A unique one is refused on a target without
// [capability.UniqueIndexOnExistingTable]; a new table's unique index is
// written inside its CREATE TABLE instead, where every line accepts it.
//
// The index names its own table here, so an index naming none is refused here.
// An index an ALTER TABLE carries takes the ALTER's table instead, and that
// path does not repeat the check: the ALTER is what names the table.
func (r *Renderer) renderIndex(index *ast.IndexNode) error {
	if index != nil && strings.TrimSpace(index.Table) == "" {
		return refuseFact(fmt.Sprintf("index %q", index.Name), "YDB adds an index through its table, and the index names none")
	}
	statements, err := r.addIndexStatements(index)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		r.w.WriteLine(statement)
	}
	return nil
}

// addIndexStatements are the ALTER TABLE ... ADD INDEX renderIndex writes, and
// the ALTER INDEX that gives the index its partitioning after it.
func (r *Renderer) addIndexStatements(index *ast.IndexNode) ([]string, error) {
	if index == nil {
		return nil, refuseFact("an index", "the index node is nil")
	}
	subject := fmt.Sprintf("index %q", index.Name)
	switch {
	case index.IfNotExists:
		return nil, refuseFact(subject, "YDB's ADD INDEX has no IF NOT EXISTS guard")
	case index.Unique && !r.caps.Has(capability.UniqueIndexOnExistingTable):
		return nil, refuseKey(capability.UniqueIndexOnExistingTable, fmt.Sprintf(
			"unique %s is added to %s, which exists already (declare it with the table, or enable the flag on the cluster)",
			subject, tableref.Phrase(index.Table)))
	}
	clause, err := r.indexClauseOf(index)
	if err != nil {
		return nil, err
	}
	statements := []string{fmt.Sprintf("ALTER TABLE %s ADD %s;", tablePath(index.Table), clause)}
	if partitioning := clause.partitioningStatement(index.Table); partitioning != "" {
		statements = append(statements, partitioning)
	}
	if index.Comment != "" {
		statement := ydbcomment.Statement{Object: ydbcomment.Index, Path: objectPath(index.Table), Name: index.Name, Comment: index.Comment}
		comment, err := r.commentStatement(statement, fmt.Sprintf("%s of %s", subject, tableref.Phrase(index.Table)))
		if err != nil {
			return nil, err
		}
		statements = append(statements, comment)
	}
	return statements, nil
}

// setIndexPartitioning writes the ALTER INDEX that changes an existing index's
// partitioning in place: the settings op.Partitioning names, over the ones
// op.Previous says the index holds.
func (r *Renderer) setIndexPartitioning(table string, op *ast.SetIndexPartitioningOperation) ([]string, error) {
	subject := fmt.Sprintf("index %q of %s", op.IndexName, tableref.Phrase(table))
	if strings.TrimSpace(op.IndexName) == "" {
		return nil, refuseFact(tableref.Phrase(table), "ALTER INDEX ... SET names no index")
	}
	if !r.caps.Has(capability.IndexPartitioning) {
		return nil, refuseKey(capability.IndexPartitioning, "changing the partitioning of "+subject)
	}
	previous, err := ydbindex.Held(op.Previous)
	if err != nil {
		return nil, refuseFact(subject, "the settings it holds: "+err.Error())
	}
	desired, err := ydbindex.Resolve(op.Partitioning, previous)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	clause := ydbpartition.Clause(desired, previous)
	if len(clause) == 0 {
		return nil, nil
	}
	return []string{alterIndexSet(table, op.IndexName, clause)}, nil
}

// renderDropIndex drops an index through its table, which is the only spelling
// YDB has: `DROP INDEX` is a parse error. There is no IF EXISTS on it
// (`mismatched input 'EXISTS'`).
func (r *Renderer) renderDropIndex(node *ast.DropIndexNode) error {
	subject := fmt.Sprintf("DROP INDEX %s", node.Name)
	switch {
	case strings.TrimSpace(node.Table) == "":
		return refuseFact(subject, "YDB drops an index through its table, and the statement names none")
	case node.IfExists:
		return r.keyed(capability.DropIndexIfExists, "guarded index drop", subject+" IF EXISTS")
	case node.Cascade:
		return refuseFact(subject, "YDB's DROP INDEX has no CASCADE")
	}
	r.w.WriteLinef("ALTER TABLE %s DROP INDEX %s;", tablePath(node.Table), quote(node.Name))
	return nil
}

// fullTextSettings validates full-text options before the renderer writes DDL.
func (r *Renderer) fullTextSettings(subject string, index *ast.IndexNode) (map[string]string, error) {
	switch {
	case !r.caps.Has(capability.FullTextIndexes):
		return nil, refuseKey(capability.FullTextIndexes, subject+" is a full-text index")
	case index.Unique:
		return nil, refuseFact(subject, "a YDB full-text index cannot be unique")
	case len(index.EffectiveParts()) != 1:
		return nil, refuseFact(subject, "a YDB full-text index requires exactly one text column; this release does not support prefix columns")
	case !index.Partitioning.IsZero():
		return nil, refuseFact(subject, "Ptah does not alter a full-text index's internal table partitioning")
	}
	options, err := ydbindex.ResolveFullText(index.StorageParams)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	return options, nil
}

func (r *Renderer) localIndexSettings(subject string, kind ydbindex.Kind, index *ast.IndexNode) (map[string]string, error) {
	if !r.caps.Has(kind.LocalCapability()) {
		return nil, refuseKey(kind.LocalCapability(), subject)
	}
	if index.Unique || len(index.IncludeColumns) > 0 || !index.Partitioning.IsZero() {
		return nil, refuseFact(subject, "local indexes cannot be unique, covering or independently partitioned")
	}
	return ydbindex.ResolveLocal(kind, index.StorageParams)
}
