package ydb

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbgap"
	"ptah.run/internal/ydbindex"
)

// indexClause is an index as YQL writes it after the table, in CREATE TABLE
// and in ALTER TABLE ... ADD INDEX alike:
//
//	INDEX `name` GLOBAL [UNIQUE] SYNC|ASYNC ON (`a`, `b`) [COVER (`c`)]
type indexClause struct {
	name    string
	kind    ydbindex.Kind
	unique  bool
	columns []string
	cover   []string
	// settings is the partitioning the index takes. An index starts with
	// [ydbindex.DefaultSettings], and YDB takes no partitioning in the
	// clause that creates it, so any other settings are written by an ALTER
	// INDEX of their own after it; see [indexClause.partitioningStatement].
	settings ydbindex.Settings
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
	return clause
}

// partitioningStatement is the ALTER INDEX that gives a new index on table the
// partitioning it declares, or "" for an index that takes YDB's defaults.
//
// It is a statement of its own because no clause that creates an index takes
// the settings. Measured on 25.1.4.7 and 26.2.1.14, `INDEX i GLOBAL ON (v)
// WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3)` answers `Unknown index
// setting: auto_partitioning_min_partitions_count` on 26.2 and `with:
// alternative is not implemented yet` on 25.1, inline in CREATE TABLE and in
// ADD INDEX alike, while ALTER INDEX ... SET takes it once the index exists.
func (c indexClause) partitioningStatement(table string) string {
	clause := ydbindex.Clause(c.settings, ydbindex.DefaultSettings())
	if len(clause) == 0 {
		return ""
	}
	return alterIndexSet(table, c.name, clause)
}

// alterIndexSet writes `ALTER TABLE t ALTER INDEX i SET (...)` with settings.
func alterIndexSet(table, index string, settings []string) string {
	return fmt.Sprintf("ALTER TABLE %s ALTER INDEX %s SET (%s);", tablePath(table), quote(index), strings.Join(settings, ", "))
}

// indexSettings resolves an index's declared partitioning, refusing it on a
// target without [capability.IndexPartitioning] and refusing what YDB would
// refuse.
func (r *Renderer) indexSettings(subject string, spec *ast.IndexPartitioningSpec) (ydbindex.Settings, error) {
	if spec.IsZero() {
		return ydbindex.DefaultSettings(), nil
	}
	if !r.caps.Has(capability.IndexPartitioning) {
		return ydbindex.Settings{}, refuseKey(capability.IndexPartitioning, subject+" declares its partitioning")
	}
	settings, err := ydbindex.Resolve(spec)
	if err != nil {
		return ydbindex.Settings{}, refuseFact(subject, err.Error())
	}
	return settings, nil
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
	if err := r.refuseIndexDeclarations(subject, index); err != nil {
		return indexClause{}, err
	}
	kind, err := ydbindex.KindOf(index.Type)
	if err != nil {
		return indexClause{}, refuseFact(subject, err.Error())
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
	settings, err := r.indexSettings(subject, index.Partitioning)
	if err != nil {
		return indexClause{}, err
	}
	return indexClause{
		name: index.Name, kind: kind, unique: index.Unique,
		columns: columns, cover: slices.Clone(index.IncludeColumns), settings: settings,
	}, nil
}

// refuseIndexDeclarations refuses what an index may say that a YDB global
// index has no clause for.
func (r *Renderer) refuseIndexDeclarations(subject string, index *ast.IndexNode) error {
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
	case index.Operator != "":
		return refuseFact(subject, "YDB has no index operator class")
	case index.Granularity != 0:
		return refuseFact(subject, "GRANULARITY is ClickHouse's")
	case len(index.StorageParams) > 0:
		return refuseFact(subject, "a YDB global index takes no storage parameters; its settings are its "+
			"partitioning and read replicas, declared with the auto_partitioning_* and read_replicas_settings attributes")
	case index.Comment != "":
		return refuseGap(ydbgap.Comments, "the comment on "+subject)
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
func (r *Renderer) inlineIndex(table string, index *ast.IndexNode, keyColumns []string, columnTypes map[string]string) (indexClause, error) {
	clause, err := r.indexClauseOf(index)
	if err != nil {
		return indexClause{}, err
	}
	columnType := func(column string) (string, bool) {
		ydbType, declared := columnTypes[column]
		return ydbType, declared
	}
	if reason := ydbindex.ShapeRefusal(clause.columns, clause.cover, keyColumns, columnType); reason != "" {
		return indexClause{}, refuseFact(fmt.Sprintf("index %q on table %q", index.Name, table), reason)
	}
	return clause, nil
}

// renderIndex writes an index added to a table that exists, as ALTER TABLE
// ... ADD INDEX: YDB has no CREATE INDEX statement (`no viable alternative at
// input 'CREATE INDEX'`). A unique one is refused on a target without
// [capability.UniqueIndexOnExistingTable]; a new table's unique index is
// written inside its CREATE TABLE instead, where every line accepts it.
func (r *Renderer) renderIndex(index *ast.IndexNode) error {
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
	case strings.TrimSpace(index.Table) == "":
		return nil, refuseFact(subject, "YDB adds an index through its table, and the index names none")
	case index.IfNotExists:
		return nil, refuseFact(subject, "YDB's ADD INDEX has no IF NOT EXISTS guard")
	case index.Unique && !r.caps.Has(capability.UniqueIndexOnExistingTable):
		return nil, refuseKey(capability.UniqueIndexOnExistingTable, fmt.Sprintf(
			"unique %s is added to table %q, which exists already (declare it with the table, or enable the flag on the cluster)",
			subject, index.Table))
	}
	clause, err := r.indexClauseOf(index)
	if err != nil {
		return nil, err
	}
	statements := []string{fmt.Sprintf("ALTER TABLE %s ADD %s;", tablePath(index.Table), clause)}
	if partitioning := clause.partitioningStatement(index.Table); partitioning != "" {
		statements = append(statements, partitioning)
	}
	return statements, nil
}

// setIndexPartitioning writes the ALTER INDEX that changes an existing index's
// partitioning in place, refusing the change YDB cannot make that way.
func (r *Renderer) setIndexPartitioning(table string, op *ast.SetIndexPartitioningOperation) ([]string, error) {
	subject := fmt.Sprintf("index %q of table %q", op.IndexName, table)
	if strings.TrimSpace(op.IndexName) == "" {
		return nil, refuseFact(fmt.Sprintf("table %q", table), "ALTER INDEX ... SET names no index")
	}
	if !r.caps.Has(capability.IndexPartitioning) {
		return nil, refuseKey(capability.IndexPartitioning, "changing the partitioning of "+subject)
	}
	desired, err := ydbindex.Resolve(op.Partitioning)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	previous, err := ydbindex.Resolve(op.Previous)
	if err != nil {
		return nil, refuseFact(subject, "the settings it holds: "+err.Error())
	}
	if reason := ydbindex.ChangeRefusal(desired, previous); reason != "" {
		return nil, refuseFact(subject, reason)
	}
	clause := ydbindex.Clause(desired, previous)
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
