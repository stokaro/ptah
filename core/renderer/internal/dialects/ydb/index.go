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
	return indexClause{
		name: index.Name, kind: kind, unique: index.Unique,
		columns: columns, cover: slices.Clone(index.IncludeColumns),
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
		return refuseGap(ydbgap.TableSettings, "the storage parameters of "+subject)
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

// inlineIndex writes an index declared inside CREATE TABLE, where the table's
// key and column types are known and the index is held to
// [ydbindex.ShapeRefusal].
func (r *Renderer) inlineIndex(table string, index *ast.IndexNode, keyColumns []string, columnTypes map[string]string) (string, error) {
	clause, err := r.indexClauseOf(index)
	if err != nil {
		return "", err
	}
	columnType := func(column string) (string, bool) {
		ydbType, declared := columnTypes[column]
		return ydbType, declared
	}
	if reason := ydbindex.ShapeRefusal(clause.columns, clause.cover, keyColumns, columnType); reason != "" {
		return "", refuseFact(fmt.Sprintf("index %q on table %q", index.Name, table), reason)
	}
	return clause.String(), nil
}

// renderIndex writes an index added to a table that exists, as ALTER TABLE
// ... ADD INDEX: YDB has no CREATE INDEX statement (`no viable alternative at
// input 'CREATE INDEX'`). A unique one is refused on a target without
// [capability.UniqueIndexOnExistingTable]; a new table's unique index is
// written inside its CREATE TABLE instead, where every line accepts it.
func (r *Renderer) renderIndex(index *ast.IndexNode) error {
	statement, err := r.addIndexStatement(index)
	if err != nil {
		return err
	}
	r.w.WriteLine(statement)
	return nil
}

// addIndexStatement is the ALTER TABLE ... ADD INDEX renderIndex writes.
func (r *Renderer) addIndexStatement(index *ast.IndexNode) (string, error) {
	if index == nil {
		return "", refuseFact("an index", "the index node is nil")
	}
	subject := fmt.Sprintf("index %q", index.Name)
	switch {
	case strings.TrimSpace(index.Table) == "":
		return "", refuseFact(subject, "YDB adds an index through its table, and the index names none")
	case index.IfNotExists:
		return "", refuseFact(subject, "YDB's ADD INDEX has no IF NOT EXISTS guard")
	case index.Unique && !r.caps.Has(capability.UniqueIndexOnExistingTable):
		return "", refuseKey(capability.UniqueIndexOnExistingTable, fmt.Sprintf(
			"unique %s is added to table %q, which exists already (declare it with the table, or enable the flag on the cluster)",
			subject, index.Table))
	}
	clause, err := r.indexClauseOf(index)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER TABLE %s ADD %s;", tablePath(index.Table), clause), nil
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
