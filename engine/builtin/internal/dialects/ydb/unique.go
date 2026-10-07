package ydb

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbindex"
)

// A UNIQUE constraint renders as a global unique index on a target without
// [capability.UniqueConstraints]: YDB has unique indexes and no UNIQUE
// constraint (`CONSTRAINT x UNIQUE (n)` is a parse error on every line), and
// the two hold the same rows. Measured on 25.1.4.7 and 26.2.1.14, a `GLOBAL
// UNIQUE SYNC` index refuses a second row with the same value and accepts two
// rows whose value is NULL, which is what a UNIQUE constraint does on
// PostgreSQL by default (stokaro/ptah#4015, decision 7). The index takes the
// name [ydbindex.UniqueIndexName] gives it, which the comparison reads a
// declared constraint under too.

// uniqueIndexes are the unique indexes a new table's UNIQUE constraints and
// UNIQUE columns render as, in declaration order: the constraints, then the
// columns. A UNIQUE over the key folds into the key; see
// [ydbindex.UniqueIsTheKey].
func (r *Renderer) uniqueIndexes(node *ast.CreateTableNode, keyColumns []string) ([]*ast.IndexNode, error) {
	if r.caps.Has(capability.UniqueConstraints) {
		return nil, nil
	}
	var indexes []*ast.IndexNode
	for _, constraint := range node.Constraints {
		if constraint == nil || constraint.Type != ast.UniqueConstraint {
			continue
		}
		index, err := uniqueConstraintIndex(node.Name, constraint)
		if err != nil {
			return nil, err
		}
		if !ydbindex.UniqueIsTheKey(index.Columns, keyColumns) {
			indexes = append(indexes, index)
		}
	}
	for _, column := range node.Columns {
		if column.Unique && !ydbindex.UniqueIsTheKey([]string{column.Name}, keyColumns) {
			indexes = append(indexes, uniqueColumnIndex(node.Name, column.Name))
		}
	}
	return indexes, nil
}

// uniqueConstraintIndex is the unique index a UNIQUE constraint on table
// renders as, refusing what the constraint says that an index cannot carry.
func uniqueConstraintIndex(table string, constraint *ast.ConstraintNode) (*ast.IndexNode, error) {
	subject := fmt.Sprintf("UNIQUE constraint %q on %s", constraint.Name, tableref.Phrase(table))
	switch {
	case constraint.Deferrable || constraint.Initially != "":
		return nil, refuseKey(capability.DeferrableConstraints, subject+" is deferrable, and YDB checks a unique index at every write")
	case constraint.NotEnforced:
		return nil, refuseFact(subject, "it is NOT ENFORCED, and YDB enforces every unique index")
	case constraint.NotValid:
		return nil, refuseKey(capability.AddConstraintNotValid, subject+" is NOT VALID")
	case strings.TrimSpace(constraint.WhereCondition) != "":
		return nil, refuseFact(subject, "YDB has no partial index")
	case constraint.UsingMethod != "":
		return nil, refuseFact(subject, fmt.Sprintf("it names the index method %q, and a YDB unique index is a global one", constraint.UsingMethod))
	case constraint.KeyBlockSize != 0:
		return nil, refuseFact(subject, "KEY_BLOCK_SIZE is the MySQL family's")
	}
	for _, part := range constraint.ColumnParts {
		if part.Expr != "" || part.Desc || part.Prefix != "" {
			return nil, refuseFact(subject, "a YDB index is a list of columns, with no expression, order or prefix")
		}
	}
	columns := constraintColumns(constraint)
	return &ast.IndexNode{
		Name:           ydbindex.UniqueIndexName(bareTableName(table), constraint.Name, columns),
		Table:          table,
		Columns:        columns,
		Unique:         true,
		IncludeColumns: constraint.IncludeColumns,
		NullsDistinct:  constraint.NullsDistinct,
		// The constraint is the index on YDB, so its comment is the
		// index's.
		Comment: constraint.Comment,
	}, nil
}

// uniqueColumnIndex is the unique index a column's own UNIQUE on table renders
// as.
func uniqueColumnIndex(table, column string) *ast.IndexNode {
	return &ast.IndexNode{
		Name:    ydbindex.UniqueIndexName(bareTableName(table), "", []string{column}),
		Table:   table,
		Columns: []string{column},
		Unique:  true,
	}
}

// bareTableName is a table's own name, without the directory a qualified name
// carries.
func bareTableName(name string) string {
	if ref, ok := tableref.Parse(name); ok {
		return ref.Name
	}
	return name
}
