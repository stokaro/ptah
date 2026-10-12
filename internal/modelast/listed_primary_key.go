package modelast

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// withoutKeyTheListRedeclares takes the table's own primary key out of
// createTable when declared holds a PRIMARY KEY of table over the same columns,
// in the same order.
//
// The model reads such a pair as one key, the entry of the list first (see
// [schemaprep.FoldedIndexConstraints]), and only the entry can carry a name, a
// comment and settings. A CREATE TABLE that wrote the key twice is refused:
// measured on MySQL 26.7 as `Multiple primary key defined`, and on PostgreSQL
// 18.6 as `multiple primary keys for table "hk" are not allowed`. A pair over
// different columns declares two keys, and is left for the server to refuse.
func withoutKeyTheListRedeclares(createTable *ast.CreateTableNode, table schemamodel.Table, declared []schemamodel.Constraint) {
	listed := slices.IndexFunc(declared, func(constraint schemamodel.Constraint) bool {
		return schemaprep.ConstraintBelongsToTable(constraint, table) &&
			strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY")
	})
	if listed < 0 {
		return
	}
	columns := declared[listed].Columns
	own := slices.IndexFunc(createTable.Constraints, func(constraint *ast.ConstraintNode) bool {
		return constraint.Type == ast.PrimaryKeyConstraint
	})
	if own >= 0 {
		if slices.Equal(createTable.Constraints[own].Columns, columns) {
			createTable.Constraints = slices.Delete(createTable.Constraints, own, own+1)
		}
		return
	}
	var primary []string
	for _, column := range createTable.Columns {
		if column.Primary {
			primary = append(primary, column.Name)
		}
	}
	if len(primary) == 0 || !slices.Equal(primary, columns) {
		return
	}
	for _, column := range createTable.Columns {
		column.Primary = false
	}
}
