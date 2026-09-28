// Package constraintowner answers which declared table a constraint of a
// desired schema belongs to.
//
// A constraint names its table two ways: by the struct that declares it and by
// the table name it carries, and either may be empty. The schema comparison
// keys a declared constraint by the table this package answers, and the column
// key rule reads the table's key names through it (see
// [ptah.run/internal/columnkey.Taken]), so the SQL reader and the comparison
// agree about which constraints a table holds.
package constraintowner

import (
	"strings"

	"ptah.run/core/schemamodel"
)

// Table answers the one table of tables that constraint belongs to: the table
// its struct declares, narrowed by the table name it carries, bare or
// qualified. It answers false when no table matches, and when more than one
// does, since the constraint then belongs to none of them for certain.
func Table(constraint schemamodel.Constraint, tables []schemamodel.Table) (schemamodel.Table, bool) {
	tableName := strings.TrimSpace(constraint.Table)
	var owner schemamodel.Table
	found := false
	for _, table := range tables {
		if constraint.StructName != "" && table.StructName != constraint.StructName {
			continue
		}
		if tableName != "" &&
			table.Name != tableName &&
			table.QualifiedName() != tableName {
			continue
		}
		if found {
			return schemamodel.Table{}, false
		}
		owner = table
		found = true
	}
	return owner, found
}

// TableName answers the qualified name of the table [Table] finds, and the
// table name the constraint carries, trimmed, when it finds none.
func TableName(constraint schemamodel.Constraint, tables []schemamodel.Table) string {
	table, ok := Table(constraint, tables)
	if !ok {
		return strings.TrimSpace(constraint.Table)
	}
	return table.QualifiedName()
}
