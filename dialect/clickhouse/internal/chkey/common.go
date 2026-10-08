package chkey

import (
	"slices"
	"strings"

	"ptah.run/core/schemacapture"
	"ptah.run/internal/schemaprep"
)

// CommonColumns returns the ordered common key that table lowering supplies
// to ClickHouse's creation fallback. Named/composite keys lower as a table
// constraint, so field order must not replace their declared key order.
func CommonColumns(declaration schemacapture.TableDeclaration) []string {
	table := declaration.Table
	tableLevel := schemaprep.NeedsPrimaryKeyConstraint(table, declaration.Fields)
	var columns []string
	for _, field := range declaration.Fields {
		if field.Primary && (!tableLevel || !slices.Contains(table.PrimaryKey, field.Name)) {
			columns = append(columns, field.Name)
		}
	}
	if len(columns) > 0 {
		return columns
	}
	if tableLevel {
		for _, part := range table.PrimaryKeyParts {
			columns = append(columns, part.Name)
		}
		if len(columns) > 0 {
			return columns
		}
		return slices.Clone(table.PrimaryKey)
	}
	for _, constraint := range declaration.Constraints {
		if strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") && len(constraint.Columns) > 0 {
			return slices.Clone(constraint.Columns)
		}
	}
	return nil
}
