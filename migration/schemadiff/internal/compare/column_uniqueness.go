package compare

import (
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/identifier"
)

type columnIdentity struct {
	table  tableIdentity
	column string
}

type tableIdentity struct {
	schema string
	table  string
}

// collectGeneratedObjectOwnedUniqueColumns identifies desired single-column
// uniqueness whose lifecycle is explicitly represented by an index or table
// constraint. Live objects alone cannot suppress a column difference because
// database-side index filters may intentionally exclude backing indexes.
func collectGeneratedObjectOwnedUniqueColumns(
	generated *goschema.Database,
	semantics identifier.Semantics,
) map[columnIdentity]struct{} {
	columns := make(map[columnIdentity]struct{})
	collectGeneratedUniqueIndexColumns(columns, generated, semantics)
	collectGeneratedUniqueConstraintColumns(columns, generated, semantics)
	return columns
}

func collectGeneratedUniqueIndexColumns(
	columns map[columnIdentity]struct{},
	generated *goschema.Database,
	semantics identifier.Semantics,
) {
	owners := goschema.ResolveIndexTableNames(generated.Indexes, generated.Tables)
	for position, index := range generated.Indexes {
		column, ok := singleGeneratedUniqueIndexColumn(index)
		if !ok || owners[position] == "" {
			continue
		}
		table, ok := generatedIndexTable(index, owners[position], generated.Tables)
		if !ok {
			continue
		}
		columns[newColumnIdentityForTable(
			table.Schema,
			table.Name,
			column,
			semantics,
		)] = struct{}{}
	}
}

func generatedIndexTable(
	index goschema.Index,
	owner string,
	tables []goschema.Table,
) (goschema.Table, bool) {
	var match goschema.Table
	matchCount := 0
	for _, table := range tables {
		if table.QualifiedName() != owner {
			continue
		}
		match = table
		matchCount++
	}
	if matchCount == 1 {
		return match, true
	}
	if matchCount == 0 || index.StructName == "" {
		return goschema.Table{}, false
	}

	matchCount = 0
	for _, table := range tables {
		if table.QualifiedName() != owner || table.StructName != index.StructName {
			continue
		}
		match = table
		matchCount++
	}
	return match, matchCount == 1
}

func singleGeneratedUniqueIndexColumn(index goschema.Index) (string, bool) {
	if !index.Unique || strings.TrimSpace(index.Condition) != "" {
		return "", false
	}
	if len(index.Parts) > 0 {
		if len(index.Parts) != 1 || index.Parts[0].Name == "" || index.Parts[0].Expr != "" {
			return "", false
		}
		return index.Parts[0].Name, true
	}
	if len(index.Fields) != 1 {
		return "", false
	}
	return index.Fields[0], true
}

func collectGeneratedUniqueConstraintColumns(
	columns map[columnIdentity]struct{},
	generated *goschema.Database,
	semantics identifier.Semantics,
) {
	for _, constraint := range generated.Constraints {
		table, ok := generatedConstraintTable(constraint, generated.Tables)
		if !strings.EqualFold(constraint.Type, "UNIQUE") ||
			!ok ||
			len(constraint.Columns) != 1 {
			continue
		}
		columns[newColumnIdentityForTable(
			table.Schema,
			table.Name,
			constraint.Columns[0],
			semantics,
		)] = struct{}{}
	}
}

func generatedConstraintTableName(
	constraint goschema.Constraint,
	tables []goschema.Table,
) string {
	table, ok := generatedConstraintTable(constraint, tables)
	if !ok {
		return strings.TrimSpace(constraint.Table)
	}
	return table.QualifiedName()
}

func generatedConstraintTable(
	constraint goschema.Constraint,
	tables []goschema.Table,
) (goschema.Table, bool) {
	tableName := strings.TrimSpace(constraint.Table)
	var owner goschema.Table
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
			return goschema.Table{}, false
		}
		owner = table
		found = true
	}
	return owner, found
}

func newColumnIdentityForTable(
	schema string,
	table string,
	column string,
	semantics identifier.Semantics,
) columnIdentity {
	return columnIdentity{
		table:  newTableIdentity(schema, table, semantics),
		column: semantics.ColumnIdentityKey(column),
	}
}

func newTableIdentity(
	schema string,
	table string,
	semantics identifier.Semantics,
) tableIdentity {
	if strings.TrimSpace(schema) == "" {
		schema = semantics.DefaultSchema
	}
	return tableIdentity{
		schema: semantics.TableIdentityKey(strings.TrimSpace(schema)),
		table:  semantics.TableIdentityKey(strings.TrimSpace(table)),
	}
}
