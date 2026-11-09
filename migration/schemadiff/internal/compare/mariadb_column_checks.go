package compare

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// mariaDBColumnCheckChange moves a column CHECK change onto its column. MariaDB
// reports these CHECKs in CHECK_CONSTRAINTS with LEVEL=Column, but refuses
// DROP CONSTRAINT for them. MODIFY COLUMN replaces the column's CHECK.
func mariaDBColumnCheckChange(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	current catalog.Constraint,
	wanted *schemamodel.Constraint,
	dialect string,
	semantics identifier.Semantics,
) bool {
	if !comparisonOf(dialect).ColumnCheckDropByModify ||
		current.Type != "CHECK" || !current.ColumnLevelCheck || current.ColumnName == "" ||
		semantics.ColumnIdentityKey(current.ColumnName) != semantics.ColumnIdentityKey(current.Name) {
		return false
	}
	currentTable := newQualifiedTableIdentity(current.QualifiedTableName(), semantics)
	for _, removed := range diff.TablesRemoved {
		if newQualifiedTableIdentity(removed.Name, semantics) == currentTable {
			return true // Dropping the table also drops its column CHECK.
		}
	}

	declared, observed := mariaDBCheckTables(desired, database, currentTable, semantics)
	if declared.Name == "" {
		return false
	}
	if observed.Name == "" || !slices.ContainsFunc(observed.Columns, func(column catalog.Column) bool {
		return semantics.ColumnIdentityKey(column.Name) == semantics.ColumnIdentityKey(current.ColumnName)
	}) {
		return false
	}

	tableDiff := mariaDBCheckTableDiff(desired, database, diff, declared, observed, dialect, semantics)
	for _, removed := range tableDiff.ColumnsRemoved {
		if semantics.ColumnIdentityKey(removed.Name) == semantics.ColumnIdentityKey(current.ColumnName) {
			return true // Dropping the column also drops its CHECK.
		}
	}
	var field schemamodel.Field
	for _, candidate := range tableDiff.Desired.Fields {
		if semantics.ColumnIdentityKey(candidate.Name) == semantics.ColumnIdentityKey(current.ColumnName) {
			field = candidate
			break
		}
	}
	if field.Name == "" {
		return false
	}
	field = setMariaDBColumnCheckChange(tableDiff, field, current, dialect, semantics)
	addMariaDBTableCheck(diff, wanted, field, dialect, semantics)
	return true
}

func mariaDBCheckTableDiff(desired *schemamodel.Database, database *catalog.Database, diff *difftypes.SchemaDiff, declared schemamodel.Table, observed catalog.Table, dialect string, semantics identifier.Semantics) *difftypes.TableDiff {
	key := newQualifiedTableIdentity(declared.QualifiedName(), semantics)
	for i := range diff.TablesModified {
		if newQualifiedTableIdentity(diff.TablesModified[i].TableName, semantics) == key {
			return &diff.TablesModified[i]
		}
	}
	diff.TablesModified = append(diff.TablesModified, difftypes.TableDiff{
		TableName: declared.QualifiedName(),
		Desired:   schemacapture.DeclareTable(desired, declared, semantics),
		Current:   difftypes.TableObservationFor(database, observed, dialect, semantics),
	})
	return &diff.TablesModified[len(diff.TablesModified)-1]
}

func setMariaDBColumnCheckChange(tableDiff *difftypes.TableDiff, field schemamodel.Field, current catalog.Constraint, dialect string, semantics identifier.Semantics) schemamodel.Field {
	for i := range tableDiff.ColumnsModified {
		column := &tableDiff.ColumnsModified[i]
		if semantics.ColumnIdentityKey(column.ColumnName) == semantics.ColumnIdentityKey(current.ColumnName) {
			field = column.Desired
			column.Changes["check"] = fmt.Sprintf("%s -> %s", checkClauseText(current), schemaprep.WithJSONCheck(field, dialect).Check)
			return field
		}
	}
	tableDiff.ColumnsModified = append(tableDiff.ColumnsModified, difftypes.ColumnDiff{
		ColumnName: field.Name,
		Changes: map[string]string{
			"check": fmt.Sprintf("%s -> %s", checkClauseText(current), schemaprep.WithJSONCheck(field, dialect).Check),
		},
		Desired: field,
	})
	slices.SortFunc(tableDiff.ColumnsModified, func(a, b difftypes.ColumnDiff) int {
		return strings.Compare(a.ColumnName, b.ColumnName)
	})
	return field
}

func mariaDBCheckTables(desired *schemamodel.Database, database *catalog.Database, key tableIdentity, semantics identifier.Semantics) (schemamodel.Table, catalog.Table) {
	var declared schemamodel.Table
	for _, table := range desired.Tables {
		if newQualifiedTableIdentity(table.QualifiedName(), semantics) == key {
			declared = table
			break
		}
	}
	var observed catalog.Table
	for _, table := range database.Tables {
		if newQualifiedTableIdentity(table.QualifiedName(), semantics) == key {
			observed = table
			break
		}
	}
	return declared, observed
}

func checkClauseText(current catalog.Constraint) string {
	if current.CheckClause == nil {
		return ""
	}
	return *current.CheckClause
}

// A table-level CHECK with the same name still needs ADD CONSTRAINT after the
// column's old CHECK is removed by MODIFY COLUMN.
func addMariaDBTableCheck(diff *difftypes.SchemaDiff, wanted *schemamodel.Constraint, field schemamodel.Field, dialect string, semantics identifier.Semantics) {
	if wanted == nil {
		return
	}
	inline := schemaprep.WithJSONCheck(field, dialect).Check
	if wanted.Type != "CHECK" || inline != wanted.CheckExpression {
		diff.ConstraintsAdded = appendConstraintAddition(diff.ConstraintsAdded, *wanted, semantics)
	}
}
