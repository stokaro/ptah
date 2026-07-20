package compare

import (
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
)

// isFieldLevelConstraint determines if a database constraint represents a field-level constraint
// that is already represented in the field definitions (NOT NULL, PRIMARY KEY, UNIQUE, FOREIGN KEY).
//
// synthesizedFKKeys holds the table.constraint_name of every field-level FK that
// Constraints() synthesized into the generated set (see
// synthesizeFieldLevelForeignKeyConstraints). When a DB-side FK has a synthesized
// counterpart it is NOT treated as field-level here, so it stays in the
// comparison and on_delete / on_update drift flows through
// foreignKeyConstraintChanged (issue #189). FKs without a synthesized
// counterpart (e.g. a column that is not yet in the database, which never gets
// synthesized) keep the previous filter-out behavior.
func isFieldLevelConstraint(dbConstraint types.DBConstraint, generated *goschema.Database, synthesizedFKKeys map[string]struct{}) bool {
	// Create a map of table.column -> field for quick lookup
	fieldMap := make(map[string]goschema.Field)
	for _, field := range generated.Fields {
		// Get table name for this field
		tableName := field.StructName // default to struct name
		for _, table := range generated.Tables {
			if table.StructName == field.StructName {
				tableName = table.QualifiedName()
				break
			}
		}
		key := tableName + "." + field.Name
		fieldMap[key] = field
	}

	// Check if this constraint corresponds to a field-level constraint
	switch dbConstraint.Type {
	case "NOT NULL":
		// Check if there's a field with not_null=true for this column
		key := dbConstraint.QualifiedTableName() + "." + getConstraintColumn(dbConstraint)
		if field, exists := fieldMap[key]; exists && !field.Nullable {
			return true
		}
	case "PRIMARY KEY":
		// Check if there's a field with primary=true for this column
		key := dbConstraint.QualifiedTableName() + "." + getConstraintColumn(dbConstraint)
		if field, exists := fieldMap[key]; exists && field.Primary {
			return true
		}
	case "UNIQUE":
		// Check if there's a field with unique=true for this column
		key := dbConstraint.QualifiedTableName() + "." + getConstraintColumn(dbConstraint)
		if field, exists := fieldMap[key]; exists && field.Unique {
			return true
		}
	case "FOREIGN KEY":
		// A field-level FK that was synthesized into the generated set (see
		// synthesizeFieldLevelForeignKeyConstraints) must participate in the
		// comparison so on_delete / on_update drift is detected (issue #189).
		// Letting the DB-side FK through means it gets matched by name against
		// the synthesized entry, so add / remove / action-change cases all
		// flow through the standard Constraints() comparison path. Match on
		// the constraint name rather than the column, because the synthesized
		// name is keyed on table.constraint_name and getConstraintColumn does
		// not always resolve the FK column.
		if _, synthesized := synthesizedFKKeys[dbConstraint.QualifiedTableName()+"."+dbConstraint.Name]; synthesized {
			return false
		}
		// Check if there's a field with foreign key reference for this column.
		// No synthesized counterpart (e.g. the column is not yet in the
		// database): keep the historical behavior and treat it as field-level
		// so it is owned by the column/table lifecycle.
		key := dbConstraint.QualifiedTableName() + "." + getConstraintColumn(dbConstraint)
		if field, exists := fieldMap[key]; exists && field.Foreign != "" {
			return true
		}
	case "CHECK":
		// PostgreSQL exposes NOT NULL declarations as synthetic CHECK rows in
		// information_schema with a `<table>_<column>_not_null` naming
		// convention (and, in PG18, as named NOT NULL constraints in
		// pg_constraint). Ptah never emits these directly — they are owned by
		// the column's NOT NULL clause, which the table/column lifecycle
		// already handles:
		//   - field exists in target with NOT NULL → covered by the CREATE
		//     TABLE / ALTER COLUMN SET NOT NULL emitted elsewhere.
		//   - field exists in target as nullable → an ALTER COLUMN DROP
		//     NOT NULL handles it.
		//   - field has been dropped → the column drop cascades.
		//   - table has been dropped → the table drop cascades.
		// In every case an explicit ALTER TABLE … DROP CONSTRAINT for the
		// synthetic name would be redundant at best and illegal at worst (PG
		// rejects with 42P16 "column X is in a primary key" when the column
		// is part of a PK). Treat these as field-level always; skip the diff.
		if strings.Contains(dbConstraint.Name, "_not_null") {
			return true
		}
		// Regular CHECK constraints from `check=` annotations are surfaced
		// to the diff via synthesized goschema.Constraint entries (see
		// synthesizeFieldLevelCheckConstraints in Constraints). Letting the
		// DB constraint participate here means it gets matched against the
		// synthesized entry by name, so add/remove/expression-change cases
		// all flow through the standard Constraints() comparison path.
	}

	return false
}

// getConstraintColumn extracts the column name from a constraint
// This is a simplified implementation - in practice, constraints can span multiple columns
func getConstraintColumn(constraint types.DBConstraint) string {
	// For single-column constraints, try to extract column name from constraint name
	// This is database-specific and may need refinement

	// Use the ColumnName field if available (MySQL/MariaDB provide this directly)
	if constraint.ColumnName != "" {
		return constraint.ColumnName
	}

	// PostgreSQL NOT NULL constraints often follow pattern: schema_table_column_not_null
	if constraint.Type == "NOT NULL" && strings.Contains(constraint.Name, "_not_null") {
		return extractPostgreSQLNotNullColumn(constraint)
	}

	// For PRIMARY KEY constraints: table_pkey (PostgreSQL) or PRIMARY (MySQL/MariaDB)
	if constraint.Type == "PRIMARY KEY" {
		if strings.HasSuffix(constraint.Name, "_pkey") {
			// PostgreSQL pattern: table_pkey
			// This is a table-level primary key, we need to check if it's single-column
			// For now, assume single-column primary keys are field-level
			return "id" // common convention, but this is a limitation
		} else if constraint.Name == "PRIMARY" {
			// MySQL/MariaDB pattern: PRIMARY
			// For single-column primary keys, assume it's the id field
			return "id" // common convention, but this is a limitation
		}
	}

	// For UNIQUE constraints: table_column_key (PostgreSQL) or column name (MySQL/MariaDB)
	if constraint.Type == "UNIQUE" {
		if strings.HasSuffix(constraint.Name, "_key") {
			// PostgreSQL pattern
			return extractPostgreSQLUniqueColumn(constraint)
		}

		// MySQL/MariaDB often use the column name as the constraint name for single-column unique constraints
		return constraint.Name
	}

	// For other constraints, return empty string (will not match any field)
	return ""
}

// extractPostgreSQLNotNullColumn extracts column name from PostgreSQL NOT NULL constraint
func extractPostgreSQLNotNullColumn(constraint types.DBConstraint) string {
	parts := strings.Split(constraint.Name, "_")
	if len(parts) < 3 {
		return ""
	}

	// Remove schema prefix (2200_) and suffix (_not_null)
	if !strings.HasPrefix(constraint.Name, "2200_") {
		return ""
	}

	// Format: 2200_table_column_not_null -> extract column
	tableParts := strings.Split(constraint.Name[5:], "_not_null")
	if len(tableParts) == 0 {
		return ""
	}

	remaining := tableParts[0]
	// Remove table name prefix to get column
	tablePrefix := constraint.TableName + "_"
	if strings.HasPrefix(remaining, tablePrefix) {
		return remaining[len(tablePrefix):]
	}

	return ""
}

// extractPostgreSQLUniqueColumn extracts column name from PostgreSQL UNIQUE constraint
func extractPostgreSQLUniqueColumn(constraint types.DBConstraint) string {
	parts := strings.Split(constraint.Name, "_")
	if len(parts) < 3 {
		return ""
	}

	// Remove table prefix and _key suffix
	tablePrefix := constraint.TableName + "_"
	if !strings.HasPrefix(constraint.Name, tablePrefix) {
		return ""
	}

	remaining := constraint.Name[len(tablePrefix):]
	if !strings.HasSuffix(remaining, "_key") {
		return ""
	}

	return remaining[:len(remaining)-4] // remove "_key"
}
