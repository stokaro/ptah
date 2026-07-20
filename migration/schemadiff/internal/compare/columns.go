package compare

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/normalize"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
	"github.com/stokaro/ptah/migration/typechange"
)

// TableColumns performs detailed column-level comparison within a specific table.
//
// This function is responsible for the complex task of comparing column structures
// between a generated table definition and an existing database table. It handles
// embedded field processing, column mapping, and detailed property comparison.
//
// # Embedded Field Processing
//
// The function's most complex aspect is handling embedded fields:
//  1. **Field Expansion**: Uses transform.ProcessEmbeddedFields() to expand embedded structs
//  2. **Field Combination**: Merges original fields with embedded-generated fields
//  3. **Struct Filtering**: Only processes fields belonging to the target struct
//
// This ensures that embedded fields (like timestamps, audit info) are properly
// compared against their corresponding database columns.
//
// # Comparison Algorithm
//
// The function performs comparison in three phases:
//  1. **Column Discovery**: Creates lookup maps for efficient column comparison
//  2. **Addition/Removal Detection**: Identifies new and removed columns
//  3. **Modification Analysis**: Compares properties of existing columns
//
// # Example Scenarios
//
// **Embedded field handling**:
//
//	```go
//	type User struct {
//	    ID   int    `db:"id"`
//	    Name string `db:"name"`
//	    Timestamps // Embedded struct with CreatedAt, UpdatedAt
//	}
//	```
//	The function expands Timestamps fields and compares them against database columns.
//
// **Column addition detection**:
//   - Generated schema has "email" column
//   - Database table doesn't have "email" column
//   - Result: "email" added to TableDiff.ColumnsAdded
//
// **Column modification detection**:
//   - Both have "name" column
//   - Generated: VARCHAR(255), Database: VARCHAR(100)
//   - Result: ColumnDiff added to TableDiff.ColumnsModified
//
// # Parameters
//
//   - genTable: Generated table definition from Go struct annotations
//   - dbTable: Current database table structure from introspection
//   - generated: Complete parse result containing all fields and embedded field definitions
//
// # Return Value
//
// Returns a TableDiff containing:
//   - ColumnsAdded: New columns that need to be added
//   - ColumnsRemoved: Existing columns that should be removed
//   - ColumnsModified: Columns with property differences
//
// # Performance Considerations
//
// - Time Complexity: O(n + m + k) where n=generated columns, m=database columns, k=embedded fields
// - Space Complexity: O(n + m) for lookup maps
// - Embedded field processing adds overhead but is necessary for accurate comparison
//
// # Output Consistency
//
// Column lists are sorted alphabetically for deterministic output and reliable testing.
func TableColumns(genTable goschema.Table, dbTable types.DBTable, generated *goschema.Database) difftypes.TableDiff {
	tableDiff := difftypes.TableDiff{TableName: genTable.QualifiedName()}

	// Process embedded fields to get the complete field list (same as generators do)
	embeddedGeneratedFields := processEmbeddedFieldsForStruct(generated.EmbeddedFields, generated.Fields, genTable.StructName)

	// Combine original fields with embedded-generated fields
	allFields := append(([]goschema.Field)(nil), generated.Fields...)
	allFields = append(allFields, embeddedGeneratedFields...)

	// Create maps for quick lookup
	genColumns := make(map[string]goschema.Field)
	for _, field := range allFields {
		if field.StructName == genTable.StructName {
			genColumns[field.Name] = field
		}
	}

	dbColumns := make(map[string]types.DBColumn)
	for _, col := range dbTable.Columns {
		dbColumns[col.Name] = col
	}

	// Find added and removed columns
	for colName := range genColumns {
		if _, exists := dbColumns[colName]; !exists {
			tableDiff.ColumnsAdded = append(tableDiff.ColumnsAdded, colName)
		}
	}

	for colName := range dbColumns {
		if _, exists := genColumns[colName]; !exists {
			tableDiff.ColumnsRemoved = append(tableDiff.ColumnsRemoved, colName)
		}
	}

	// Find modified columns
	for colName, genCol := range genColumns {
		if dbCol, exists := dbColumns[colName]; exists {
			if columnInTablePrimaryKey(genTable, genCol.Name) {
				genCol = normalizeTablePrimaryKeyColumn(genCol, dbCol)
			}
			colDiff := Columns(genCol, dbCol)
			if len(colDiff.Changes) > 0 {
				tableDiff.ColumnsModified = append(tableDiff.ColumnsModified, colDiff)
			}
		}
	}

	// Sort for consistent output
	sort.Strings(tableDiff.ColumnsAdded)
	sort.Strings(tableDiff.ColumnsRemoved)
	sort.Slice(tableDiff.ColumnsModified, func(i, j int) bool {
		return tableDiff.ColumnsModified[i].ColumnName < tableDiff.ColumnsModified[j].ColumnName
	})

	return tableDiff
}

// Columns performs detailed property-level comparison between a generated column and database column.
//
// This function is the most granular level of schema comparison, analyzing individual
// column properties to detect differences that require migration. It handles complex
// cross-database type normalization and property comparison logic.
//
// # Property Comparison Categories
//
// The function compares five main categories of column properties:
//  1. **Data Types**: Handles cross-database type normalization and comparison
//  2. **Nullability**: Considers primary key implications and explicit nullable settings
//  3. **Primary Key**: Compares primary key constraint status
//  4. **Uniqueness**: Compares unique constraint status
//  5. **Default Values**: Handles auto-increment special cases and type-specific normalization
//
// # Complex Logic Areas
//
// **Type Normalization**:
//   - Uses Type() to handle cross-database type variations
//   - Considers both DataType and UDTName from database introspection
//   - Handles PostgreSQL user-defined types vs standard types
//
// **Nullability Logic**:
//   - Primary key columns are always NOT NULL regardless of field definition
//   - Explicit nullable settings override default behavior
//   - Database "YES"/"NO" strings converted to boolean for comparison
//
// **Auto-increment Handling**:
//   - SERIAL columns have special default value handling
//   - Database shows sequence defaults, but entities expect empty defaults
//   - Prevents false positives for auto-increment columns
//
// # Example Comparisons
//
// **Type difference detection**:
//
//	```
//	Generated: VARCHAR(255)
//	Database:  VARCHAR(100)
//	Result:    Changes["type"] = "varchar -> varchar" (normalized)
//	```
//
// **Nullability change**:
//
//	```
//	Generated: nullable=false
//	Database:  nullable=true
//	Result:    Changes["nullable"] = "true -> false"
//	```
//
// **Primary key promotion**:
//
//	```
//	Generated: primary=true
//	Database:  primary=false
//	Result:    Changes["primary_key"] = "false -> true"
//	```
//
// **Default value normalization**:
//
//	```
//	Generated: default=""
//	Database:  default_expr="NULL"
//	Result:    No change (both normalize to empty string)
//	```
//
// # Parameters
//
//   - genCol: Generated column definition from Go struct field
//   - dbCol: Current database column from introspection
//
// # Return Value
//
// Returns a ColumnDiff with:
//   - ColumnName: Name of the column being compared
//   - Changes: Map of property changes in "old -> new" format
//
// # Cross-Database Considerations
//
// The function handles database-specific variations:
//   - **PostgreSQL**: UDT names, SERIAL types, native boolean types
//   - **MySQL/MariaDB**: TINYINT boolean representation, AUTO_INCREMENT
//   - **Type mapping**: Intelligent normalization for accurate comparison
func Columns(genCol goschema.Field, dbCol types.DBColumn) difftypes.ColumnDiff {
	colDiff := difftypes.ColumnDiff{
		ColumnName: genCol.Name,
		Changes:    make(map[string]string),
	}

	// ClickHouse-only guard: older goschema models cannot express
	// MATERIALIZED / ALIAS / EPHEMERAL columns. Once the schema side carries a
	// generated expression, compare it normally below.
	if dbCol.GeneratedKind != "" && genCol.GeneratedExpression == "" {
		return colDiff
	}

	// Compare data types (simplified)
	genType := normalize.Type(genCol.Type)
	dbRawType := rawDBColumnType(dbCol)
	dbType := normalize.Type(dbRawType)

	if genType != dbType {
		colDiff.Changes["type"] = fmt.Sprintf("%s -> %s", dbType, genType)
	} else if typechange.IsNarrowing(dbRawType, genCol.Type) {
		colDiff.Changes["type"] = fmt.Sprintf("%s -> %s", dbRawType, genCol.Type)
	}

	// Compare nullable (primary keys are always NOT NULL regardless of the field definition)
	genNullable := genCol.Nullable
	if genCol.Primary {
		genNullable = false // Primary keys are always NOT NULL
	}
	dbNullable := dbCol.IsNullable == "YES"
	if genNullable != dbNullable {
		colDiff.Changes["nullable"] = fmt.Sprintf("%t -> %t", dbNullable, genNullable)
	}

	// Compare primary key
	genPrimary := genCol.Primary
	dbPrimary := dbCol.IsPrimaryKey
	if genPrimary != dbPrimary {
		colDiff.Changes["primary_key"] = fmt.Sprintf("%t -> %t", dbPrimary, genPrimary)
	}

	// Compare unique
	genUnique := genCol.Unique
	dbUnique := dbCol.IsUnique
	if genUnique != dbUnique {
		colDiff.Changes["unique"] = fmt.Sprintf("%t -> %t", dbUnique, genUnique)
	}
	if diff := generatedColumnDiff(genCol, dbCol); diff != "" {
		colDiff.Changes["generated"] = diff
	}

	// Compare default values (simplified)
	genDefault := genCol.Default
	if genDefault == "" {
		genDefault = genCol.DefaultExpr
	}
	dbDefault := ""
	if dbCol.ColumnDefault != nil {
		dbDefault = *dbCol.ColumnDefault
	}

	// For auto-increment/SERIAL columns, ignore default value differences
	// because the database will show the sequence default but the entity expects empty
	isAutoIncrement := dbCol.IsAutoIncrement || strings.Contains(strings.ToUpper(genCol.Type), "SERIAL")
	if !isAutoIncrement {
		normalizedDbDefault := normalize.DefaultValue(dbDefault, dbType)

		idxName := "default"
		if normalize.IsDefaultExpr(dbDefault) {
			idxName = "default_expr"
		}

		normalizeGenDefaultFn := normalize.DefaultValue(genDefault, "")

		if normalizeGenDefaultFn != normalizedDbDefault {
			colDiff.Changes[idxName] = fmt.Sprintf("%s -> %s", dbDefault, genDefault)
		}
	}

	return colDiff
}

func normalizeTablePrimaryKeyColumn(genCol goschema.Field, dbCol types.DBColumn) goschema.Field {
	genCol.Nullable = false
	genCol.Primary = dbCol.IsPrimaryKey
	return genCol
}

func columnInTablePrimaryKey(table goschema.Table, column string) bool {
	return slices.Contains(tablePrimaryKeyColumns(table), column)
}

func tablePrimaryKeyColumns(table goschema.Table) []string {
	if len(table.PrimaryKeyParts) == 0 {
		return nonEmptyNames(table.PrimaryKey)
	}
	columns := make([]string, 0, len(table.PrimaryKeyParts))
	for _, part := range table.PrimaryKeyParts {
		if name := strings.TrimSpace(part.Name); name != "" {
			columns = append(columns, name)
		}
	}
	return columns
}

func generatedColumnDiff(genCol goschema.Field, dbCol types.DBColumn) string {
	genExpr := strings.TrimSpace(genCol.GeneratedExpression)
	dbExpr := ""
	if dbCol.GeneratedExpression != nil {
		dbExpr = strings.TrimSpace(*dbCol.GeneratedExpression)
	}
	genKind := strings.ToUpper(strings.TrimSpace(genCol.GeneratedKind))
	dbKind := strings.ToUpper(strings.TrimSpace(dbCol.GeneratedKind))
	if genExpr == dbExpr && genKind == dbKind {
		return ""
	}
	return fmt.Sprintf("%s %s -> %s %s", dbKind, dbExpr, genKind, genExpr)
}
