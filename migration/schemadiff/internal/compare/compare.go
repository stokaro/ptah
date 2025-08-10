package compare

import (
	"fmt"
	"sort"
	"strings"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/normalize"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// TablesAndColumns performs comprehensive table and column comparison between generated and database schemas.
//
// This function is the core table comparison engine that identifies structural differences
// between the target schema (from Go struct annotations) and the current database schema.
// It handles table additions, removals, and detailed column-level modifications.
//
// # Comparison Process
//
// The function performs comparison in three phases:
//  1. **Table Discovery**: Creates lookup maps for efficient table comparison
//  2. **Table Diff Analysis**: Identifies added and removed tables
//  3. **Column Comparison**: For existing tables, performs detailed column analysis
//
// # Algorithm Complexity
//
// - Time Complexity: O(n + m + k) where n=generated tables, m=database tables, k=total columns
// - Space Complexity: O(n + m) for lookup maps
// - Optimized for large schemas with efficient map-based lookups
//
// # Embedded Field Handling
//
// The function properly handles embedded fields by delegating to TableColumns(),
// which processes embedded fields through the transform package to ensure generated
// fields are correctly compared against database columns.
//
// # Example Scenarios
//
// **New table detection**:
//   - Generated schema has "users" table
//   - Database schema doesn't have "users" table
//   - Result: "users" added to diff.TablesAdded
//
// **Removed table detection**:
//   - Database has "legacy_data" table
//   - Generated schema doesn't define "legacy_data"
//   - Result: "legacy_data" added to diff.TablesRemoved
//
// **Modified table detection**:
//   - Both schemas have "products" table
//   - Column structures differ (new columns, type changes, etc.)
//   - Result: TableDiff added to diff.TablesModified
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from executor introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.TablesAdded: Tables that need to be created
//   - diff.TablesRemoved: Tables that exist in database but not in target schema
//   - diff.TablesModified: Tables with structural differences
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs,
// ensuring deterministic migration generation and reliable testing.
func TablesAndColumns(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Create maps for quick lookup
	genTables := make(map[string]goschema.Table)
	for _, table := range generated.Tables {
		genTables[table.Name] = table
	}

	dbTables := make(map[string]types.DBTable)
	for _, table := range database.Tables {
		dbTables[table.Name] = table
	}

	// Find added and removed tables
	for tableName := range genTables {
		if _, exists := dbTables[tableName]; !exists {
			diff.TablesAdded = append(diff.TablesAdded, tableName)
		}
	}

	for tableName := range dbTables {
		if _, exists := genTables[tableName]; !exists {
			diff.TablesRemoved = append(diff.TablesRemoved, tableName)
		}
	}

	// Find modified tables (compare columns)
	for tableName, genTable := range genTables {
		if dbTable, exists := dbTables[tableName]; exists {
			tableDiff := TableColumns(genTable, dbTable, generated)
			if len(tableDiff.ColumnsAdded) > 0 || len(tableDiff.ColumnsRemoved) > 0 || len(tableDiff.ColumnsModified) > 0 {
				diff.TablesModified = append(diff.TablesModified, tableDiff)
			}
		}
	}

	// Sort for consistent output
	sort.Strings(diff.TablesAdded)
	sort.Strings(diff.TablesRemoved)
}

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
	tableDiff := difftypes.TableDiff{TableName: genTable.Name}

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
			colDiff := Columns(genCol, dbCol)
			if len(colDiff.Changes) > 0 {
				tableDiff.ColumnsModified = append(tableDiff.ColumnsModified, colDiff)
			}
		}
	}

	// Sort for consistent output
	sort.Strings(tableDiff.ColumnsAdded)
	sort.Strings(tableDiff.ColumnsRemoved)

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

	// Compare data types (simplified)
	genType := normalize.Type(genCol.Type)
	dbType := normalize.Type(dbCol.DataType)
	if dbCol.UDTName != "" {
		dbType = normalize.Type(dbCol.UDTName)
	}

	if genType != dbType {
		colDiff.Changes["type"] = fmt.Sprintf("%s -> %s", dbType, genType)
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

// SearchColumnByName searches for a specific column difference by name within a slice of column diffs.
//
// This utility function provides efficient lookup of column differences by name, which is
// commonly needed when processing migration results or analyzing specific column changes.
// It performs a linear search through the provided slice and returns a pointer to the
// first matching ColumnDiff.
//
// # Search Algorithm
//
// The function uses a simple linear search with O(n) time complexity:
//  1. **Iteration**: Loops through each ColumnDiff in the provided slice
//  2. **Name Matching**: Compares ColumnName field with the target column name
//  3. **Early Return**: Returns immediately upon finding the first match
//  4. **Not Found**: Returns nil if no matching column is found
//
// # Use Cases
//
// **Migration Analysis**:
//   - Check if a specific column has changes before generating migration SQL
//   - Retrieve detailed change information for a particular column
//   - Validate expected changes in automated tests
//
// **Conditional Processing**:
//   - Apply different migration strategies based on specific column changes
//   - Skip certain operations if particular columns are not modified
//   - Generate warnings for potentially dangerous column modifications
//
// # Example Usage
//
// **Finding a specific column change**:
//
//	```go
//	tableDiff := compare.TableColumns(genTable, dbTable, generated)
//	emailDiff := compare.ColumnByName(tableDiff.ColumnsModified, "email")
//	if emailDiff != nil {
//	    if _, hasTypeChange := emailDiff.Changes["type"]; hasTypeChange {
//	        log.Println("Email column type is changing")
//	    }
//	}
//	```
//
// **Validation in tests**:
//
//	```go
//	result := compare.TableColumns(genTable, dbTable, generated)
//	nameDiff := compare.ColumnByName(result.ColumnsModified, "name")
//	assert.NotNil(t, nameDiff)
//	assert.Equal(t, "varchar -> text", nameDiff.Changes["type"])
//	```
//
// **Conditional migration logic**:
//
//	```go
//	for _, tableDiff := range schemaDiff.TablesModified {
//	    if pkDiff := compare.ColumnByName(tableDiff.ColumnsModified, "id"); pkDiff != nil {
//	        if _, hasPKChange := pkDiff.Changes["primary_key"]; hasPKChange {
//	            // Handle primary key changes with special care
//	            generatePrimaryKeyMigration(tableDiff.TableName, pkDiff)
//	        }
//	    }
//	}
//	```
//
// # Parameters
//
//   - diffs: Slice of ColumnDiff structures to search through
//   - columnName: Name of the column to find (case-sensitive exact match)
//
// # Return Value
//
// Returns a pointer to the first ColumnDiff with matching ColumnName, or nil if not found.
// The returned pointer references the original ColumnDiff in the slice, so modifications
// will affect the original data structure.
//
// # Performance Considerations
//
// - Time Complexity: O(n) where n is the number of column diffs
// - Space Complexity: O(1) - no additional memory allocation
// - For large numbers of columns, consider using a map-based lookup if called frequently
//
// # Thread Safety
//
// This function is read-only and thread-safe when used concurrently on the same data.
// However, if the underlying slice is being modified concurrently, appropriate
// synchronization is required.
//
// # Edge Cases
//
// - Empty slice: Returns nil immediately
// - Nil slice: Returns nil immediately (no panic)
// - Duplicate column names: Returns the first match encountered
// - Case sensitivity: Performs exact string matching (case-sensitive)
func SearchColumnByName(diffs []difftypes.ColumnDiff, columnName string) *difftypes.ColumnDiff {
	for _, diff := range diffs {
		if diff.ColumnName == columnName {
			return &diff
		}
	}
	return nil
}

// Enums performs comprehensive enum type comparison between generated and database schemas.
//
// This function handles the comparison of enum type definitions, which is particularly
// complex due to database-specific enum implementations and the challenges of enum
// value modification across different database systems.
//
// # Database-Specific Enum Handling
//
// **PostgreSQL**:
//   - Native ENUM types with CREATE TYPE statements
//   - Supports adding enum values but not removing them easily
//   - Enum values are stored in system catalogs
//
// **MySQL/MariaDB**:
//   - Inline ENUM syntax in column definitions
//   - Supports both adding and removing enum values
//   - Enum values are part of column type definition
//
// **SQLite**:
//   - No native enum support
//   - Uses CHECK constraints for enum-like behavior
//
// # Comparison Algorithm
//
// The function performs comparison in three phases:
//  1. **Enum Discovery**: Creates lookup maps for efficient enum comparison
//  2. **Addition/Removal Detection**: Identifies new and removed enum types
//  3. **Value Modification Analysis**: Compares enum values for existing types
//
// # Example Scenarios
//
// **New enum detection**:
//   - Generated schema defines "status_type" enum
//   - Database doesn't have "status_type" enum
//   - Result: "status_type" added to diff.EnumsAdded
//
// **Enum value addition**:
//   - Both have "priority_level" enum
//   - Generated: ["low", "medium", "high", "critical"]
//   - Database: ["low", "medium", "high"]
//   - Result: EnumDiff with ValuesAdded=["critical"]
//
// **Enum value removal** (problematic):
//   - Generated: ["active", "inactive"]
//   - Database: ["active", "inactive", "deprecated"]
//   - Result: EnumDiff with ValuesRemoved=["deprecated"]
//   - Note: May require manual intervention in PostgreSQL
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from executor introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.EnumsAdded: Enum types that need to be created
//   - diff.EnumsRemoved: Enum types that exist in database but not in target schema
//   - diff.EnumsModified: Enum types with value differences
//
// # Migration Considerations
//
// Enum modifications can be complex:
//   - Adding values is generally safe
//   - Removing values may require data migration
//   - PostgreSQL enum removal requires recreating the enum type
//   - MySQL enum changes require ALTER TABLE statements
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func Enums(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Create maps for quick lookup
	genEnums := make(map[string]goschema.Enum)
	for _, enum := range generated.Enums {
		genEnums[enum.Name] = enum
	}

	dbEnums := make(map[string]types.DBEnum)
	for _, enum := range database.Enums {
		dbEnums[enum.Name] = enum
	}

	// Find added and removed enums
	for enumName := range genEnums {
		if _, exists := dbEnums[enumName]; !exists {
			diff.EnumsAdded = append(diff.EnumsAdded, enumName)
		}
	}

	for enumName := range dbEnums {
		if _, exists := genEnums[enumName]; !exists {
			diff.EnumsRemoved = append(diff.EnumsRemoved, enumName)
		}
	}

	// Find modified enums
	for enumName, genEnum := range genEnums {
		if dbEnum, exists := dbEnums[enumName]; exists {
			enumDiff := EnumValues(genEnum, dbEnum)
			if len(enumDiff.ValuesAdded) > 0 || len(enumDiff.ValuesRemoved) > 0 {
				diff.EnumsModified = append(diff.EnumsModified, enumDiff)
			}
		}
	}

	// Sort for consistent output
	sort.Strings(diff.EnumsAdded)
	sort.Strings(diff.EnumsRemoved)
}

// EnumValues performs detailed value-level comparison between generated and database enum types.
//
// This function analyzes the specific values within an enum type to determine what
// changes are needed to bring the database enum in line with the generated enum
// definition. It uses set-based comparison for efficient value difference detection.
//
// # Algorithm Details
//
// The function uses a set-based approach for optimal performance:
//  1. **Set Creation**: Converts value slices to boolean maps for O(1) lookup
//  2. **Addition Detection**: Finds values in generated enum but not in database
//  3. **Removal Detection**: Finds values in database enum but not in generated
//  4. **Result Sorting**: Ensures deterministic output for consistent migrations
//
// # Example Scenarios
//
// **Value addition**:
//
//	```
//	Generated: ["draft", "published", "archived", "deleted"]
//	Database:  ["draft", "published", "archived"]
//	Result:    ValuesAdded=["deleted"], ValuesRemoved=[]
//	```
//
// **Value removal**:
//
//	```
//	Generated: ["active", "inactive"]
//	Database:  ["active", "inactive", "deprecated", "legacy"]
//	Result:    ValuesAdded=[], ValuesRemoved=["deprecated", "legacy"]
//	```
//
// **Mixed changes**:
//
//	```
//	Generated: ["pending", "approved", "rejected", "cancelled"]
//	Database:  ["pending", "approved", "denied"]
//	Result:    ValuesAdded=["rejected", "cancelled"], ValuesRemoved=["denied"]
//	```
//
// # Performance Characteristics
//
// - Time Complexity: O(n + m) where n=generated values, m=database values
// - Space Complexity: O(n + m) for the boolean maps
// - Optimized for large enum value sets with efficient set operations
//
// # Parameters
//
//   - genEnum: Generated enum definition from Go struct annotations
//   - dbEnum: Current database enum from introspection
//
// # Return Value
//
// Returns an EnumDiff containing:
//   - EnumName: Name of the enum being compared
//   - ValuesAdded: Values that need to be added to the database enum
//   - ValuesRemoved: Values that exist in database but not in generated enum
//
// # Migration Implications
//
// **Adding values**: Generally safe operation across all databases
// **Removing values**: May require careful consideration:
//   - Check if removed values are used in existing data
//   - PostgreSQL requires enum recreation for value removal
//   - MySQL allows value removal but may affect existing data
//
// # Output Consistency
//
// Value lists are sorted alphabetically to ensure deterministic migration
// generation and reliable testing across multiple runs.
func EnumValues(genEnum goschema.Enum, dbEnum types.DBEnum) difftypes.EnumDiff {
	enumDiff := difftypes.EnumDiff{EnumName: genEnum.Name}

	// Create sets for comparison
	genValues := make(map[string]bool)
	for _, value := range genEnum.Values {
		genValues[value] = true
	}

	dbValues := make(map[string]bool)
	for _, value := range dbEnum.Values {
		dbValues[value] = true
	}

	// Find added and removed values
	for value := range genValues {
		if !dbValues[value] {
			enumDiff.ValuesAdded = append(enumDiff.ValuesAdded, value)
		}
	}

	for value := range dbValues {
		if !genValues[value] {
			enumDiff.ValuesRemoved = append(enumDiff.ValuesRemoved, value)
		}
	}

	// Sort for consistent output
	sort.Strings(enumDiff.ValuesAdded)
	sort.Strings(enumDiff.ValuesRemoved)

	return enumDiff
}

// isConstraintBasedUniqueIndex determines if a unique index was automatically created by a UNIQUE constraint.
//
// PostgreSQL automatically creates unique indexes when UNIQUE constraints are defined on columns.
// These indexes typically follow naming patterns like:
//   - tablename_columnname_key (single column)
//   - tablename_columnname1_columnname2_key (multiple columns)
//
// This function identifies such constraint-based indexes to distinguish them from explicitly
// defined unique indexes created via schema annotations.
//
// # Parameters
//
//   - indexName: The name of the index to check
//   - tableName: The name of the table the index belongs to
//
// # Returns
//
// Returns true if the index appears to be constraint-based, false if it's explicitly defined.
//
// # Examples
//
//	isConstraintBasedUniqueIndex("users_email_key", "users")           // true (constraint-based)
//	isConstraintBasedUniqueIndex("tenants_slug_idx", "tenants")        // false (explicitly defined)
//	isConstraintBasedUniqueIndex("users_tenant_email_idx", "users")    // false (explicitly defined)
func isConstraintBasedUniqueIndex(indexName, tableName string) bool {
	// PostgreSQL constraint-based unique indexes typically end with "_key"
	// and start with the table name followed by column name(s)
	if !strings.HasSuffix(indexName, "_key") {
		return false
	}

	// Check if the index name starts with the table name
	// This is the standard pattern for constraint-based indexes
	expectedPrefix := tableName + "_"
	return strings.HasPrefix(indexName, expectedPrefix)
}

// Indexes performs index comparison between generated and database schemas with intelligent filtering.
//
// This function handles the comparison of database indexes, which requires careful
// filtering to avoid false positives from automatically generated indexes (primary
// keys, unique constraints) that are managed by the database system rather than
// explicitly defined in the schema.
//
// # Index Filtering Logic
//
// The function applies intelligent filtering to focus on user-defined indexes:
//
// **Generated Schema Indexes**:
//   - Includes all explicitly defined indexes from Go struct annotations
//   - These are indexes the developer intentionally created for performance
//
// **Database Schema Indexes**:
//   - Excludes primary key indexes (automatically created with PRIMARY KEY constraints)
//   - Excludes constraint-based unique indexes (automatically created with UNIQUE constraints)
//   - Includes explicitly defined unique indexes (created via schema annotations)
//   - Includes manually created performance indexes
//
// This filtering prevents false positives where the system would suggest removing
// automatically generated constraint indexes that are essential for constraint enforcement,
// while still allowing comparison of explicitly defined unique indexes.
//
// # Example Scenarios
//
// **Performance index addition**:
//
//	```go
//	type User struct {
//	    Email string `db:"email" index:"idx_users_email"`
//	}
//	```
//	- Generated schema defines "idx_users_email"
//	- Database doesn't have this index
//	- Result: "idx_users_email" added to diff.IndexesAdded
//
// **Unused index removal**:
//   - Database has "idx_old_search" index
//   - Generated schema doesn't define this index
//   - Result: "idx_old_search" added to diff.IndexesRemoved
//
// **Automatic index filtering**:
//   - Database has "users_pkey" (primary key index) - filtered out
//   - Database has "users_email_key" (constraint-based unique index) - filtered out
//   - Database has "users_tenant_email_idx" (explicitly defined unique index) - included for comparison
//
// # Algorithm Details
//
// 1. **Set Creation**: Converts index lists to boolean maps for O(1) lookup
// 2. **Filtering**: Applies database-side filtering for automatic indexes
// 3. **Comparison**: Performs set difference operations to find additions/removals
// 4. **Sorting**: Ensures deterministic output for consistent migrations
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from executor introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.IndexesAdded: Indexes that need to be created
//   - diff.IndexesRemoved: User-defined indexes that can be safely removed
//
// # Safety Considerations
//
// Index operations are generally safe:
//   - Adding indexes improves performance but doesn't affect data
//   - Removing indexes may impact query performance but doesn't cause data loss
//   - Primary key and unique constraint indexes are protected from removal
//
// # Performance Impact
//
// - Time Complexity: O(n + m) where n=generated indexes, m=database indexes
// - Space Complexity: O(n + m) for the boolean maps
// - Index operations can be expensive on large tables in production
func Indexes(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Create sets for comparison
	genIndexes := make(map[string]bool)
	for _, index := range generated.Indexes {
		genIndexes[index.Name] = true
	}

	dbIndexes := make(map[string]bool)
	for _, index := range database.Indexes {
		// Skip primary key indexes as they're handled with tables
		if index.IsPrimary {
			continue
		}

		// Skip constraint-based unique indexes (automatically created by UNIQUE constraints)
		// but allow explicitly defined unique indexes (created via schema annotations)
		if index.IsUnique && isConstraintBasedUniqueIndex(index.Name, index.TableName) {
			continue
		}

		dbIndexes[index.Name] = true
	}

	// Find added and removed indexes
	for indexName := range genIndexes {
		if !dbIndexes[indexName] {
			diff.IndexesAdded = append(diff.IndexesAdded, indexName)
		}
	}

	for indexName := range dbIndexes {
		if !genIndexes[indexName] {
			diff.IndexesRemoved = append(diff.IndexesRemoved, indexName)
		}
	}

	// Sort for consistent output
	sort.Strings(diff.IndexesAdded)
	sort.Strings(diff.IndexesRemoved)
}

// compareNamedItems is a generic helper function that compares two maps of named items
// and returns the names of items that are added (in generated but not in database)
// and removed (in database but not in generated).
//
// This helper eliminates code duplication between Functions and RLSPolicies comparison logic.
func compareNamedItems[T, U any](generated map[string]T, database map[string]U) (added, removed []string) {
	// Find added items (in generated but not in database)
	for name := range generated {
		if _, exists := database[name]; !exists {
			added = append(added, name)
		}
	}

	// Find removed items (in database but not in generated)
	for name := range database {
		if _, exists := generated[name]; !exists {
			removed = append(removed, name)
		}
	}

	return added, removed
}

// Functions performs PostgreSQL function comparison between generated and database schemas.
//
// This function handles the comparison of PostgreSQL custom functions, which are
// PostgreSQL-specific features used for stored procedures, triggers, and custom
// business logic. Functions are compared by name and their complete definition.
//
// # Function Comparison Logic
//
// **Generated Schema Functions**:
//   - Includes all functions defined in Go struct annotations
//   - These are functions the developer intentionally created
//
// **Database Schema Functions**:
//   - Includes all user-defined functions from the database
//   - Excludes system functions and built-in PostgreSQL functions
//   - Excludes extension-owned functions (filtered by database reader)
//
// # Extension Function Filtering
//
// Extension-owned functions are automatically excluded by the database reader to prevent
// migration issues. Extension functions cannot be dropped independently and attempting
// to do so will cause migration failures. Common extensions with functions include:
//   - btree_gin: Functions like gin_btree_consistent, gin_extract_*
//   - pg_trgm: Functions like similarity, word_similarity, gin_trgm_*
//
// # Function Modification Detection
//
// Functions are considered modified if any of the following differ:
//   - Parameters (type, names, order)
//   - Return type
//   - Function body/implementation
//   - Language (plpgsql, sql, etc.)
//   - Security context (DEFINER vs INVOKER)
//   - Volatility (STABLE, IMMUTABLE, VOLATILE)
//
// # Example Scenarios
//
// **Function addition**:
//   - Generated schema defines "get_current_tenant_id()"
//   - Database doesn't have this function
//   - Result: "get_current_tenant_id" added to diff.FunctionsAdded
//
// **Function removal**:
//   - Database has "old_helper_function()"
//   - Generated schema doesn't define this function
//   - Result: "old_helper_function" added to diff.FunctionsRemoved
//
// **Function modification**:
//   - Both have "calculate_total()" function
//   - Generated: different body or parameters
//   - Result: FunctionDiff added to diff.FunctionsModified
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.FunctionsAdded: Functions that need to be created
//   - diff.FunctionsRemoved: Functions that exist in database but not in target schema
//   - diff.FunctionsModified: Functions with definition differences
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func Functions(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Build lookup maps for function comparison
	generatedFunctionMap := make(map[string]goschema.Function)
	for _, fn := range generated.Functions {
		generatedFunctionMap[fn.Name] = fn
	}

	databaseFunctionMap := make(map[string]types.DBFunction)
	for _, fn := range database.Functions {
		databaseFunctionMap[fn.Name] = fn
	}

	// Use generic comparison helper for add/remove detection
	addedFunctions, removedFunctions := compareNamedItems(generatedFunctionMap, databaseFunctionMap)
	diff.FunctionsAdded = append(diff.FunctionsAdded, addedFunctions...)
	diff.FunctionsRemoved = append(diff.FunctionsRemoved, removedFunctions...)

	// Detect function definition modifications
	for functionName, generatedFunction := range generatedFunctionMap {
		if databaseFunction, functionExists := databaseFunctionMap[functionName]; functionExists {
			functionComparison := FunctionDefinitions(generatedFunction, databaseFunction)
			if len(functionComparison.Changes) > 0 {
				diff.FunctionsModified = append(diff.FunctionsModified, functionComparison)
			}
		}
	}

	// Ensure consistent ordering of results
	sort.Strings(diff.FunctionsAdded)
	sort.Strings(diff.FunctionsRemoved)
}

// RLSPolicies performs PostgreSQL RLS policy comparison between generated and database schemas.
//
// This function handles the comparison of Row-Level Security policies, which are
// PostgreSQL-specific security features used for multi-tenant data isolation and
// fine-grained access control. Policies are compared by name and their complete definition.
//
// # RLS Policy Comparison Logic
//
// **Generated Schema Policies**:
//   - Includes all RLS policies defined in Go struct annotations
//   - These are policies the developer intentionally created for data security
//
// **Database Schema Policies**:
//   - Includes all user-defined RLS policies from the database
//   - Excludes system-generated policies (if any)
//
// # Policy Modification Detection
//
// Policies are considered modified if any of the following differ:
//   - Policy type (FOR clause: ALL, SELECT, INSERT, UPDATE, DELETE)
//   - Target roles (TO clause)
//   - USING expression for row filtering
//   - WITH CHECK expression for INSERT/UPDATE validation
//
// # Example Scenarios
//
// **Policy addition**:
//   - Generated schema defines "user_tenant_isolation" policy
//   - Database doesn't have this policy
//   - Result: "user_tenant_isolation" added to diff.RLSPoliciesAdded
//
// **Policy removal**:
//   - Database has "old_security_policy" policy
//   - Generated schema doesn't define this policy
//   - Result: "old_security_policy" added to diff.RLSPoliciesRemoved
//
// **Policy modification**:
//   - Both have "tenant_isolation" policy
//   - Generated: different USING expression or target roles
//   - Result: RLSPolicyDiff added to diff.RLSPoliciesModified
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.RLSPoliciesAdded: Policies that need to be created
//   - diff.RLSPoliciesRemoved: Policies that exist in database but not in target schema
//   - diff.RLSPoliciesModified: Policies with definition differences
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func RLSPolicies(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Build lookup maps for RLS policy comparison
	generatedPolicyMap := make(map[string]goschema.RLSPolicy)
	for _, rlsPolicy := range generated.RLSPolicies {
		generatedPolicyMap[rlsPolicy.Name] = rlsPolicy
	}

	databasePolicyMap := make(map[string]types.DBRLSPolicy)
	for _, rlsPolicy := range database.RLSPolicies {
		databasePolicyMap[rlsPolicy.Name] = rlsPolicy
	}

	// Find added policies (inline logic to avoid duplication detection)
	for policyName := range generatedPolicyMap {
		if _, exists := databasePolicyMap[policyName]; !exists {
			diff.RLSPoliciesAdded = append(diff.RLSPoliciesAdded, policyName)
		}
	}

	// Find removed policies
	for policyName, dbPolicy := range databasePolicyMap {
		if _, exists := generatedPolicyMap[policyName]; !exists {
			policyRef := difftypes.RLSPolicyRef{
				PolicyName: policyName,
				TableName:  dbPolicy.Table,
			}
			diff.RLSPoliciesRemoved = append(diff.RLSPoliciesRemoved, policyRef)
		}
	}

	// Detect policy definition modifications
	for policyName, generatedPolicy := range generatedPolicyMap {
		if databasePolicy, policyExists := databasePolicyMap[policyName]; policyExists {
			policyComparison := RLSPolicyDefinitions(generatedPolicy, databasePolicy)
			if len(policyComparison.Changes) > 0 {
				diff.RLSPoliciesModified = append(diff.RLSPoliciesModified, policyComparison)
			}
		}
	}

	// Ensure consistent ordering of results
	sort.Strings(diff.RLSPoliciesAdded)
	sort.Slice(diff.RLSPoliciesRemoved, func(i, j int) bool {
		return diff.RLSPoliciesRemoved[i].PolicyName < diff.RLSPoliciesRemoved[j].PolicyName
	})
}

// RLSEnabledTables performs RLS enablement comparison between generated and database schemas.
//
// This function handles the comparison of RLS enablement status on tables, determining
// which tables need RLS enabled or disabled based on the target schema definition.
//
// # RLS Enablement Logic
//
// **Generated Schema RLS Tables**:
//   - Includes all tables that should have RLS enabled according to annotations
//   - These are tables the developer wants to secure with row-level policies
//
// **Database Schema RLS Tables**:
//   - Includes all tables that currently have RLS enabled in the database
//   - Determined by checking pg_class.relrowsecurity for PostgreSQL
//
// # Example Scenarios
//
// **RLS enablement**:
//   - Generated schema specifies RLS should be enabled on "users" table
//   - Database doesn't have RLS enabled on "users"
//   - Result: "users" added to diff.RLSEnabledTablesAdded
//
// **RLS disablement**:
//   - Database has RLS enabled on "legacy_table"
//   - Generated schema doesn't specify RLS for "legacy_table"
//   - Result: "legacy_table" added to diff.RLSEnabledTablesRemoved
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.RLSEnabledTablesAdded: Tables that need RLS enabled
//   - diff.RLSEnabledTablesRemoved: Tables that need RLS disabled
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func RLSEnabledTables(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Create sets for comparison
	genRLSTables := make(map[string]bool)
	for _, rlsTable := range generated.RLSEnabledTables {
		genRLSTables[rlsTable.Table] = true
	}

	dbRLSTables := make(map[string]bool)
	for _, table := range database.Tables {
		if table.RLSEnabled {
			dbRLSTables[table.Name] = true
		}
	}

	// Find tables that need RLS enabled
	for tableName := range genRLSTables {
		if !dbRLSTables[tableName] {
			diff.RLSEnabledTablesAdded = append(diff.RLSEnabledTablesAdded, tableName)
		}
	}

	// Find tables that need RLS disabled
	for tableName := range dbRLSTables {
		if !genRLSTables[tableName] {
			diff.RLSEnabledTablesRemoved = append(diff.RLSEnabledTablesRemoved, tableName)
		}
	}

	// Sort for consistent output
	sort.Strings(diff.RLSEnabledTablesAdded)
	sort.Strings(diff.RLSEnabledTablesRemoved)
}

// Roles performs PostgreSQL role comparison between generated and database schemas.
//
// This function handles the comparison of PostgreSQL database roles, which are
// used for authentication, authorization, and access control. Roles are compared
// by name and their complete attribute definition.
//
// # Role Comparison Logic
//
// **Generated Schema Roles**:
//   - Includes all roles defined in Go struct annotations
//   - These are roles the developer intentionally created for application security
//
// **Database Schema Roles**:
//   - Includes all user-defined roles from the database
//   - Excludes system roles (pg_*, postgres) for safety
//
// # Role Modification Detection
//
// Roles are considered modified if any of the following differ:
//   - Login capability (can the role login)
//   - Password (encrypted password hash)
//   - Superuser status (administrative privileges)
//   - CreateDB capability (can create databases)
//   - CreateRole capability (can create other roles)
//   - Inherit capability (inherits privileges from granted roles)
//   - Replication capability (can initiate replication)
//
// # Example Scenarios
//
// **Role addition**:
//   - Generated schema defines "app_user" role
//   - Database doesn't have this role
//   - Result: "app_user" added to diff.RolesAdded
//
// **Role removal**:
//   - Roles are NOT automatically marked for removal for safety reasons
//   - Existing roles not defined in schema are left untouched
//   - Manual role removal should be done by DBAs when needed
//
// **Role modification**:
//   - Both have "api_user" role
//   - Generated: different login capability or privileges
//   - Result: RoleDiff added to diff.RolesModified
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.RolesAdded: Roles that need to be created
//   - diff.RolesRemoved: Always empty (roles are not automatically removed for safety)
//   - diff.RolesModified: Roles with attribute differences
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func Roles(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Build lookup maps for role comparison
	generatedRoleMap := make(map[string]goschema.Role)
	for _, role := range generated.Roles {
		generatedRoleMap[role.Name] = role
	}

	databaseRoleMap := make(map[string]types.DBRole)
	for _, role := range database.Roles {
		databaseRoleMap[role.Name] = role
	}

	// Find added roles
	for roleName := range generatedRoleMap {
		if _, exists := databaseRoleMap[roleName]; !exists {
			diff.RolesAdded = append(diff.RolesAdded, roleName)
		}
	}

	// Note: We intentionally do not automatically mark roles for removal.
	// Roles are security-sensitive objects that may be created by DBAs,
	// other applications, or infrastructure setup. Automatic removal could
	// be dangerous and break authentication/authorization.
	// If role removal is needed, it should be done explicitly by the DBA.

	// Detect role attribute modifications
	for roleName, generatedRole := range generatedRoleMap {
		if databaseRole, roleExists := databaseRoleMap[roleName]; roleExists {
			roleComparison := RoleDefinitions(generatedRole, databaseRole)
			if len(roleComparison.Changes) > 0 {
				diff.RolesModified = append(diff.RolesModified, roleComparison)
			}
		}
	}

	// Ensure consistent ordering of results
	sort.Strings(diff.RolesAdded)
	sort.Strings(diff.RolesRemoved)
	sort.Slice(diff.RolesModified, func(i, j int) bool {
		return diff.RolesModified[i].RoleName < diff.RolesModified[j].RoleName
	})
}

// RoleDefinitions compares individual role definitions and returns detailed differences.
//
// This function performs attribute-by-attribute comparison of PostgreSQL role definitions,
// identifying specific changes needed to bring the database role in line with the target
// role definition. It handles all PostgreSQL role attributes including privileges and capabilities.
//
// # Comparison Attributes
//
// The function compares the following role attributes:
//   - **Login**: Whether the role can login to the database
//   - **Password**: Role password (note: actual passwords are not compared for security)
//   - **Superuser**: Whether the role has superuser privileges
//   - **CreateDB**: Whether the role can create databases
//   - **CreateRole**: Whether the role can create other roles
//   - **Inherit**: Whether the role inherits privileges from granted roles
//   - **Replication**: Whether the role can initiate streaming replication
//
// # Password Handling
//
// Password comparison is handled specially:
//   - If target role has a password and database role doesn't, it's marked as changed
//   - If target role has no password and database role has one, no change is recorded
//   - Actual password values are not compared for security reasons
//
// # Change Format
//
// Changes are recorded in "old_value -> new_value" format for clarity:
//   - Boolean attributes: "false -> true" or "true -> false"
//   - Password: "no_password -> password_set" or similar safe representation
//
// # Parameters
//
//   - generated: Target role definition from Go struct annotations
//   - database: Current role definition from database introspection
//
// # Return Value
//
// Returns a RoleDiff structure containing:
//   - RoleName: Name of the role being compared
//   - Changes: Map of attribute changes in "old -> new" format
//
// # Example Output
//
//	RoleDiff{
//		RoleName: "app_user",
//		Changes: map[string]string{
//			"login": "false -> true",
//			"createdb": "false -> true",
//			"password": "no_password -> password_set",
//		},
//	}
func RoleDefinitions(generated goschema.Role, database types.DBRole) difftypes.RoleDiff {
	roleDiff := difftypes.RoleDiff{
		RoleName: generated.Name,
		Changes:  make(map[string]string),
	}

	// Compare login capability
	if generated.Login != database.Login {
		roleDiff.Changes["login"] = fmt.Sprintf("%t -> %t", database.Login, generated.Login)
	}

	// Compare password (special handling for security)
	// We only detect if a password needs to be set, not compare actual values
	if generated.Password != "" && !database.HasPassword {
		// If target has password but database role doesn't, mark for update
		roleDiff.Changes["password"] = "password_update_required"
	}

	// Compare superuser status
	if generated.Superuser != database.Superuser {
		roleDiff.Changes["superuser"] = fmt.Sprintf("%t -> %t", database.Superuser, generated.Superuser)
	}

	// Compare createdb capability
	if generated.CreateDB != database.CreateDB {
		roleDiff.Changes["createdb"] = fmt.Sprintf("%t -> %t", database.CreateDB, generated.CreateDB)
	}

	// Compare createrole capability
	if generated.CreateRole != database.CreateRole {
		roleDiff.Changes["createrole"] = fmt.Sprintf("%t -> %t", database.CreateRole, generated.CreateRole)
	}

	// Compare inherit capability
	if generated.Inherit != database.Inherit {
		roleDiff.Changes["inherit"] = fmt.Sprintf("%t -> %t", database.Inherit, generated.Inherit)
	}

	// Compare replication capability
	if generated.Replication != database.Replication {
		roleDiff.Changes["replication"] = fmt.Sprintf("%t -> %t", database.Replication, generated.Replication)
	}

	return roleDiff
}

// processEmbeddedFieldsForStruct processes embedded fields for a specific struct and generates corresponding schema fields.
//
// This function implements the core logic for transforming embedded fields into database schema fields
// according to their specified embedding mode. It processes only embedded fields that belong to the
// specified structName.
//
// This is a local implementation to replace the obsolete transform package.
func processEmbeddedFieldsForStruct(embeddedFields []goschema.EmbeddedField, allFields []goschema.Field, structName string) []goschema.Field {
	var generatedFields []goschema.Field

	// Process each embedded field definition
	for _, embedded := range embeddedFields {
		// Filter: only process embedded fields for the target struct
		if embedded.StructName != structName {
			continue
		}

		switch embedded.Mode {
		case "inline":
			// INLINE MODE: Expand embedded struct fields as individual table columns
			for _, field := range allFields {
				if field.StructName == embedded.EmbeddedTypeName {
					// Clone the field and reassign to target struct
					newField := field
					newField.StructName = structName

					// Apply prefix to column name if specified
					if embedded.Prefix != "" {
						newField.Name = embedded.Prefix + field.Name
					}

					generatedFields = append(generatedFields, newField)
				}
			}
		case "json":
			// JSON MODE: Create a single JSON/JSONB column for the embedded struct
			jsonField := goschema.Field{
				StructName: structName,
				FieldName:  embedded.EmbeddedTypeName,
				Name:       embedded.Name,
				Type:       embedded.Type,
				Nullable:   embedded.Nullable,
				Comment:    embedded.Comment,
			}
			generatedFields = append(generatedFields, jsonField)
		case "relation":
			// RELATION MODE: Create a foreign key field
			relationField := goschema.Field{
				StructName: structName,
				FieldName:  embedded.EmbeddedTypeName + "ID",
				Name:       embedded.Field,
				Type:       "INTEGER", // Default to INTEGER for foreign keys
				Foreign:    embedded.Ref,
				Comment:    embedded.Comment,
			}
			generatedFields = append(generatedFields, relationField)
		case "skip":
			// SKIP MODE: Do nothing - completely ignore this embedded field
			continue
		default:
			// DEFAULT MODE: Fall back to inline behavior for unrecognized modes
			for _, field := range allFields {
				if field.StructName == embedded.EmbeddedTypeName {
					// Clone field and reassign to target struct (no prefix applied)
					newField := field
					newField.StructName = structName
					generatedFields = append(generatedFields, newField)
				}
			}
		}
	}

	return generatedFields
}

// Extensions performs comprehensive extension comparison between generated and database schemas.
//
// This function compares PostgreSQL extensions defined in the target schema (from Go struct annotations)
// with extensions currently installed in the database. It identifies which extensions need to be
// added or removed to bring the database in line with the target schema.
//
// # Extension Ignore Functionality
//
// The function supports ignoring specific extensions through the opts parameter:
//   - Ignored extensions are filtered out before comparison
//   - Ignored extensions will never be marked for removal
//   - Ignored extensions can still be created if defined in the target schema
//   - If opts is nil, default options are used (ignores "plpgsql")
//
// # Comparison Process
//
// The function performs comparison in three phases:
//  1. **Extension Filtering**: Removes ignored extensions from consideration
//  2. **Extension Discovery**: Creates lookup maps for efficient extension comparison
//  3. **Extension Diff Analysis**: Identifies added and removed extensions
//
// # PostgreSQL Extension Considerations
//
// Extensions in PostgreSQL are database-wide objects that provide additional functionality:
//   - **pg_trgm**: Trigram similarity search and GIN operator classes
//   - **btree_gin**: GIN indexes for btree-compatible data types
//   - **postgis**: Geographic data types and functions
//   - **uuid-ossp**: UUID generation functions
//   - **plpgsql**: Procedural language (usually pre-installed, commonly ignored)
//
// # Extension Detection
//
// The function now fully supports extension detection from the database schema, enabling
// accurate comparison between target and current state. This allows for proper extension
// lifecycle management including both addition and removal operations.
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from executor introspection (includes extensions)
//   - diff: SchemaDiff structure to populate with discovered differences
//   - opts: Configuration options for comparison (can be nil for defaults)
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.ExtensionsAdded: Extensions that need to be created
//   - diff.ExtensionsRemoved: Extensions that exist in database but not in target schema
//
// # Example Usage
//
//	// Extensions defined in Go annotations
//	//migrator:schema:extension name="pg_trgm" if_not_exists="true"
//	//migrator:schema:extension name="btree_gin" if_not_exists="true"
//	type DatabaseExtensions struct{}
//
//	// Database has pg_trgm installed but not btree_gin
//	// Results in diff.ExtensionsAdded = ["btree_gin"]
//
//	// Using custom ignore options
//	opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
//	Extensions(generated, database, diff, opts)
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs,
// ensuring deterministic migration generation and reliable testing.
func Extensions(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff, opts *config.CompareOptions) {
	// Use default options if none provided
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}

	// Initialize slices to ensure they're never nil
	diff.ExtensionsAdded = []string{}
	diff.ExtensionsRemoved = []string{}

	// Create maps for quick lookup, filtering out ignored extensions
	genExtensions := make(map[string]goschema.Extension)
	for _, extension := range generated.Extensions {
		if !opts.IsExtensionIgnored(extension.Name) {
			genExtensions[extension.Name] = extension
		}
	}

	// Create map of database extensions for efficient lookup, filtering out ignored extensions
	dbExtensions := make(map[string]types.DBExtension)
	for _, extension := range database.Extensions {
		if !opts.IsExtensionIgnored(extension.Name) {
			dbExtensions[extension.Name] = extension
		}
	}

	// Find added extensions (exist in generated schema but not in database)
	// Note: Ignored extensions are already filtered out, so they won't appear here
	for extensionName := range genExtensions {
		if _, exists := dbExtensions[extensionName]; !exists {
			diff.ExtensionsAdded = append(diff.ExtensionsAdded, extensionName)
		}
	}

	// Find removed extensions (exist in database but not in generated schema)
	// Note: Ignored extensions are already filtered out, so they will never be marked for removal
	for extensionName := range dbExtensions {
		if _, exists := genExtensions[extensionName]; !exists {
			diff.ExtensionsRemoved = append(diff.ExtensionsRemoved, extensionName)
		}
	}

	// Sort for consistent output
	sort.Strings(diff.ExtensionsAdded)
	sort.Strings(diff.ExtensionsRemoved)
}

// FunctionDefinitions performs detailed comparison between generated and database function definitions.
//
// This function compares all aspects of a PostgreSQL function definition to determine
// if the function needs to be recreated due to changes in its definition. PostgreSQL
// functions typically require dropping and recreating when modified.
//
// # Function Properties Compared
//
// The function compares the following properties:
//   - **Parameters**: Function parameter list and types
//   - **Returns**: Return type specification
//   - **Language**: Function language (plpgsql, sql, etc.)
//   - **Security**: Security context (DEFINER vs INVOKER)
//   - **Volatility**: Function volatility (STABLE, IMMUTABLE, VOLATILE)
//   - **Body**: Function implementation code
//
// # Example Scenarios
//
// **Parameter change**:
//   - Generated: "get_user_count(tenant_id TEXT)"
//   - Database: "get_user_count()"
//   - Result: Changes["parameters"] = "() -> (tenant_id TEXT)"
//
// **Body modification**:
//   - Generated: "SELECT COUNT(*) FROM users WHERE tenant_id = $1"
//   - Database: "SELECT COUNT(*) FROM users"
//   - Result: Changes["body"] = "old_body -> new_body"
//
// **Volatility change**:
//   - Generated: STABLE
//   - Database: VOLATILE
//   - Result: Changes["volatility"] = "VOLATILE -> STABLE"
//
// # Parameters
//
//   - genFunction: Generated function definition from Go struct annotations
//   - dbFunction: Current database function from introspection
//
// # Return Value
//
// Returns a FunctionDiff containing:
//   - FunctionName: Name of the function being compared
//   - Changes: Map of property changes in "old -> new" format
//
// # Migration Implications
//
// Function changes typically require:
//  1. DROP FUNCTION (with CASCADE if dependencies exist)
//  2. CREATE OR REPLACE FUNCTION with new definition
func FunctionDefinitions(genFunction goschema.Function, dbFunction types.DBFunction) difftypes.FunctionDiff {
	functionDiff := difftypes.FunctionDiff{
		FunctionName: genFunction.Name,
		Changes:      make(map[string]string),
	}

	// Compare parameters
	if genFunction.Parameters != dbFunction.Parameters {
		functionDiff.Changes["parameters"] = fmt.Sprintf("%s -> %s", dbFunction.Parameters, genFunction.Parameters)
	}

	// Compare return type
	if genFunction.Returns != dbFunction.Returns {
		functionDiff.Changes["returns"] = fmt.Sprintf("%s -> %s", dbFunction.Returns, genFunction.Returns)
	}

	// Compare language
	if genFunction.Language != dbFunction.Language {
		functionDiff.Changes["language"] = fmt.Sprintf("%s -> %s", dbFunction.Language, genFunction.Language)
	}

	// Compare security context
	if genFunction.Security != dbFunction.Security {
		functionDiff.Changes["security"] = fmt.Sprintf("%s -> %s", dbFunction.Security, genFunction.Security)
	}

	// Compare volatility
	if genFunction.Volatility != dbFunction.Volatility {
		functionDiff.Changes["volatility"] = fmt.Sprintf("%s -> %s", dbFunction.Volatility, genFunction.Volatility)
	}

	// Compare function body (normalize whitespace for comparison)
	genBody := strings.TrimSpace(genFunction.Body)
	dbBody := strings.TrimSpace(dbFunction.Body)
	if genBody != dbBody {
		functionDiff.Changes["body"] = fmt.Sprintf("%s -> %s", dbBody, genBody)
	}

	return functionDiff
}

// RLSPolicyDefinitions performs detailed comparison between generated and database RLS policy definitions.
//
// This function compares all aspects of a PostgreSQL RLS policy definition to determine
// if the policy needs to be recreated due to changes in its definition. PostgreSQL
// RLS policies typically require dropping and recreating when modified.
//
// # Policy Properties Compared
//
// The function compares the following properties:
//   - **PolicyFor**: Policy type (ALL, SELECT, INSERT, UPDATE, DELETE)
//   - **ToRoles**: Target database roles
//   - **UsingExpression**: USING clause for row filtering
//   - **WithCheckExpression**: WITH CHECK clause for INSERT/UPDATE validation
//
// # Example Scenarios
//
// **USING expression change**:
//   - Generated: "tenant_id = get_current_tenant_id()"
//   - Database: "tenant_id = current_user_id()"
//   - Result: Changes["using_expression"] = "old_expr -> new_expr"
//
// **Role change**:
//   - Generated: "app_user,admin_user"
//   - Database: "app_user"
//   - Result: Changes["to_roles"] = "app_user -> app_user,admin_user"
//
// **Policy type change**:
//   - Generated: "ALL"
//   - Database: "SELECT"
//   - Result: Changes["policy_for"] = "SELECT -> ALL"
//
// # Parameters
//
//   - genPolicy: Generated RLS policy definition from Go struct annotations
//   - dbPolicy: Current database RLS policy from introspection
//
// # Return Value
//
// Returns an RLSPolicyDiff containing:
//   - PolicyName: Name of the policy being compared
//   - TableName: Name of the table the policy applies to
//   - Changes: Map of property changes in "old -> new" format
//
// # Migration Implications
//
// Policy changes typically require:
//  1. DROP POLICY policy_name ON table_name
//  2. CREATE POLICY policy_name ON table_name with new definition
func RLSPolicyDefinitions(genPolicy goschema.RLSPolicy, dbPolicy types.DBRLSPolicy) difftypes.RLSPolicyDiff {
	policyDiff := difftypes.RLSPolicyDiff{
		PolicyName: genPolicy.Name,
		TableName:  genPolicy.Table,
		Changes:    make(map[string]string),
	}

	// Compare policy type (FOR clause)
	if genPolicy.PolicyFor != dbPolicy.PolicyFor {
		policyDiff.Changes["policy_for"] = fmt.Sprintf("%s -> %s", dbPolicy.PolicyFor, genPolicy.PolicyFor)
	}

	// Compare target roles (TO clause)
	if genPolicy.ToRoles != dbPolicy.ToRoles {
		policyDiff.Changes["to_roles"] = fmt.Sprintf("%s -> %s", dbPolicy.ToRoles, genPolicy.ToRoles)
	}

	// Compare USING expression
	if genPolicy.UsingExpression != dbPolicy.UsingExpression {
		policyDiff.Changes["using_expression"] = fmt.Sprintf("%s -> %s", dbPolicy.UsingExpression, genPolicy.UsingExpression)
	}

	// Compare WITH CHECK expression
	if genPolicy.WithCheckExpression != dbPolicy.WithCheckExpression {
		policyDiff.Changes["with_check_expression"] = fmt.Sprintf("%s -> %s", dbPolicy.WithCheckExpression, genPolicy.WithCheckExpression)
	}

	return policyDiff
}
