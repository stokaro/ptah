package compare

import (
	"sort"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
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
//   - Database has "archived_data" table
//   - Generated schema doesn't define "archived_data"
//   - Result: "archived_data" added to diff.TablesRemoved
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
		genTables[table.QualifiedName()] = table
	}

	dbTables := make(map[string]types.DBTable)
	for _, table := range database.Tables {
		dbTables[table.QualifiedName()] = table
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
	sort.Slice(diff.TablesModified, func(i, j int) bool {
		return diff.TablesModified[i].TableName < diff.TablesModified[j].TableName
	})
}
