package compare

import (
	"sort"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/tableref"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
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
	TablesAndColumnsWithDialect(generated, database, diff, "")
}

// TablesAndColumnsWithDialect performs table and column comparison with
// dialect-aware normalization for surfaces whose catalogs rewrite expressions.
func TablesAndColumnsWithDialect(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	TablesAndColumnsWithSemantics(
		generated,
		database,
		diff,
		dialect,
		identifier.ForDialect(dialect),
		CoverageOf(generated, database),
	)
}

// TablesAndColumnsWithSemantics compares tables and columns using explicit
// identifier rules while retaining target spelling in the produced diff.
func TablesAndColumnsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	cov Coverage,
) {
	// Create maps for quick lookup
	genTables := make(map[tableIdentity]goschema.Table)
	for _, table := range generated.Tables {
		genTables[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
	}

	dbTables := make(map[tableIdentity]types.DBTable)
	for _, table := range database.Tables {
		dbTables[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
	}
	objectOwnedUniqueColumns := collectGeneratedObjectOwnedUniqueColumns(
		generated,
		semantics,
	)

	// Find added and removed tables
	for identity, table := range genTables {
		if _, exists := dbTables[identity]; !exists {
			diff.TablesAdded = append(diff.TablesAdded, tableDiffName(table.Schema, table.Name, dialect))
		}
	}

	for identity, table := range dbTables {
		if _, exists := genTables[identity]; !exists {
			diff.TablesRemoved = append(diff.TablesRemoved, tableDiffName(table.Schema, table.Name, dialect))
		}
	}

	// Find modified tables (compare columns)
	for identity, genTable := range genTables {
		if dbTable, exists := dbTables[identity]; exists {
			// A SQLite virtual table has no column list of its own: its
			// columns are the module's answer, and when the module is not
			// registered in this build the catalog reports none at all.
			// Comparing them against a desired table's columns plans
			// `ALTER TABLE ... ADD COLUMN` against an object ALTER TABLE
			// cannot touch.
			//
			// This is the fail-safe, not the report. Reaching it means the
			// desired state declares an ordinary table whose live counterpart
			// is virtual, and two different kinds of object have collided;
			// silently reporting no difference would leave the incompatible
			// object in place while every surface said the schema was synced.
			// [go.5x5.cz/ptah/internal/sqlitevirtual.ValidateComparison]
			// refuses that collision by name at the seams that can return an
			// error, which is every verb comparing a live database. What
			// remains here is the direct library API, which has no error to
			// return and must still not emit an unrunnable ALTER.
			// See stokaro/ptah#1028.
			if dbTable.VirtualModule != "" {
				continue
			}
			tableDiff := tableColumnsWithSemantics(
				genTable,
				dbTable,
				generated,
				dialect,
				semantics,
				objectOwnedUniqueColumns,
			)
			if len(tableDiff.ColumnsAdded) > 0 || len(tableDiff.ColumnsRemoved) > 0 || len(tableDiff.ColumnsModified) > 0 {
				diff.TablesModified = append(diff.TablesModified, tableDiff)
			}
		}
	}

	// A table in a schema one side never read is not a table that side says is
	// gone. Widening the schema reader without this made applying a database's
	// own description back to it plan CREATE SCHEMA and CREATE TABLE for a
	// schema and a table that exist (stokaro/ptah#1264, stokaro/ptah#1276).
	//
	// `CREATE TABLE` is rendered without a guard on every dialect Ptah supports,
	// so a table in an unread schema cannot be planned safely; it is withheld
	// and named rather than dropped in silence.
	keptTables, withheldTables := cov.keepPlannedAdditions(
		coverage.Schema, diff.TablesAdded, tableSchemaOnly, unguardedCreations(),
	)
	diff.TablesAdded = keptTables
	cov.recordUndecidedAdditions(coverage.Schema, withheldTables)
	diff.TablesRemoved = cov.keepPlannedRemovals(coverage.Schema, diff.TablesRemoved, tableSchemaOnly)

	// Sort for consistent output
	sort.Strings(diff.TablesAdded)
	sort.Strings(diff.TablesRemoved)
	sort.Slice(diff.TablesModified, func(i, j int) bool {
		return diff.TablesModified[i].TableName < diff.TablesModified[j].TableName
	})
}

// tableMapIdentity preserves SQLite's exact catalog bytes at the table-map
// boundary. This path decides whether a table is added, removed, or paired for
// comparison; trimming here can pair a virtual table named " docs " with the
// distinct ordinary table docs before the exact removal name reaches the
// planner. Other dialects retain the established comparison behavior.
func tableMapIdentity(schema, name, dialect string, semantics identifier.Semantics) tableIdentity {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		return newTableIdentity(schema, name, semantics)
	}
	if schema == "" {
		schema = semantics.DefaultSchema
	}
	return tableIdentity{
		schema: semantics.TableIdentityKey(schema),
		table:  semantics.TableIdentityKey(name),
	}
}

// tableDiffName preserves the exact catalog identifier on SQLite. Quoted
// leading and trailing whitespace is part of a SQLite table name; normalizing
// it here can redirect an authorized DROP from a virtual table to a distinct
// ordinary near-twin. Other dialects retain the established canonical spelling.
func tableDiffName(schema, name, dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.SQLite {
		return tableref.CanonicalExact(schema, name)
	}
	return types.QualifyTableName(schema, name)
}
