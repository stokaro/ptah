package compare

import (
	"sort"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/crdbttl"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/spannerttl"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
func TablesAndColumns(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	TablesAndColumnsWithDialect(desired, current, diff, "")
}

// TablesAndColumnsWithDialect performs table and column comparison with
// dialect-aware normalization for surfaces whose catalogs rewrite expressions.
func TablesAndColumnsWithDialect(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	TablesAndColumnsWithSemantics(
		desired,
		database,
		diff,
		dialect,
		identifier.ForDialect(dialect),
		CoverageOf(desired, database),
	)
}

// TablesAndColumnsWithSemantics compares tables and columns using explicit
// identifier rules while retaining target spelling in the produced diff.
func TablesAndColumnsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	cov Coverage,
) {
	TablesAndColumnsWithGeneratedExpressions(desired, database, diff, dialect, semantics, cov, nil)
}

// TablesAndColumnsWithGeneratedExpressions is [TablesAndColumnsWithSemantics]
// told how the target itself spells each declared generated expression.
//
// A nil map is every comparison that could not ask a server, which is what the
// six-argument form passes. See [config.CompareOptions.GeneratedExpressions].
func TablesAndColumnsWithGeneratedExpressions(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	cov Coverage,
	generatedExpressions map[string]config.GeneratedExpression,
) {
	// Create maps for quick lookup
	genTables := make(map[tableIdentity]schemamodel.Table)
	for _, table := range desired.Tables {
		genTables[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
	}

	dbTables := make(map[tableIdentity]catalog.Table)
	for _, table := range database.Tables {
		dbTables[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = table
	}
	objectOwnedUniqueColumns := collectGeneratedObjectOwnedUniqueColumns(
		desired,
		semantics,
	)

	// Find added and removed tables.
	//
	// A creation carries what CREATE TABLE renders from rather than a name a
	// planner would have to resolve back into a declaration: this table's
	// columns, with embedded fields already folded in, and the enums those
	// columns name (stokaro/ptah#2315).
	// The type vocabulary a created column may name, carried once for the whole
	// diff rather than per table: a column may name a type nothing here changes
	// (stokaro/ptah#2315).
	diff.DeclaredUserTypes = difftypes.UserTypeVocabularyOf(desired)
	// Every declared table, for resolving the table a foreign key references --
	// usually one this diff does not touch.
	diff.DeclaredTables = desired.Tables
	// Every declared view and materialized view, for resolving what a DROP
	// cascades to -- usually a view this diff does not touch.
	diff.DeclaredViewLikes = difftypes.ViewLikeVocabularyOf(desired)
	for identity, table := range genTables {
		if _, exists := dbTables[identity]; !exists {
			diff.TablesAdded = append(diff.TablesAdded, difftypes.TableCreationFor(
				desired, table, tableDiffName(table.Schema, table.Name, dialect)))
		}
	}

	for identity, table := range dbTables {
		if _, exists := genTables[identity]; !exists {
			// An object type the desired source cannot represent is outside
			// the surface that source manages. Go annotations, HCL and YAML
			// have no virtual-table construct, so their silence about a live
			// FTS5 index is not a request to drop it -- and the drop takes the
			// index and everything in it. A native `.sql` document can express
			// one and records nothing in NotDescribed, so its silence still
			// removes (stokaro/ptah#1028).
			if table.VirtualModule != "" &&
				!cov.PlansRemoval(coverage.VirtualTable, table.Schema, table.Name) {
				continue
			}
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
				desired,
				dialect,
				semantics,
				objectOwnedUniqueColumns,
				generatedExpressions,
			)
			// The TTL policy is compared here rather than inside
			// tableColumnsWithSemantics because it is a property of the table
			// and not of any column, and because a table whose ONLY difference
			// is its retention policy still has to reach TablesModified -- the
			// column-count condition below would otherwise drop it, and the
			// schema would report as synced while rows expire on a schedule
			// nobody declared (stokaro/ptah#1027).
			tableDiff.RowTTLChange = rowTTLChange(genTable.RowTTL, dbTable.RowTTL)
			// The row deletion policy is compared here for exactly the reasons
			// above: it belongs to the table, and a table whose only difference
			// is its retention has to reach TablesModified or the schema
			// reports synced while rows expire on a schedule nobody declared
			// (stokaro/ptah#2236).
			tableDiff.RowDeletionPolicyChange = rowDeletionPolicyChange(
				genTable.RowDeletionPolicy, dbTable.RowDeletionPolicy, semantics)
			// A comment is compared here for the same reason as the TTL policy,
			// and reaches TablesModified for the same reason: it belongs to the
			// table rather than to any column, and a table whose only
			// difference is its comment has to arrive here or the schema
			// reports synced while the declaration and the database say
			// different things about what the table is for -- on every run,
			// with nothing able to fix it (stokaro/ptah#2168).
			tableDiff.CommentChange = commentChange(genTable.Comment, dbTable.Comment)
			if len(tableDiff.ColumnsAdded) > 0 || len(tableDiff.ColumnsRemoved) > 0 ||
				len(tableDiff.ColumnsModified) > 0 || tableDiff.RowTTLChange != nil ||
				tableDiff.RowDeletionPolicyChange != nil || tableDiff.CommentChange != nil {
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
	keptTables, withheldTables := keepPlannedAdditions(cov,
		coverage.Schema, diff.TablesAdded, tableCreationSchemaOnly, tableCreationName, unguardedCreations(),
	)
	diff.TablesAdded = keptTables
	cov.recordUndecidedAdditions(withheldTables)
	diff.TablesRemoved = keepPlannedRemovals(cov, coverage.Schema, diff.TablesRemoved, tableSchemaOnly)

	// Sort for consistent output
	sort.Slice(diff.TablesAdded, func(i, j int) bool {
		return diff.TablesAdded[i].Name < diff.TablesAdded[j].Name
	})
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
	// Verbatim on SQLite: a quoted leading or trailing space is part of the
	// name there, so trimming merges two distinct tables.
	return objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema, name).Key()
}

// tableDiffName preserves the exact catalog identifier on SQLite. Quoted
// leading and trailing whitespace is part of a SQLite table name; normalizing
// it here can redirect an authorized DROP from a virtual table to a distinct
// ordinary near-twin. Other dialects retain the established canonical spelling.
func tableDiffName(schema, name, dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.SQLite {
		return tableref.CanonicalExact(schema, name)
	}
	return catalog.QualifyTableName(schema, name)
}

// rowTTLChange reports the row-level TTL transition between a declaration and a
// target, and nil when there is none.
//
// Equality is exact, which is the whole guarantee: every parameter Ptah models
// was measured to read back from the catalog exactly as it was written, so two
// values that differ are two policies that differ. A comparison that normalized
// here would be re-deriving a rule the server does not apply, and the failure
// mode is the one stokaro/ptah#1027 names -- reporting convergence while a
// table's data-lifecycle policy differs.
func rowTTLChange(desired, current *ast.RowTTLSpec) *difftypes.RowTTLChange {
	if crdbttl.Equal(desired, current) {
		return nil
	}
	return &difftypes.RowTTLChange{Desired: desired.Clone(), Current: current.Clone()}
}

// rowDeletionPolicyChange is the transition a table's row deletion policy makes,
// and nil when there is none.
//
// Equality is NOT exact here, and that is the difference from rowTTLChange
// above. The server rewrites the interval it stores -- measured against the
// Cloud Spanner emulator behind PGAdapter 0.55.2, `INTERVAL '30 days'` reads
// back as `INTERVAL '4 WEEKS 2 DAYS'` -- so comparing the two as text would
// report a difference between a database and its own description, forever.
// [go.5x5.cz/ptah/internal/spannerttl] owns that comparison (stokaro/ptah#2236).
func rowDeletionPolicyChange(
	desired, current *ast.RowDeletionPolicySpec,
	semantics identifier.Semantics,
) *difftypes.RowDeletionPolicyChange {
	if spannerttl.Equal(desired, current, semantics.ColumnIdentityKey) {
		return nil
	}
	return &difftypes.RowDeletionPolicyChange{Desired: desired.Clone(), Current: current.Clone()}
}

// tableCreationSchemaOnly and tableCreationName are the coverage filter's two
// accessors for a creation, reading the name the diff carries rather than one
// derived from the declaration: the comparison qualifies a name per dialect,
// and a filter asking a different question than the plan does would withhold
// the wrong tables.
func tableCreationSchemaOnly(creation difftypes.TableCreation) (string, []string) {
	return tableSchemaOnly(creation.Name)
}

func tableCreationName(creation difftypes.TableCreation) string { return creation.Name }
