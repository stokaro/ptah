package compare

import (
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/indexscope"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// isConstraintBasedUniqueIndex determines if a unique index was automatically created by a UNIQUE constraint.
//
// Different database systems create unique indexes with different naming patterns when UNIQUE
// constraints are defined on columns:
//
// **PostgreSQL**:
//   - tablename_columnname_key (single column)
//   - tablename_columnname1_columnname2_key (multiple columns)
//
// **MySQL/MariaDB**:
//   - Simple column names (e.g., "email", "username") for single-column constraints
//   - Constraint names for multi-column constraints (e.g., "uk_users_email_name")
//
// This function identifies such constraint-based indexes to distinguish them from explicitly
// defined unique indexes created via schema annotations.
//
// # Assumptions
//
// This function relies on standard naming conventions used by database systems for
// constraint-based indexes. These patterns may vary with different database versions,
// configurations, or custom naming schemes. The detection is based on common patterns
// observed in PostgreSQL 12+, MySQL 8.0+, and MariaDB 10.5+.
//
// # Parameters
//
//   - indexName: The name of the index to check
//   - tableName: The name of the table the index belongs to
//   - columns: The columns that the index covers (used for MySQL/MariaDB detection)
//
// # Returns
//
// Returns true if the index appears to be constraint-based, false if it's explicitly defined.
//
// # Examples
//
//	// PostgreSQL
//	isConstraintBasedUniqueIndex("users_email_key", "users", []string{"email"})     // true
//	isConstraintBasedUniqueIndex("tenants_slug_idx", "tenants", []string{"slug"})   // false
//
//	// MySQL/MariaDB
//	isConstraintBasedUniqueIndex("email", "users", []string{"email"})               // true
//	isConstraintBasedUniqueIndex("idx_users_custom", "users", []string{"email"})    // false
func isConstraintBasedUniqueIndex(indexName, tableName string, columns []string) bool {
	// PostgreSQL pattern: tablename_columnname_key
	if strings.HasSuffix(indexName, "_key") {
		expectedPrefix := tableName + "_"
		return strings.HasPrefix(indexName, expectedPrefix) && postgresConstraintPattern.MatchString(indexName)
	}

	// MySQL/MariaDB pattern: simple column name for single-column unique constraints
	// MySQL automatically creates indexes with the same name as the column for UNIQUE constraints
	if len(columns) == 1 {
		// Only consider it constraint-based if the index name matches the column name,
		// and it does NOT match custom index patterns (e.g., does not start with "idx_" or "index_").
		// We don't check mysqlTableColumnsPattern here because simple column names like "email"
		// don't match that pattern (it requires table_column format).
		return indexName == columns[0] &&
			!customIndexPattern.MatchString(indexName)
	}

	// MySQL/MariaDB constraint-based indexes with "uk_" prefix
	if mysqlUKPattern.MatchString(indexName) {
		return true
	}

	// Be more conservative about table_column patterns - only consider it constraint-based
	// if it follows a very specific pattern and doesn't look like a custom index name
	if isMySQLConstraintBasedUniqueIndex(indexName, tableName) {
		return true
	}

	return false
}

// isMySQLConstraintBasedUniqueIndex checks if an index follows MySQL/MariaDB constraint-based patterns.
// This helper function encapsulates the complex logic for detecting MySQL/MariaDB constraint-based
// unique indexes that follow table_column naming patterns but are not custom indexes.
func isMySQLConstraintBasedUniqueIndex(indexName, tableName string) bool {
	return mysqlTableColumnsPattern.MatchString(indexName) &&
		strings.HasPrefix(indexName, tableName+"_") &&
		!customIndexPattern.MatchString(indexName)
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
//		```go
//		type User struct {
//		    Email string `db:"email" index:"idx_users_email"`
//		}
//		```
//	  - Generated schema defines "idx_users_email" on "users"
//	  - Database doesn't have this index
//	  - Result: IndexRef{Name: "idx_users_email", TableName: "users"} is added
//	    to diff.IndexesAdded
//
// **Unused index removal**:
//   - Database has "idx_old_search" on "users"
//   - Generated schema doesn't define this index
//   - Result: IndexRef{Name: "idx_old_search", TableName: "users"} is added
//     to diff.IndexesRemoved
//
// **Automatic index filtering**:
//   - Database has "users_pkey" (primary key index) - filtered out
//   - Database has "users_email_key" (constraint-based unique index) - filtered out
//   - Database has "users_tenant_email_idx" (explicitly defined unique index) - included for comparison
//
// # Algorithm Details
//
//  1. **Set Creation**: Keys index definitions by dialect-aware owning table and
//     index identity while preserving the declared spelling for generated DDL
//  2. **Filtering**: Applies database-side filtering for automatic indexes
//  3. **Comparison**: Performs set difference operations to find additions/removals
//  4. **Canonicalization**: Stores and sorts table-qualified IndexRef values
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
//   - diff.IndexesAdded: Table-qualified indexes that need to be created
//   - diff.IndexesRemoved: Table-qualified user-defined indexes that can be safely removed
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
// - Space Complexity: O(n + m) for the identity maps
// - Index operations can be expensive on large tables in production
func Indexes(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	IndexesWithDialect(generated, database, diff, "")
}

type generatedIndexEntry struct {
	ref   difftypes.IndexRef
	index goschema.Index
}

type databaseIndexEntry struct {
	ref   difftypes.IndexRef
	index types.DBIndex
}

func IndexesWithDialect(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff, dialect string) {
	IndexesWithSemantics(generated, database, diff, dialect, identifier.ForDialect(dialect))
}

// IndexesWithSemantics compares indexes using explicit identifier semantics.
func IndexesWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
) {
	genIndexes, ambiguousGenerated := collectGeneratedIndexes(generated, semantics)
	fkBackedIndexes, uniqueConstraintIndexes := constraintBackedIndexIdentities(
		database,
		dialect,
		semantics,
	)
	dbIndexes := collectDatabaseIndexes(
		database,
		dialect,
		semantics,
		fkBackedIndexes,
		uniqueConstraintIndexes,
	)
	appendIndexDifferences(
		diff,
		genIndexes,
		ambiguousGenerated,
		dbIndexes,
		dialect,
		semantics,
	)
	diff.SetIndexAdditions(diff.IndexAdditions())
	diff.SetIndexRemovals(diff.IndexRemovals())
}

func collectGeneratedIndexes(
	generated *goschema.Database,
	semantics identifier.Semantics,
) (
	map[difftypes.IndexRef]generatedIndexEntry,
	map[difftypes.IndexRef][]difftypes.IndexRef,
) {
	indexes := make(map[difftypes.IndexRef]generatedIndexEntry)
	ambiguous := make(map[difftypes.IndexRef][]difftypes.IndexRef)
	tableNames := goschema.ResolveIndexTableNames(generated.Indexes, generated.Tables)
	for position, index := range generated.Indexes {
		ref := difftypes.IndexRef{
			Name:      index.Name,
			TableName: tableNames[position],
		}
		identity := indexscope.IdentityKeyWithSemantics(semantics, ref)
		if previous, exists := indexes[identity]; exists {
			refs := ambiguous[identity]
			if len(refs) == 0 {
				refs = append(refs, previous.ref)
			}
			ambiguous[identity] = append(refs, ref)
		}
		indexes[identity] = generatedIndexEntry{
			ref:   ref,
			index: index,
		}
	}
	return indexes, ambiguous
}

func constraintBackedIndexIdentities(
	database *types.DBSchema,
	dialect string,
	semantics identifier.Semantics,
) (
	foreignKeys map[difftypes.IndexRef]struct{},
	uniqueConstraints map[difftypes.IndexRef]struct{},
) {
	// MySQL/MariaDB transparently create a backing index for every FOREIGN KEY
	// named after the constraint. Structured identities ensure the filter only
	// suppresses the backing index on its owning table.
	foreignKeys = make(map[difftypes.IndexRef]struct{}, len(database.Constraints))
	uniqueConstraints = make(map[difftypes.IndexRef]struct{}, len(database.Constraints))
	normalizedDialect := platform.NormalizeDialect(dialect)
	for _, constraint := range database.Constraints {
		ref := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{
			Name:      constraint.Name,
			TableName: constraint.QualifiedTableName(),
		})
		switch constraint.Type {
		case "FOREIGN KEY":
			if normalizedDialect == platform.MySQL ||
				normalizedDialect == platform.MariaDB {
				foreignKeys[ref] = struct{}{}
			}
		case "UNIQUE":
			if normalizedDialect != platform.SQLServer {
				uniqueConstraints[ref] = struct{}{}
			}
		}
	}
	return foreignKeys, uniqueConstraints
}

func collectDatabaseIndexes(
	database *types.DBSchema,
	dialect string,
	semantics identifier.Semantics,
	foreignKeys map[difftypes.IndexRef]struct{},
	uniqueConstraints map[difftypes.IndexRef]struct{},
) map[difftypes.IndexRef]databaseIndexEntry {
	indexes := make(map[difftypes.IndexRef]databaseIndexEntry)
	for _, index := range database.Indexes {
		if ignoreDatabaseIndex(index, dialect) {
			continue
		}
		ref := difftypes.IndexRef{
			Name:      index.Name,
			TableName: index.QualifiedTableName(),
		}
		identity := indexscope.IdentityKeyWithSemantics(semantics, ref)
		if _, constraintBacked := uniqueConstraints[identity]; constraintBacked {
			continue
		}
		if _, foreignKeyBacked := foreignKeys[identity]; foreignKeyBacked {
			continue
		}
		indexes[identity] = databaseIndexEntry{
			ref:   ref,
			index: index,
		}
	}
	return indexes
}

func ignoreDatabaseIndex(index types.DBIndex, dialect string) bool {
	if index.IsPrimary || isSQLiteInternalAutoindex(index.Name, dialect) {
		return true
	}
	return platform.NormalizeDialect(dialect) != platform.SQLServer &&
		index.IsUnique &&
		isConstraintBasedUniqueIndex(index.Name, index.TableName, index.Columns)
}

func appendIndexDifferences(
	diff *difftypes.SchemaDiff,
	generated map[difftypes.IndexRef]generatedIndexEntry,
	ambiguousGenerated map[difftypes.IndexRef][]difftypes.IndexRef,
	database map[difftypes.IndexRef]databaseIndexEntry,
	dialect string,
	semantics identifier.Semantics,
) {
	for identity, refs := range ambiguousGenerated {
		for _, ref := range refs {
			appendIndexAddition(diff, ref)
		}
		delete(generated, identity)
	}
	for identity, generatedEntry := range generated {
		databaseEntry, exists := database[identity]
		switch {
		case !exists:
			appendIndexAddition(diff, generatedEntry.ref)
		case indexReplacementRequired(
			generatedEntry,
			databaseEntry,
			dialect,
			semantics,
		):
			appendIndexAddition(diff, generatedEntry.ref)
			appendIndexRemoval(diff, databaseEntry.ref)
		}
	}

	for identity, entry := range database {
		if _, ambiguous := ambiguousGenerated[identity]; ambiguous {
			continue
		}
		if _, exists := generated[identity]; !exists {
			appendIndexRemoval(diff, entry.ref)
		}
	}
}

func appendIndexAddition(diff *difftypes.SchemaDiff, ref difftypes.IndexRef) {
	diff.IndexesAdded = append(diff.IndexesAdded, ref)
}

func appendIndexRemoval(diff *difftypes.SchemaDiff, ref difftypes.IndexRef) {
	diff.IndexesRemoved = append(diff.IndexesRemoved, ref)
}

func isSQLiteInternalAutoindex(indexName, dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.SQLite &&
		strings.HasPrefix(indexName, "sqlite_autoindex_")
}

func indexReplacementRequired(
	generated generatedIndexEntry,
	database databaseIndexEntry,
	dialect string,
	semantics identifier.Semantics,
) bool {
	if platform.NormalizeDialect(dialect) == platform.SQLServer &&
		generated.ref.Name != database.ref.Name {
		return true
	}
	return indexDefinitionsChanged(generated.index, database.index, dialect, semantics)
}

func indexDefinitionsChanged(
	generated goschema.Index,
	database types.DBIndex,
	dialect string,
	semantics identifier.Semantics,
) bool {
	if !boolPtrEqual(generated.NullsDistinct, database.NullsDistinct) ||
		indexPredicateChanged(generated.Condition, database.Condition, dialect) {
		return true
	}
	if platform.NormalizeDialect(dialect) != platform.SQLServer {
		return false
	}
	return generated.Unique != database.IsUnique ||
		indexKeyPartsChanged(generated, database, semantics)
}

func indexKeyPartsChanged(
	generated goschema.Index,
	database types.DBIndex,
	semantics identifier.Semantics,
) bool {
	generatedParts := effectiveGeneratedIndexParts(generated)
	databaseParts := effectiveDatabaseIndexParts(database)
	if len(generatedParts) != len(databaseParts) {
		return true
	}
	for position, generatedPart := range generatedParts {
		databasePart := databaseParts[position]
		if semantics.ColumnIdentityKey(generatedPart.Name) !=
			semantics.ColumnIdentityKey(databasePart.Name) ||
			generatedPart.Desc != databasePart.Desc {
			return true
		}
	}
	return false
}

func effectiveGeneratedIndexParts(index goschema.Index) []goschema.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]goschema.IndexPart, len(index.Fields))
	for position, field := range index.Fields {
		parts[position] = goschema.IndexPart{Name: field}
	}
	return parts
}

func effectiveDatabaseIndexParts(index types.DBIndex) []types.DBIndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]types.DBIndexPart, len(index.Columns))
	for position, column := range index.Columns {
		parts[position] = types.DBIndexPart{Name: column}
	}
	return parts
}

func indexPredicateChanged(generated, database, dialect string) bool {
	if strings.TrimSpace(generated) == "" || strings.TrimSpace(database) == "" {
		return strings.TrimSpace(generated) != strings.TrimSpace(database)
	}
	if checkExpressionHasUnsupportedRewrite(generated, database) {
		return false
	}
	return normalizePredicate(generated, dialect) != normalizePredicate(database, dialect)
}

// normalizePredicate reduces an index predicate to a spelling-insensitive
// comparison key. SQL Server additionally strips the catalog's canonical
// filter_definition decorations (bracket quoting, parenthesized numeric
// literals) so user-authored predicates converge with introspected ones
// instead of planning a replacement on every run.
func normalizePredicate(value, dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
		value = normalizeSQLServerPredicateSpelling(value)
	}
	return normalizeCheckExpression(value)
}
