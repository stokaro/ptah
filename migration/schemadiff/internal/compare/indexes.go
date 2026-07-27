package compare

import (
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/indexscope"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
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
	// Create sets for comparison
	genIndexes := make(map[difftypes.IndexRef]generatedIndexEntry)
	tableNames := goschema.ResolveIndexTableNames(generated.Indexes, generated.Tables)
	for position, index := range generated.Indexes {
		ref := difftypes.IndexRef{
			Name:      index.Name,
			TableName: tableNames[position],
		}
		genIndexes[indexscope.IdentityKey(dialect, ref)] = generatedIndexEntry{
			ref:   ref,
			index: index,
		}
	}

	// MySQL/MariaDB transparently create a backing index for every FOREIGN KEY,
	// named after the constraint, when no usable index already exists on the
	// referencing column. That index is owned by the FK, not by the user's
	// schema, so it must not be reported as a removable index — otherwise an
	// unchanged field-level FK round-trips to a spurious DROP INDEX (issue #189
	// follow-up). PostgreSQL does not auto-create FK indexes, so this set is
	// naturally empty there and the filter is a no-op. Structured
	// table-qualified identity ensures a constraint name only suppresses the
	// index on its own table.
	fkBackedIndexes := make(map[difftypes.IndexRef]struct{}, len(database.Constraints))
	uniqueConstraintIndexes := make(map[difftypes.IndexRef]struct{}, len(database.Constraints))
	for _, c := range database.Constraints {
		ref := indexscope.IdentityKey(dialect, difftypes.IndexRef{
			Name:      c.Name,
			TableName: c.QualifiedTableName(),
		})
		switch c.Type {
		case "FOREIGN KEY":
			fkBackedIndexes[ref] = struct{}{}
		case "UNIQUE":
			uniqueConstraintIndexes[ref] = struct{}{}
		}
	}

	dbIndexes := make(map[difftypes.IndexRef]databaseIndexEntry)
	for _, index := range database.Indexes {
		// Skip primary key indexes as they're handled with tables
		if index.IsPrimary {
			continue
		}
		if isSQLiteInternalAutoindex(index.Name, dialect) {
			continue
		}

		// Skip constraint-based unique indexes (automatically created by UNIQUE constraints)
		// but allow explicitly defined unique indexes (created via schema annotations)
		if index.IsUnique && isConstraintBasedUniqueIndex(index.Name, index.TableName, index.Columns) {
			continue
		}
		ref := difftypes.IndexRef{
			Name:      index.Name,
			TableName: index.QualifiedTableName(),
		}
		identity := indexscope.IdentityKey(dialect, ref)
		if _, ok := uniqueConstraintIndexes[identity]; ok {
			continue
		}

		// Skip MySQL/MariaDB FK-backing indexes (named after the FK constraint
		// on the same table). They are auto-managed by the foreign key.
		if _, ok := fkBackedIndexes[identity]; ok {
			continue
		}

		dbIndexes[identity] = databaseIndexEntry{
			ref:   ref,
			index: index,
		}
	}

	// Find added and removed indexes
	for identity, generatedEntry := range genIndexes {
		databaseEntry, exists := dbIndexes[identity]
		switch {
		case !exists:
			appendIndexAddition(diff, generatedEntry.ref)
		case indexDefinitionsChanged(generatedEntry.index, databaseEntry.index):
			appendIndexAddition(diff, generatedEntry.ref)
			appendIndexRemoval(diff, databaseEntry.ref)
		}
	}

	for identity, entry := range dbIndexes {
		if _, exists := genIndexes[identity]; !exists {
			appendIndexRemoval(diff, entry.ref)
		}
	}

	diff.SetIndexAdditions(diff.IndexAdditions())
	diff.SetIndexRemovals(diff.IndexRemovals())
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

func indexDefinitionsChanged(genIndex goschema.Index, dbIndex types.DBIndex) bool {
	return !boolPtrEqual(genIndex.NullsDistinct, dbIndex.NullsDistinct) ||
		indexPredicateChanged(genIndex.Condition, dbIndex.Condition)
}

func indexPredicateChanged(generated, database string) bool {
	if strings.TrimSpace(generated) == "" || strings.TrimSpace(database) == "" {
		return strings.TrimSpace(generated) != strings.TrimSpace(database)
	}
	if checkExpressionHasUnsupportedRewrite(generated, database) {
		return false
	}
	return normalizePredicate(generated) != normalizePredicate(database)
}

func normalizePredicate(value string) string {
	return normalizeCheckExpression(value)
}
