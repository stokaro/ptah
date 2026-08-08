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
		genIndexes,
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

// collectDatabaseIndexes keys the database's indexes by identity, leaving out
// the ones that are not addressable as indexes and the ones another object
// owns.
//
// **Ownership follows the desired state's spelling.** An identity the desired
// state declares as an index is never dropped here, whichever filter below
// would otherwise have claimed it, because a database object the desired state
// names as an index has to reach the index comparison to be matched, replaced,
// or left alone. Filtering it out is what strands it: the desired side is never
// filtered, so the declared index has nothing to match and is reported as an
// addition while the owning constraint, having no counterpart on the desired
// side, is reported as a removal -- one object, created down one path and
// dropped down the other, in the same plan.
//
// That is not a hypothetical desired state. Both `ptah-compat schema inspect`
// and the pinned community binary v1.3.0 write these objects back out as
// `index` blocks, so replaying a database's own inspect output against the
// database it came from hit it. Measured on MySQL 9.7.1 and MariaDB 11.8.8,
// where the pinned binary reported "Schema is synced":
//
//	fixture                     | filter that stranded it
//	UNIQUE KEY uq_users_email   | the same-named UNIQUE constraint (#1245)
//	UNIQUE KEY uk_users_email   | isConstraintBasedUniqueIndex's name pattern
//	CONSTRAINT fk_posts_user    | the FOREIGN KEY backing index (#1258)
//
// and on PostgreSQL 17.10 for `CONSTRAINT uq_users_email UNIQUE (email)`
// against a desired `index "uq_users_email" { unique = true }`, where the
// pinned binary also reported "Schema is synced".
//
// The plans were not merely noisy. MySQL and MariaDB answer the CREATE with
// `Error 1061 (42000): Duplicate key name 'uq_users_email'` and the apply exits
// 1 -- and not before the statements ahead of it in the same plan have run, so
// adding one column to a table that has a named unique key left the column
// added and the run failed. PostgreSQL, where the statements carry
// IF NOT EXISTS and IF EXISTS, skipped the create, ran the drop, exited 0, and
// left the table with no unique index at all.
//
// Letting a declared identity through cannot start dropping anything: an
// identity only survives the filters when the desired state names it, and an
// identity the desired state names is matched rather than removed.
func collectDatabaseIndexes(
	database *types.DBSchema,
	dialect string,
	semantics identifier.Semantics,
	declared map[difftypes.IndexRef]generatedIndexEntry,
	foreignKeys map[difftypes.IndexRef]struct{},
	uniqueConstraints map[difftypes.IndexRef]struct{},
) map[difftypes.IndexRef]databaseIndexEntry {
	indexes := make(map[difftypes.IndexRef]databaseIndexEntry)
	for _, index := range database.Indexes {
		if unaddressableDatabaseIndex(index, dialect) {
			continue
		}
		ref := difftypes.IndexRef{
			Name:      index.Name,
			TableName: index.QualifiedTableName(),
		}
		identity := indexscope.IdentityKeyWithSemantics(semantics, ref)
		if _, isDeclared := declared[identity]; !isDeclared &&
			constraintOwnedDatabaseIndex(
				index,
				dialect,
				identity,
				foreignKeys,
				uniqueConstraints,
			) {
			continue
		}
		indexes[identity] = databaseIndexEntry{
			ref:   ref,
			index: index,
		}
	}
	return indexes
}

// unaddressableDatabaseIndex reports whether an index has no standalone
// existence to compare at all: a primary key's index is created and dropped
// with the key, and SQLite's sqlite_autoindex_* rows name an internal structure
// no statement can refer to. Neither can be declared by a desired state, so
// neither is narrowed by one.
func unaddressableDatabaseIndex(index types.DBIndex, dialect string) bool {
	return index.IsPrimary || isSQLiteInternalAutoindex(index.Name, dialect)
}

// constraintOwnedDatabaseIndex reports whether a database index is a
// constraint's backing index, and so is created and dropped through that
// constraint rather than on its own.
//
// A UNIQUE constraint is enforced by an index of the constraint's own name on
// its own table everywhere except SQL Server, and MySQL and MariaDB
// additionally create one for every FOREIGN KEY: with a bare
// `CONSTRAINT ... FOREIGN KEY` and no `KEY` clause -- the ordinary way a MySQL
// schema is written -- `information_schema.STATISTICS` reports an index named
// `fk_posts_user` alongside the constraint named `fk_posts_user`. A desired
// state that says nothing about either must not plan a DROP INDEX that would
// take the constraint with it, which is what these two arms prevent.
//
// The name-pattern arm is a guess about a naming convention where the identity
// arms are facts read from the catalog, but they are alike in what they mean
// for the comparison, and all three are subject to the declaration narrowing in
// [collectDatabaseIndexes] -- the FOREIGN KEY arm since
// [#1258](https://github.com/stokaro/ptah/issues/1258), the other two since
// [#1245](https://github.com/stokaro/ptah/issues/1245).
func constraintOwnedDatabaseIndex(
	index types.DBIndex,
	dialect string,
	identity difftypes.IndexRef,
	foreignKeys map[difftypes.IndexRef]struct{},
	uniqueConstraints map[difftypes.IndexRef]struct{},
) bool {
	if _, uniqueBacked := uniqueConstraints[identity]; uniqueBacked {
		return true
	}
	if _, foreignKeyBacked := foreignKeys[identity]; foreignKeyBacked {
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
	// Every other property is compared per dialect, because "these two indexes
	// are the same index" is a dialect question. Adding a dialect here means
	// deciding what its catalog reports and what it leaves implicit; a dialect
	// that has not been measured keeps the name-only comparison rather than
	// getting guessed semantics (issue #1272 scope).
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLServer:
		return generated.Unique != database.IsUnique ||
			indexKeyPartsChanged(generated, database, semantics)
	case platform.Postgres:
		return postgresIndexDefinitionChanged(generated, database, semantics)
	case platform.MySQL, platform.MariaDB:
		return mysqlIndexDefinitionChanged(generated, database, semantics)
	default:
		return false
	}
}

// mysqlIndexDefinitionChanged answers whether a MySQL or MariaDB index has to
// be rebuilt to match the desired definition.
//
// Uniqueness has to be compared here, and it is the reason this branch exists.
// On these engines a UNIQUE constraint *is* its index, so
// [collectDatabaseIndexes] lets a declared identity through even when a UNIQUE
// constraint of that name owns it -- and without a uniqueness comparison a
// desired plain `index "uq_users_email"` would match a database
// `UNIQUE KEY uq_users_email` and report synced, which is the opposite mistake
// from the one #1245 fixed: two genuinely different objects made equal.
// Measured on MySQL 9.7.1, the pinned community binary v1.3.0 plans
// `ALTER TABLE users DROP INDEX uq_users_email` followed by
// `ALTER TABLE users ADD INDEX uq_users_email (email)` for that pair.
//
// Key columns are compared for the same reason: `UNIQUE KEY uq (email)` against
// a desired unique `uq (name)` is a different index, and the pinned binary
// plans a drop and an add for it. Comparing them is also what keeps this
// change from trading a loud failure for a silent one -- before it, that pair
// planned CREATE plus DROP of one name and the apply failed with
// `Error 1061 (42000): Duplicate key name`.
//
// Nothing else is compared, and the omissions are the reader's, not a
// judgement that they do not matter. Ptah's MySQL reader projects
// information_schema.STATISTICS through GROUP_CONCAT(COLUMN_NAME) and keeps
// only the column names and NON_UNIQUE, so a descending key and a prefix key
// both arrive as a plain column. Comparing a direction the reader always
// reports as ascending against a desired `desc = true` would plan a rebuild on
// every run forever, which is the oscillation this change exists to remove. An
// expression key never reaches the comparison at all: COLUMN_NAME is NULL for
// one and the read fails first. Those are reader gaps, recorded as such.
func mysqlIndexDefinitionChanged(
	generated goschema.Index,
	database types.DBIndex,
	semantics identifier.Semantics,
) bool {
	return generated.Unique != database.IsUnique ||
		mysqlIndexKeyColumnsChanged(generated, database, semantics)
}

// mysqlIndexKeyColumnsChanged compares the key columns in order, and only the
// columns. See [mysqlIndexDefinitionChanged] for why a key's direction is not
// part of the comparison.
func mysqlIndexKeyColumnsChanged(
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
		if semantics.ColumnIdentityKey(generatedPart.Name) !=
			semantics.ColumnIdentityKey(databaseParts[position].Name) {
			return true
		}
	}
	return false
}

// postgresIndexDefinitionChanged answers whether a PostgreSQL index has to be
// rebuilt to match the desired definition.
//
// Until issue #1272 the PostgreSQL branch compared nothing beyond the partial
// predicate and NULLS DISTINCT, so every property #1246 and #1271 taught the
// reader to preserve was read and then discarded at reconciliation time.
// Measured on PostgreSQL 17.10 against the pinned community binary v1.3.0,
// each of these pairs made `schema diff` print "Schemas are synced, no changes
// to be made" while the pinned binary planned DROP INDEX + CREATE INDEX:
//
//	USING btree (value)        -> USING hash (value)
//	USING gin (tsv)            -> USING gist (tsv)
//	(value)                    -> (value text_pattern_ops)   and the reverse
//	(value)                    -> (value DESC)
//	(value)                    -> (value NULLS FIRST)
//	(value DESC)               -> (value DESC NULLS LAST)
//	(a) INCLUDE (b)            -> (a) INCLUDE (c), added, and removed
//	(a)                        -> (lower(a))                 and the reverse
//	(lower(a))                 -> (upper(a))
//	(value)                    -> UNIQUE (value)
//	(a)                        -> (b)
//
// PostgreSQL cannot alter any of them in place: there is no ALTER INDEX form
// for the access method, the key list, an operator class, a key's sort or
// NULLS ordering, or the INCLUDE payload. Reporting the change is therefore
// enough -- the planner already emits DROP INDEX followed by CREATE INDEX for
// an index that is both added and removed, which is the transition the pinned
// binary plans and the one the server accepts.
func postgresIndexDefinitionChanged(
	generated goschema.Index,
	database types.DBIndex,
	semantics identifier.Semantics,
) bool {
	return generated.Unique != database.IsUnique ||
		postgresAccessMethod(generated.Type) != postgresAccessMethod(database.Method) ||
		postgresIndexKeysChanged(generated, database, semantics) ||
		postgresIncludeColumnsChanged(generated.IncludeColumns, database.IncludeColumns, semantics)
}

// postgresDefaultAccessMethod is the method PostgreSQL uses when CREATE INDEX
// carries no USING clause.
const postgresDefaultAccessMethod = "btree"

// postgresAccessMethod reduces an access method to a comparison key.
//
// The two sides spell it differently by construction: an annotation or HCL
// source writes BTREE/GIN, while the reader reports pg_am.amname verbatim,
// which PostgreSQL spells btree/gin. An absent method is the default one, so
// `USING btree (x)` and `(x)` are the same index and must not churn.
func postgresAccessMethod(method string) string {
	normalized := strings.ToLower(strings.TrimSpace(method))
	if normalized == "" {
		return postgresDefaultAccessMethod
	}
	return normalized
}

func postgresIndexKeysChanged(
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
		generatedKey := postgresGeneratedIndexKey(generatedPart, generated.Operator, semantics)
		if generatedKey != postgresDatabaseIndexKey(databaseParts[position], semantics) {
			return true
		}
	}
	return false
}

// postgresIndexKey is one index key reduced to the form the two sides can be
// compared in. It is comparable, so a key is one `!=` rather than a chain of
// per-attribute comparisons that a later attribute could be forgotten from.
//
// column and expr are separate fields on purpose, and exactly one is ever set.
// That is the distinction #1246 established: an index on lower(name) and an
// index on a column literally named "lower(name)" are different indexes, and
// collapsing them is how the reader used to emit
// CREATE INDEX ... ("lower(name)"), which psql rejects.
type postgresIndexKey struct {
	column     string
	expr       string
	operator   string
	desc       bool
	nullsOrder string
}

// resolvedNullsOrder resolves this key's NULLS ordering to the ordering the
// server actually applies.
//
// The default is direction-dependent -- NULLS LAST for ASC, NULLS FIRST for
// DESC -- so an omitted ordering and an explicit spelling of that direction's
// default are the same key. #1271's reader records only the deviating spelling,
// so resolving both sides is what keeps `ASC` and `ASC NULLS LAST` from
// planning a rebuild against each other while `ASC` and `ASC NULLS FIRST` still
// report one.
func (k postgresIndexKey) resolvedNullsOrder(order string) string {
	switch strings.ToUpper(strings.TrimSpace(order)) {
	case types.NullsOrderFirst:
		return types.NullsOrderFirst
	case types.NullsOrderLast:
		return types.NullsOrderLast
	}
	if k.desc {
		return types.NullsOrderFirst
	}
	return types.NullsOrderLast
}

// postgresGeneratedIndexKey reduces one desired key.
//
// indexOperator is the index-level operator class an annotation may set for
// every key at once. The renderer applies it to each key that has none, so the
// comparison resolves it the same way.
func postgresGeneratedIndexKey(
	part goschema.IndexPart,
	indexOperator string,
	semantics identifier.Semantics,
) postgresIndexKey {
	key := postgresIndexKey{
		operator: postgresOperatorClass(part.Operator, indexOperator),
		desc:     part.Desc,
	}
	key.column, key.expr = postgresIndexKeyTarget(part.Name, part.Expr, semantics)
	key.nullsOrder = key.resolvedNullsOrder(part.NullsOrder)
	return key
}

// postgresDatabaseIndexKey reduces one introspected key. The database shape has
// no index-level operator class slot, so every class it reports is already
// per-key.
func postgresDatabaseIndexKey(
	part types.DBIndexPart,
	semantics identifier.Semantics,
) postgresIndexKey {
	key := postgresIndexKey{
		operator: postgresOperatorClass(part.Operator, ""),
		desc:     part.Desc,
	}
	key.column, key.expr = postgresIndexKeyTarget(part.Name, part.Expr, semantics)
	key.nullsOrder = key.resolvedNullsOrder(part.NullsOrder)
	return key
}

// postgresIndexKeyTarget splits what a key indexes into its column slot and its
// expression slot, filling exactly one. An expression is normalized the way a
// check-constraint expression is, so spacing and keyword case do not make a new
// index; a column goes through the dialect's identifier semantics.
func postgresIndexKeyTarget(
	name, expr string,
	semantics identifier.Semantics,
) (column, expression string) {
	trimmedExpr := strings.TrimSpace(expr)
	if trimmedExpr != "" {
		return "", normalizeCheckExpression(trimmedExpr)
	}
	return semantics.ColumnIdentityKey(name), ""
}

// postgresOperatorClass reduces an operator class to a comparison key.
//
// Only case and surrounding space are normalized. PostgreSQL reports an
// operator class in pg_index.indclass for every key, and #1271's reader
// deliberately records only the ones that are not the key type's default, so
// an introspected index names a class exactly when the choice was not the
// default and both sides of an inspect/replay round trip agree.
//
// A hand-written source that spells the default out -- `ops = text_ops` on a
// text column -- is the one case where this diverges from the pinned community
// binary v1.3.0, which resolves type defaults and reported "Schemas are synced"
// for that fixture on PostgreSQL 17.10 where Ptah plans a rebuild. Ptah does
// not resolve default operator classes, and inventing the resolution from a
// version-pinned catalog table would be worse than the churn: the alternative
// is to ignore a named class, which would silently accept a real operator-class
// change. Neither binary's `schema inspect` emits a default class, so no
// round trip reaches this branch. Recorded in docs/conformance.md.
func postgresOperatorClass(partOperator, indexOperator string) string {
	operator := strings.TrimSpace(partOperator)
	if operator == "" {
		operator = strings.TrimSpace(indexOperator)
	}
	return strings.ToLower(operator)
}

// postgresIncludeColumnsChanged compares the INCLUDE payload.
//
// Order is significant, matching both PostgreSQL -- which stores the payload
// columns in the order they were written and reports them that way -- and the
// pinned community binary v1.3.0, which planned a rebuild for
// `INCLUDE (b, c)` against `INCLUDE (c, b)` on PostgreSQL 17.10.
//
// The payload is compared separately from the keys and never merged into them:
// an index on (a) INCLUDE (b) is not an index on (a, b), and treating the
// payload as a trailing key would make the two compare equal.
func postgresIncludeColumnsChanged(
	generated, database []string,
	semantics identifier.Semantics,
) bool {
	if len(generated) != len(database) {
		return true
	}
	for position, column := range generated {
		if semantics.ColumnIdentityKey(column) !=
			semantics.ColumnIdentityKey(database[position]) {
			return true
		}
	}
	return false
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
