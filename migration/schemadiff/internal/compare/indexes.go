package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/constraintowner"
	"ptah.run/internal/exprkey"
	"ptah.run/internal/indexbacking"
	"ptah.run/internal/indexscope"
)

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
func Indexes(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	IndexesWithDialect(desired, current, diff, "", schemacomparison.Rules{})
}

type generatedIndexEntry struct {
	ref   difftypes.IndexRef
	index schemamodel.Index
}

type databaseIndexEntry struct {
	ref   difftypes.IndexRef
	index catalog.Index
	// constraintBacked reports that a UNIQUE constraint of this identity
	// enforces the index, so the object is the constraint's and both directions
	// of a plan have to say so. See [uniqueConstraintEnforcesTheIndex].
	constraintBacked bool
	// partitionAttached reports that the index is a partition's copy of an
	// index on its partitioned parent, so no statement of its own creates or
	// drops it. See [partitionAttachedIndexIsNotPlannable].
	partitionAttached bool
}

// IndexesWithDialect compares indexes for dialect, with its identifier
// semantics, its default capability preset and rules, the target's comparison
// rules.
func IndexesWithDialect(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff, dialect string, rules schemacomparison.Rules) {
	IndexesWithSemantics(desired, current, diff, dialect, identifier.ForDialect(dialect), nil, capability.ForDialect(dialect), rules)
}

// IndexesWithSemantics compares indexes using explicit identifier semantics,
// on a target with caps and the target's comparison rules, whose index members
// are nil where it registers none. A target with [capability.IndexRename] gets
// an index the declaration renamed as a rename; see [pairIndexRenames].
func IndexesWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	indexes map[string]config.IndexExpression,
	caps capability.Capabilities,
	rules schemacomparison.Rules,
) {
	genIndexes, ambiguousGenerated := collectGeneratedIndexes(desired, semantics)
	if comparisonOf(dialect).PrimaryKeyStoresColumns {
		withoutImplicitPrimaryKeyPayload(genIndexes, desired, semantics)
	}
	owned := constraintBackedIndexIdentities(database, dialect, semantics, rules.ForeignKeyIndexes)
	dbIndexes := collectDatabaseIndexes(
		database,
		dialect,
		semantics,
		genIndexes,
		owned,
	)
	appendIndexDifferences(
		diff,
		genIndexes,
		ambiguousGenerated,
		dbIndexes,
		indexComparison{dialect: dialect, semantics: semantics, expressions: indexes, caps: caps, rules: rules},
	)
	diff.SetIndexAdditions(diff.IndexesAdded)
	diff.SetIndexRemovals(diff.IndexRemovals())
	diff.SetConstraintBackedIndexRemovals(diff.ConstraintBackedIndexRemovals)
}

func collectGeneratedIndexes(
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) (
	map[difftypes.IndexRef]generatedIndexEntry,
	map[difftypes.IndexRef][]generatedIndexEntry,
) {
	indexes := make(map[difftypes.IndexRef]generatedIndexEntry)
	ambiguous := make(map[difftypes.IndexRef][]generatedIndexEntry)
	// Materialized views are relations an index can belong to, not just
	// tables: PostgreSQL accepts CREATE INDEX on one, and a UNIQUE index on
	// one is what REFRESH MATERIALIZED VIEW CONCURRENTLY requires. Resolving
	// against tables alone left the owner empty, and the refusal that
	// followed named a position in a slice rather than the index or the view
	// (stokaro/ptah#1725).
	tableNames := schemamodel.ResolveIndexOwners(desired.Indexes, desired.Tables, desired.MaterializedViews)
	for position, index := range desired.Indexes {
		ref := difftypes.IndexRef{
			Name:      index.Name,
			TableName: tableNames[position],
		}
		identity := indexscope.IdentityKeyWithSemantics(semantics, ref)
		entry := generatedIndexEntry{ref: ref, index: index}
		if previous, exists := indexes[identity]; exists {
			entries := ambiguous[identity]
			if len(entries) == 0 {
				entries = append(entries, previous)
			}
			ambiguous[identity] = append(entries, entry)
		}
		indexes[identity] = entry
	}
	return indexes, ambiguous
}

// withoutImplicitPrimaryKeyPayload removes the owning table's primary-key
// columns from each declared CockroachDB index payload.
//
// CockroachDB holds the primary key in every secondary index already, so it
// accepts a STORING or INCLUDE of a primary-key column and ignores it.
// Measured on v26.3.2, `CREATE INDEX bad ON child (parent_id) STORING (id)`
// answers `index "bad" already contains column "id" which is part of the
// primary key and therefore implicit in all indexes`, and the index reads back
// with no payload. Compared as declared, such an index differed from what the
// server holds on every run and was planned again each time.
//
// The entries are rewritten with a fresh payload slice, so the desired schema
// the caller handed in is not changed.
func withoutImplicitPrimaryKeyPayload(
	entries map[difftypes.IndexRef]generatedIndexEntry,
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) {
	keys := declaredPrimaryKeys(desired, semantics)
	for identity, entry := range entries {
		key := keys[semantics.QualifiedTableIdentityKey(entry.ref.TableName)]
		if len(key) == 0 || len(entry.index.IncludeColumns) == 0 {
			continue
		}
		payload := make([]string, 0, len(entry.index.IncludeColumns))
		for _, column := range entry.index.IncludeColumns {
			if !slices.Contains(key, semantics.ColumnIdentityKey(column)) {
				payload = append(payload, column)
			}
		}
		entry.index.IncludeColumns = payload
		entries[identity] = entry
	}
}

// declaredPrimaryKeys returns each declared table's primary-key columns, keyed
// by the table's identity, as column identities. A primary key is declared on
// the table, on its columns, or as a PRIMARY KEY constraint, and each spelling
// counts.
func declaredPrimaryKeys(desired *schemamodel.Database, semantics identifier.Semantics) map[string][]string {
	keys := make(map[string][]string, len(desired.Tables))
	for _, table := range desired.Tables {
		columns := slices.Concat(tablePrimaryKeyColumns(table), columnPrimaryKey(table, desired.Fields))
		for _, constraint := range desired.Constraints {
			if !strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") {
				continue
			}
			if owner, found := constraintowner.Table(constraint, desired.Tables); found && owner.QualifiedName() == table.QualifiedName() {
				columns = append(columns, constraint.Columns...)
			}
		}
		identities := make([]string, 0, len(columns))
		for _, column := range columns {
			identities = append(identities, semantics.ColumnIdentityKey(column))
		}
		keys[semantics.QualifiedTableIdentityKey(table.QualifiedName())] = identities
	}
	return keys
}

// constraintOwnedIndexes is the database index identity of every constraint
// that owns one, kept apart by the kind that owns it because they are not
// interchangeable: only the UNIQUE set also decides how a removal is SPELLED,
// through [uniqueConstraintEnforcesTheIndex] and the `unique_protections_removed`
// finding it feeds.
type constraintOwnedIndexes struct {
	foreignKeys map[difftypes.IndexRef]struct{}
	unique      map[difftypes.IndexRef]struct{}
}

// constraintBackedIndexIdentities is the identity of every database index a
// constraint owns: a UNIQUE constraint's own index where the server backs one
// with it, and the index the server built for a foreign key where it builds
// one, which foreignKeys, the target's rule, finds. Structured identities
// ensure the filter only suppresses a backing index on its owning table.
func constraintBackedIndexIdentities(
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
	foreignKeys schemacomparison.ForeignKeyIndexes,
) constraintOwnedIndexes {
	owned := constraintOwnedIndexes{
		foreignKeys: make(map[difftypes.IndexRef]struct{}, len(database.Constraints)),
		unique:      make(map[difftypes.IndexRef]struct{}, len(database.Constraints)),
	}
	normalizedDialect := platform.NormalizeDialect(dialect)
	for _, constraint := range database.Constraints {
		ref := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{
			Name:      constraint.Name,
			TableName: constraint.QualifiedTableName(),
		})
		// Whether this server backs the kind at all is the shared answer;
		// WHICH index backs a foreign key is the target's, because the match
		// is on the constraint's columns and on the name the server gives an
		// index, not on the constraint's name alone.
		kind := indexbacking.KindOf(constraint.Type)
		if !indexbacking.ServerBacks(normalizedDialect, kind) {
			continue
		}
		switch kind {
		case indexbacking.ForeignKey:
			if foreignKeys == nil {
				continue
			}
			for _, backing := range foreignKeys.BackingIndexes(database, constraint, semantics) {
				owned.foreignKeys[indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{
					Name:      backing.Name,
					TableName: backing.QualifiedTableName(),
				})] = struct{}{}
			}
		case indexbacking.Unique:
			owned.unique[ref] = struct{}{}
		case indexbacking.None, indexbacking.PrimaryKey:
			// A primary key's index is recognized by its own mark rather than
			// by attribution from the constraint; see unaddressableDatabaseIndex.
		}
	}
	return owned
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
//	UNIQUE KEY uk_users_email   | a scan of the index's NAME, removed in #2615
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
// Letting a declared identity through never drops an object the desired state
// did not ask to change: an identity only survives the filters when the desired
// state names it, and a named identity is either matched or replaced by the
// definition the desired state gives it -- [appendIndexDifferences] pairs that
// removal with the addition that supersedes it, and never emits it alone.
//
// The removal is still a removal, and what it removes depends on the object. A
// UNIQUE constraint's backing index is the constraint's object, so each entry
// records that; the drop is spelled through the constraint everywhere except
// MySQL and MariaDB, and the restoration through it everywhere. See
// [uniqueConstraintEnforcesTheIndex].
func collectDatabaseIndexes(
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
	declared map[difftypes.IndexRef]generatedIndexEntry,
	owned constraintOwnedIndexes,
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
			constraintOwnedDatabaseIndex(index, identity, owned) {
			continue
		}
		indexes[identity] = databaseIndexEntry{
			ref:               ref,
			index:             index,
			constraintBacked:  uniqueConstraintEnforcesTheIndex(identity, owned.unique),
			partitionAttached: index.PartitionAttached,
		}
	}
	return indexes
}

// uniqueConstraintEnforcesTheIndex reports whether a UNIQUE constraint of this
// identity enforces the database index -- whether, in other words, the object
// the removal names belongs to the constraint currentCatalog and not only the index
// catalog.
//
// It is a fact about the object, not a spelling, and the two consumers of the
// mark need it for opposite reasons:
//
//   - The UP drop is spelled per engine. On MySQL and MariaDB a unique key and
//     its constraint are one currentCatalog row and `ALTER TABLE ... DROP INDEX`
//     removes it, which is what the pinned community binary v1.3.0 plans there,
//     so their planners ignore the mark. Everywhere else the constraint is an
//     object of its own that owns the index and the server refuses to drop the
//     index alone: PostgreSQL 17.10 answers `DROP INDEX "uq_users_email"` with
//     `cannot drop index uq_users_email because constraint uq_users_email on
//     table users requires it (SQLSTATE 2BP01)`, and the pinned binary plans
//     `ALTER TABLE "users" DROP CONSTRAINT "uq_users_email"`.
//   - The DOWN direction has to put the object back, and there the engines do
//     not split. What was dropped was a UNIQUE constraint on every one of them,
//     so the statement that restores it is ADD CONSTRAINT ... UNIQUE; on MySQL
//     and MariaDB that lands the same currentCatalog row `CREATE UNIQUE INDEX` would.
//     Reversing the removal into an index addition instead loses the constraint
//     on PostgreSQL and fails outright on MySQL, because the down direction's
//     target schema omits a constraint-backed index by construction
//     (ConvertDBSchemaToGoSchema). See [generator.reverseIndexRemovals].
//
// Marking only the engines whose UP spelling differs is what made the down
// direction wrong on the other two, so the mark records the fact and each
// planner decides its own spelling.
//
// SQL Server never reaches here: a UNIQUE constraint and a unique index are
// separate objects there, so [constraintBackedIndexIdentities] records no
// unique-constraint identities for it at all.
func uniqueConstraintEnforcesTheIndex(
	identity difftypes.IndexRef,
	uniqueConstraints map[difftypes.IndexRef]struct{},
) bool {
	_, constraintBacked := uniqueConstraints[identity]
	return constraintBacked
}

// unaddressableDatabaseIndex reports whether an index has no standalone
// existence to compare at all: a primary key's index is created and dropped
// with the key, and SQLite's sqlite_autoindex_* rows name an internal structure
// no statement can refer to. Neither can be declared by a desired state, so
// neither is narrowed by one.
func unaddressableDatabaseIndex(index catalog.Index, dialect string) bool {
	return indexbacking.Unaddressable(index, dialect)
}

// constraintOwnedDatabaseIndex recognizes catalog ownership rather than a name
// pattern. Common UNIQUE and FOREIGN KEY constraints supply structural evidence;
// feature owners mark their backing indexes with ManagedBy. Explicit index
// declarations still reach comparison through collectDatabaseIndexes.
func constraintOwnedDatabaseIndex(
	index catalog.Index,
	identity difftypes.IndexRef,
	owned constraintOwnedIndexes,
) bool {
	if _, uniqueBacked := owned.unique[identity]; uniqueBacked {
		return true
	}
	if _, foreignKeyBacked := owned.foreignKeys[identity]; foreignKeyBacked {
		return true
	}
	return index.ManagedBy != (objectidentity.ID{})
}

// indexComparison is what deciding whether two indexes are one index takes
// beyond the indexes themselves.
type indexComparison struct {
	dialect     string
	semantics   identifier.Semantics
	expressions map[string]config.IndexExpression
	caps        capability.Capabilities
	// rules are the target's comparison rules.
	rules schemacomparison.Rules
}

// replacementRequired reports whether the database index has to be dropped and
// the desired one built in its place.
func (c indexComparison) replacementRequired(desired generatedIndexEntry, database databaseIndexEntry) bool {
	return indexReplacementRequired(
		desired,
		database,
		c.dialect,
		c.semantics,
		c.expressions[exprkey.Index(c.semantics, desired.ref.TableName, desired.ref.Name)],
		c.rules,
	)
}

// renameKeeps reports whether an index renamed in place keeps every facet in
// facets: each facet's kind is one the target's rules say a rename carries. An
// index without facets keeps everything, and without index rules an index
// with any facet is never renamed, since no rule says the rename keeps it.
func (c indexComparison) renameKeeps(facets schemaext.Facets) bool {
	return !slices.ContainsFunc(facets.DeclaredKinds(), func(kind schemaext.Kind) bool {
		return c.rules.Indexes == nil || !c.rules.Indexes.RenameCarries(kind)
	})
}

func appendIndexDifferences(
	diff *difftypes.SchemaDiff,
	desired map[difftypes.IndexRef]generatedIndexEntry,
	ambiguousGenerated map[difftypes.IndexRef][]generatedIndexEntry,
	database map[difftypes.IndexRef]databaseIndexEntry,
	comparison indexComparison,
) {
	for identity, entries := range ambiguousGenerated {
		for _, entry := range entries {
			appendIndexAddition(diff, entry)
		}
		delete(desired, identity)
	}
	var additions []generatedIndexEntry
	for identity, generatedEntry := range desired {
		databaseEntry, exists := database[identity]
		switch {
		case !exists:
			additions = append(additions, generatedEntry)
		case partitionAttachedIndexIsNotPlannable(databaseEntry):
			continue
		case comparison.replacementRequired(generatedEntry, databaseEntry):
			// Not a pair: the plan drops the index and creates it again, and it
			// is counted as the removal and the addition.
			appendIndexAddition(diff, generatedEntry)
			appendIndexRemoval(diff, databaseEntry)
			appendIndexCommentChange(diff, comparison, databaseEntry.ref, "", generatedEntry.index.Comment, databaseEntry.index.Comment)
		case generatedEntry.index.Invisible != databaseEntry.index.Invisible:
			diff.Matched.Indexes++
			diff.IndexVisibilityChanged = append(diff.IndexVisibilityChanged, difftypes.IndexVisibilityChange{
				TableName: databaseEntry.ref.TableName,
				Name:      databaseEntry.ref.Name,
				Invisible: generatedEntry.index.Invisible,
			})
			appendIndexCommentChange(diff, comparison, databaseEntry.ref, "", generatedEntry.index.Comment, databaseEntry.index.Comment)
			appendKeptIndexCommentChange(diff, comparison, databaseEntry.ref, generatedEntry.index.Comment, databaseEntry.index.Comment)
		default:
			diff.Matched.Indexes++
			appendIndexCommentChange(diff, comparison, databaseEntry.ref, "", generatedEntry.index.Comment, databaseEntry.index.Comment)
			appendKeptIndexCommentChange(diff, comparison, databaseEntry.ref, generatedEntry.index.Comment, databaseEntry.index.Comment)
		}
	}
	slices.SortFunc(diff.IndexVisibilityChanged, func(a, b difftypes.IndexVisibilityChange) int {
		return cmp.Or(strings.Compare(a.TableName, b.TableName), strings.Compare(a.Name, b.Name))
	})

	var removals []databaseIndexEntry
	for identity, entry := range database {
		if _, ambiguous := ambiguousGenerated[identity]; ambiguous {
			continue
		}
		if partitionAttachedIndexIsNotPlannable(entry) {
			continue
		}
		if _, exists := desired[identity]; !exists {
			removals = append(removals, entry)
		}
	}

	additions, removals = pairIndexRenames(diff, additions, removals, comparison)
	for _, entry := range additions {
		appendIndexAddition(diff, entry)
	}
	for _, entry := range removals {
		appendIndexRemoval(diff, entry)
		appendIndexCommentChange(diff, comparison, entry.ref, "", "", entry.index.Comment)
	}
	slices.SortFunc(diff.IndexCommentsChanged, func(a, b difftypes.IndexCommentChange) int {
		return cmp.Or(strings.Compare(a.TableName, b.TableName), strings.Compare(a.Name, b.Name))
	})
}

// appendIndexCommentChange records the comment the index ref names has to
// take, on a target that keeps an index's comment apart from its definition
// ([capability.CommentAttributes]); on any other target an index's comment is
// part of what creates it, or not compared. from is the name the database
// holds for an index the plan renames, and empty otherwise.
//
// YDB keeps the comment as an attribute of the index's table under a key
// that names the index, and keeps it after the index is dropped or renamed
// (measured on 25.1.4.7 and 26.2.1.14). So a change is recorded where the
// comments differ, where a renamed index has a comment to move, and where a
// dropped index leaves one behind -- desired is empty for a dropped index.
func appendIndexCommentChange(
	diff *difftypes.SchemaDiff,
	comparison indexComparison,
	ref difftypes.IndexRef,
	from, desired, current string,
) {
	if !comparison.caps.Has(capability.CommentAttributes) {
		return
	}
	moves := from != "" && (desired != "" || current != "")
	if desired == current && !moves {
		return
	}
	diff.IndexCommentsChanged = append(diff.IndexCommentsChanged, difftypes.IndexCommentChange{
		TableName: ref.TableName,
		Name:      ref.Name,
		From:      from,
		Current:   current,
		Desired:   desired,
	})
}

// appendKeptIndexCommentChange records the comment an index both sides hold,
// and the plan keeps, has to take, on a target that comments an index in place
// with a statement of its own ([capability.IndexComments]): the PostgreSQL
// family's COMMENT ON INDEX. There the comment belongs to the index, so an
// index the plan creates writes its own and one it drops takes its comment
// with it; only a kept index needs a change.
func appendKeptIndexCommentChange(diff *difftypes.SchemaDiff, comparison indexComparison, ref difftypes.IndexRef, desired, current string) {
	if !comparison.caps.Has(capability.IndexComments) {
		return
	}
	desired, current = strings.TrimSpace(desired), strings.TrimSpace(current)
	if desired == current {
		return
	}
	diff.IndexCommentsChanged = append(diff.IndexCommentsChanged, difftypes.IndexCommentChange{
		TableName: ref.TableName,
		Name:      ref.Name,
		Current:   current,
		Desired:   desired,
	})
}

// pairIndexRenames records, on a target with [capability.IndexRename], each
// index the declaration adds that is an index the database drops under another
// name: the same table and the same definition, so that a drop and a create
// would rebuild the index the database already has. It returns the additions
// and the removals left unpaired, which the plan creates and drops.
//
// Only a name absent from the other side is paired. A rename onto a name the
// database holds would have to wait for that index to go, and a pair of
// indexes swapping names needs a third name; both stay a drop and a create,
// which is what every target without the key plans for every rename. Each
// side is visited in table and name order, and an addition takes the first
// removal that matches it, so two equal indexes renamed at once pair the same
// way on every run.
//
// A removal whose object a constraint owns is never paired: dropping it drops
// the constraint, which a rename would leave in place. Nor is an index that
// carries an owner's facet a rename cannot carry
// ([schemacomparison.Indexes.RenameCarries]), such as a YDB vector index's
// settings: this comparison cannot read them, and a rename would keep the
// settings the index was built with whatever the declaration states. A YDB
// index's partitioning is carried: the owner compares it under the new name
// and changes it in place.
func pairIndexRenames(
	diff *difftypes.SchemaDiff,
	additions []generatedIndexEntry,
	removals []databaseIndexEntry,
	comparison indexComparison,
) ([]generatedIndexEntry, []databaseIndexEntry) {
	if !comparison.caps.Has(capability.IndexRename) || len(additions) == 0 || len(removals) == 0 {
		return additions, removals
	}
	semantics := comparison.semantics
	order := func(table, name string) func(string, string) int {
		return func(otherTable, otherName string) int {
			return cmp.Or(
				strings.Compare(semantics.TableIdentityKey(table), semantics.TableIdentityKey(otherTable)),
				strings.Compare(name, otherName),
			)
		}
	}
	slices.SortFunc(additions, func(a, b generatedIndexEntry) int {
		return order(a.ref.TableName, a.ref.Name)(b.ref.TableName, b.ref.Name)
	})
	slices.SortFunc(removals, func(a, b databaseIndexEntry) int {
		return order(a.ref.TableName, a.ref.Name)(b.ref.TableName, b.ref.Name)
	})

	paired := make([]bool, len(removals))
	unpaired := additions[:0:0]
	for _, addition := range additions {
		match := -1
		for position, removal := range removals {
			if paired[position] || removal.constraintBacked ||
				!comparison.renameKeeps(addition.index.Facets) || !comparison.renameKeeps(removal.index.Facets) ||
				semantics.TableIdentityKey(removal.ref.TableName) != semantics.TableIdentityKey(addition.ref.TableName) ||
				comparison.replacementRequired(addition, removal) {
				continue
			}
			match = position
			break
		}
		if match < 0 {
			unpaired = append(unpaired, addition)
			continue
		}
		paired[match] = true
		// One index under two names is a pair, as an index both sides name
		// alike is.
		diff.Matched.Indexes++
		removal := removals[match]
		diff.IndexesRenamed = append(diff.IndexesRenamed, difftypes.IndexRename{
			TableName: removal.ref.TableName,
			From:      removal.ref.Name,
			To:        addition.ref.Name,
		})
		renamed := difftypes.IndexRef{TableName: removal.ref.TableName, Name: addition.ref.Name}
		appendIndexCommentChange(diff, comparison, renamed, removal.ref.Name, addition.index.Comment, removal.index.Comment)
	}
	left := removals[:0:0]
	for position, removal := range removals {
		if !paired[position] {
			left = append(left, removal)
		}
	}
	return unpaired, left
}

// partitionAttachedIndexIsNotPlannable reports whether a database index is a
// partition's copy of an index on its partitioned parent, which no statement of
// its own creates or drops.
//
// PostgreSQL builds one copy per partition when an index is created on a
// partitioned parent, and names it itself. A desired state written against the
// parent never mentions those names, so the ordinary "in the database, not in
// the desired state" arm below reads every one of them as an index to remove --
// and PostgreSQL 17.10 answers the drop with `cannot drop index
// events_2026_tenant_idx because index idx_events_tenant requires it
// (SQLSTATE 2BP01)`. Because the copies appear only after the parent index is
// applied, the first generate/apply cycle is clean and the next generate is the
// one that publishes the unexecutable file, by which point it has a checksum and
// a commit. See #997.
//
// The replacement arm is suppressed for the same reason and not only the
// removal: a copy carries the parent's definition, so the only way a desired
// state disagrees with it is by disagreeing with the parent, and neither half of
// a replacement addresses the copy -- the DROP is refused and the CREATE
// collides with a name the server owns. The parent index is what a plan
// changes, and the parent is a separate entry here, compared and planned on its
// own.
func partitionAttachedIndexIsNotPlannable(entry databaseIndexEntry) bool {
	return entry.partitionAttached
}

// appendIndexAddition records an index the plan creates, with the declaration
// it renders from. A reference alone is not enough to write a CREATE INDEX,
// and looking one back up is what needed the whole document
// (stokaro/ptah#2315).
func appendIndexAddition(diff *difftypes.SchemaDiff, entry generatedIndexEntry) {
	diff.IndexesAdded = append(diff.IndexesAdded, difftypes.IndexChange{
		Index:     entry.index.Clone(),
		TableName: entry.ref.TableName,
	})
}

// appendIndexRemoval records a database index the plan drops, and — when a
// UNIQUE constraint of the same identity enforces it — records that the object
// is that constraint's. The two lists are parallel by construction: the second
// is a subset of the first, keyed by the same reference.
func appendIndexRemoval(diff *difftypes.SchemaDiff, entry databaseIndexEntry) {
	diff.IndexesRemoved = append(diff.IndexesRemoved, entry.ref)
	if entry.constraintBacked {
		diff.ConstraintBackedIndexRemovals = append(
			diff.ConstraintBackedIndexRemovals,
			entry.ref,
		)
	}
}

func indexReplacementRequired(
	desired generatedIndexEntry,
	database databaseIndexEntry,
	dialect string,
	semantics identifier.Semantics,
	resolved config.IndexExpression,
	rules schemacomparison.Rules,
) bool {
	// An index whose name the catalog spells otherwise than the declaration is
	// rebuilt under the declared spelling, on a target that rebuilds it.
	if comparisonOf(dialect).RespelledIndexReplaced && desired.ref.Name != database.ref.Name {
		return true
	}
	return indexDefinitionsChanged(desired.index, database.index, dialect, semantics, resolved, rules)
}

func indexDefinitionsChanged(
	desired schemamodel.Index,
	database catalog.Index,
	dialect string,
	semantics identifier.Semantics,
	resolved config.IndexExpression,
	rules schemacomparison.Rules,
) bool {
	// The predicate is compared in the server's spelling where the server
	// answered. PostgreSQL stores a parse tree rather than the text it was
	// given, and the cast it inserts depends on the column's type: measured on
	// 17.11, `unit >= 0` over a numeric column is stored as
	// `(unit >= (0)::numeric)`, so a partial index nobody had touched was
	// dropped and rebuilt on every run (stokaro/ptah#2047).
	declaredPredicate := desired.Condition
	if resolved.Resolved {
		declaredPredicate = resolved.Predicate
	}
	if !nullsDistinctEqual(desired.NullsDistinct, database.NullsDistinct) ||
		indexPredicateChanged(declaredPredicate, database.Condition, rules.IndexPredicates) ||
		indexPayloadChanged(desired.IncludeColumns, database.IncludeColumns, dialect, semantics) {
		return true
	}
	// Everything else is the target's question, because "these two indexes
	// are the same index" depends on what its catalog reports and what it
	// leaves implicit. A target that compares whole definitions decides
	// alone. A target that registers index rules has its uniqueness, key
	// parts and covered columns compared here, and its kind and build settings
	// by the rules; the covered columns are compared in order, as the key
	// columns are. A target that registers neither keeps the name-only
	// comparison rather than getting guessed semantics (issue #1272 scope).
	switch {
	case rules.IndexDefinitions != nil:
		return rules.IndexDefinitions.DefinitionChanged(generatedIndexWithResolvedExpression(desired, resolved), database, semantics)
	case rules.Indexes != nil:
		return desired.Unique != database.IsUnique ||
			ruledIndexKeysChanged(generatedIndexWithResolvedExpression(desired, resolved), database, semantics) ||
			coveredColumnsChanged(desired.IncludeColumns, database.IncludeColumns, semantics) ||
			!rules.Indexes.OptionsEqual(desired, database)
	default:
		return false
	}
}

// indexPayloadChanged answers whether an index's payload columns -- INCLUDE on
// the PostgreSQL family, STORING on CockroachDB, COVER on YDB -- differ, on
// every dialect, since a target either carries the payload or refuses it.
//
// On a target whose preset has [capability.IndexCoveringColumns], Ptah renders
// the payload and the reader reports it, so the two lists are compared as the
// PostgreSQL comparison compares them, order included. It is asked before the
// per-target comparison, so a target that compares nothing else past the
// predicate still sees an index that only gained a payload (stokaro/ptah#4112).
//
// On a target without the key, the renderer refuses an index that declares a
// payload, and the reader reports none. A declared payload is therefore a
// change, so the plan writes the index and refuses it with the renderer's own
// message, as a render of the same schema does. The read side is left out
// there: a payload Ptah cannot render is never one a plan could restore.
func indexPayloadChanged(desired, database []string, dialect string, semantics identifier.Semantics) bool {
	if !capability.ForDialect(dialect).Has(capability.IndexCoveringColumns) {
		return len(desired) > 0
	}
	return coveredColumnsChanged(desired, database, semantics)
}

// namedKeyPartsContradict answers the only question a partly-read key
// ([catalog.Index.KeyPartsIncomplete]) can answer: do the parts the reader
// COULD name already prove this is not the desired key?
//
// The MySQL reader names every part except the functional ones, and it does
// not record where in the key those sat, so the two key lists cannot be lined
// up by position. What survives is the order of the named parts, which is enough for
// the shape that matters: a database `KEY idx_mixed (b, (b + 1))` against a
// desired `idx_mixed (c, (c + 1))` names `b` where the desired key names only
// `c`, so the keys differ no matter what the unread part turns out to be. That
// pair was reported synchronized while the comparison declined on sight of an
// unreadable part.
//
// Proof runs one way only. When every named part does appear, in order, the key
// may still differ in a part the reader could not read -- `(b, (b + 1))` against
// a desired `(b, (c + 1))` reports no change here. Reporting one instead would
// plan a rebuild on every run for a key that never changed, which is the
// oscillation the incomplete-key record exists to prevent, and the rebuild
// could not be reversed: the down direction rebuilds an index from the same
// reader's output and would recreate the key without its expression. Reading
// information_schema.STATISTICS.EXPRESSION is what closes that half, and it is
// a reader change with its own dialect sweep (MariaDB has no such column).
// Recorded in docs/conformance.md.
func namedKeyPartsContradict(
	desired schemamodel.Index,
	database catalog.Index,
	semantics identifier.Semantics,
) bool {
	generatedParts := effectiveGeneratedIndexParts(desired)
	databaseParts := effectiveDatabaseIndexParts(database)
	if len(databaseParts) > len(generatedParts) {
		return true
	}
	next := 0
	for _, databasePart := range databaseParts {
		named := semantics.ColumnIdentityKey(databasePart.Name)
		position := next
		for position < len(generatedParts) &&
			semantics.ColumnIdentityKey(generatedParts[position].Name) != named {
			position++
		}
		if position == len(generatedParts) {
			return true
		}
		next = position + 1
	}
	return false
}

// generatedIndexWithResolvedExpression replaces the declared expression with
// the one the server printed back, and leaves everything else alone.
//
// An index over plain columns has no expression to replace, and a declaration
// the server refused leaves the textual comparison in charge -- which is every
// offline path.
func generatedIndexWithResolvedExpression(
	desired schemamodel.Index,
	resolved config.IndexExpression,
) schemamodel.Index {
	if !resolved.Resolved || strings.TrimSpace(resolved.Expression) == "" {
		return desired
	}
	parts := make([]schemamodel.IndexPart, 0, len(desired.Parts))
	replaced := false
	for _, part := range desired.Parts {
		if !replaced && strings.TrimSpace(part.Expr) != "" {
			part.Expr = resolved.Expression
			replaced = true
		}
		parts = append(parts, part)
	}
	if !replaced {
		return desired
	}
	desired.Parts = parts
	return desired
}

// indexKeyTarget splits what a key indexes into its column slot and its
// expression slot, filling exactly one. That is the distinction #1246
// established: an index on lower(name) and an index on a column literally
// named "lower(name)" are different indexes. An expression is normalized the
// way a check-constraint expression is, so spacing and keyword case do not
// make a new index; a column goes through the dialect's identifier semantics.
func indexKeyTarget(
	name, expr string,
	semantics identifier.Semantics,
) (column, expression string) {
	trimmedExpr := strings.TrimSpace(expr)
	if trimmedExpr != "" {
		return "", normalizeCheckExpression(trimmedExpr)
	}
	return semantics.ColumnIdentityKey(name), ""
}

// coveredColumnsChanged compares an index's covered columns: the INCLUDE,
// STORING or COVER payload.
//
// Order is significant, matching both PostgreSQL -- which stores the payload
// columns in the order they were written and reports them that way -- and the
// pinned community binary v1.3.0, which planned a rebuild for
// `INCLUDE (b, c)` against `INCLUDE (c, b)` on PostgreSQL 17.10.
//
// The payload is compared separately from the keys and never merged into them:
// an index on (a) INCLUDE (b) is not an index on (a, b), and treating the
// payload as a trailing key would make the two compare equal.
func coveredColumnsChanged(
	desired, database []string,
	semantics identifier.Semantics,
) bool {
	if len(desired) != len(database) {
		return true
	}
	for position, column := range desired {
		if semantics.ColumnIdentityKey(column) !=
			semantics.ColumnIdentityKey(database[position]) {
			return true
		}
	}
	return false
}

// ruledIndexKeysChanged compares the keys of an index on a target that
// registers index rules: each key's column, its direction and its expression,
// in order. A key is a column or an expression and never both, so a column
// key and an expression key differ, as two expressions with different text
// do. An expression is compared in the server's spelling where the server
// answered ([generatedIndexWithResolvedExpression]), and otherwise normalized
// the way a predicate is, so spacing and keyword case do not make a new index.
//
// A key the reader could not read whole is compared as far as it was read:
// see [namedKeyPartsContradict]. Reading what is left as the whole key would
// find it short by one part on every run and plan a rebuild forever, on a
// database the pinned community binary v1.3.0 reports synced.
func ruledIndexKeysChanged(
	desired schemamodel.Index,
	database catalog.Index,
	semantics identifier.Semantics,
) bool {
	if database.KeyPartsIncomplete {
		return namedKeyPartsContradict(desired, database, semantics)
	}
	generatedParts := effectiveGeneratedIndexParts(desired)
	databaseParts := effectiveDatabaseIndexParts(database)
	if len(generatedParts) != len(databaseParts) {
		return true
	}
	for position, generatedPart := range generatedParts {
		databasePart := databaseParts[position]
		generatedColumn, generatedExpr := indexKeyTarget(generatedPart.Name, generatedPart.Expr, semantics)
		databaseColumn, databaseExpr := indexKeyTarget(databasePart.Name, databasePart.Expr, semantics)
		if generatedColumn != databaseColumn || generatedExpr != databaseExpr || generatedPart.Desc != databasePart.Desc {
			return true
		}
	}
	return false
}

func effectiveGeneratedIndexParts(index schemamodel.Index) []schemamodel.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]schemamodel.IndexPart, len(index.Fields))
	for position, field := range index.Fields {
		parts[position] = schemamodel.IndexPart{Name: field}
	}
	return parts
}

func effectiveDatabaseIndexParts(index catalog.Index) []catalog.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]catalog.IndexPart, len(index.Columns))
	for position, column := range index.Columns {
		parts[position] = catalog.IndexPart{Name: column}
	}
	return parts
}

func indexPredicateChanged(desired, database string, predicates schemacomparison.IndexPredicates) bool {
	if strings.TrimSpace(desired) == "" || strings.TrimSpace(database) == "" {
		return strings.TrimSpace(desired) != strings.TrimSpace(database)
	}
	if checkExpressionHasUnsupportedRewrite(desired, database) {
		return false
	}
	return normalizePredicate(desired, predicates) != normalizePredicate(database, predicates)
}

// normalizePredicate reduces an index predicate to a spelling-insensitive
// comparison key. A target whose catalog decorates the predicate it stores
// removes the decorations first, through predicates, its rule, so
// user-authored predicates converge with introspected ones instead of
// planning a replacement on every run.
func normalizePredicate(value string, predicates schemacomparison.IndexPredicates) string {
	if predicates != nil {
		value = predicates.Spelling(value)
	}
	return normalizeCheckExpression(value)
}

// IndexRenames returns the renames [IndexesWithSemantics] plans for desired
// against database, so a comparison that runs before it can bind what the
// database holds for a renamed index to the name the declaration gives it.
// It is the same pairing, run once more, so the two cannot disagree.
func IndexRenames(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
	indexes map[string]config.IndexExpression,
	caps capability.Capabilities,
	rules schemacomparison.Rules,
) []difftypes.IndexRename {
	scratch := &difftypes.SchemaDiff{}
	IndexesWithSemantics(desired, database, scratch, dialect, semantics, indexes, caps, rules)
	return scratch.IndexesRenamed
}
