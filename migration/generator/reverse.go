package generator

// Reversing a schema diff: the entry points, and the helpers every object
// family shares. The families themselves are in the reverse_*.go files.

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/indexscope"
	"ptah.run/migration/schemadiff/difftypes"
)

// reverseSchemaDiffWithPrior creates a reverse diff for generating down migrations with schema context.
//
// schema is the generated (target) Go schema, used to resolve table names for
// RLS policies. dbSchema is the introspected (pre-change) database schema, used
// to rebuild prior FK/PK/CHECK/UNIQUE definitions for reversed constraint
// additions; it may be nil when callers only have the generated schema (the
// reversed additions then fall back to the name-only path).
//
// # Every field of SchemaDiff is accounted for here
//
// This function builds a fresh SchemaDiff literal, and a literal that
// enumerates fields has no compiler check for the ones it forgets. Nine
// absent fields -- views, materialized views and triggers, added/removed/
// modified -- make every down migration silently drop those whole categories:
// an up that creates a view rolls back to "No rollback operations needed" and
// leaves the view in place (issue #1287). Three dispositions are available, and
// every field must have exactly one:
//
//   - Exchanged. Added and removed swap where both sides carry the same kind of
//     value and the reverse operation is the inverse of the forward one:
//     tables, enums, indexes, extensions, functions, sequences, domains,
//     composite types, ranges, views, materialized views, triggers, RLS
//     policies, RLS enablement, roles, grants, grant options and constraints.
//   - Carried. A Modified entry is not the inverse of itself. The planner
//     re-renders a modified object from the schema it is handed, and the down
//     direction is handed the pre-change database schema, so carrying the entry
//     across is what restores the prior definition. Only the recorded
//     "old -> new" description is flipped, plus any recorded prior state (a
//     view's PreviousBody) that names a side rather than a change.
//   - Derived. IdentifierSemantics is cloned rather than reversed: it describes
//     the catalog the diff was measured against, which does not have a
//     direction. The table-qualified constraint collections are rebuilt from
//     the pre-change database schema by reverseConstraintAdditions and
//     reverseConstraintRemovals rather than swapped, because a down migration
//     must restore the prior body, not the new one.
//     ForeignKeysRemovedWithTables is supplemental input for forward removal
//     ordering and is likewise rebuilt from the reversed FK additions rather
//     than itself creating a reverse operation. ConstraintBackedIndexRemovals
//     is derived too, and it redirects rather than reverses: it names the subset
//     of the index removals whose object is really a UNIQUE constraint, so
//     reverseIndexRemovals turns exactly that subset into constraint additions
//     rebuilt from the introspected constraint, and leaves the rest as index
//     additions.
//
// No field is deliberately dropped, and none is unreachable in the down
// direction. TestReverseSchemaDiff_AccountsForEverySchemaDiffField enforces
// that by reflection: it zeroes one field of a fully populated diff at a time
// and fails when doing so leaves the reverse plan unchanged.
func reverseSchemaDiffWithPrior(
	diff *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	dbSchema *catalog.Database,
	prior *schemamodel.Database,
	dialect string,
) *difftypes.SchemaDiff {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	reversed := &difftypes.SchemaDiff{
		IdentifierSemantics: cloneIdentifierSemantics(diff.IdentifierSemantics),
		TablePreparation:    cloneTablePreparation(diff.TablePreparation),

		// Reverse table operations.
		//
		// The caller restores removal captures into the prior context before
		// converting it. The resulting declaration therefore retains the
		// removed table's own fields and feature state even when the surrounding
		// source document no longer contains that table.
		TablesAdded: tableCreationsFromRemovals(diff.TablesRemoved.Names(), prior, semantics),
		// The PRE-CHANGE declaration's vocabulary, not the desired one: the
		// tables this direction creates are the ones that database held, and a
		// column of theirs names a type as that database declared it
		// (stokaro/ptah#2315).
		DeclaredUserTypes: difftypes.UserTypeVocabularyOf(prior),
		// The same reasoning for the tables a foreign key may point at: this
		// direction restores what the pre-change database held, and a reference
		// of theirs names a table as that database had it.
		DeclaredTables: priorTables(prior),
		// And the same for the schemas this direction recreates: a rollback
		// that puts a table back puts its schema back too, and the comment,
		// character set and collation that schema carries are the ones the
		// pre-change database had rather than the ones the change was moving
		// to (stokaro/ptah#2618).
		DeclaredSchemas: priorSchemas(prior),
		// And the same for the views a cascade may reach: a rollback recreates
		// what the pre-change database held, so the collateral set is that
		// database's views rather than the ones the change was moving to.
		DeclaredViewLikes: difftypes.ViewLikeVocabularyOf(prior),
		// And the same for the foreign keys a column type change takes with
		// it: a rollback restores the column the pre-change database had, so
		// the keys to drop and put back are that database's.
		DeclaredForeignKeys: difftypes.ForeignKeyDeclarationsOf(prior),
		// And the same for the graph the removals are ordered by. A table the
		// pre-change database did not hold is not in it, so it orders as it
		// arrived -- which is how the ordering this function already computed
		// for TablesRemoved survives the planner reading it again.
		DeclaredTableDependencies: priorTableDependencies(prior),
		// And the same for the functions this direction creates: they are the
		// ones that database held, and what they call is what it recorded.
		DeclaredFunctions: difftypes.FunctionOrderingOf(prior),
		// Reverse the databases of a whole MySQL-family server: a created one
		// is dropped, a dropped one is created with the character set and
		// collation the pre-change server held, and a changed one is set back.
		SchemasAdded:    schemaCreationsFromRemovals(diff.SchemasRemoved, prior),
		SchemasRemoved:  schemaNames(diff.SchemasAdded),
		SchemasModified: reverseSchemaChanges(diff.SchemasModified),
		TablesRemoved:   reverseTableRemovals(diff.TablesAdded),
		TablesModified:  reverseTableDiffs(diff.TablesModified, prior, semantics),

		// Reverse enum operations
		EnumsAdded:    diff.EnumsRemoved, // Enums to remove become enums to add
		EnumsRemoved:  diff.EnumsAdded,   // Enums to add become enums to remove
		EnumsModified: reverseEnumDiffs(diff.EnumsModified, prior),

		// Reverse extension operations
		ExtensionsAdded:    diff.ExtensionsRemoved, // Extensions to remove become extensions to add
		ExtensionsRemoved:  diff.ExtensionsAdded,   // Extensions to add become extensions to remove
		ExtensionsModified: reverseExtensionDiffs(diff.ExtensionsModified),

		// Reverse function operations. A removed routine of either kind comes
		// back as an addition carrying its own kind, so the two removal lists
		// merge into one addition list without losing which was which.
		FunctionsAdded:    append(slices.Clone(diff.FunctionsRemoved), diff.ProceduresRemoved...),
		FunctionsRemoved:  reverseFunctionsRemoved(diff.FunctionsAdded),
		FunctionsModified: reverseFunctionDiffs(diff.FunctionsModified, dbSchema, prior),
		// A removed procedure comes back as an addition, and the planner reads
		// its kind off the declaration -- which is why the reverse of a removal
		// needs no kind of its own. The reverse of an ADDITION does: nothing
		// here knows whether the added routine was a procedure, so
		// reverseProceduresRemoved asks the desired schema.
		ProceduresRemoved: reverseProceduresRemoved(diff.FunctionsAdded),

		// Reverse sequence operations
		// Reverse user-defined type operations
		DomainsAdded:           diff.DomainsRemoved,
		DomainsRemoved:         diff.DomainsAdded,
		DomainsModified:        reverseDomainDiffs(diff.DomainsModified, schema, prior, semantics),
		CompositeTypesAdded:    diff.CompositeTypesRemoved,
		CompositeTypesRemoved:  diff.CompositeTypesAdded,
		CompositeTypesModified: reverseCompositeTypeDiffs(diff.CompositeTypesModified, schema, prior, semantics),
		RangesAdded:            diff.RangesRemoved,
		RangesRemoved:          diff.RangesAdded,
		RangesModified:         reverseRangeDiffs(diff.RangesModified, schema, prior, semantics),

		SequencesAdded:    diff.SequencesRemoved, // Sequences to remove become sequences to add
		SequencesRemoved:  diff.SequencesAdded,   // Sequences to add become sequences to remove
		SequencesModified: reverseSequenceDiffs(diff.SequencesModified, prior, semantics),

		// Reverse view, materialized view and trigger operations.
		//
		// Each side carries the same kind of value (view names, materialized
		// view names, table-qualified trigger refs) and DROP is the inverse of
		// CREATE for all three, so the plain swap is the correct reversal.
		//
		// The Modified entries are carried across rather than swapped: the
		// planner re-renders a modified object from the schema it is handed,
		// which in the down direction is the pre-change database schema, so the
		// entry itself is what selects the prior definition. A view carries the
		// body it will be replacing as well, and THAT is a side rather than a
		// change, so it is exchanged for the up migration's target body -- the
		// state the database is actually in when the rollback runs.
		ViewsAdded:    diff.ViewsRemoved, // Views to remove become views to add
		ViewsRemoved:  diff.ViewsAdded,   // Views to add become views to remove
		ViewsModified: reverseViewDiffs(diff.ViewsModified, schema, prior, semantics),

		// The context the down plan reads is the database the up migration
		// left: the declared objects become the current ones, each with the
		// value the database held where it held one already, which carries
		// the state a replication or a transfer reported.
		Features: reverseFeatureContext(diff.Features),

		// Materialized views to remove become materialized views to add, each
		// as the pre-change database held it, settings included.
		MaterializedViewsAdded:    priorMaterializedViews(diff.MaterializedViewsRemoved, prior, semantics),
		MaterializedViewsRemoved:  diff.MaterializedViewsAdded, // Materialized views to add become materialized views to remove
		MaterializedViewsModified: reverseMaterializedViewDiffs(diff.MaterializedViewsModified, prior, semantics),

		// Exchanged, but not carried across untouched. An addition renders from
		// its operand and a removal from its names, so the two directions want
		// opposite things: the reversed addition needs the definition the
		// pre-change database held, and the reversed removal needs none
		// (stokaro/ptah#2315).
		TriggersAdded:    triggerAdditionsFromRemovals(diff.TriggersRemoved, prior, semantics),
		TriggersRemoved:  triggerRemovalsFromAdditions(diff.TriggersAdded),
		TriggersModified: reverseTriggerDiffs(diff.TriggersModified, prior, semantics),

		// Reverse RLS policy operations. Both directions carry the owning
		// table, so reversing is a swap and no name-to-table resolution is
		// needed. A resolution here would key a map by policy name and lose one
		// of two policies that share one.
		// Exchanged, and rewritten on the way. An addition renders CREATE POLICY
		// from its operand and a removal is written from its two names, so the
		// reversed addition needs the declaration the pre-change database held
		// and the reversed removal needs none (stokaro/ptah#2315).
		RLSPoliciesAdded:    rlsAdditionsFromRemovals(diff.RLSPoliciesRemoved, prior, semantics),
		RLSPoliciesRemoved:  rlsRemovalsFromAdditions(diff.RLSPoliciesAdded),
		RLSPoliciesModified: reverseRLSPolicyDiffs(diff.RLSPoliciesModified, prior, semantics),
		// Carried, not dropped. A declaration in which two policies share one
		// identity cannot be planned in either direction, and a rollback that
		// became plannable by forgetting the conflict would plan against the
		// very declaration the forward direction refused (stokaro/ptah#2440).
		RLSPolicyIdentityConflicts: diff.RLSPolicyIdentityConflicts,

		// A comment transition carries both of its states, so the reversal
		// swaps them and needs nothing from the schema beside it.
		ObjectCommentsChanged: reverseObjectComments(diff.ObjectCommentsChanged),

		// Reverse RLS table enablement operations
		RLSEnabledTablesAdded:   diff.RLSEnabledTablesRemoved, // Tables to disable RLS become tables to enable RLS
		RLSEnabledTablesRemoved: diff.RLSEnabledTablesAdded,   // Tables to enable RLS become tables to disable RLS
		RLSForceChanged:         reverseRLSForceChanges(diff.RLSForceChanged),

		// Reverse role operations
		RolesAdded:    diff.RolesRemoved, // Roles to remove become roles to add
		RolesRemoved:  diff.RolesAdded,   // Roles to add become roles to remove
		RolesModified: reverseRoleDiffs(diff.RolesModified, prior),
		// A membership carries both of its roles, so the swap needs nothing
		// from the schema beside it.
		RoleMembershipsAdded:   diff.RoleMembershipsRemoved,
		RoleMembershipsRemoved: diff.RoleMembershipsAdded,
		GrantsAdded:            diff.GrantsRemoved,       // Grants to remove become grants to add
		GrantsRemoved:          diff.GrantsAdded,         // Grants to add become grants to revoke
		GrantOptionsAdded:      diff.GrantOptionsRevoked, // Revoked grant options become grant-option additions
		GrantOptionsRevoked:    diff.GrantOptionsAdded,   // Grant-option additions become grant-option revocations

		// A default privilege reverses like a grant: both directions are
		// statements the server accepts, and each entry carries its whole
		// identity, so the swap needs nothing from the schema beside it.
		DefaultPrivilegesAdded:         diff.DefaultPrivilegesRemoved,
		DefaultPrivilegesRemoved:       diff.DefaultPrivilegesAdded,
		DefaultPrivilegeOptionsAdded:   diff.DefaultPrivilegeOptionsRevoked,
		DefaultPrivilegeOptionsRevoked: diff.DefaultPrivilegeOptionsAdded,

		// Reverse constraint operations. A modified constraint is expressed by
		// the comparator as remove + add of the SAME name (e.g. an on_delete
		// change on a field-level FK, issue #189). Swapping the two slices makes
		// the down migration drop the new definition and re-add the old one.
		// reverseConstraintAdditions restores the prior table-qualified body
		// from the introspected schema for the constraint types whose down
		// add-path needs more than a name.
		//
		// ConstraintsAdded carries the table-qualified prior body so
		// the down add-path can fan a shared constraint name out to every real
		// host table. Without it the down add-path falls back to name-only
		// resolution, which can emit one ADD for a single host while per-host
		// DROP also resolves only one host; the 2nd host's re-add then collides
		// with its still-present old constraint (Postgres 42710, MySQL 1826)
		// and the rollback aborts half-applied.
		ConstraintsRemoved:           reverseConstraintRemovals(diff, schema, semantics),
		ForeignKeysRemovedWithTables: reverseForeignKeyRemovals(diff, schema, dialect),
		ConstraintsAdded:             reverseConstraintAdditions(diff, dbSchema, semantics),
		// A comment transition carries both of its states, as an object's
		// does, so the reversal swaps them.
		ConstraintCommentsChanged: reverseConstraintComments(diff.ConstraintCommentsChanged),
		// A validation has no reverse statement: PostgreSQL has no way to mark
		// a validated constraint NOT VALID again short of dropping it, and the
		// rows the validation checked satisfy the constraint whether it is
		// marked or not. A declaration that allowed NOT VALID is satisfied by
		// the validated constraint the rollback leaves.
		ConstraintsValidated: nil,
	}
	// A re-created table brings its own primary key and field-level foreign keys
	// back with it, so listing those a second time as constraint additions is
	// how a rollback of a DROP TABLE became unexecutable. This runs before the
	// index-removal restorations are appended purely for readability: those are
	// UNIQUE constraints, which the rule deliberately never drops.
	dropReverseConstraintsRestoredByTableCreation(reversed, diff.ConstraintsRemoved, prior)
	indexAdditions, constraintRestorations := reverseIndexRemovals(diff, dbSchema, prior)
	// The definitions come from the PRE-CHANGE database: this direction
	// re-creates the indexes the change dropped, and an index the declaration
	// holds may not be one of them (stokaro/ptah#2315).
	reversed.SetIndexAdditions(priorIndexChanges(prior, indexAdditions, semantics, diff.IndexesAdded))
	reversed.SetIndexRemovals(diff.IndexAdditions())
	// A visibility change carries the state it asks for, and the other state
	// is the only one there is, so the rollback asks for that one.
	for _, change := range diff.IndexVisibilityChanged {
		change.Invisible = !change.Invisible
		reversed.IndexVisibilityChanged = append(reversed.IndexVisibilityChanged, change)
	}
	reversed.IndexesRenamed = reverseIndexRenames(diff.IndexesRenamed)
	reversed.IndexCommentsChanged = reverseIndexComments(diff.IndexCommentsChanged)
	for _, restored := range constraintRestorations {
		reversed.ConstraintsAdded = append(reversed.ConstraintsAdded, restored)
	}
	// The tables those constraint changes name, as the PRE-CHANGE database
	// declared them: a rollback rebuilds the table that database had, so the
	// columns, indexes and triggers the rebuild renders are its. Filled last
	// because the two lists it reads are still being appended to above
	// (stokaro/ptah#2315).
	reversed.DeclaredConstraintHosts = difftypes.ConstraintHostDeclarationsOf(
		prior, reversed.ConstraintsAdded, reversed.ConstraintsRemoved, semantics)
	// A rollback runs against the same database, whose read declined the same
	// settings.
	reversed.CurrentNotDescribed = diff.CurrentNotDescribed
	// The same database's path and grants: a rollback names an object by the
	// same absolute path, and a table it rebuilds held the same grants.
	reversed.CurrentDatabasePath = diff.CurrentDatabasePath
	reversed.CurrentGrants = diff.CurrentGrants
	return reversed
}

func uniqueStringsPreserveOrder(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// derefString returns the pointed-to string or "" when nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func reverseChangeMap(changes map[string]string) map[string]string {
	reversed := make(map[string]string, len(changes))
	for key, change := range changes {
		parts := strings.Split(change, " -> ")
		if len(parts) == 2 {
			reversed[key] = parts[1] + " -> " + parts[0]
		} else {
			reversed[key] = change
		}
	}
	return reversed
}

// schemaCreationsFromRemovals is the creation of each removed schema, carrying
// what the pre-change database declared for it. A removal is a name, and a
// database recreated without its character set and collation is not the one
// the rollback puts back.
func schemaCreationsFromRemovals(removed []string, prior *schemamodel.Database) []schemamodel.Schema {
	if len(removed) == 0 {
		return nil
	}
	creations := make([]schemamodel.Schema, 0, len(removed))
	for _, name := range removed {
		creation := schemamodel.Schema{Name: name}
		for _, held := range priorSchemas(prior) {
			if held.Name == name {
				creation = held
				break
			}
		}
		creations = append(creations, creation)
	}
	return creations
}

// schemaNames is the names of schemas.
func schemaNames(schemas []schemamodel.Schema) []string {
	if len(schemas) == 0 {
		return nil
	}
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

// reverseSchemaChanges sets each changed schema back to what the server held.
func reverseSchemaChanges(changes []difftypes.SchemaChange) []difftypes.SchemaChange {
	if len(changes) == 0 {
		return nil
	}
	reversed := make([]difftypes.SchemaChange, 0, len(changes))
	for _, change := range changes {
		reversed = append(reversed, difftypes.SchemaChange{
			Name:           change.Name,
			Charset:        change.CurrentCharset,
			Collate:        change.CurrentCollate,
			CurrentCharset: change.Charset,
			CurrentCollate: change.Collate,
		})
	}
	return reversed
}

// priorSchemas is the schema declarations of the pre-change database, for the
// reason [priorTables] gives about tables.
func priorSchemas(prior *schemamodel.Database) []schemamodel.Schema {
	if prior == nil {
		return nil
	}
	return prior.Schemas
}

// priorTables is every table the pre-change database declared.
func priorTables(prior *schemamodel.Database) []schemamodel.Table {
	if prior == nil {
		return nil
	}
	return prior.Tables
}

// reverseCommentChange swaps the two sides of a comment transition, so the
// rollback restores the comment the database had.
func reverseCommentChange(change *difftypes.CommentChange) *difftypes.CommentChange {
	if change == nil {
		return nil
	}
	return &difftypes.CommentChange{Current: change.Desired, Desired: change.Current}
}

// priorTableDependencies is the dependency graph of the pre-change database.
//
// nil is a real input: a reversal without a database read passes none, and the
// planner treats an absent graph as no edges, which orders the removals exactly
// as they arrived.
func priorTableDependencies(prior *schemamodel.Database) map[string][]string {
	if prior == nil {
		return nil
	}
	return deporder.GeneratedTableDependencies(prior)
}

// priorIndexChanges pairs each reference with the declaration the pre-change
// database had for it.
//
// A reference the prior schema does not hold keeps its identity and carries an
// index with that name and nothing else. The plan then renders what a bare
// reference always rendered, which is the same answer as before rather than a
// silently dropped statement (stokaro/ptah#2315).
func priorIndexChanges(
	prior *schemamodel.Database,
	refs []difftypes.IndexRef,
	semantics identifier.Semantics,
	forward difftypes.IndexChanges,
) difftypes.IndexChanges {
	if len(refs) == 0 {
		return nil
	}
	required := make(map[difftypes.IndexRef]bool)
	for _, change := range forward {
		key := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{Name: change.Index.Name, TableName: change.TableName})
		required[key] = change.RequiresTableCopy
	}
	declared := make(map[difftypes.IndexRef]difftypes.IndexChange, len(refs))
	for _, declaration := range difftypes.IndexDeclarationsOf(prior) {
		key := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{
			Name:      declaration.Index.Name,
			TableName: declaration.TableName,
		})
		declared[key] = declaration
	}
	changes := make(difftypes.IndexChanges, 0, len(refs))
	for _, ref := range refs {
		if declaration, ok := declared[indexscope.IdentityKeyWithSemantics(semantics, ref)]; ok {
			changes = append(changes, difftypes.IndexChange{
				Index: declaration.Index, TableName: declaration.TableName,
				RequiresTableCopy: required[indexscope.IdentityKeyWithSemantics(semantics, ref)],
			})
			continue
		}
		changes = append(changes, difftypes.IndexChange{
			Index:     schemamodel.Index{Name: ref.Name},
			TableName: ref.TableName,
		})
	}
	return changes
}

// reverseRLSForceChanges moves each FORCE flag back. The flag is two-valued and
// the forward change moved it off the state the database held, so the database
// held the opposite of each target.
func reverseRLSForceChanges(changes difftypes.RLSForceChanges) difftypes.RLSForceChanges {
	if changes == nil {
		return nil
	}
	reversed := make(difftypes.RLSForceChanges, 0, len(changes))
	for _, change := range changes {
		change.Forced = !change.Forced
		reversed = append(reversed, change)
	}
	return reversed
}

// reverseIndexRenames is the rollback of the renames a forward diff makes:
// each index back to the name it had. A change of a renamed index's
// partitioning is the YDB owner's, and its rollback names the index by that
// name too; see [renamedBack].
func reverseIndexRenames(renames []difftypes.IndexRename) []difftypes.IndexRename {
	var reversed []difftypes.IndexRename
	for _, rename := range renames {
		reversed = append(reversed, difftypes.IndexRename{TableName: rename.TableName, From: rename.To, To: rename.From})
	}
	return reversed
}

// reverseIndexComments is the rollback of the index comments a forward diff
// writes: each comment back to the one the database held, under the name the
// index has once the rollback's renames run, so a renamed index's comment
// moves back with its name. An index the forward diff drops is one the
// rollback adds again, and its comment comes back with it.
func reverseIndexComments(changes []difftypes.IndexCommentChange) []difftypes.IndexCommentChange {
	if changes == nil {
		return nil
	}
	reversed := make([]difftypes.IndexCommentChange, 0, len(changes))
	for _, change := range changes {
		name, from := change.Name, change.From
		if from != "" {
			name, from = from, name
		}
		reversed = append(reversed, difftypes.IndexCommentChange{
			TableName: change.TableName,
			Name:      name,
			From:      from,
			Current:   change.Desired,
			Desired:   change.Current,
		})
	}
	return reversed
}

// reverseFeatureContext is the context a down plan reads: the database the up
// migration left holds what the target schema declared, and the down plan
// declares what the database held. An object both sides held keeps the
// database's value, which carries what only a read reports, such as the
// state of a YDB replication; the forward plan changed no such value an
// object has outside its declaration.
func reverseFeatureContext(context difftypes.FeatureContext) difftypes.FeatureContext {
	current := context.DesiredObjects
	for _, ref := range context.DesiredObjects.Refs() {
		held, found, err := context.CurrentObjects.Get(ref)
		if err != nil || !found {
			continue
		}
		if replaced, err := current.Replace(held); err == nil {
			current = replaced
		}
	}
	return difftypes.FeatureContext{DesiredObjects: context.CurrentObjects, CurrentObjects: current,
		CurrentCoverage: context.CurrentCoverage}
}
