package generator

// Reversing a schema diff: the entry points, and the helpers every object
// family shares. The families themselves are in the reverse_*.go files.

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/deporder"
	"ptah.run/internal/indexscope"
	"ptah.run/migration/schemadiff/difftypes"
)

// reverseSchemaDiff creates a reverse diff for generating down migrations
//
// Deprecated: Use reverseSchemaDiffWithSchema for proper RLS policy table name resolution
func reverseSchemaDiff(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	return reverseSchemaDiffWithSchema(diff, nil, nil)
}

// reverseSchemaDiffWithSchema creates a reverse diff for generating down migrations with schema context.
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
// enumerates fields has no compiler check for the ones it forgets. Nine fields
// -- views, materialized views and triggers, added/removed/modified -- were
// once simply absent, so every down migration silently dropped those whole
// categories: an up that created a view rolled back to "No rollback operations
// needed" and left the view in place (issue #1287). Three dispositions are
// available, and every field must have exactly one:
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
func reverseSchemaDiffWithSchema(diff *difftypes.SchemaDiff, schema *schemamodel.Database, dbSchema *catalog.Database) *difftypes.SchemaDiff {
	return reverseSchemaDiffWithSchemaForDialect(diff, schema, dbSchema, "")
}

func reverseSchemaDiffWithSchemaForDialect(
	diff *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
) *difftypes.SchemaDiff {
	// The identity the two producers agree on. The reversed diff carries the
	// same rules the forward one was compared under, so a down migration pairs
	// its drops exactly as the up migration it undoes did.
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	// The pre-change database as a desired schema. This is the schema the DOWN
	// plan is rendered against -- see generateDownMigrationSQLQualified, which
	// hands the planner exactly this -- so it is where a reversed modification
	// finds the definition it must restore (stokaro/ptah#2315).
	// nil is a real input here: callers that reverse a diff without a database
	// read pass one, and the conversion dereferences it.
	var prior *schemamodel.Database
	if dbSchema != nil {
		prior = dbschematogo.ConvertDBSchemaToGoSchema(dbSchema, dialect)
	}
	reversed := &difftypes.SchemaDiff{
		IdentifierSemantics: cloneIdentifierSemantics(diff.IdentifierSemantics),

		// Reverse table operations.
		//
		// A removal is a name, which is all DROP TABLE needs. The addition it
		// becomes renders CREATE TABLE, so the declaration has to be recovered
		// from the pre-change database or the rollback drops a table it never
		// puts back (stokaro/ptah#2315).
		TablesAdded: tableCreationsFromRemovals(diff.TablesRemoved, prior),
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
		TablesRemoved:     deporder.TableDropOrder(diff.TablesAdded.Names(), schema), // Tables to add become tables to remove
		TablesModified:    reverseTableDiffs(diff.TablesModified, prior),

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
		FunctionsModified: reverseFunctionDiffs(diff.FunctionsModified, prior),
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

		// A synonym is an alias with no body, so reversing it needs no schema
		// side: the down direction drops what the up direction created and
		// recreates what it dropped. A retarget is the one entry that carries
		// state, and swapping its two targets is the whole reversal -- the
		// planner drops and recreates either way.
		SynonymsAdded:    diff.SynonymsRemoved,
		SynonymsRemoved:  diff.SynonymsAdded,
		SynonymsModified: reverseSynonymDiffs(diff.SynonymsModified, prior),

		// A hypertable reverses like a synonym in the diff and unlike one in
		// the plan. The swap is the same -- what the up direction partitioned,
		// the down direction stops declaring -- but the DOWN plan is a refusal
		// rather than a statement: TimescaleDB has no drop_hypertable, so a
		// rollback of a create_hypertable is a table the operator has to drop
		// and recreate. The swap belongs here anyway, because refusing is what
		// the planner does with a removal and refusing loudly is the point.
		HypertablesAdded:    slices.Clone(diff.HypertablesRemoved),
		HypertablesRemoved:  slices.Clone(diff.HypertablesAdded),
		HypertablesModified: reverseHypertableDiffs(diff.HypertablesModified),

		// A continuous aggregate reverses like a view and unlike the
		// hypertable above it: both directions are statements the server
		// accepts. What the up direction created, the down direction drops
		// with DROP MATERIALIZED VIEW; what it dropped, the down direction
		// creates from the body the pre-change read carried, which is why the
		// down plan's desired schema is the introspected one.
		ContinuousAggregatesAdded:    slices.Clone(diff.ContinuousAggregatesRemoved),
		ContinuousAggregatesRemoved:  slices.Clone(diff.ContinuousAggregatesAdded),
		ContinuousAggregatesModified: reverseContinuousAggregateDiffs(diff.ContinuousAggregatesModified, prior, semantics),

		// An extended property is a name, an address and a value, and the
		// reversal needs no schema side because all three are already in the
		// diff: the down direction drops what the up direction added, adds
		// back what it dropped with the value the removal carried, and swaps
		// the two values of a modification.
		ExtendedPropertiesAdded:    slices.Clone(diff.ExtendedPropertiesRemoved),
		ExtendedPropertiesRemoved:  slices.Clone(diff.ExtendedPropertiesAdded),
		ExtendedPropertiesModified: reverseExtendedPropertyDiffs(diff.ExtendedPropertiesModified),

		MaterializedViewsAdded:    diff.MaterializedViewsRemoved, // Materialized views to remove become materialized views to add
		MaterializedViewsRemoved:  diff.MaterializedViewsAdded,   // Materialized views to add become materialized views to remove
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
		// needed -- the resolution that used to happen here keyed a map by
		// policy name and lost one of two policies that shared one.
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

		// Reverse RLS table enablement operations
		RLSEnabledTablesAdded:   diff.RLSEnabledTablesRemoved, // Tables to disable RLS become tables to enable RLS
		RLSEnabledTablesRemoved: diff.RLSEnabledTablesAdded,   // Tables to enable RLS become tables to disable RLS

		// Reverse role operations
		RolesAdded:          diff.RolesRemoved, // Roles to remove become roles to add
		RolesRemoved:        diff.RolesAdded,   // Roles to add become roles to remove
		RolesModified:       reverseRoleDiffs(diff.RolesModified, prior),
		GrantsAdded:         diff.GrantsRemoved,       // Grants to remove become grants to add
		GrantsRemoved:       diff.GrantsAdded,         // Grants to add become grants to revoke
		GrantOptionsAdded:   diff.GrantOptionsRevoked, // Revoked grant options become grant-option additions
		GrantOptionsRevoked: diff.GrantOptionsAdded,   // Grant-option additions become grant-option revocations

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
	}
	// A re-created table brings its own primary key and field-level foreign keys
	// back with it, so listing those a second time as constraint additions is
	// how a rollback of a DROP TABLE became unexecutable. This runs before the
	// index-removal restorations are appended purely for readability: those are
	// UNIQUE constraints, which the rule deliberately never drops.
	dropReverseConstraintsRestoredByTableCreation(reversed, diff.ConstraintsRemoved, dbSchema, dialect)
	indexAdditions, constraintRestorations := reverseIndexRemovals(diff, dbSchema)
	// The definitions come from the PRE-CHANGE database: this direction
	// re-creates the indexes the change dropped, and an index the declaration
	// holds may not be one of them (stokaro/ptah#2315).
	reversed.SetIndexAdditions(priorIndexChanges(prior, indexAdditions, semantics))
	reversed.SetIndexRemovals(diff.IndexAdditions())
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

func priorTableSchema(prior *schemamodel.Database, tableName string) string {
	for _, table := range prior.Tables {
		if table.Name == tableName {
			return table.Schema
		}
	}
	return ""
}

// priorTables is every table the pre-change database declared.
// priorSchemas is the schema declarations of the pre-change database, for the
// reason [priorTables] gives about tables.
func priorSchemas(prior *schemamodel.Database) []schemamodel.Schema {
	if prior == nil {
		return nil
	}
	return prior.Schemas
}

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

// reverseRowTTLChange swaps the two sides of a row-level TTL transition.
func reverseRowTTLChange(change *difftypes.RowTTLChange) *difftypes.RowTTLChange {
	if change == nil {
		return nil
	}
	return &difftypes.RowTTLChange{Desired: change.Current, Current: change.Desired}
}

// reverseRowDeletionPolicyChange swaps the two sides of a row deletion policy
// transition.
func reverseRowDeletionPolicyChange(
	change *difftypes.RowDeletionPolicyChange,
) *difftypes.RowDeletionPolicyChange {
	if change == nil {
		return nil
	}
	return &difftypes.RowDeletionPolicyChange{Desired: change.Current, Current: change.Desired}
}

// reverseRefreshChange swaps the two sides of a materialized view's refresh
// schedule transition.
func reverseRefreshChange(change *difftypes.MatViewRefreshChange) *difftypes.MatViewRefreshChange {
	if change == nil {
		return nil
	}
	return &difftypes.MatViewRefreshChange{Desired: change.Current, Current: change.Desired}
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
) difftypes.IndexChanges {
	if len(refs) == 0 {
		return nil
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
			changes = append(changes, declaration)
			continue
		}
		changes = append(changes, difftypes.IndexChange{
			Index:     schemamodel.Index{Name: ref.Name},
			TableName: ref.TableName,
		})
	}
	return changes
}
