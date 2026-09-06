package clickhouse

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/objectlookup"
	"ptah.run/migration/schemadiff/difftypes"
)

// reportUnsupportedObjectsBeforeTables and planObjectsAfterTables append the
// shared schema-object AST nodes in the same order as offline rendering. The
// ClickHouse renderer turns supported plain views into DDL and unsupported
// shapes into named diagnostics.
//
// Both paths route the same nodes through the renderer. Views and materialized
// views preserve their bodies and render as executable DDL; object kinds the
// ClickHouse model cannot represent safely retain their named not-supported
// diagnostics.
//
// The split around the table statements reproduces the order the converter uses
// for `render`, so the two surfaces agree line for line and not merely as sets.
//
// Unsupported nodes carry identity only because that is all their diagnostics
// read. View-like additions and replacements resolve the declaration, carry
// their bodies through modelast, and share one deporder.ViewLikesForCreate
// pass so dependencies precede the objects that read them. Diagnostic comments
// are stripped before execution by atlasschema.SplitApplyStatements.
func reportUnsupportedObjectsBeforeTables(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	result = reportRemovedUserTypes(result, diff)
	result = reportExtensions(result, diff)
	result = reportSequences(result, diff)
	return result
}

// planObjectsAfterTables sequences everything the ClickHouse plan emits once the
// tables exist. Two of its phases are no longer diagnostics: roles and grants are
// planned as real statements by rbac.go, which is what stokaro/ptah#1025 asked
// for.
//
// They keep the slots their diagnostics held, and each slot is now load-bearing
// twice over. Roles come first because a grant to a role the server does not know
// fails with Code 511 (UNKNOWN_ROLE), and grants sit between the row-level
// security phase and the triggers because that is where the offline render path
// emits them -- the two surfaces have to agree on order, not merely on content.
func planObjectsAfterTables(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	caps capability.Capabilities,
) ([]ast.Node, error) {
	result = planRoles(result, diff)
	result = reportFunctions(result, diff)
	var err error
	result, err = reportViewLikes(result, diff, caps)
	if err != nil {
		return nil, err
	}
	result = reportRowLevelSecurity(result, diff, caps)
	result = planRowPolicies(result, diff, caps)
	result = planGrants(result, diff)
	result = reportTriggers(result, diff)
	return result, nil
}

// reportRemovedUserTypes names the domains, composite types and range types a
// desired schema no longer declares.
//
// Creation is refused before any SQL by usertypescope.ValidateDeclared, because
// a named skip would leave the declaring table naming a type the server has no
// definition of (stokaro/ptah#1717). Removal has no declaration to refuse and
// dropping a type this target never created is not an error, so it is named --
// the answer every other unhostable kind here gives.
//
// The path is unreachable today: no ClickHouse read reports a domain, a
// composite or a range, so the diff cannot carry one. The collections were
// unwalked, which is how #1628 closed with grants and row-level security fixed
// and these three still silent (stokaro/ptah#1708). Writing it now means a
// reader that learns them later produces a sentence rather than nothing.
func reportRemovedUserTypes(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, domain := range diff.DomainsRemoved {
		result = append(result, ast.NewDropType(domain.QualifiedName()).SetDomain())
	}
	for _, composite := range diff.CompositeTypesRemoved {
		result = append(result, ast.NewDropType(composite.QualifiedName()))
	}
	for _, rangeType := range diff.RangesRemoved {
		result = append(result, ast.NewDropType(rangeType.QualifiedName()))
	}
	return result
}

func reportExtensions(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, extension := range diff.ExtensionsAdded {
		result = append(result, ast.NewExtension(extension.Name))
	}
	for _, extension := range diff.ExtensionsRemoved {
		result = append(result, ast.NewDropExtension(extension.Name))
	}
	for _, extension := range diff.ExtensionsModified {
		result = append(result, ast.NewExtension(extension.Name).SetSchema(extension.ToSchema))
	}
	return result
}

func reportSequences(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, sequence := range diff.SequencesAdded {
		result = append(result, ast.NewCreateSequence(sequence.QualifiedName()))
	}
	for _, sequence := range diff.SequencesModified {
		result = append(result, ast.NewAlterSequence(sequence.SequenceName))
	}
	for _, sequence := range diff.SequencesRemoved {
		result = append(result, ast.NewDropSequence(sequence.QualifiedName()))
	}
	return result
}

func reportFunctions(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, routine := range diff.FunctionsAdded {
		result = append(result, ast.NewCreateFunction(routine.Name))
	}
	for _, function := range diff.FunctionsModified {
		result = append(result, ast.NewCreateFunction(function.FunctionName))
	}
	for _, routine := range diff.FunctionsRemoved {
		result = append(result, ast.NewDropFunction(routine.Name))
	}
	return result
}

// viewLikeIdentity keys a planned view-like node by kind as well as name, so a
// map lookup can never hand a materialized view's node to a plain view.
type viewLikeIdentity struct {
	name         string
	materialized bool
}

func identityOf(object deporder.ViewLike) viewLikeIdentity {
	return viewLikeIdentity{name: object.Name, materialized: object.Materialized}
}

// reportViewLikes plans plain views and materialized views in one pass.
//
// The two kinds share one dependency order because either can read the other
// and ClickHouse resolves a query when the object is created: a CREATE VIEW
// naming an object that does not exist yet is refused with
// "Unknown table expression identifier ... (UNKNOWN_TABLE)" rather than left to
// fail later. The offline render path orders the two kinds together for the
// same reason, so planning them separately would make the two surfaces disagree
// as well as emit an unexecutable order.
//
// A changed materialized view is planned as a drop followed by a create, the
// shape the PostgreSQL planner uses. Those drops are emitted before every create
// so the object being replaced is gone first. A changed plain view needs none,
// because CREATE OR REPLACE VIEW is one statement.
//
// ClickHouse does have an in-place edit, and it is deliberately not what this
// emits. Measured on 26.7.3.19 and on 24.10.4.191,
// `ALTER TABLE <mv> MODIFY QUERY <select>` rewrites system.tables.as_select and
// leaves every accumulated row in the storage table -- but only while the
// projection is unchanged. A select naming one different output column is
// refused outright:
//
//	ALTER TABLE mq MODIFY QUERY SELECT sum(id) AS total FROM users
//	-> Code: 16. DB::Exception: Column total does not exist in the
//	   materialized view's inner table. (NO_SUCH_COLUMN_IN_TABLE)
//
// A schemamodel.MaterializedView carries a body and no column list, and nothing
// here parses the select, so the planner cannot tell offline which of the two a
// given body change is. Drop and create is the shape that covers both, and what
// it costs is the stored rows. See the discussion on #1519.
//
// The plain view beside it is not affected either way: MODIFY QUERY on a View is
// "Alter of type 'MODIFY_QUERY' is not supported by storage View.
// (NOT_IMPLEMENTED)", and CREATE OR REPLACE VIEW already covers it.
//
// An object can also change KIND without changing its name. The plain-view and
// materialized-view comparators are independent, so a name declared as one kind
// and living in the database as the other arrives as an addition of the desired
// kind next to a removal of the live kind, and the two do not have to spell the
// name the same way: measured through migration/schemadiff, a declared view "x"
// against a database holding a materialized view produces
// ViewsAdded=[x] with MaterializedViewsRemoved=[ptah_test.x]. ClickHouse
// refuses the create while the old object still owns the name — measured on
// server 26.7.3.19, both directions answer "Code: 57. DB::Exception: Table
// ptah_test.x already exists. (TABLE_ALREADY_EXISTS)" — so those removals are
// emitted BEFORE the create pass. They are moved rather than added, so the plan
// still names each object exactly once.
func reportViewLikes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	caps capability.Capabilities,
) ([]ast.Node, error) {
	semantics := diff.EffectiveIdentifierSemantics(platform.ClickHouse)
	capacity := len(diff.ViewsAdded) + len(diff.ViewsModified) +
		len(diff.MaterializedViewsAdded) + len(diff.MaterializedViewsModified)
	objects := make([]deporder.ViewLike, 0, capacity)
	nodes := make(map[viewLikeIdentity]ast.Node, capacity)

	replacedMaterializedViews := crossKindReplacements(
		diff.MaterializedViewsRemoved.Names(),
		diff.ViewsAdded.Names(),
		semantics,
	)
	replacedViews := crossKindReplacements(
		diff.ViewsRemoved.Names(),
		diff.MaterializedViewsAdded.Names(),
		semantics,
	)
	for _, name := range replacedMaterializedViews {
		result = append(result, ast.NewDropMaterializedView(name).SetIfExists())
	}
	for _, name := range replacedViews {
		result = append(result, ast.NewDropView(name).SetIfExists())
	}

	for _, view := range diff.ViewsAdded {
		object, node := clickHouseViewChangeFor(view, caps)
		objects = append(objects, object)
		nodes[identityOf(object)] = node
	}
	// The view a modification leaves behind travels WITH it, in the direction
	// being planned (stokaro/ptah#2315). #2411 converted the other three
	// planners and left this one looking the name back up, so a ClickHouse
	// rollback still rendered from whichever schema it was handed rather than
	// from the change.
	for _, view := range diff.ViewsModified {
		declared := view.Desired
		// A target whose capability set declines views renders identity only,
		// and identity is the diff's own name -- so it needs no declaration,
		// which is what TestGenerateMigrationAST_DisabledViewsNeedNoDesiredDeclaration
		// pins. The refusal below is for the case where a statement WOULD be
		// written.
		if declared.Name == "" {
			if caps.Has(capability.Views) {
				// A REFUSAL rather than a skip, which is what the name lookup
				// did here: this planner reported a modification it could not
				// resolve instead of planning past it. The other three
				// planners skip, because a failed lookup is what they did.
				return nil, fmt.Errorf(
					"%w: ClickHouse view %q named by diff carries no declaration",
					ptaherr.ErrInvalidSchemaDiff,
					view.ViewName,
				)
			}
			declared.Name = view.ViewName
		}
		object, node := clickHouseViewChangeFor(declared, caps)
		node.SetReplace()
		objects = append(objects, object)
		nodes[identityOf(object)] = node
	}
	for _, view := range diff.MaterializedViewsAdded {
		object, node := clickHouseMaterializedViewChangeFor(view, caps)
		objects = append(objects, object)
		nodes[identityOf(object)] = node
	}
	for _, view := range diff.MaterializedViewsModified {
		// A schedule change on its own is an ALTER that keeps the view's rows.
		// Everything else is a drop and a create, which does not.
		if alter := clickHouseRefreshAlter(view); alter != nil {
			result = append(result, alter)
			continue
		}
		object, node, err := clickHouseMaterializedViewChange(view, caps)
		if err != nil {
			return nil, err
		}
		result = appendMaterializedViewReplacementDrop(result, view.ViewName, caps)
		objects = append(objects, object)
		nodes[identityOf(object)] = node
	}

	for _, object := range deporder.ViewLikesForCreateForDialect(objects, platform.ClickHouse) {
		result = append(result, nodes[identityOf(object)])
	}
	for _, view := range diff.ViewsRemoved {
		if slices.Contains(replacedViews, view.Name) {
			continue
		}
		result = append(result, ast.NewDropView(view.Name).SetIfExists())
	}
	for _, view := range diff.MaterializedViewsRemoved {
		if slices.Contains(replacedMaterializedViews, view.Name) {
			continue
		}
		result = append(result, ast.NewDropMaterializedView(view.Name).SetIfExists())
	}
	return result, nil
}

// crossKindReplacements returns the removed names the other kind's additions
// claim, in the order the removals were reported.
//
// The two sides come from different schemas and do not have to spell the name
// the same way — the database reader qualifies what a declaration leaves bare —
// so membership is decided by objectlookup's identity rule rather than by `==`,
// which is the mistake objectlookup exists to stop. The returned names are the
// removal list's own elements, so the caller can skip them from that list by
// exact string.
func crossKindReplacements(removed, added []string, semantics identifier.Semantics) []string {
	if len(removed) == 0 || len(added) == 0 {
		return nil
	}
	replaced := make([]string, 0, len(removed))
	for _, name := range removed {
		if objectlookup.Contains(added, name, semantics) {
			replaced = append(replaced, name)
		}
	}
	return replaced
}

// appendMaterializedViewReplacementDrop writes the drop half of a replacement
// only where the create half will be a real statement. A target whose
// capability set declines materialized views renders both halves as
// diagnostics, and naming the same object twice would say the plan does two
// things to it.
func appendMaterializedViewReplacementDrop(
	result []ast.Node,
	name string,
	caps capability.Capabilities,
) []ast.Node {
	if !caps.Has(capability.MaterializedViews) {
		return result
	}
	return append(result, ast.NewDropMaterializedView(name).SetIfExists())
}

// clickHouseViewChangeFor is clickHouseViewChange for a change that carries the
// view, which is every addition (stokaro/ptah#2315).
//
// It returns no error because there is nothing left to fail on: the missing
// declaration the name-taking form reports is a lookup that no longer happens.
func clickHouseViewChangeFor(
	view schemamodel.View,
	caps capability.Capabilities,
) (deporder.ViewLike, *ast.CreateViewNode) {
	if !caps.Has(capability.Views) {
		return deporder.ViewLike{Name: view.Name}, ast.NewCreateView(view.Name)
	}
	node := modelast.FromView(view)
	return deporder.ViewLike{Name: node.Name, Body: node.Body}, node
}

// clickHouseMaterializedViewChange is clickHouseViewChange for the materialized
// kind; the Materialized flag is what keeps the two apart in the shared
// dependency order and in the node map.
func clickHouseMaterializedViewChange(
	change difftypes.MaterializedViewDiff,
	caps capability.Capabilities,
) (deporder.ViewLike, *ast.CreateMaterializedViewNode, error) {
	name := change.ViewName
	// The capability check comes first, and it stays first: a target that has
	// no materialized views renders a placeholder from the name alone, so it
	// must not be refused for carrying no declaration it would never read.
	if !caps.Has(capability.MaterializedViews) {
		return deporder.ViewLike{Name: name, Materialized: true},
			ast.NewCreateMaterializedView(name),
			nil
	}
	if change.Desired.Name == "" {
		return deporder.ViewLike{}, nil, fmt.Errorf(
			"%w: ClickHouse materialized view %q named by diff carries no declaration to recreate it from",
			ptaherr.ErrInvalidSchemaDiff,
			name,
		)
	}
	object, node := clickHouseMaterializedViewChangeFor(change.Desired, caps)
	return object, node, nil
}

// clickHouseMaterializedViewChangeFor is clickHouseMaterializedViewChange for a
// change that carries the view, which is every addition (stokaro/ptah#2315).
//
// It returns no error for the reason [clickHouseViewChangeFor] returns none:
// the missing declaration the name-taking form reports is a lookup that no
// longer happens.
func clickHouseMaterializedViewChangeFor(
	view schemamodel.MaterializedView,
	caps capability.Capabilities,
) (deporder.ViewLike, *ast.CreateMaterializedViewNode) {
	if !caps.Has(capability.MaterializedViews) {
		return deporder.ViewLike{Name: view.Name, Materialized: true},
			ast.NewCreateMaterializedView(view.Name)
	}
	node := modelast.FromMaterializedView(view)
	return deporder.ViewLike{Name: node.Name, Body: node.Body, Materialized: true}, node
}

func reportRowLevelSecurity(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	caps capability.Capabilities,
) []ast.Node {
	if caps.Has(capability.RowLevelSecurity) {
		// planRowPolicies emits the real DDL for this target. Reporting here as
		// well would put a skip comment beside the statement it says was
		// skipped.
		return result
	}
	for _, table := range diff.RLSEnabledTablesAdded.Names() {
		result = append(result, ast.NewAlterTableEnableRLS(table))
	}
	for _, table := range diff.RLSEnabledTablesRemoved.Names() {
		result = append(result, ast.NewAlterTableDisableRLS(table))
	}
	for _, policy := range diff.RLSPoliciesAdded {
		result = append(result, ast.NewCreatePolicy(policy.PolicyName, policy.TableName))
	}
	for _, policy := range diff.RLSPoliciesModified {
		result = append(result, ast.NewCreatePolicy(policy.PolicyName, policy.TableName))
	}
	for _, policy := range diff.RLSPoliciesRemoved {
		result = append(result, ast.NewDropPolicy(policy.PolicyName, policy.TableName))
	}
	return result
}

func reportTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, trigger := range diff.TriggersAdded {
		result = append(result, ast.NewCreateTrigger(trigger.TriggerName, trigger.TableName))
	}
	for _, trigger := range diff.TriggersModified {
		result = append(result, ast.NewCreateTrigger(trigger.TriggerName, trigger.TableName))
	}
	for _, trigger := range diff.TriggersRemoved {
		result = append(result, ast.NewDropTrigger(trigger.TriggerName, trigger.TableName))
	}
	return result
}

// clickHouseRefreshAlter returns the ALTER that changes a materialized view's
// refresh schedule in place, or nil when this change cannot be made that way.
//
// The distinction is measured, not assumed. `ALTER TABLE <view> MODIFY REFRESH
// ...` changes the schedule of an already-refreshable view and keeps every row
// it accumulated; the same statement against a PLAIN materialized view is
// answered `Code: 48 ... Alter of type 'MODIFY_REFRESH' is not supported by
// storage MaterializedView`. So a view gaining its first schedule, or losing
// its last, has to be dropped and recreated -- and the drop takes the rows,
// which is why the in-place path is worth having at all (stokaro/ptah#1802).
//
// A change that also touches the body falls through for the same reason: the
// body is what a drop and a create exist to replace.
func clickHouseRefreshAlter(view difftypes.MaterializedViewDiff) ast.Node {
	change := view.RefreshChange
	if change == nil || change.Desired == nil || change.Current == nil {
		return nil
	}
	if len(view.Changes) != 1 {
		return nil
	}
	if _, only := view.Changes["refresh"]; !only {
		return nil
	}
	return ast.NewAlterMaterializedViewRefresh(view.ViewName, change.Desired.Clone())
}
