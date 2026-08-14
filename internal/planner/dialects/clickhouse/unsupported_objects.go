package clickhouse

import (
	"fmt"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// reportUnsupportedObjectsBeforeTables and reportUnsupportedObjectsAfterTables
// append the shared schema-object AST nodes in the same order as offline
// rendering. The ClickHouse renderer turns supported plain views into DDL and
// unsupported shapes into named diagnostics.
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
// their bodies through fromschema, and share one deporder.ViewLikesForCreate
// pass so dependencies precede the objects that read them. Diagnostic comments
// are stripped before execution by atlasschema.SplitApplyStatements.
func reportUnsupportedObjectsBeforeTables(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	result = reportExtensions(result, diff)
	result = reportSequences(result, diff)
	return result
}

func reportUnsupportedObjectsAfterTables(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
	caps capability.Capabilities,
) ([]ast.Node, error) {
	result = reportRoles(result, diff)
	result = reportFunctions(result, diff)
	var err error
	result, err = reportViewLikes(result, diff, generated, caps)
	if err != nil {
		return nil, err
	}
	result = reportRowLevelSecurity(result, diff)
	result = reportGrants(result, diff)
	result = reportTriggers(result, diff)
	return result, nil
}

func reportExtensions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.ExtensionsAdded {
		result = append(result, ast.NewExtension(name))
	}
	for _, name := range diff.ExtensionsRemoved {
		result = append(result, ast.NewDropExtension(name))
	}
	for _, extension := range diff.ExtensionsModified {
		result = append(result, ast.NewExtension(extension.Name).SetSchema(extension.ToSchema))
	}
	return result
}

func reportSequences(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.SequencesAdded {
		result = append(result, ast.NewCreateSequence(name))
	}
	for _, sequence := range diff.SequencesModified {
		result = append(result, ast.NewAlterSequence(sequence.SequenceName))
	}
	for _, name := range diff.SequencesRemoved {
		result = append(result, ast.NewDropSequence(name))
	}
	return result
}

func reportRoles(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.RolesAdded {
		result = append(result, ast.NewCreateRole(name))
	}
	for _, role := range diff.RolesModified {
		result = append(result, ast.NewAlterRole(role.RoleName))
	}
	for _, name := range diff.RolesRemoved {
		result = append(result, ast.NewDropRole(name))
	}
	return result
}

func reportFunctions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.FunctionsAdded {
		result = append(result, ast.NewCreateFunction(name))
	}
	for _, function := range diff.FunctionsModified {
		result = append(result, ast.NewCreateFunction(function.FunctionName))
	}
	for _, name := range diff.FunctionsRemoved {
		result = append(result, ast.NewDropFunction(name))
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
// shape the PostgreSQL planner uses, because ClickHouse has no statement that
// edits the query of an existing materialized view in place. Those drops are
// emitted before every create so the object being replaced is gone first. A
// changed plain view needs none, because CREATE OR REPLACE VIEW is one
// statement.
func reportViewLikes(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
	caps capability.Capabilities,
) ([]ast.Node, error) {
	semantics := diff.EffectiveIdentifierSemantics(platform.ClickHouse)
	capacity := len(diff.ViewsAdded) + len(diff.ViewsModified) +
		len(diff.MaterializedViewsAdded) + len(diff.MaterializedViewsModified)
	objects := make([]deporder.ViewLike, 0, capacity)
	nodes := make(map[viewLikeIdentity]ast.Node, capacity)

	for _, name := range diff.ViewsAdded {
		object, node, err := clickHouseViewChange(generated, name, semantics, caps)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
		nodes[identityOf(object)] = node
	}
	for _, view := range diff.ViewsModified {
		object, node, err := clickHouseViewChange(generated, view.ViewName, semantics, caps)
		if err != nil {
			return nil, err
		}
		node.SetReplace()
		objects = append(objects, object)
		nodes[identityOf(object)] = node
	}
	for _, name := range diff.MaterializedViewsAdded {
		object, node, err := clickHouseMaterializedViewChange(generated, name, semantics, caps)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
		nodes[identityOf(object)] = node
	}
	for _, view := range diff.MaterializedViewsModified {
		object, node, err := clickHouseMaterializedViewChange(
			generated,
			view.ViewName,
			semantics,
			caps,
		)
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
	for _, name := range diff.ViewsRemoved {
		result = append(result, ast.NewDropView(name).SetIfExists())
	}
	for _, name := range diff.MaterializedViewsRemoved {
		result = append(result, ast.NewDropMaterializedView(name).SetIfExists())
	}
	return result, nil
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

// clickHouseViewChange builds the create node for one plain view.
//
// A capability set without Views yields a node carrying identity only, because
// that is all the renderer's diagnostic reads, and requiring a desired
// declaration for an object that will never be emitted would fail a plan that
// the render path completes.
func clickHouseViewChange(
	generated *goschema.Database,
	name string,
	semantics identifier.Semantics,
	caps capability.Capabilities,
) (deporder.ViewLike, *ast.CreateViewNode, error) {
	if !caps.Has(capability.Views) {
		return deporder.ViewLike{Name: name}, ast.NewCreateView(name), nil
	}
	view := objectlookup.View(generated.Views, name, semantics)
	if view == nil {
		return deporder.ViewLike{}, nil, fmt.Errorf(
			"%w: ClickHouse view %q named by diff is missing from the desired schema",
			ptaherr.ErrInvalidSchemaDiff,
			name,
		)
	}
	node := fromschema.FromView(*view)
	return deporder.ViewLike{Name: node.Name, Body: node.Body}, node, nil
}

// clickHouseMaterializedViewChange is clickHouseViewChange for the materialized
// kind; the Materialized flag is what keeps the two apart in the shared
// dependency order and in the node map.
func clickHouseMaterializedViewChange(
	generated *goschema.Database,
	name string,
	semantics identifier.Semantics,
	caps capability.Capabilities,
) (deporder.ViewLike, *ast.CreateMaterializedViewNode, error) {
	if !caps.Has(capability.MaterializedViews) {
		return deporder.ViewLike{Name: name, Materialized: true},
			ast.NewCreateMaterializedView(name),
			nil
	}
	view := objectlookup.MaterializedView(generated.MaterializedViews, name, semantics)
	if view == nil {
		return deporder.ViewLike{}, nil, fmt.Errorf(
			"%w: ClickHouse materialized view %q named by diff is missing from the desired schema",
			ptaherr.ErrInvalidSchemaDiff,
			name,
		)
	}
	node := fromschema.FromMaterializedView(*view)
	return deporder.ViewLike{Name: node.Name, Body: node.Body, Materialized: true}, node, nil
}

func reportRowLevelSecurity(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, table := range diff.RLSEnabledTablesAdded {
		result = append(result, ast.NewAlterTableEnableRLS(table))
	}
	for _, table := range diff.RLSEnabledTablesRemoved {
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

func reportGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantsAdded {
		result = append(result, grantNode(grant))
	}
	for _, grant := range diff.GrantOptionsAdded {
		result = append(result, grantNode(grant))
	}
	for _, grant := range diff.GrantsRemoved {
		result = append(result, revokeNode(grant))
	}
	for _, grant := range diff.GrantOptionsRevoked {
		result = append(result, revokeNode(grant))
	}
	return result
}

func grantNode(grant types.GrantRef) ast.Node {
	return ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege})
}

func revokeNode(grant types.GrantRef) ast.Node {
	return ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege})
}

func reportTriggers(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
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
