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
// Both paths route the same nodes through the renderer. Plain views preserve
// their bodies and render as executable DDL; object kinds the ClickHouse model
// cannot represent safely retain their named not-supported diagnostics.
//
// The split around the table statements reproduces the order the converter uses
// for `render`, so the two surfaces agree line for line and not merely as sets.
//
// Unsupported nodes carry identity only because that is all their diagnostics
// read. Plain-view additions and replacements resolve the declaration, carry
// its body through fromschema.FromView, and share deporder.ViewLikesForCreate
// so dependencies precede the views that read them. Diagnostic comments are
// stripped before execution by atlasschema.SplitApplyStatements.
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
	result, err = reportViews(result, diff, generated, caps)
	if err != nil {
		return nil, err
	}
	result = reportMaterializedViews(result, diff)
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

func reportViews(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
	caps capability.Capabilities,
) ([]ast.Node, error) {
	semantics := diff.EffectiveIdentifierSemantics(platform.ClickHouse)
	if caps.Has(capability.Views) {
		var err error
		result, err = appendOrderedViewChanges(result, diff, generated, semantics)
		if err != nil {
			return nil, err
		}
	} else {
		result = appendViewChangeDiagnostics(result, diff)
	}
	for _, name := range diff.ViewsRemoved {
		result = append(result, ast.NewDropView(name).SetIfExists())
	}
	return result, nil
}

func appendViewChangeDiagnostics(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.ViewsAdded {
		result = append(result, ast.NewCreateView(name))
	}
	for _, view := range diff.ViewsModified {
		result = append(result, ast.NewCreateView(view.ViewName).SetReplace())
	}
	return result
}

func appendOrderedViewChanges(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) ([]ast.Node, error) {
	objects := make([]deporder.ViewLike, 0, len(diff.ViewsAdded)+len(diff.ViewsModified))
	nodes := make(map[string]*ast.CreateViewNode, cap(objects))
	for _, name := range diff.ViewsAdded {
		object, node, err := clickHouseViewChange(generated, name, semantics)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
		nodes[object.Name] = node
	}
	for _, view := range diff.ViewsModified {
		object, node, err := clickHouseViewChange(generated, view.ViewName, semantics)
		if err != nil {
			return nil, err
		}
		node.SetReplace()
		objects = append(objects, object)
		nodes[object.Name] = node
	}

	for _, object := range deporder.ViewLikesForCreateForDialect(objects, platform.ClickHouse) {
		result = append(result, nodes[object.Name])
	}
	return result, nil
}

func clickHouseViewChange(
	generated *goschema.Database,
	name string,
	semantics identifier.Semantics,
) (deporder.ViewLike, *ast.CreateViewNode, error) {
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

func reportMaterializedViews(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.MaterializedViewsAdded {
		result = append(result, ast.NewCreateMaterializedView(name))
	}
	for _, view := range diff.MaterializedViewsModified {
		result = append(result, ast.NewCreateMaterializedView(view.ViewName))
	}
	for _, name := range diff.MaterializedViewsRemoved {
		result = append(result, ast.NewDropMaterializedView(name))
	}
	return result
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
