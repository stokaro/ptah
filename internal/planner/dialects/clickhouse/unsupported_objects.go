package clickhouse

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// reportUnsupportedObjectsBeforeTables and reportUnsupportedObjectsAfterTables
// append one AST node for every object kind the diff carries that ClickHouse
// cannot host, so the ClickHouse renderer's own notSupported() diagnostic runs
// over each one and the plan names what it will not carry.
//
// `ptah schema render --dialect clickhouse` names all of these: the offline
// converter routes the nodes to the renderer, which turns each into
// `-- CLICKHOUSE: <what> "<name>" is not supported`. The plan path had no such
// route, so the two surfaces disagreed. Measured on live ClickHouse 24.8 with a
// fixture declaring an extension, a sequence, a role, a function, a view, a
// materialized view and a trigger, `schema apply --dry-run` planned the one
// CREATE TABLE and said nothing about the other seven; a fixture declaring
// nothing but a view answered "Schema is synced, no changes to be made." against
// an empty database, which is an affirmative false report rather than mere
// under-generation (stokaro/ptah#931 item 7).
//
// The split around the table statements reproduces the order the converter uses
// for `render`, so the two surfaces agree line for line and not merely as sets.
//
// The nodes carry the object's identity and nothing more, because identity is
// all the ClickHouse renderer reads for these kinds -- it renders DDL for none
// of them. The resulting comments are stripped before execution by
// atlasschema.SplitApplyStatements, so the plan reports what it cannot carry
// without sending anything to the server.
func reportUnsupportedObjectsBeforeTables(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	result = reportExtensions(result, diff)
	result = reportSequences(result, diff)
	return result
}

func reportUnsupportedObjectsAfterTables(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	result = reportRoles(result, diff)
	result = reportFunctions(result, diff)
	result = reportViews(result, diff)
	result = reportMaterializedViews(result, diff)
	result = reportRowLevelSecurity(result, diff)
	result = reportGrants(result, diff)
	result = reportTriggers(result, diff)
	return result
}

func reportExtensions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.ExtensionsAdded {
		result = append(result, ast.NewExtension(name))
	}
	for _, name := range diff.ExtensionsRemoved {
		result = append(result, ast.NewDropExtension(name))
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

func reportViews(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.ViewsAdded {
		result = append(result, ast.NewCreateView(name))
	}
	for _, view := range diff.ViewsModified {
		result = append(result, ast.NewCreateView(view.ViewName))
	}
	for _, name := range diff.ViewsRemoved {
		result = append(result, ast.NewDropView(name))
	}
	return result
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
