package mysql

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// planRoles emits real role DDL for a target in this family that manages roles,
// and nothing for one that does not.
//
// Roles go before tables and grants go after them, because a grant names an
// object that has to exist and a role that has to exist. The removals mirror
// that: revoke, then drop the role, after the tables its grants named are gone.
//
// The capability is the switch, not the dialect name. MySQL and MariaDB leave
// RoleManagement off and keep the named skip reportUnsupportedRoutinesAndRoles
// writes; SQL Server turns it on (stokaro/ptah#1698).
func (p *Planner) planRoles(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	declared := make(map[string]goschema.Role)
	for _, role := range generated.Roles {
		declared[role.Name] = role
	}
	for _, name := range diff.RolesAdded {
		role, found := declared[name]
		if !found {
			continue
		}
		result = append(result, fromschema.FromRole(role))
	}
	for _, roleDiff := range diff.RolesModified {
		// A database role has no attributes to alter, and the renderer says so
		// by name. The node is still emitted so the plan reports the intent
		// rather than dropping it.
		result = append(result, ast.NewAlterRole(roleDiff.RoleName))
	}
	return result
}

// planGrants emits the GRANT statements, including the ones that only add the
// grant option.
func (p *Planner) planGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	for _, grant := range diff.GrantsAdded {
		result = append(result, ast.NewGrantPrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(grant.WithOption))
	}
	for _, grant := range diff.GrantOptionsAdded {
		result = append(result, ast.NewGrantPrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(true))
	}
	return result
}

// removeGrantsAndRoles revokes first and drops the roles afterwards.
//
// A role still holding permissions is not droppable on any engine in this
// family that has roles at all, so the order is the engine's rather than a
// preference.
func (p *Planner) removeGrantsAndRoles(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	for _, grant := range diff.GrantOptionsRevoked {
		result = append(result, ast.NewRevokePrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetGrantOptionFor(true))
	}
	for _, grant := range diff.GrantsRemoved {
		result = append(result, ast.NewRevokePrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}))
	}
	for _, name := range diff.RolesRemoved {
		result = append(result, ast.NewDropRole(name).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this role"))
	}
	return result
}
