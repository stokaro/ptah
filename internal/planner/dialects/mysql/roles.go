package mysql

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/modelast"
	"ptah.run/migration/schemadiff/difftypes"
)

// planRoles emits real role DDL for a target in this family that manages roles,
// and nothing for one that does not.
//
// Roles go before tables and grants go after them, because a grant names an
// object that has to exist and a role that has to exist. The removals mirror
// that: revoke, then drop the role, after the tables its grants named are gone.
//
// The capability is the switch, not the dialect name. A target that leaves
// RoleManagement off keeps the named skip reportUnsupportedRoutinesAndRoles
// writes; MySQL, MariaDB and SQL Server turn it on (stokaro/ptah#1698).
func (p *Planner) planRoles(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	// The attributes travel WITH the change, so this renders what it was
	// handed rather than looking the name back up (stokaro/ptah#2315).
	for _, role := range diff.RolesAdded {
		result = append(result, modelast.FromRole(role))
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
func (p *Planner) planGrants(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
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
func (p *Planner) removeGrantsAndRoles(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	ghosts, removed := p.splitGrantOptionGhosts(diff.GrantsRemoved)
	revokes := diff.GrantOptionsRevoked
	if len(ghosts) > 0 {
		revokes = append(slices.Clone(revokes), ghosts...)
	}
	for _, revoke := range p.grantOptionRevocations(revokes) {
		result = append(result, revoke)
	}
	for _, grant := range removed {
		result = append(result, ast.NewRevokePrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}))
	}
	for _, role := range diff.RolesRemoved {
		result = append(result, ast.NewDropRole(role.Name).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this role"))
	}
	return result
}

// usageGrantOptionGhost is the pseudo-privilege MySQL and MariaDB report for a
// grantee that holds a WITH GRANT OPTION row and no real privilege left on the
// object. information_schema.TABLE_PRIVILEGES and SCHEMA_PRIVILEGES, and SHOW
// GRANTS, name it USAGE -- the same placeholder they use for an account
// granted nothing at all -- because that is the whole of what the row reports.
const usageGrantOptionGhost = "USAGE"

// splitGrantOptionGhosts separates the GrantsRemoved entries that are really an
// orphaned grant option in disguise from the rest.
//
// Measured on MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3: revoking
// the last privilege a WITH GRANT OPTION grant covered leaves exactly this row
// behind -- USAGE, grantable -- and REVOKE USAGE ON db.t FROM r changes
// nothing, since USAGE names no privilege bit to clear. Left alone, the
// comparison reports the same row removed on every later run. REVOKE GRANT
// OPTION ON db.t FROM r, the statement [grantOptionRevocations] already
// renders for a kept privilege losing its option, removes it -- and is exactly
// as correct here, because a grantable USAGE row means the grant option is the
// only thing left on the object to take away.
//
// SQL Server and Oracle share this planner's type but not this quirk: neither
// reports a bare USAGE this way, so the split runs only where
// [grantOptionCoversTheObject] already says the grant option belongs to the
// object rather than to the privilege -- MySQL and MariaDB.
func (p *Planner) splitGrantOptionGhosts(refs []difftypes.GrantRef) (ghosts, rest []difftypes.GrantRef) {
	if !p.grantOptionCoversTheObject() {
		return nil, refs
	}
	for _, ref := range refs {
		if ref.WithOption && ref.Privilege == usageGrantOptionGhost {
			ghosts = append(ghosts, ref)
			continue
		}
		rest = append(rest, ref)
	}
	return ghosts, rest
}

// grantOptionRevocations turns the grant options a diff revokes into the
// statements that revoke them.
//
// On MySQL and MariaDB a grant option belongs to the grantee at one object, not
// to one privilege, and the statement that revokes it names no privilege. So
// every privilege that loses the option on one object becomes one node, which
// carries all of them. Two statements for the same object would fail the plan
// on MySQL: measured on 8.4.11 and 26.7.0, the second `REVOKE GRANT OPTION ON
// db.t FROM r` answers error 1147, there is no such grant defined. MariaDB
// 11.8.9 and 12.3.3 accept it.
//
// SQL Server and Oracle share this planner and keep one node per privilege.
func (p *Planner) grantOptionRevocations(refs []difftypes.GrantRef) []*ast.RevokePrivilegeNode {
	perObject := p.grantOptionCoversTheObject()
	nodes := make([]*ast.RevokePrivilegeNode, 0, len(refs))
	byObject := make(map[grantObject]*ast.RevokePrivilegeNode, len(refs))
	for _, grant := range refs {
		object := grantObject{
			role:       grant.Role,
			objectType: grant.ObjectType,
			objectName: grant.ObjectName,
			arguments:  grant.Arguments,
			column:     grant.Column,
		}
		if node, seen := byObject[object]; seen && perObject {
			node.Privileges = append(node.Privileges, grant.Privilege)
			continue
		}
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetGrantOptionFor(true)
		byObject[object] = node
		nodes = append(nodes, node)
	}
	return nodes
}

// grantObject is the grantee and object a grant option belongs to on MySQL and
// MariaDB.
type grantObject struct {
	role       string
	objectType string
	objectName string
	arguments  string
	column     string
}

// grantOptionCoversTheObject reports whether the target keeps one grant option
// per grantee and object rather than one per privilege.
//
// It is a question about the engines, not about Ptah, so it defers to
// [platform.GrantOptionIsPerObject] rather than keeping its own dialect
// switch: [schemamodel.ValidateGrantOptionConsistency] asks the same question
// before this planner ever sees a diff, and the two must agree on every
// target, or a declaration valid to one and refused by the other would be
// Ptah's own inconsistency, not the engine's.
func (p *Planner) grantOptionCoversTheObject() bool {
	return platform.GrantOptionIsPerObject(p.targetDialect())
}
