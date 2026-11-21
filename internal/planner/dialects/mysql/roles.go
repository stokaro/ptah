package mysql

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/rolemembership"
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
	return p.planRoleMemberships(result, diff)
}

// planRoleMemberships removes the memberships the declaration dropped, then
// adds the declared ones and changes the options of the ones both sides hold.
// MySQL has no statement that revokes the admin option alone, so a membership
// that loses it is revoked and granted again.
func (p *Planner) planRoleMemberships(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleMembership) {
		return result
	}
	result = append(result, rolemembership.Removals(diff)...)
	return append(result, rolemembership.Additions(diff, p.capabilities(), rolemembership.RevokeAndGrant)...)
}

// planGrants emits the GRANT statements, including the ones that only add the
// grant option.
//
// A grant limited to a column carries it to the renderer, which spells it or
// refuses it by name. A node without it is a grant on the whole table: the
// role could read every column, the one the declaration left out included, and
// the next comparison would revoke the table grant and plan it again.
//
// On MySQL and MariaDB a revoke reaches further than the row it names, so the
// revokes run before the grants, and the column privileges a revoke takes
// with it are granted again after them. Measured on MySQL 8.4.13 and MariaDB
// 11.8.9: `REVOKE SELECT ON t FROM r` takes every column SELECT r holds on t,
// and `REVOKE GRANT OPTION ON t FROM r` takes the grant option of every
// privilege r holds on t. A plan that narrows `GRANT SELECT ON t` to
// `GRANT SELECT (name) ON t` and runs the GRANT first leaves the role with
// nothing on t, and the next plan grants the column again.
func (p *Planner) planGrants(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	perObject := p.grantOptionCoversTheObject()
	if perObject {
		result = p.revokeGrants(result, diff)
	}
	for _, grant := range diff.GrantsAdded {
		result = append(result, ast.NewGrantPrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(grantColumns(grant)).
			SetWithOption(grant.WithOption))
	}
	for _, grant := range diff.GrantOptionsAdded {
		result = append(result, ast.NewGrantPrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(grantColumns(grant)).
			SetWithOption(true))
	}
	if perObject {
		result = append(result, columnGrantsRevokedWithTheirTable(diff)...)
	}
	return result
}

// grantColumns is the column list of the statement a grant reference plans:
// its column, or none for a grant on the whole object.
func grantColumns(grant difftypes.GrantRef) []string {
	if grant.Column == "" {
		return nil
	}
	return []string{grant.Column}
}

// revokeGrants emits the REVOKE statements: the grant options a diff takes
// away, the orphaned grant options, then the privileges.
func (p *Planner) revokeGrants(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	ghosts, removed := p.splitGrantOptionGhosts(diff.GrantsRemoved)
	revokes := slices.Concat(diff.GrantOptionsRevoked, ghosts)
	if p.grantOptionCoversTheObject() {
		revokes = append(revokes, grantOptionsLeftBehind(revokes, removed, diff.CurrentGrants)...)
	}
	for _, revoke := range p.grantOptionRevocations(revokes) {
		result = append(result, revoke)
	}
	for _, grant := range removed {
		result = append(result, ast.NewRevokePrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(grantColumns(grant)))
	}
	return result
}

// grantOptionsLeftBehind is a grant option revocation for each object a
// revoked privilege carried the grant option on, where the role keeps no
// privilege with the option there and the diff revokes no option there.
//
// On MySQL and MariaDB the option is the grantee's at the object, and a
// privilege revoke leaves it: measured on MySQL 8.4.13 and MariaDB 11.8.9,
// after `REVOKE SELECT ON t FROM r` a role that held `SELECT ON t WITH GRANT
// OPTION` and `INSERT ON t` holds INSERT grantable, and a role that held
// SELECT alone holds a grantable USAGE row. Without the revocation an apply
// leaves the role able to grant what the declaration gives it without the
// option, until the next plan takes the option away. A privilege the plan
// grants with the option runs after this and has it again.
func grantOptionsLeftBehind(revokes, removed, current []difftypes.GrantRef) []difftypes.GrantRef {
	revoked := make(map[grantObject]bool, len(revokes))
	for _, grant := range revokes {
		revoked[grantObjectOf(grant)] = true
	}
	removedKeys := make(map[difftypes.GrantRef]bool, len(removed))
	for _, grant := range removed {
		removedKeys[columnGrantKey(grant)] = true
	}
	keepsOption := make(map[grantObject]bool)
	for _, grant := range current {
		if grant.WithOption && !removedKeys[columnGrantKey(grant)] {
			keepsOption[grantObjectOf(grant)] = true
		}
	}
	var left []difftypes.GrantRef
	for _, grant := range removed {
		object := grantObjectOf(grant)
		if !grant.WithOption || revoked[object] || keepsOption[object] {
			continue
		}
		revoked[object] = true
		left = append(left, grant)
	}
	return left
}

// grantObjectOf is the grantee and object a grant option belongs to on MySQL
// and MariaDB: a column privilege holds it at its table.
func grantObjectOf(grant difftypes.GrantRef) grantObject {
	return grantObject{
		role: grant.Role, objectType: grant.ObjectType, objectName: grant.ObjectName, arguments: grant.Arguments,
	}
}

// grantedPrivilege is a privilege a role holds on one object, as the read
// names them.
type grantedPrivilege struct {
	role, objectName, privilege string
}

// columnGrantsRevokedWithTheirTable is a GRANT for each column privilege the
// database holds and the diff keeps, where the diff revokes the same
// privilege on the whole table: the table revoke takes the column privilege
// with it, and without the GRANT the role loses a privilege the declaration
// keeps until the next plan grants it again. A REVOKE ALL takes every
// privilege. The grant option comes back with the privilege unless the diff
// takes the option away from the role on that table.
//
// The diff's removals and the current grants both name an object as the read
// does, so they compare exactly.
func columnGrantsRevokedWithTheirTable(diff *difftypes.SchemaDiff) []ast.Node {
	tableRevokes := make(map[grantedPrivilege]bool)
	columnRevokes := make(map[difftypes.GrantRef]bool)
	for _, grant := range diff.GrantsRemoved {
		switch {
		case grant.Column != "":
			columnRevokes[columnGrantKey(grant)] = true
		case strings.EqualFold(grant.ObjectType, "SCHEMA") || grant.Privilege == usageGrantOptionGhost:
		default:
			tableRevokes[grantedPrivilege{grant.Role, grant.ObjectName, grant.Privilege}] = true
		}
	}
	if len(tableRevokes) == 0 {
		return nil
	}
	optionRevoked := make(map[grantObject]bool)
	for _, grant := range diff.GrantOptionsRevoked {
		optionRevoked[grantObjectOf(grant)] = true
	}
	var nodes []ast.Node
	for _, grant := range diff.CurrentGrants {
		if grant.Column == "" || columnRevokes[columnGrantKey(grant)] {
			continue
		}
		revoked := tableRevokes[grantedPrivilege{grant.Role, grant.ObjectName, grant.Privilege}] ||
			tableRevokes[grantedPrivilege{grant.Role, grant.ObjectName, "ALL"}]
		if !revoked {
			continue
		}
		keepsOption := grant.WithOption && !optionRevoked[grantObjectOf(grant)]
		nodes = append(nodes, ast.NewGrantPrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(grantColumns(grant)).
			SetWithOption(keepsOption))
	}
	return nodes
}

// columnGrantKey is the identity of a column privilege, the grant option
// aside.
func columnGrantKey(grant difftypes.GrantRef) difftypes.GrantRef {
	return difftypes.GrantRef{
		Role: grant.Role, Privilege: grant.Privilege, ObjectType: grant.ObjectType,
		ObjectName: grant.ObjectName, Column: grant.Column,
	}
}

// removeGrantsAndRoles revokes first and drops the roles afterwards. On
// MySQL and MariaDB [Planner.planGrants] runs the revokes, before the grants.
//
// A role still holding permissions is not droppable on any engine in this
// family that has roles at all, so the order is the engine's rather than a
// preference.
func (p *Planner) removeGrantsAndRoles(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	if !p.grantOptionCoversTheObject() {
		result = p.revokeGrants(result, diff)
	}
	for _, role := range diff.RolesRemoved {
		drop := ast.NewDropRole(role.Name).SetIfExists()
		if p.describes() {
			drop.SetComment("WARNING: Ensure no other objects depend on this role")
		}
		result = append(result, drop)
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
		}
		columns := grantColumns(grant)
		// Where the option covers the object, a column privilege holds it at
		// its table too: measured on MySQL 8.4.13 and MariaDB 11.8.9,
		// `GRANT INSERT (a) ON t TO r WITH GRANT OPTION` makes every column
		// privilege r holds on t grantable, and `REVOKE GRANT OPTION ON t`
		// takes it from all of them. One statement per table is the whole
		// revoke, and on MySQL a second one fails with ERROR 1147.
		if perObject {
			columns = nil
		} else {
			object.column = grant.Column
		}
		if node, seen := byObject[object]; seen && perObject {
			node.Privileges = append(node.Privileges, grant.Privilege)
			continue
		}
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(columns).
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
