package ydb

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/modelast"
	"ptah.run/internal/ydbacl"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan changes the access model.
//
// Users and groups are created before the tables and changed in place with
// ALTER USER; memberships are added once both ends exist, and grants once
// their objects do. Every statement takes one change, so an interrupted plan
// resumes at the change that did not run. The order the phases take, around
// the table phases the package documentation gives:
//
//  1. REVOKE for every permission entry the plan removes, while its object
//     still exists; an entry on a table the plan drops goes with the table
//     and is not revoked, and a table the plan creates holds none;
//  2. ALTER GROUP ... DROP USER for every membership the plan removes;
//  3. CREATE USER and CREATE GROUP for every principal the plan adds, a group
//     empty, and ALTER USER for every user whose login or password changes;
//  4. the table phases;
//  5. ALTER GROUP ... ADD USER for every membership the plan adds;
//  6. GRANT for every permission entry the plan adds, and for every entry a
//     rebuilt table held, which the rebuild dropped with the old table;
//  7. DROP USER and DROP GROUP for every principal the plan removes, last:
//     both leave the principal's permission entries behind, so its revokes
//     run first.
//
// YDB keeps no statement that turns a user into a group, and a user carries
// no attribute but its password and whether it logs in, so any other change
// is refused before anything is emitted.

// accessPlan is the access model's part of a plan, split around the table
// phases.
type accessPlan struct {
	before []ast.Node
	after  []ast.Node
	last   []ast.Node
}

// planAccess plans the access model's changes. removedTables and addedTables
// are the identity keys of the tables the plan drops and creates, and
// rebuilt those of the tables it recreates.
func (p *Planner) planAccess(
	diff *difftypes.SchemaDiff,
	removedTables, addedTables map[string]bool,
	rebuilt []string,
	semantics identifier.Semantics,
) (accessPlan, error) {
	if err := p.refuseAccessChanges(diff); err != nil {
		return accessPlan{}, err
	}
	var plan accessPlan
	for _, grant := range diff.GrantsRemoved {
		key, onTable := grantTableKey(grant, semantics)
		if onTable && (removedTables[key] || addedTables[key]) {
			continue
		}
		node, err := p.revokeNode(grant, diff.CurrentDatabasePath)
		if err != nil {
			return accessPlan{}, err
		}
		plan.before = append(plan.before, node)
	}
	for _, membership := range diff.RoleMembershipsRemoved {
		plan.before = append(plan.before, ast.NewRevokeRoleMembership(membership.Role, membership.Member))
	}
	for _, role := range diff.RolesAdded {
		plan.before = append(plan.before, modelast.FromRole(role))
	}
	for _, change := range diff.RolesModified {
		node, err := alterUser(change)
		if err != nil {
			return accessPlan{}, err
		}
		if node != nil {
			plan.before = append(plan.before, node)
		}
	}
	for _, membership := range diff.RoleMembershipsAdded {
		plan.after = append(plan.after, ast.NewGrantRoleMembership(membership.Role, membership.Member))
	}
	for _, grant := range slices.Concat(diff.GrantsAdded, regrants(diff, rebuilt, semantics)) {
		node, err := p.grantNode(grant, diff.CurrentDatabasePath)
		if err != nil {
			return accessPlan{}, err
		}
		plan.after = append(plan.after, node)
	}
	for _, role := range diff.RolesRemoved {
		plan.last = append(plan.last, ast.NewDropRole(role.Name).SetIfExists().SetGroup(role.Group))
	}
	return plan, nil
}

// refuseAccessChanges refuses, before anything is emitted, an access change
// YDB has no statement for.
func (p *Planner) refuseAccessChanges(diff *difftypes.SchemaDiff) error {
	changes := len(diff.RolesAdded) + len(diff.RolesRemoved) + len(diff.RolesModified) +
		len(diff.RoleMembershipsAdded) + len(diff.RoleMembershipsRemoved) +
		len(diff.GrantsAdded) + len(diff.GrantsRemoved)
	if changes > 0 && !p.caps.Has(capability.RoleManagement) {
		return refuseKey(capability.RoleManagement, "the plan changes a user, a group or a permission")
	}
	if len(diff.RoleMembershipsAdded)+len(diff.RoleMembershipsRemoved) > 0 && !p.caps.Has(capability.RoleMembership) {
		return refuseKey(capability.RoleMembership, "the plan changes the members of a group")
	}
	for _, role := range slices.Concat(diff.RolesAdded, diff.RolesRemoved) {
		if role.Group && !p.caps.Has(capability.GroupPrincipals) {
			return refuseKey(capability.GroupPrincipals, "the plan creates or drops group "+role.Name)
		}
	}
	if len(diff.GrantOptionsAdded)+len(diff.GrantOptionsRevoked) > 0 {
		return refuseFact("the plan changes a grant option", "YDB records WITH GRANT OPTION as the permission "+
			ydbacl.GrantPermission+" beside the one it is written with, so a grant option is not a change YDB can make")
	}
	if len(diff.DefaultPrivilegesAdded)+len(diff.DefaultPrivilegesRemoved)+
		len(diff.DefaultPrivilegeOptionsAdded)+len(diff.DefaultPrivilegeOptionsRevoked) > 0 {
		return refuseFact("the plan changes a default privilege", "YDB has no default privileges; "+
			"a permission granted on a directory is inherited by every object created in it")
	}
	for _, grant := range slices.Concat(diff.GrantsAdded, diff.GrantsRemoved) {
		if strings.EqualFold(grant.ObjectType, ydbacl.ObjectDatabase) && !p.caps.Has(capability.DatabaseGrants) {
			return refuseKey(capability.DatabaseGrants, "the plan changes a permission on the database")
		}
	}
	return nil
}

// alterUser is the ALTER USER a role change asks for, nil for none, or the
// refusal of a change YDB cannot make in place.
func alterUser(change difftypes.RoleDiff) (*ast.AlterRoleNode, error) {
	subject := "role " + change.RoleName
	node := ast.NewAlterRole(change.RoleName)
	for _, attribute := range slices.Sorted(maps.Keys(change.Changes)) {
		value := change.Changes[attribute]
		switch attribute {
		case "login":
			node.AddOperation(ast.NewSetLoginOperation(strings.HasSuffix(value, "-> true")))
		case "password":
			if change.Desired.Password != "" {
				node.AddOperation(ast.NewSetPasswordOperation(change.Desired.Password))
			}
		case "group":
			return nil, refuseFact(subject, "YDB has no statement that turns a user into a group or a "+
				"group into a user; drop the principal and declare it again")
		default:
			return nil, refuseFact(subject, fmt.Sprintf("its %s changes (%s), which a YDB user or group "+
				"does not carry", attribute, value))
		}
	}
	if len(node.Operations) == 0 {
		return nil, nil
	}
	if change.Desired.Group {
		return nil, refuseFact(subject, "a YDB group never logs in and has no password")
	}
	return node, nil
}

// grantNode is the GRANT of one permission entry, naming its object by the
// path YDB resolves.
func (p *Planner) grantNode(grant difftypes.GrantRef, databasePath string) (*ast.GrantPrivilegeNode, error) {
	objectName, err := p.grantObjectName(grant, databasePath, "granting")
	if err != nil {
		return nil, err
	}
	return ast.NewGrantPrivilege(grant.Role, grant.ObjectType, objectName, []string{grant.Privilege}).
		SetColumns(columnsOf(grant)).
		SetArguments(grant.Arguments), nil
}

// revokeNode is the REVOKE of one permission entry. Measured, it removes the
// entry with exactly that permission and no other.
func (p *Planner) revokeNode(grant difftypes.GrantRef, databasePath string) (*ast.RevokePrivilegeNode, error) {
	objectName, err := p.grantObjectName(grant, databasePath, "revoking")
	if err != nil {
		return nil, err
	}
	return ast.NewRevokePrivilege(grant.Role, grant.ObjectType, objectName, []string{grant.Privilege}).
		SetColumns(columnsOf(grant)).
		SetArguments(grant.Arguments), nil
}

// grantObjectName is how a node names a grant's object: as the grant does,
// for the renderer to write relative to the database root, or by the absolute
// path under databasePath where the line resolves no relative one. A plan
// with no database path, compared with no live read, refuses an object only
// the absolute path reaches.
func (p *Planner) grantObjectName(grant difftypes.GrantRef, databasePath, verb string) (string, error) {
	path, err := ydbacl.StatementPath(grant.ObjectType, grant.ObjectName, databasePath, p.caps)
	subject := fmt.Sprintf("%s %s on %s to %s", verb, grant.Privilege,
		ydbacl.Describe(grant.ObjectType, grant.ObjectName), grant.Role)
	switch {
	case errors.Is(err, ydbacl.ErrNeedsDatabasePath):
		return "", refuseFact(subject, err.Error()+", and this plan was compared with no live database to take it from")
	case err != nil:
		return "", refuseFact(subject, err.Error())
	case strings.HasPrefix(path, "/"):
		return path, nil
	default:
		return grant.ObjectName, nil
	}
}

// columnsOf is the column a grant is limited to, as the list a node carries.
// YDB has none, and the renderer refuses one.
func columnsOf(grant difftypes.GrantRef) []string {
	if grant.Column == "" {
		return nil
	}
	return []string{grant.Column}
}

// grantTableKey is the identity of the table a grant is on, and whether it is
// on a table at all.
func grantTableKey(grant difftypes.GrantRef, semantics identifier.Semantics) (string, bool) {
	if !strings.EqualFold(strings.TrimSpace(grant.ObjectType), ydbacl.ObjectTable) {
		return "", false
	}
	return semantics.TableIdentityKey(grant.ObjectName), true
}

// regrants are the permission entries each rebuilt table held, which the plan
// grants again on the table that replaces it: the entries live in the old
// table's own access list, and the rebuild drops the old table. An entry the
// plan removes or grants anyway is left out.
func regrants(diff *difftypes.SchemaDiff, rebuilt []string, semantics identifier.Semantics) []difftypes.GrantRef {
	if len(rebuilt) == 0 {
		return nil
	}
	tables := make(map[string]bool, len(rebuilt))
	for _, key := range rebuilt {
		tables[key] = true
	}
	planned := make(map[string]bool, len(diff.GrantsRemoved)+len(diff.GrantsAdded))
	for _, grant := range slices.Concat(diff.GrantsRemoved, diff.GrantsAdded) {
		planned[grantIdentity(grant, semantics)] = true
	}
	var again []difftypes.GrantRef
	for _, grant := range diff.CurrentGrants {
		key, onTable := grantTableKey(grant, semantics)
		if !onTable || !tables[key] || planned[grantIdentity(grant, semantics)] {
			continue
		}
		again = append(again, grant)
	}
	return again
}

// grantIdentity is what makes two references one permission entry: the
// subject, the permission and the object.
func grantIdentity(grant difftypes.GrantRef, semantics identifier.Semantics) string {
	object := grant.ObjectName
	if key, onTable := grantTableKey(grant, semantics); onTable {
		object = key
	}
	return strings.Join([]string{grant.Role, strings.ToLower(grant.Privilege), strings.ToUpper(grant.ObjectType), object}, "\x00")
}
