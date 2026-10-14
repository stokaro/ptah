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
//     and is not revoked, and a table the plan creates holds none. The same
//     holds for a view and for a scheme object an owner manages, such as a
//     topic: an entry on one the plan drops, or drops and creates again,
//     goes with it;
//  2. ALTER GROUP ... DROP USER for every membership the plan removes;
//  3. CREATE USER and CREATE GROUP for every principal the plan adds, a group
//     empty, and ALTER USER for every user whose login or password changes;
//  4. the table phases;
//  5. ALTER GROUP ... ADD USER for every membership the plan adds;
//  6. GRANT for every permission entry the plan adds, and for every entry a
//     rebuilt table held, which the rebuild dropped with the old table, and
//     every entry a view or a scheme object the plan drops and creates again
//     held. A
//     GRANT or REVOKE on a scheme object an owner manages is ordered against
//     the owner's statements too, after the object's creation or change and
//     before its drop (see [permissionEdges]);
//  7. DROP USER and DROP GROUP for every principal the plan removes, last:
//     both leave the principal's permission entries behind, so its revokes
//     run first.
//
// A GRANT on a streaming query the plan creates or changes goes after all of
// them, at the end of the plan: the streaming query owner places those
// statements after every other one, so the query reads the schema the plan
// leaves, and a GRANT in the phases above would be ordered before the query
// it names.
//
// YDB keeps no statement that turns a user into a group, and a user carries
// no attribute but its password and whether it logs in, so any other change
// is refused before anything is emitted.

// accessPlan is the access model's part of a plan, split around the table
// phases, with the GRANTs that go after every statement the plan schedules.
type accessPlan struct {
	before []ast.Node
	after  []ast.Node
	last   []ast.Node
	tail   []ast.Node
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
	objects := ownedObjectLifecycle(diff, p.caps)
	views := viewsOf(diff, p.caps, semantics)
	if err := refuseGrantsOnDroppedObjects(diff, objects, views, semantics); err != nil {
		return accessPlan{}, err
	}
	var plan accessPlan
	for _, grant := range diff.GrantsRemoved {
		key, onTable := grantTableKey(grant, semantics)
		if onTable && (removedTables[key] || addedTables[key] || views.dropped[key] || views.recreated[key]) {
			continue
		}
		if objects.revokedWithObject(grant, diff.CurrentDatabasePath) {
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
	if err := p.planGrants(&plan, diff, slices.Concat(rebuilt, views.recreatedKeys()), objects, semantics); err != nil {
		return accessPlan{}, err
	}
	for _, role := range diff.RolesRemoved {
		plan.last = append(plan.last, ast.NewDropRole(role.Name).SetIfExists().SetGroup(role.Group))
	}
	return plan, nil
}

// planGrants adds to plan a GRANT for every permission entry the plan adds,
// for every entry a table or a view the plan drops to create again held, and
// for every entry a scheme object it drops to create again held. rebuilt are
// the identity keys of those tables and views. A GRANT whose object an owner
// places after every other statement goes to the plan's tail.
func (p *Planner) planGrants(
	plan *accessPlan, diff *difftypes.SchemaDiff, rebuilt []string, objects objectLifecycle, semantics identifier.Semantics,
) error {
	for _, grant := range slices.Concat(diff.GrantsAdded, regrants(diff, rebuilt, semantics), objects.regrants(diff, diff.CurrentDatabasePath)) {
		node, err := p.grantNode(grant, diff.CurrentDatabasePath)
		if err != nil {
			return err
		}
		if objects.grantsLast(grant, diff.CurrentDatabasePath) {
			plan.tail = append(plan.tail, node)
			continue
		}
		plan.after = append(plan.after, node)
	}
	return nil
}

// refuseGrantsOnDroppedObjects refuses a grant on a view or a scheme object
// the plan drops, which no GRANT can reach.
func refuseGrantsOnDroppedObjects(
	diff *difftypes.SchemaDiff, objects objectLifecycle, views viewLifecycle, semantics identifier.Semantics,
) error {
	if grant, dropped := objects.grantedOnDroppedObject(diff.GrantsAdded, diff.CurrentDatabasePath); dropped {
		return refuseFact(fmt.Sprintf("granting %s on %s to %s", grant.Privilege,
			ydbacl.Describe(grant.ObjectType, grant.ObjectName), grant.Role),
			"the plan drops the object; declare it, or leave the grant out")
	}
	if grant, dropped := views.grantedOnDroppedView(diff.GrantsAdded, semantics); dropped {
		return refuseFact(fmt.Sprintf("granting %s on view %s to %s", grant.Privilege, grant.ObjectName, grant.Role),
			"the plan drops the view; declare it, or leave the grant out")
	}
	return nil
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
	for _, grant := range slices.Concat(diff.GrantsAdded, diff.GrantsRemoved) {
		if strings.EqualFold(grant.ObjectType, ydbacl.ObjectDatabase) && !p.caps.Has(capability.DatabaseGrants) {
			return refuseKey(capability.DatabaseGrants, "the plan changes a permission on the database")
		}
		if key, owned := ydbacl.ObjectCapability(grant.ObjectType); owned && !p.caps.Has(key) {
			return refuseKey(key, "the plan changes a permission on "+ydbacl.Describe(grant.ObjectType, grant.ObjectName))
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

// regrants are the permission entries each rebuilt table and each view the
// plan drops and creates again held, which the plan grants again on the object
// that replaces it: the entries live in the old object's own access list, and
// the plan drops the old object. rebuilt are the identity keys of both. An
// entry the plan removes or grants anyway is left out.
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

// viewLifecycle is what a plan does to each view, by identity key: the views
// it drops, and the ones it drops to create again, which on a target without
// [capability.CreateOrReplaceView] is every view whose query changed. A
// view's permission entries live in its own access list, so either takes them
// with it, as a DROP TABLE takes a table's; measured on 25.1.4.7 and
// 26.2.1.14, a view created again holds no entry of its own.
type viewLifecycle struct {
	dropped, recreated map[string]bool
}

// viewsOf reads the views diff drops and drops to create again, on a target
// with caps.
func viewsOf(diff *difftypes.SchemaDiff, caps capability.Capabilities, semantics identifier.Semantics) viewLifecycle {
	views := viewLifecycle{dropped: make(map[string]bool), recreated: make(map[string]bool)}
	for _, view := range diff.ViewsRemoved {
		views.dropped[semantics.TableIdentityKey(view.Name)] = true
	}
	if !caps.Has(capability.CreateOrReplaceView) {
		for _, change := range diff.ViewsModified {
			views.recreated[semantics.TableIdentityKey(change.ViewName)] = true
		}
	}
	return views
}

// recreatedKeys are the identity keys of the views the plan drops to create
// again, sorted.
func (v viewLifecycle) recreatedKeys() []string {
	return slices.Sorted(maps.Keys(v.recreated))
}

// grantedOnDroppedView is the first grant of grants on a view the plan drops,
// which no GRANT can reach.
func (v viewLifecycle) grantedOnDroppedView(grants []difftypes.GrantRef, semantics identifier.Semantics) (difftypes.GrantRef, bool) {
	for _, grant := range grants {
		if key, onTable := grantTableKey(grant, semantics); onTable && v.dropped[key] {
			return grant, true
		}
	}
	return difftypes.GrantRef{}, false
}
