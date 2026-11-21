package mysql

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/rolemembership"
)

// planRoles emits real role DDL for a target in this family that manages roles,
// and nothing for one that does not.
//
// Roles go before tables and grants go after them, because a grant names an
// object that has to exist and a role that has to exist. The removals mirror
// that: revoke, then drop the role, after the tables its grants named are gone.
// On MySQL, MariaDB and SQL Server the revokes go first of all instead; see
// [Planner.planRevokesFirst].
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
// On MySQL, MariaDB and SQL Server the revokes have run by now, at the start
// of the plan, and so have the grants that give back the column privileges a
// revoke on their table takes, where the column exists, and the grants that
// give a privilege the revokes take back under the declared spelling (see
// [Planner.planRevokesFirst]). The ones on a column the plan creates come back
// here, first, after the tables, views and columns. A column is named as
// [columnSpellings] says.
func (p *Planner) planGrants(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	if p.revokesFirst() {
		_, later := p.columnGrantsGivenBack(diff)
		result = append(result, later...)
	}
	_, added := p.grantsAdded(diff)
	result = p.grantPrivileges(result, diff, added)
	spellings := newColumnSpellings(diff, p.targetDialect())
	for _, grant := range diff.GrantOptionsAdded {
		result = append(result, ast.NewGrantPrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(spellings.columns(grant)).
			SetWithOption(true))
	}
	return result
}

// planRevokesFirst emits the REVOKE statements at the start of the plan on
// MySQL, MariaDB and SQL Server, before any database, table or column is
// created.
//
// A revoke reaches further than the row it names there. Measured on MySQL
// 8.4.13 and MariaDB 11.8.9: `REVOKE SELECT ON t FROM r` takes every column
// SELECT r holds on t, and `REVOKE GRANT OPTION ON t FROM r` takes the grant
// option of every privilege r holds on t. A plan that narrows
// `GRANT SELECT ON t` to `GRANT SELECT (name) ON t` and runs the GRANT first
// leaves the role with nothing on t.
//
// And a privilege outlives its object. Both engines keep a table or column
// privilege after DROP TABLE and DROP COLUMN, and the privilege applies again
// to an object created under the same name. Revoked after the CREATE TABLE or
// ADD COLUMN that brings the name back, it is live in between, and a
// statement failing in between keeps it: measured on MariaDB 12.3.3, a role
// whose SELECT (secret) a failed apply left behind reads the secret column
// once a later apply adds it again. A REVOKE succeeds on a table or a column
// that does not exist, on both engines, so running every revoke first can
// only leave the role with less than it held.
//
// The column privileges the diff keeps and a revoke on their table takes come
// back right after the revokes, before anything else runs, where the column
// exists: a statement that fails later in the plan leaves the role without the
// privileges the diff takes away, and with every one it keeps. A privilege on
// a column the plan creates comes back with the other grants, after the CREATE
// TABLE, CREATE VIEW or ADD COLUMN: the server keeps the privilege of a dropped
// column, table or view, so the read reports it held, and a GRANT on the
// missing column fails with ERROR 1054 or 1146, measured on MySQL 8.4 and
// 26.10 and MariaDB 11.8 and 12.3. The role then holds nothing on it until the
// apply completes.
//
// On SQL Server a permission on a whole table takes the place of the column
// permissions of the same name, and a column revoke under one withholds the
// column. Measured on SQL Server 2019, 2022 and 2025: `GRANT SELECT ON t`
// over `SELECT (id)` leaves one row for the table, `REVOKE SELECT ON t` takes
// the column rows too, and `REVOKE SELECT (id) ON t` under a table grant
// leaves a REVOKE row, so a member of the role gets Msg 230 reading id. A plan
// that widens `SELECT (id)` to `SELECT ON t` and grants first leaves the role
// unable to read id. The reader reports a table permission with column rows
// of the same name under it as the permission on each column, so a diff never
// holds both and nothing comes back after the revokes there. A permission
// goes with its column or table when either is dropped.
//
// On MySQL a column privilege serves only the spelling it was granted under,
// so a privilege the declaration spells another way is revoked and granted
// again under the declared spelling. That GRANT runs with the ones above where
// the column exists: placed after the tables, a statement that fails in
// between leaves the role without the column, measured on MySQL 8.4 and
// 26.10 with a UNIQUE key failing on duplicate rows.
func (p *Planner) planRevokesFirst(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) || !p.revokesFirst() {
		return result
	}
	result = p.revokeGrants(result, diff)
	now, _ := p.columnGrantsGivenBack(diff)
	result = append(result, now...)
	respelled, _ := p.grantsAdded(diff)
	return p.grantPrivileges(result, diff, respelled)
}

// grantPrivileges emits a GRANT for each of grants, naming the column as
// [columnSpellings] says.
func (p *Planner) grantPrivileges(result []ast.Node, diff *difftypes.SchemaDiff, grants []difftypes.GrantRef) []ast.Node {
	spellings := newColumnSpellings(diff, p.targetDialect())
	for _, grant := range grants {
		result = append(result, ast.NewGrantPrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(spellings.columns(grant)).
			SetWithOption(grant.WithOption))
	}
	return result
}

// grantsAdded splits diff.GrantsAdded by when a plan that revokes first runs
// each GRANT: respelled holds the grants that give back, under the declared
// spelling, a column privilege the diff removes under another spelling, on a
// column the database holds; later holds the rest. respelled is empty where
// a column privilege serves every spelling of its column, or where the plan
// does not revoke first.
func (p *Planner) grantsAdded(diff *difftypes.SchemaDiff) (respelled, later []difftypes.GrantRef) {
	spellings := newColumnSpellings(diff, p.targetDialect())
	if !p.revokesFirst() || !spellings.bySpelling {
		return nil, diff.GrantsAdded
	}
	// removed is the spellings each removed column privilege is held under,
	// keyed with the column compared as the server compares it.
	removed := make(map[columnGrant][]string)
	for _, grant := range diff.GrantsRemoved {
		if grant.Column != "" {
			key := spellings.columnGrant(grant)
			removed[key] = append(removed[key], strings.TrimSpace(grant.Column))
		}
	}
	created := newCreatedColumns(diff, spellings.semantics)
	for _, grant := range diff.GrantsAdded {
		spelled := strings.TrimSpace(grant.Column)
		otherwise := slices.ContainsFunc(removed[spellings.columnGrant(grant)], func(held string) bool { return held != spelled })
		if spelled != "" && otherwise && !created.holds(grant) {
			respelled = append(respelled, grant)
			continue
		}
		later = append(later, grant)
	}
	return respelled, later
}

// columnGrantsGivenBack is [columnGrantsRevokedWithTheirTable] split by where
// the column is: now holds the grants on a column the database holds, later
// the grants on a column of a table or view the plan creates or a column it
// adds.
func (p *Planner) columnGrantsGivenBack(diff *difftypes.SchemaDiff) (now, later []ast.Node) {
	spellings := newColumnSpellings(diff, p.targetDialect())
	created := newCreatedColumns(diff, spellings.semantics)
	for _, kept := range columnGrantsRevokedWithTheirTable(diff, spellings) {
		node := ast.NewGrantPrivilege(kept.grant.Role, kept.grant.ObjectType, kept.grant.ObjectName, []string{kept.grant.Privilege}).
			SetColumns(grantColumns(kept.grant)).
			SetWithOption(kept.withOption)
		if created.holds(kept.grant) {
			later = append(later, node)
			continue
		}
		now = append(now, node)
	}
	return now, later
}

// createdColumns are the tables and views a plan creates or replaces and the
// columns it adds to the tables the database holds, keyed the way the target compares
// their names. A privilege on a view is a privilege on a table to the server.
type createdColumns struct {
	semantics identifier.Semantics
	tables    map[string]bool
	columns   map[createdColumn]bool
}

// createdColumn is one column of one table, keyed as [createdColumns] keys it.
type createdColumn struct{ table, column string }

// newCreatedColumns reads the tables and the columns diff creates.
func newCreatedColumns(diff *difftypes.SchemaDiff, semantics identifier.Semantics) createdColumns {
	created := createdColumns{semantics: semantics, tables: make(map[string]bool), columns: make(map[createdColumn]bool)}
	for _, table := range diff.TablesAdded {
		created.tables[semantics.QualifiedTableIdentityKey(table.Name)] = true
	}
	for _, view := range diff.ViewsAdded {
		created.tables[semantics.QualifiedTableIdentityKey(view.Name)] = true
	}
	// A view the plan replaces may gain the column only when CREATE OR REPLACE
	// VIEW runs, and a GRANT on it before then fails with ERROR 1054.
	for _, view := range diff.ViewsModified {
		created.tables[semantics.QualifiedTableIdentityKey(view.ViewName)] = true
	}
	for _, table := range diff.TablesModified {
		key := semantics.QualifiedTableIdentityKey(table.TableName)
		for _, column := range table.ColumnsAdded {
			created.columns[createdColumn{table: key, column: semantics.ColumnIdentityKey(column.Name)}] = true
		}
	}
	return created
}

// holds reports whether grant is on a table or view the plan creates or on a
// column it adds.
func (c createdColumns) holds(grant difftypes.GrantRef) bool {
	table := c.semantics.QualifiedTableIdentityKey(grant.ObjectName)
	return c.tables[table] || c.columns[createdColumn{table: table, column: c.semantics.ColumnIdentityKey(grant.Column)}]
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
// away, the orphaned grant options, then the privileges. Where
// [Planner.revokesGrantOptionFirst] holds, a privilege held with the grant
// option loses the option before the privilege is revoked.
func (p *Planner) revokeGrants(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	spellings := newColumnSpellings(diff, p.targetDialect())
	ghosts, removed := p.splitGrantOptionGhosts(diff.GrantsRemoved)
	revokes := slices.Concat(diff.GrantOptionsRevoked, ghosts)
	if p.grantOptionCoversTheObject() {
		revokes = append(revokes, grantOptionsLeftBehind(removed, diff.CurrentGrants, spellings)...)
		removed = withoutColumnRevokesTheirTableTakes(removed, spellings)
	}
	if p.revokesGrantOptionFirst() {
		for _, grant := range removed {
			if grant.WithOption {
				revokes = append(revokes, grant)
			}
		}
	}
	for _, revoke := range p.grantOptionRevocations(revokes) {
		result = append(result, revoke)
	}
	for _, grant := range removed {
		result = append(result, ast.NewRevokePrivilege(
			grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetColumns(spellings.columns(grant)))
	}
	return result
}

// withoutColumnRevokesTheirTableTakes answers removed without the column
// privileges that a revoke of the same privilege, or of ALL, on their table
// takes with it.
//
// On MySQL and MariaDB a revoke of a column privilege the role no longer holds
// fails: measured on all four servers, a role holding SELECT on t1, SELECT
// (secret) and SELECT (id) and declared SELECT (id) answers
// `REVOKE SELECT (secret)` after `REVOKE SELECT ON t1` with ERROR 1147, and
// the apply stops before it grants SELECT (id) again. The privilege stays a
// removal of the diff, so [columnGrantsRevokedWithTheirTable] does not grant
// it back. A revoke of ALL comes from a comparison of two schema files, which
// keeps the ALL a file grants; a catalog read reports each privilege.
func withoutColumnRevokesTheirTableTakes(removed []difftypes.GrantRef, spellings columnSpellings) []difftypes.GrantRef {
	tableRevokes := make(map[revokedPrivilege]bool)
	for _, grant := range removed {
		if grant.Column == "" {
			tableRevokes[spellings.revokedPrivilege(grant)] = true
		}
	}
	return slices.DeleteFunc(slices.Clone(removed), func(grant difftypes.GrantRef) bool {
		if grant.Column == "" {
			return false
		}
		revoked := spellings.revokedPrivilege(grant)
		all := revoked
		all.privilege = "ALL"
		return tableRevokes[revoked] || tableRevokes[all]
	})
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
//
// An object can come back more than once, and the diff can revoke the option
// there itself: [Planner.grantOptionRevocations] makes one statement of every
// revocation on one object, so each is left in.
func grantOptionsLeftBehind(removed, current []difftypes.GrantRef, spellings columnSpellings) []difftypes.GrantRef {
	removedKeys := make(map[columnGrant]bool, len(removed))
	for _, grant := range removed {
		removedKeys[spellings.columnGrant(grant)] = true
	}
	keepsOption := make(map[grantObject]bool)
	for _, grant := range current {
		if grant.WithOption && !removedKeys[spellings.columnGrant(grant)] {
			keepsOption[grantObjectOf(grant)] = true
		}
	}
	var left []difftypes.GrantRef
	for _, grant := range removed {
		if grant.WithOption && !keepsOption[grantObjectOf(grant)] {
			left = append(left, grant)
		}
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

// revokedPrivilege is a privilege of a role on one object, the object keyed
// the way the target compares it.
type revokedPrivilege struct {
	role, objectType, object, privilege string
}

// revokedPrivilege is the privilege grant names on its object, its column
// aside.
func (s columnSpellings) revokedPrivilege(grant difftypes.GrantRef) revokedPrivilege {
	target := s.target(grant)
	return revokedPrivilege{
		role: target.role, objectType: target.objectType, object: target.object,
		privilege: strings.ToUpper(strings.TrimSpace(grant.Privilege)),
	}
}

// columnGrant is the identity of a column privilege, the grant option aside,
// with the column keyed the way the target compares column names.
type columnGrant struct {
	target    columnTarget
	privilege string
}

// columnGrant is the identity of the column privilege grant names.
func (s columnSpellings) columnGrant(grant difftypes.GrantRef) columnGrant {
	return columnGrant{target: s.target(grant), privilege: strings.ToUpper(strings.TrimSpace(grant.Privilege))}
}

// keptColumnGrant is a column privilege a plan gives back, and whether it
// comes back with the grant option.
type keptColumnGrant struct {
	grant      difftypes.GrantRef
	withOption bool
}

// columnGrantsRevokedWithTheirTable is each column privilege the database
// holds and the diff keeps, where the diff revokes the same privilege on the
// whole table: the table revoke takes the column privilege with it, and
// without a GRANT the role loses a privilege the declaration keeps until the
// next plan grants it again. A REVOKE ALL takes every privilege; a catalog
// read reports one row for each privilege, and a comparison of two schema
// files carries the ALL a file grants. The grant option comes back with the
// privilege unless the diff takes the option away from the role on that table.
//
// A column privilege the diff removes is left revoked, whether or not the
// plan names it in a statement of its own; see
// [withoutColumnRevokesTheirTableTakes].
func columnGrantsRevokedWithTheirTable(diff *difftypes.SchemaDiff, spellings columnSpellings) []keptColumnGrant {
	tableRevokes := make(map[revokedPrivilege]bool)
	columnRevokes := make(map[columnGrant]bool)
	for _, grant := range diff.GrantsRemoved {
		switch {
		case grant.Column != "":
			columnRevokes[spellings.columnGrant(grant)] = true
		case strings.EqualFold(grant.ObjectType, "SCHEMA") || grant.Privilege == usageGrantOptionGhost:
		default:
			tableRevokes[spellings.revokedPrivilege(grant)] = true
		}
	}
	if len(tableRevokes) == 0 {
		return nil
	}
	optionRevoked := make(map[grantObject]bool)
	for _, grant := range diff.GrantOptionsRevoked {
		optionRevoked[grantObjectOf(grant)] = true
	}
	var kept []keptColumnGrant
	for _, grant := range diff.CurrentGrants {
		if grant.Column == "" || columnRevokes[spellings.columnGrant(grant)] {
			continue
		}
		revoked := spellings.revokedPrivilege(grant)
		all := revoked
		all.privilege = "ALL"
		if !tableRevokes[revoked] && !tableRevokes[all] {
			continue
		}
		kept = append(kept, keptColumnGrant{grant: grant, withOption: grant.WithOption && !optionRevoked[grantObjectOf(grant)]})
	}
	return kept
}

// removeGrantsAndRoles revokes first and drops the roles afterwards. On
// MySQL, MariaDB and SQL Server [Planner.planRevokesFirst] has run the
// revokes.
//
// A role still holding permissions is not droppable on any engine in this
// family that has roles at all, so the order is the engine's rather than a
// preference.
func (p *Planner) removeGrantsAndRoles(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.capabilities().Has(capability.RoleManagement) {
		return result
	}
	if !p.revokesFirst() {
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
