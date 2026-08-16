package clickhouse

import (
	"fmt"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// planRoles emits the role half of ClickHouse access control.
//
// It runs before every phase that can name a principal, because the server
// refuses a grant to a role it does not know: measured on clickhouse-server
// 26.7.3.19 and 24.10.4.191, granting to a role that was never created answers
// `Code: 511. DB::Exception: ... (UNKNOWN_ROLE)` and the statement does nothing.
// The ordering is therefore a correctness requirement rather than a preference,
// which is also why this phase keeps the slot the old role diagnostics had --
// after the tables and before everything that reads a role, the same place the
// offline render path emits roles from.
func planRoles(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	result = createRoles(result, diff)
	result = reportRoleModifications(result, diff)
	return reportRoleRemovals(result, diff)
}

// createRoles plans one CREATE ROLE per added role, carrying the name and
// nothing else.
//
// There is nothing else to carry. `system.roles` is exactly (name, id, storage)
// on both measured servers: no LOGIN, PASSWORD, SUPERUSER, CREATEDB, CREATEROLE,
// INHERIT or REPLICATION, and not even a comment -- `CREATE ROLE r COMMENT 'x'`
// is refused with Code 62 as a syntax error. So this deliberately does not look
// the role up in the desired schema the way the PostgreSQL planner's
// addNewRoles does. [go.5x5.cz/ptah/internal/clickhouserbac.ValidateDeclared]
// has already refused any declaration that named an attribute, before the
// schema reached a planner at all, and what survives that gate is a name.
//
// Skipping the lookup also removes a failure mode the PostgreSQL planner has.
// There, a role named by the diff but missing from generated.Roles emits no
// node at all; here that silence would be followed by a GRANT naming a role the
// server does not know, which is the Code 511 above.
//
// [ast.NewCreateRole] defaults Inherit to true, and that default is ClickHouse's
// only behavior: role membership always inherits, which is why clickhouserbac
// refuses a declared inherit="false" instead of emitting one.
func createRoles(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.RolesAdded {
		result = append(result, ast.NewCreateRole(name))
	}
	return result
}

// reportRoleModifications names a role the comparison reports as changed, and
// plans no statement for it.
//
// There is no attribute to change, so any difference the comparison found is
// between the PostgreSQL-shaped attributes [goschema.Role] carries and whatever
// the ClickHouse reader answered for them. ClickHouse's own ALTER ROLE renames a
// role or attaches settings profiles, neither of which is what
// [types.RoleDiff] describes.
//
// It is named rather than ignored for the reason the rest of this planner names
// what it cannot host (stokaro/ptah#931 item 7): a diff category the planner
// passes over outright produces a plan that exits 0 having said nothing about a
// difference the comparison did find.
func reportRoleModifications(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, role := range diff.RolesModified {
		result = append(result, ast.NewComment(fmt.Sprintf(
			"CLICKHOUSE: role %q differs only in attributes a ClickHouse role does not carry; nothing is altered",
			role.RoleName,
		)))
	}
	return result
}

// reportRoleRemovals names a role that exists on the server and not in the
// schema, and plans no DROP ROLE for it.
//
// migration/schemadiff/internal/compare.Roles never fills RolesRemoved, and says
// so: a role is not owned by the schema that happens to grant to it. On
// ClickHouse the same reasoning is stronger, because a role is cluster-wide
// while Ptah manages one database, so dropping one would take away grants no
// declaration here describes. stokaro/ptah#1025 lists auto-dropping shared roles
// as a non-goal.
//
// The category is still named, so a diff that somehow carries one is not
// planned silently past.
func reportRoleRemovals(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.RolesRemoved {
		result = append(result, ast.NewComment(fmt.Sprintf(
			"CLICKHOUSE: role %q exists on the server and not in the schema; Ptah does not drop ClickHouse roles",
			name,
		)))
	}
	return result
}

// planGrants emits the grant half of ClickHouse access control.
//
// Revokes come before grants. That is the order the PostgreSQL planner uses, and
// on ClickHouse it is load-bearing rather than tidy, because the server absorbs
// a narrower grant into a broader one in silence. With SELECT on `db`.* live and
// SELECT on `db`.`t` declared, the diff carries both an addition and a removal;
// granting first leaves the single `db`.* row the server already had -- measured
// in both orders, the table-level grant is recorded nowhere -- and the REVOKE
// that follows then takes the whole database away, leaving the role with
// nothing. Revoking first converges on exactly the declared grant.
//
// The order costs nothing when the two sets do not overlap: revoking a grant a
// role does not hold is a silent no-op on both measured servers.
func planGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	result = revokeGrants(result, diff)
	result = revokeGrantOptions(result, diff)
	return addGrants(result, diff)
}

// addGrants plans GRANT for a new grant, and for a privilege the role already
// holds whose WITH GRANT OPTION the schema now asks for.
//
// SetWithOption(true) on the second loop is the entire meaning of that
// statement, not a decoration. A GrantOptionsAdded entry describes a privilege
// the comparison found on both sides, differing only in grant_option, so a GRANT
// emitted without the option re-issues a grant the server already has: the
// statement succeeds, grant_option stays 0, the plan reports success, and the
// next comparison asks for the same change again. This planner used to emit
// exactly that, invisibly, because the ClickHouse renderer reduced every grant
// node to a comment.
func addGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantsAdded {
		result = append(result, grantNode(grant).SetWithOption(grant.WithOption))
	}
	for _, grant := range diff.GrantOptionsAdded {
		result = append(result, grantNode(grant).SetWithOption(true))
	}
	return result
}

// revokeGrants plans REVOKE for a grant the schema no longer declares.
func revokeGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantsRemoved {
		result = append(result, revokeNode(grant))
	}
	return result
}

// revokeGrantOptions plans the downgrade that takes WITH GRANT OPTION away and
// leaves the privilege itself in place.
//
// SetGrantOptionFor(true) is what separates the two statements:
// `REVOKE GRANT OPTION FOR SELECT ON db.t FROM r` moves grant_option from 1 to 0
// and creates no partial-revoke row, while the same statement without the clause
// removes the privilege the schema still declares. Measured on 26.7.3.19 and
// 24.10.4.191, that downgrade is one statement, so nothing has to be re-granted
// after it -- which is why this phase emits one node and not a revoke/grant
// pair. Omitting the clause was this planner's other invisible defect.
func revokeGrantOptions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantOptionsRevoked {
		result = append(result, revokeNode(grant).SetGrantOptionFor(true))
	}
	return result
}

// grantNode builds the GRANT node for one grant reference.
//
// ObjectType travels with the node even though ClickHouse has no object-type
// keyword to render it into. A ClickHouse scope is a two-part pattern -- `db`.`t`
// for a table, `db`.* for a database -- and which of the two a reference means is
// precisely what ObjectType records, so dropping it here would leave the
// renderer guessing. [go.5x5.cz/ptah/internal/clickhouserbac.Scope] reads it back
// rather than emitting it.
func grantNode(grant types.GrantRef) *ast.GrantPrivilegeNode {
	return ast.NewGrantPrivilege(
		grant.Role,
		grant.ObjectType,
		grant.ObjectName,
		[]string{grant.Privilege},
	)
}

// revokeNode is [grantNode] for the statement that takes a privilege back.
func revokeNode(grant types.GrantRef) *ast.RevokePrivilegeNode {
	return ast.NewRevokePrivilege(
		grant.Role,
		grant.ObjectType,
		grant.ObjectName,
		[]string{grant.Privilege},
	)
}
