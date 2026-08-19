package mysqllike

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
)

// MySQL and MariaDB have had roles since 8.0 and 10.0, and the shape is narrower
// than PostgreSQL's: a role is a principal that cannot log in, and it carries no
// attributes of its own. Everything below was measured on MySQL 8.4:
//
//	CREATE ROLE r_a                          -- accepted
//	CREATE ROLE IF NOT EXISTS r_a            -- accepted
//	CREATE ROLE r_login LOGIN                -- ERROR 1064, syntax error
//	CREATE ROLE r_pw PASSWORD 'x'            -- ERROR 1064, syntax error
//	DROP ROLE IF EXISTS r_absent             -- accepted
//	GRANT SELECT ON db.t TO r_a              -- accepted
//	GRANT SELECT ON db.* TO r_a              -- accepted
//	REVOKE SELECT ON db.t FROM r_a           -- accepted
//	GRANT SELECT ON db.t TO r_a WITH GRANT OPTION -- accepted
//
// The two refusals are the whole difference. A declared LOGIN or PASSWORD asks
// for a USER, which is a different object here, and rendering the role without
// them would hand the author a principal that cannot do what they wrote -- the
// same reasoning the SQL Server renderer records for its DATABASE ROLE
// (stokaro/ptah#1762).

// VisitCreateRole renders CREATE ROLE, and refuses a declaration carrying an
// attribute a MySQL-family role does not have.
//
// The refusal stays an error rather than becoming a named skip. A comment where
// a principal was asked for leaves the grants that name it dangling, and the
// server then refuses those instead -- moving the failure from one message to a
// worse one.
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	if !r.caps.Has(capability.RoleManagement) {
		return unsupportedRoleError(r.dialect, "CREATE ROLE", node.Name)
	}
	if attribute := unsupportedRoleAttribute(node); attribute != "" {
		return roleAttributeError(r.dialect, node.Name, attribute)
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	// IF NOT EXISTS unconditionally: the node carries no guard field, the
	// clause is accepted, and a plan that is safe to re-run is worth more than
	// a statement that fails the second time for a reason nobody asked about.
	r.w.WriteLinef("CREATE ROLE IF NOT EXISTS %s;", escapeIdentifier(node.Name))
	return nil
}

// VisitDropRole renders DROP ROLE.
//
// IF EXISTS is accepted on an absent role, so the guarded form needs no
// existence test.
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	if !r.caps.Has(capability.RoleManagement) {
		return unsupportedRoleError(r.dialect, "DROP ROLE", node.Name)
	}
	guard := ""
	if node.IfExists {
		guard = "IF EXISTS "
	}
	r.w.WriteLinef("DROP ROLE %s%s;", guard, escapeIdentifier(node.Name))
	return nil
}

// VisitAlterRole refuses, because there is nothing to alter.
//
// A MySQL-family role has no attributes: the CREATE takes a name and nothing
// else, so every change an ALTER could express is a change to something this
// object does not have. Reporting it as skipped would be the quieter answer and
// the wrong one -- the author asked for a change that will never happen.
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	if !r.caps.Has(capability.RoleManagement) {
		return unsupportedRoleError(r.dialect, "ALTER ROLE", node.Name)
	}
	return roleAttributeError(r.dialect, node.Name, "an altered attribute")
}

// VisitGrantPrivilege renders GRANT.
func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	if !r.caps.Has(capability.RoleManagement) {
		r.notGenerated("grant", node.Role)
		return nil
	}
	statement := "GRANT " + strings.Join(node.Privileges, ", ") +
		" ON " + grantScope(node.ObjectType, node.ObjectName) + " TO " + escapeIdentifier(node.Role)
	if node.WithOption {
		statement += " WITH GRANT OPTION"
	}
	r.w.WriteLinef("%s;", statement)
	return nil
}

// VisitRevokePrivilege renders REVOKE.
func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	if !r.caps.Has(capability.RoleManagement) {
		r.notGenerated("revoke", node.Role)
		return nil
	}
	r.w.WriteLinef("REVOKE %s ON %s FROM %s;",
		strings.Join(node.Privileges, ", "), grantScope(node.ObjectType, node.ObjectName), escapeIdentifier(node.Role))
	return nil
}

// unsupportedRoleAttribute names the first declared attribute a MySQL-family
// role cannot carry, or "" when the declaration is one this target can create.
//
// The order is fixed rather than reflective so that two runs over the same
// declaration name the same attribute.
func unsupportedRoleAttribute(node *ast.CreateRoleNode) string {
	switch {
	case node.Login:
		return "LOGIN"
	case node.Password != "":
		return "PASSWORD"
	case node.Superuser:
		return "SUPERUSER"
	case node.CreateDB:
		return "CREATEDB"
	case node.CreateRole:
		return "CREATEROLE"
	case node.Replication:
		return "REPLICATION"
	default:
		return ""
	}
}

// grantScope spells the object a grant names, in the two shapes this family
// accepts: `db`.`table` and `db`.*.
//
// Both were measured on MySQL 8.4. A grant whose object type is a schema takes
// the star form, because a database IS the schema here and there is no separate
// level to name.
func grantScope(objectType, objectName string) string {
	if strings.EqualFold(strings.TrimSpace(objectType), "SCHEMA") ||
		strings.EqualFold(strings.TrimSpace(objectType), "DATABASE") {
		return escapeIdentifier(objectName) + ".*"
	}
	parts := strings.Split(objectName, ".")
	for i, part := range parts {
		parts[i] = escapeIdentifier(part)
	}
	return strings.Join(parts, ".")
}

// roleAttributeError refuses a declared attribute a MySQL-family role cannot
// carry, naming the attribute rather than the object kind.
//
// LOGIN and PASSWORD are the two a declaration is most likely to carry, and
// both are ERROR 1064 on MySQL 8.4: what they ask for is a USER, which is a
// different object here. Creating the role without them would hand the author a
// principal that cannot do what they wrote.
func roleAttributeError(dialect, name, attribute string) error {
	return fmt.Errorf(
		"%w: %s: role %q declares %s, which a role does not carry here; "+
			"a principal that logs in is a USER here, and Ptah does not manage users",
		ptaherr.ErrUnsupportedFeature, dialect, name, attribute,
	)
}
