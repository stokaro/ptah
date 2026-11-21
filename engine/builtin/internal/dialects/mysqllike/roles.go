package mysqllike

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/internal/grantrefusal"
	"ptah.run/internal/renderdiag"
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

// renderCreateRole renders CREATE ROLE, and refuses a declaration carrying an
// attribute a MySQL-family role does not have.
//
// The refusal stays an error rather than becoming a named skip. A comment where
// a principal was asked for leaves the grants that name it dangling, and the
// server then refuses those instead -- moving the failure from one message to a
// worse one.
func (r *Renderer) renderCreateRole(node *ast.CreateRoleNode) error {
	if !r.caps.Has(capability.RoleManagement) {
		return unsupportedRoleError(r.dialect, "CREATE ROLE", node.Name)
	}
	if attribute := unsupportedRoleAttribute(node); attribute != "" {
		return roleAttributeError(r.dialect, node.Name, attribute)
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	// The line above is a SQL comment, which the server does not store: the
	// render looks like it kept the text and the database has none of it. Only
	// the PostgreSQL family has COMMENT ON ROLE.
	r.sink.RecordLostComment(renderdiag.RoleKind, node.Name, node.Comment)
	// IF NOT EXISTS unconditionally: the node carries no guard field, the
	// clause is accepted, and a plan that is safe to re-run is worth more than
	// a statement that fails the second time for a reason nobody asked about.
	r.w.WriteLinef("CREATE ROLE IF NOT EXISTS %s;", escapeIdentifier(node.Name))
	return nil
}

// renderDropRole renders DROP ROLE.
//
// IF EXISTS is accepted on an absent role, so the guarded form needs no
// existence test.
func (r *Renderer) renderDropRole(node *ast.DropRoleNode) error {
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

// renderAlterRole refuses, because there is nothing to alter.
//
// A MySQL-family role has no attributes: the CREATE takes a name and nothing
// else, so every change an ALTER could express is a change to something this
// object does not have. Reporting it as skipped would be the quieter answer and
// the wrong one -- the author asked for a change that will never happen.
func (r *Renderer) renderAlterRole(node *ast.AlterRoleNode) error {
	if !r.caps.Has(capability.RoleManagement) {
		return unsupportedRoleError(r.dialect, "ALTER ROLE", node.Name)
	}
	return roleAttributeError(r.dialect, node.Name, "an altered attribute")
}

// renderGrantRoleMembership writes `GRANT role TO member`, with WITH ADMIN
// OPTION when the node states the admin option. Measured on MySQL 8.4, the
// statement upgrades a membership that exists, a plain GRANT on a membership
// holding the option leaves it on, and the engine has no INHERIT or SET option
// per membership, so those are refused.
func (r *Renderer) renderGrantRoleMembership(node *ast.GrantRoleMembershipNode) error {
	if !r.caps.Has(capability.RoleMembership) {
		return grantrefusal.Membership(r.dialect, r.caps, "ADD "+node.Member+" TO "+node.Role)
	}
	if node.Admin != nil {
		if err := grantrefusal.MembershipOption(r.dialect, r.caps, capability.RoleMembershipAdmin, node.Role, node.Member, "WITH ADMIN OPTION"); err != nil {
			return err
		}
	}
	for _, option := range []struct {
		name  string
		value *bool
	}{{"WITH INHERIT", node.Inherit}, {"WITH SET", node.Set}} {
		if option.value == nil {
			continue
		}
		if err := grantrefusal.MembershipOption(r.dialect, r.caps, capability.RoleMembershipInheritSet, node.Role, node.Member, option.name); err != nil {
			return err
		}
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	clause := ""
	if node.Admin != nil && *node.Admin {
		clause = " WITH ADMIN OPTION"
	}
	r.w.WriteLinef("GRANT %s TO %s%s;", escapeIdentifier(node.Role), escapeIdentifier(node.Member), clause)
	return nil
}

// renderRevokeRoleMembership writes `REVOKE role FROM member`. The engine has
// no statement that revokes the admin option alone, so a node asking for one is
// refused and the planner revokes the membership and grants it again.
func (r *Renderer) renderRevokeRoleMembership(node *ast.RevokeRoleMembershipNode) error {
	if !r.caps.Has(capability.RoleMembership) {
		return grantrefusal.Membership(r.dialect, r.caps, "DROP "+node.Member+" FROM "+node.Role)
	}
	if node.AdminOptionFor {
		return &ptaherr.CapabilityError{
			Dialect: r.dialect,
			Feature: string(capability.RoleMembershipAdmin),
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("REVOKE ADMIN OPTION FOR on the membership of %s in %s: the %s server has no statement "+
				"that revokes the admin option alone", node.Member, node.Role, r.dialect),
		}
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("REVOKE %s FROM %s;", escapeIdentifier(node.Role), escapeIdentifier(node.Member))
	return nil
}

// renderGrantPrivilege renders GRANT.
func (r *Renderer) renderGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	if err := grantrefusal.Path(r.dialect, "GRANT", node.ObjectType, node.ObjectName); err != nil {
		return err
	}
	if err := grantrefusal.Routine(r.dialect, "GRANT", node.ObjectType, node.ObjectName); err != nil {
		return err
	}
	if err := r.refuseColumnPrivileges("GRANT", node.ObjectType, node.ObjectName, node.Privileges, node.Columns); err != nil {
		return err
	}
	if !r.caps.Has(capability.RoleManagement) {
		r.notGenerated("grant", node.Role)
		return nil
	}
	statement := "GRANT " + privilegeList(node.Privileges, node.Columns) +
		" ON " + grantScope(node.ObjectType, node.ObjectName) + " TO " + escapeIdentifier(node.Role)
	if node.WithOption {
		statement += " WITH GRANT OPTION"
	}
	r.w.WriteLinef("%s;", statement)
	return nil
}

// renderRevokePrivilege renders REVOKE.
//
// Revoking only the grant option has a spelling of its own, and it names no
// privilege: a grant option here belongs to the grantee at one object, not to
// one privilege. Measured on MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and
// 12.3.3, over a role holding SELECT and INSERT on a table WITH GRANT OPTION:
//
//	REVOKE GRANT OPTION ON db.t FROM r            -- both kept, neither grantable
//	REVOKE SELECT ON db.t FROM r                  -- SELECT gone
//	REVOKE GRANT OPTION FOR SELECT ON db.t FROM r -- ERROR 1064 on both engines
//
// Without the first form, a plan that only takes the option away revokes the
// privilege itself. The same form on a schema grant, `db`.*, leaves the schema
// privilege in place too.
func (r *Renderer) renderRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	if err := grantrefusal.Path(r.dialect, "REVOKE", node.ObjectType, node.ObjectName); err != nil {
		return err
	}
	if err := grantrefusal.Routine(r.dialect, "REVOKE", node.ObjectType, node.ObjectName); err != nil {
		return err
	}
	if !node.GrantOptionFor {
		if err := r.refuseColumnPrivileges("REVOKE", node.ObjectType, node.ObjectName, node.Privileges, node.Columns); err != nil {
			return err
		}
	}
	if !r.caps.Has(capability.RoleManagement) {
		r.notGenerated("revoke", node.Role)
		return nil
	}
	revoked := privilegeList(node.Privileges, node.Columns)
	// The grant option belongs to the grantee at the table, its column
	// privileges included, so taking it away names no column either.
	if node.GrantOptionFor {
		revoked = "GRANT OPTION"
	}
	r.w.WriteLinef("REVOKE %s ON %s FROM %s;",
		revoked, grantScope(node.ObjectType, node.ObjectName), escapeIdentifier(node.Role))
	return nil
}

// columnPrivileges are the privileges MySQL and MariaDB grant on columns.
// Measured on MySQL 8.4.13 and MariaDB 11.8.9, any other privilege with a
// column list is ERROR 1064, a syntax error: `GRANT DELETE (name) ON t`.
var columnPrivileges = map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "REFERENCES": true}

// refuseColumnPrivileges refuses a column list this family cannot keep, by
// name, before the server sees it. A column privilege is on a table or a view,
// and is one of [columnPrivileges].
func (r *Renderer) refuseColumnPrivileges(statement, objectType, objectName string, privileges, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	if kind := strings.ToUpper(strings.TrimSpace(objectType)); kind != "TABLE" && kind != "VIEW" {
		return fmt.Errorf("%w: %s (%s) ON %s %s: a column privilege is on a table or a view",
			ptaherr.ErrUnsupportedFeature, statement, strings.Join(columns, ", "), kind, objectName)
	}
	for _, privilege := range privileges {
		normalized := strings.ToUpper(strings.TrimSpace(privilege))
		if !columnPrivileges[normalized] {
			return fmt.Errorf("%w: %s %s (%s) ON %s: %s grants only SELECT, INSERT, UPDATE and REFERENCES on columns",
				ptaherr.ErrUnsupportedFeature, statement, normalized, strings.Join(columns, ", "), objectName, r.dialect)
		}
	}
	return nil
}

// privilegeList spells the privileges of a GRANT or a REVOKE. With columns,
// each privilege carries the list, `SELECT (`a`, `b`), UPDATE (`a`, `b`)`, as
// the node limits every privilege to them.
func privilegeList(privileges, columns []string) string {
	if len(columns) == 0 {
		return strings.Join(privileges, ", ")
	}
	list := " (" + strings.Join(escapeIdentifierList(columns), ", ") + ")"
	spelled := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		spelled = append(spelled, privilege+list)
	}
	return strings.Join(spelled, ", ")
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
