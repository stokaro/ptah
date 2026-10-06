package ydb

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/renderdiag"
	"ptah.run/internal/ydbacl"
)

// YDB's access model is users, groups and permission entries on its objects;
// see [ydbacl] for what was measured. The visitors below write it:
//
//   - a role is a user and a role declared as a group is a group, created by
//     CREATE USER and CREATE GROUP. A group is created empty and its members
//     added one statement each, because CREATE GROUP ... WITH USER is not
//     atomic: measured, naming a member that does not exist creates the group
//     and then fails with `Member account not found`;
//   - a privilege is a YDB permission, written by its name as a string, which
//     is the spelling the server reports back, and a GRANT or REVOKE names its
//     object by a path the server resolves (see [ydbacl.StatementPath]);
//   - there is no IF NOT EXISTS on CREATE USER or CREATE GROUP (a parse error
//     on every line), so a create is written bare and the comparison decides
//     whether it is needed. DROP takes IF EXISTS.
//
// [capability.RoleManagement] gates all of them, as it does on every engine.

// renderCreateRole writes CREATE USER, or CREATE GROUP for a group.
func (r *Renderer) renderCreateRole(node *ast.CreateRoleNode) error {
	subject := "user " + node.Name
	if node.Group {
		subject = "group " + node.Name
	}
	if err := r.refuseAccess(subject); err != nil {
		return err
	}
	if err := r.refusePrincipal(node, subject); err != nil {
		return err
	}
	r.writeAccessComment(node.Comment)
	// The line above is a SQL comment, which the server does not store: YDB
	// keeps no comment on a user or a group.
	r.sink.RecordLostComment(renderdiag.RoleKind, node.Name, node.Comment)
	if node.Group {
		r.w.WriteLinef("CREATE GROUP %s;", quote(node.Name))
		return nil
	}
	statement := "CREATE USER " + quote(node.Name)
	if node.Password != "" {
		r.writePasswordWarning(node.Password)
		statement += " " + ydbacl.PasswordClause(node.Password)
	}
	if !node.Login {
		statement += " NOLOGIN"
	}
	r.w.WriteLine(statement + ";")
	return nil
}

// refusePrincipal refuses what a YDB user or group cannot carry: a name YDB
// does not take, and the attributes it has no counterpart for.
func (r *Renderer) refusePrincipal(node *ast.CreateRoleNode, subject string) error {
	if node.Group && !r.caps.Has(capability.GroupPrincipals) {
		return refuseKey(capability.GroupPrincipals, subject)
	}
	if err := ydbacl.CheckName(node.Name); err != nil {
		return refuseFact(subject, err.Error())
	}
	var declared []string
	for _, attribute := range []struct {
		name     string
		declared bool
	}{
		{"superuser", node.Superuser},
		{"createdb", node.CreateDB},
		{"createrole", node.CreateRole},
		{"replication", node.Replication},
		{"login", node.Group && node.Login},
		{"a password", node.Group && node.Password != ""},
	} {
		if attribute.declared {
			declared = append(declared, attribute.name)
		}
	}
	if len(declared) == 0 {
		return nil
	}
	// The password VALUE is deliberately absent from the message: it is a
	// credential, and this error travels to stderr and into a plan.
	return refuseFact(subject, "it declares "+strings.Join(declared, ", ")+
		", which a YDB user or group does not carry; a group never logs in")
}

// renderAlterRole writes ALTER USER for a change of login or password, the
// two attributes a YDB user has. One statement sets both: measured, `ALTER
// USER u PASSWORD 'x' NOLOGIN` sets the password and blocks the user.
func (r *Renderer) renderAlterRole(node *ast.AlterRoleNode) error {
	subject := "ALTER USER " + node.Name
	if err := r.refuseAccess(subject); err != nil {
		return err
	}
	if err := ydbacl.CheckName(node.Name); err != nil {
		return refuseFact(subject, err.Error())
	}
	var password, login string
	for _, operation := range node.Operations {
		switch typed := operation.(type) {
		case *ast.SetPasswordOperation:
			r.writePasswordWarning(typed.Password)
			password = " " + ydbacl.PasswordClause(typed.Password)
		case *ast.SetLoginOperation:
			login = " NOLOGIN"
			if typed.Login {
				login = " LOGIN"
			}
		default:
			return refuseFact(subject, fmt.Sprintf("it changes %s, which a YDB user does not carry; "+
				"a user has a password and logs in or not", operationName(operation)))
		}
	}
	if password == "" && login == "" {
		return refuseFact(subject, "it changes nothing a YDB user carries")
	}
	r.writeAccessComment(node.Comment)
	r.w.WriteLinef("ALTER USER %s%s%s;", quote(node.Name), password, login)
	return nil
}

// operationName names an ALTER ROLE operation for a refusal.
func operationName(operation ast.RoleOperation) string {
	if operation == nil {
		return "nothing"
	}
	return strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(operation.GetOperationType(), "SET_"), "_", " "))
}

// renderDropRole writes DROP USER, or DROP GROUP for a group: the two do not
// reach each other's kind (`User not found`, `Group not found`). Dropping
// either leaves its permission entries behind, measured on every line, so a
// plan revokes them first.
func (r *Renderer) renderDropRole(node *ast.DropRoleNode) error {
	statement := "DROP USER "
	if node.Group {
		statement = "DROP GROUP "
	}
	if err := r.refuseAccess(statement + node.Name); err != nil {
		return err
	}
	if node.Group && !r.caps.Has(capability.GroupPrincipals) {
		return refuseKey(capability.GroupPrincipals, statement+node.Name)
	}
	if node.IfExists {
		statement += "IF EXISTS "
	}
	r.writeAccessComment(node.Comment)
	r.w.WriteLine(statement + quote(node.Name) + ";")
	return nil
}

// renderRoleMembership writes ALTER GROUP ... ADD USER or DROP USER, one
// member per statement, as clause says. subject names the change in a
// refusal. Measured: adding a member twice and dropping one that is not a
// member both succeed with a notice, and a member can be a user or a group.
func (r *Renderer) renderRoleMembership(role, member, comment, clause, subject string) error {
	if err := r.refuseAccess(subject); err != nil {
		return err
	}
	if !r.caps.Has(capability.RoleMembership) {
		return refuseKey(capability.RoleMembership, subject)
	}
	if strings.TrimSpace(role) == "" || strings.TrimSpace(member) == "" {
		return refuseFact(subject, "a membership names a group and a member")
	}
	r.writeAccessComment(comment)
	r.w.WriteLinef("ALTER GROUP %s %s %s;", quote(role), clause, quote(member))
	return nil
}

// grantStatement is what a GRANT and a REVOKE share: the permissions, the
// path and the subject, each checked and spelled.
type grantStatement struct {
	permissions string
	path        string
	subject     string
}

// renderGrantPrivilege writes GRANT.
func (r *Renderer) renderGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	subject := fmt.Sprintf("GRANT on %s to %s", ydbacl.Describe(node.ObjectType, node.ObjectName), node.Role)
	if node.WithOption {
		return refuseFact(subject, "YDB records WITH GRANT OPTION as the permission "+ydbacl.GrantPermission+
			" beside the one it is written with, so grant that permission instead")
	}
	parts, err := r.grantParts(subject, node.Role, node.ObjectType, node.ObjectName, node.Arguments,
		node.Columns, node.Privileges)
	if err != nil {
		return err
	}
	r.writeAccessComment(node.Comment)
	r.w.WriteLinef("GRANT %s ON %s TO %s;", parts.permissions, parts.path, parts.subject)
	return nil
}

// renderRevokePrivilege writes REVOKE. Measured on every line, it removes the
// entry with exactly the permission it names and no other, and revoking an
// entry nobody holds succeeds.
func (r *Renderer) renderRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	subject := fmt.Sprintf("REVOKE on %s from %s", ydbacl.Describe(node.ObjectType, node.ObjectName), node.Role)
	if node.GrantOptionFor {
		// Measured: REVOKE GRANT OPTION FOR SELECT removes the SELECT entry
		// together with ydb.access.grant, so it never takes the option
		// alone.
		return refuseFact(subject, "YDB's REVOKE GRANT OPTION FOR takes the permission it names "+
			"together with "+ydbacl.GrantPermission+", so revoke that permission instead")
	}
	parts, err := r.grantParts(subject, node.Role, node.ObjectType, node.ObjectName, node.Arguments,
		node.Columns, node.Privileges)
	if err != nil {
		return err
	}
	r.writeAccessComment(node.Comment)
	r.w.WriteLinef("REVOKE %s ON %s FROM %s;", parts.permissions, parts.path, parts.subject)
	return nil
}

// grantParts checks and spells what a GRANT and a REVOKE name.
func (r *Renderer) grantParts(
	subject, role, objectType, objectName, arguments string,
	columns, privileges []string,
) (grantStatement, error) {
	if err := r.refuseAccess(subject); err != nil {
		return grantStatement{}, err
	}
	if len(columns) > 0 {
		return grantStatement{}, refuseFact(subject, "a YDB permission is on a whole object, not on columns")
	}
	if strings.TrimSpace(arguments) != "" {
		return grantStatement{}, refuseFact(subject, "YDB has no routines")
	}
	if strings.TrimSpace(role) == "" {
		return grantStatement{}, refuseFact(subject, "it names no user or group")
	}
	if (strings.EqualFold(strings.TrimSpace(objectType), ydbacl.ObjectDatabase) ||
		strings.EqualFold(strings.TrimSpace(objectType), ydbacl.ObjectPath)) &&
		!r.caps.Has(capability.DatabaseGrants) {
		return grantStatement{}, refuseKey(capability.DatabaseGrants, subject)
	}
	path, err := ydbacl.StatementPath(objectType, objectName, "", r.caps)
	if errors.Is(err, ydbacl.ErrNeedsDatabasePath) {
		return grantStatement{}, refuseFact(subject, err.Error()+"; plan it against the database, "+
			"which names it by that path")
	}
	if err != nil {
		return grantStatement{}, refuseFact(subject, err.Error())
	}
	names := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		permission, ok := ydbacl.Permission(privilege)
		if !ok {
			return grantStatement{}, refuseFact(subject, fmt.Sprintf("%q is not a YDB permission", privilege))
		}
		names = append(names, "'"+permission+"'")
	}
	if len(names) == 0 {
		return grantStatement{}, refuseFact(subject, "it names no permission")
	}
	return grantStatement{
		permissions: strings.Join(names, ", "),
		path:        quote(path),
		subject:     quote(role),
	}, nil
}

// refuseAccess refuses every access statement on a target without
// [capability.RoleManagement].
func (r *Renderer) refuseAccess(subject string) error {
	if r.caps.Has(capability.RoleManagement) {
		return nil
	}
	return refuseKey(capability.RoleManagement, subject)
}

// writeAccessComment writes a node's comment as one `-- ` line, its line
// breaks folded so that a second line cannot reach the statement stream.
func (r *Renderer) writeAccessComment(comment string) {
	if folded := strings.Join(strings.Fields(comment), " "); folded != "" {
		r.w.WriteLine("-- " + folded)
	}
}

// writePasswordWarning writes the warning PostgreSQL's renderer writes above
// a password that is not a hash: the migration file holds it in plain text.
func (r *Renderer) writePasswordWarning(password string) {
	if ydbacl.IsPasswordHash(password) {
		return
	}
	r.w.WriteLine("-- WARNING: the password is written in plain text; declare its hash to keep it out of the file")
}
