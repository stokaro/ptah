package clickhouse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/clickhouserbac"
	"go.5x5.cz/ptah/internal/sqlident"
)

// The five visitors in this file are ClickHouse's role and privilege surface.
// It is not PostgreSQL's with different keywords, and each difference below was
// measured against live clickhouse-server 24.10.4.191 and 26.7.3.19 rather than
// read off a manual:
//
//   - `CREATE ROLE [IF NOT EXISTS] name` and `DROP ROLE [IF EXISTS] name` are
//     the whole of the role surface. system.roles is exactly (name, id,
//     storage): there is no LOGIN, PASSWORD, SUPERUSER, CREATEDB, CREATEROLE or
//     REPLICATION for an attribute to round-trip through, and
//     `CREATE ROLE r COMMENT 'x'` is a syntax error (Code 62).
//   - A grant names its scope as a two-part pattern — `db`.`t` or `db`.* — and
//     there is no object-type keyword at all. So [ast.GrantPrivilegeNode]'s
//     ObjectType is INTERPRETED here and never emitted; the pattern's shape is
//     what says whether the target is a table or a database.
//   - `REVOKE GRANT OPTION FOR priv ON scope FROM r` downgrades a grant in ONE
//     statement: grant_option goes 1 -> 0 and no is_partial_revoke row appears.
//
// These arms used to be `r.notSupported(...)` plus `return nil`. A comment is
// the honest answer for an object ClickHouse has no concept of; it was the
// wrong answer here, because ClickHouse does have roles and grants, so the
// migration reported success while leaving the target's access control
// untouched.
//
// [clickhouserbac.ValidateDeclared] refuses the declarations this file cannot
// render — role attributes, ALL, wildcard scopes, absorption pairs — before a
// server is reached. The refusals below cover what that gate cannot see: a node
// handed straight to Render carries no declaration to validate.
//
// capability.RoleManagement gates all five, as it does on PostgreSQL and as
// capability.Views gates the view visitors in clickhouse.go. The key no longer
// names "PostgreSQL role management" — it names named roles plus GRANT/REVOKE,
// which is a shape ClickHouse has — so a target whose capability set withholds
// it gets the named refusal every other withheld kind gets, and New()'s
// ClickHouse24 preset carries it.

// VisitCreateRole renders CREATE ROLE for ClickHouse.
//
// The IF NOT EXISTS guard is unconditional. [ast.CreateRoleNode] carries no
// IfNotExists field, so the choice is between always and never, and never fails
// a re-applied migration at a statement whose goal is already met. Measured on
// 26.7.3.19, a second `CREATE ROLE dup_probe` answers
//
//	Code: 493. DB::Exception: role `dup_probe`: cannot insert because role
//	`dup_probe` already exists in `local_directory`.
//	(ACCESS_ENTITY_ALREADY_EXISTS)
//
// Nothing but the name is emitted: no attribute of the node has a ClickHouse
// spelling. The attributes that assert a credential or an authority are refused
// rather than dropped, because a dropped PASSWORD leaves an operator believing
// a credential was set. Inherit is the one attribute not refused here: false is
// its Go zero value, so `&ast.CreateRoleNode{Name: "r"}` is indistinguishable at
// this boundary from a node that declared inherit="false". The distinction
// survives one layer up, where [clickhouserbac.ValidateDeclared] reads
// [goschema.Role] and the annotation parser has already defaulted it to true.
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	if !r.capabilities().Has(capability.RoleManagement) {
		r.notSupported("CREATE ROLE", node.Name)
		return nil
	}
	if err := refuseRoleAttributes(node); err != nil {
		return err
	}
	target, err := roleTarget("CREATE ROLE", node.Name)
	if err != nil {
		return err
	}
	r.writeRBACComment(node.Comment)
	r.w.WriteLinef("CREATE ROLE IF NOT EXISTS %s;", target)
	return nil
}

// VisitDropRole renders DROP ROLE for ClickHouse.
//
// The IF EXISTS guard is unconditional for the same reason the create guard is,
// read from the other end: node.IfExists distinguishes a drop that tolerates an
// absent role from one that fails on it, and the failing form only aborts a run
// at a statement whose goal — this role not existing — the server has already
// met.
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	if !r.capabilities().Has(capability.RoleManagement) {
		r.notSupported("DROP ROLE", node.Name)
		return nil
	}
	target, err := roleTarget("DROP ROLE", node.Name)
	if err != nil {
		return err
	}
	r.writeRBACComment(node.Comment)
	r.w.WriteLinef("DROP ROLE IF EXISTS %s;", target)
	return nil
}

// VisitAlterRole refuses, and refuses with an error rather than a comment.
//
// There is no representable alteration. system.roles is (name, id, storage), so
// nothing an [ast.AlterRoleNode] operation describes — PASSWORD, LOGIN,
// SUPERUSER, CREATEDB, CREATEROLE, INHERIT, REPLICATION — exists on a ClickHouse
// role to be altered. The server's own `ALTER ROLE` clauses rename a role or
// attach settings profiles, neither of which this node can express.
//
// Returning nil after writing a comment is what the previous implementation did,
// and it is how a changed declaration silently does nothing: the plan applies,
// the run reports success, and the role on the target still carries whatever it
// carried before. The fail-closed shape is
// mysqllike.ValidateDeclaredRoles' — name the role, name the reason, refuse.
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	// The capability gate comes first because the two answers mean different
	// things. Without RoleManagement the target does not do roles at all, and
	// the named refusal every other withheld kind gets is the right one. With
	// it, the target does do roles and there is simply nothing to alter — that
	// is a defect in the plan, and an error is what stops it.
	if !r.capabilities().Has(capability.RoleManagement) {
		r.notSupported("ALTER ROLE", node.Name)
		return nil
	}
	return fmt.Errorf(
		"%w: clickhouse: ALTER ROLE %s: a ClickHouse role carries no attributes to alter (system.roles is name, id, storage); manage the role's grants instead",
		ptaherr.ErrUnsupportedFeature,
		node.Name,
	)
}

// VisitGrantPrivilege renders GRANT for ClickHouse.
//
// The statement is `GRANT priv[, priv...] ON <scope> TO <role> [WITH GRANT
// OPTION]`. Privileges keep the order they were declared in, because the server
// records one row per privilege and the order carries no meaning it could
// change.
func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	if !r.capabilities().Has(capability.RoleManagement) {
		r.notSupported("GRANT", node.Role)
		return nil
	}
	parts, err := rbacGrantParts("GRANT", node.Role, node.ObjectType, node.ObjectName, node.Privileges)
	if err != nil {
		return err
	}
	option := ""
	if node.WithOption {
		option = " WITH GRANT OPTION"
	}
	r.writeRBACComment(node.Comment)
	r.w.WriteLinef("GRANT %s ON %s TO %s%s;", parts.privileges, parts.scope, parts.role, option)
	return nil
}

// VisitRevokePrivilege renders REVOKE for ClickHouse.
//
// `REVOKE GRANT OPTION FOR ...` is one statement, not a revoke followed by a
// re-grant: measured on both lines it takes grant_option from 1 to 0 and leaves
// no is_partial_revoke row behind, so the downgrade converges. Splitting it
// would leave the target with no grant at all if the second statement failed.
func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	if !r.capabilities().Has(capability.RoleManagement) {
		r.notSupported("REVOKE", node.Role)
		return nil
	}
	parts, err := rbacGrantParts("REVOKE", node.Role, node.ObjectType, node.ObjectName, node.Privileges)
	if err != nil {
		return err
	}
	subject := ""
	if node.GrantOptionFor {
		subject = "GRANT OPTION FOR "
	}
	r.writeRBACComment(node.Comment)
	r.w.WriteLinef("REVOKE %s%s ON %s FROM %s;", subject, parts.privileges, parts.scope, parts.role)
	return nil
}

// grantParts is a grant or revoke reduced to the three fragments both
// statements are built from, each already quoted or validated.
type grantParts struct {
	privileges string
	scope      string
	role       string
}

func rbacGrantParts(operation, role, objectType, objectName string, privileges []string) (grantParts, error) {
	target, err := roleTarget(operation, role)
	if err != nil {
		return grantParts{}, err
	}
	joined, err := joinPrivileges(operation, role, privileges)
	if err != nil {
		return grantParts{}, err
	}
	scope, err := grantScope(operation, role, objectType, objectName)
	if err != nil {
		return grantParts{}, err
	}
	return grantParts{privileges: joined, scope: scope.String(), role: target}, nil
}

// roleTarget quotes the principal a statement names.
//
// [sqlident.Quote] doubles an embedded backtick, so a role named "a`b" stays ONE
// identifier instead of ending the quoted name and continuing as SQL, and a name
// carrying a newline stays inside the quotes rather than starting a line the
// server would read as a statement of its own. Nothing in this file concatenates
// a name into a statement for that reason.
func roleTarget(operation, name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf(
			"%w: clickhouse: %s names no role", ptaherr.ErrUnsupportedFeature, operation)
	}
	return sqlident.Quote(DialectName, name), nil
}

// grantScope reads the ClickHouse scope out of a PostgreSQL-shaped node.
//
// The node names its target the way PostgreSQL does, an object TYPE plus a name,
// and ClickHouse has neither keyword — the shape of the two-part pattern IS the
// object type. The mapping goes through [clickhouserbac.ScopeOf] so that the
// renderer and the declaration gate agree on which patterns exist, instead of
// each one parsing `db.t` its own way.
//
// The default database is deliberately empty. A renderer is offline: `ptah
// schema render --dialect clickhouse` holds no connection and therefore has no
// current database, so an unqualified table name has no resolution here. It is
// refused rather than attached to whichever database the session happens to have
// selected when the migration is applied — a grant is an access-control decision
// and resolving it against the wrong database is not a formatting mistake.
func grantScope(operation, role, objectType, objectName string) (clickhouserbac.Scope, error) {
	grant := goschema.Grant{Role: role}
	switch strings.ToUpper(strings.TrimSpace(objectType)) {
	case "TABLE":
		grant.OnTable = objectName
	case "SCHEMA", "DATABASE":
		grant.OnSchema = objectName
	case "SEQUENCE":
		grant.OnSequence = objectName
	default:
		return clickhouserbac.Scope{}, fmt.Errorf(
			"%w: clickhouse: %s to role %q names object type %q: a ClickHouse grant is scoped to a database or a table",
			ptaherr.ErrUnsupportedFeature, operation, role, objectType)
	}
	scope, err := clickhouserbac.ScopeOf(grant, "")
	if err != nil {
		return clickhouserbac.Scope{}, fmt.Errorf("%w: clickhouse: %w", ptaherr.ErrUnsupportedFeature, err)
	}
	return scope, nil
}

// joinPrivileges renders the privilege list, refusing anything it cannot render
// safely.
//
// A privilege is SYNTAX, not an identifier: `GRANT ` + "`SELECT`" + ` ON ...` is
// a syntax error, so there is no quoting to escape a value with, and the only
// defense against a privilege carrying its own statement is to refuse it.
// ClickHouse spells every privilege it has with letters, digits, spaces and
// underscores — SELECT, ALTER UPDATE, S3, SYSTEM RELOAD CONFIG — so a value
// outside that set is not a privilege this server has, whatever else it is.
// `SELECT(id)` is refused by the same rule that [clickhouserbac] refuses it by
// at declaration time: Ptah manages ClickHouse grants at database and table
// scope, never at column scope.
func joinPrivileges(operation, role string, privileges []string) (string, error) {
	rendered := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		trimmed := strings.TrimSpace(privilege)
		if !isPrivilegeSyntax(trimmed) {
			return "", fmt.Errorf(
				"%w: clickhouse: %s to role %q names privilege %q: a ClickHouse privilege is keyword syntax and cannot be quoted, so only letters, digits, spaces and underscores are rendered",
				ptaherr.ErrUnsupportedFeature, operation, role, privilege)
		}
		rendered = append(rendered, trimmed)
	}
	if len(rendered) == 0 {
		return "", fmt.Errorf(
			"%w: clickhouse: %s to role %q names no privilege",
			ptaherr.ErrUnsupportedFeature, operation, role)
	}
	return strings.Join(rendered, ", "), nil
}

func isPrivilegeSyntax(privilege string) bool {
	if privilege == "" {
		return false
	}
	for _, char := range privilege {
		switch {
		case char >= 'a' && char <= 'z':
		case char >= 'A' && char <= 'Z':
		case char >= '0' && char <= '9':
		case char == ' ' || char == '_':
		default:
			return false
		}
	}
	return true
}

// refuseRoleAttributes reports the attributes a ClickHouse role cannot carry.
//
// They are named together in one sentence so that a role declaring three of them
// is one refusal rather than three runs of the migration.
func refuseRoleAttributes(node *ast.CreateRoleNode) error {
	attributes := []struct {
		name     string
		declared bool
	}{
		{"password", node.Password != ""},
		{"login", node.Login},
		{"superuser", node.Superuser},
		{"createdb", node.CreateDB},
		{"createrole", node.CreateRole},
		{"replication", node.Replication},
	}
	declared := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		if attribute.declared {
			declared = append(declared, attribute.name)
		}
	}
	if len(declared) == 0 {
		return nil
	}
	// The password VALUE is deliberately absent from the message. It is a
	// credential, and this error travels to stderr, into a plan, and into
	// whatever collects them.
	return fmt.Errorf(
		"%w: clickhouse: CREATE ROLE %s declares %s: a ClickHouse role carries no attributes, and Ptah does not manage ClickHouse users",
		ptaherr.ErrUnsupportedFeature,
		node.Name,
		strings.Join(declared, ", "),
	)
}

// writeRBACComment writes a node's comment as one leading `-- ` line.
//
// The line is the only place that sentence can go: ClickHouse cannot store a
// role comment at all, since `CREATE ROLE r COMMENT 'x'` is a syntax error
// (Code 62), so emitting it here is the honest rendering of the field rather
// than a decoration.
//
// Line breaks are folded to single spaces the way mssql's renderUpsertComment
// folds them. A `--` comment ends at the newline, so a two-line comment would
// put its second line into the statement stream, where the server would read it
// as SQL. The text is written rather than formatted for the same class of
// reason: a comment containing a percent verb is prose, not a format string.
func (r *Renderer) writeRBACComment(comment string) {
	folded := strings.Join(strings.Fields(comment), " ")
	if folded == "" {
		return
	}
	r.w.WriteLine("-- " + folded)
}
