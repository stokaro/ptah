package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/sijms/go-ora/v3/network"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/dbschema/types"
)

// Oracle error numbers for a catalog this account may not read.
//
//   - ORA-00942, table or view does not exist, which is what SELECT on
//     DBA_ROLES answers for an account without SELECT_CATALOG_ROLE. Oracle
//     reports an invisible view as absent rather than as forbidden, so this
//     number covers both.
//   - ORA-01031, insufficient privileges, the refusal the same account gets
//     from CREATE ROLE.
//
// There is no ALL_ROLES to fall back on. Measured on 23.26.2.0.0 as the
// ordinary schema owner: `SELECT count(*) FROM all_roles` answers ORA-00942
// naming PTAH.ALL_ROLES, because the view does not exist in any schema.
const (
	errViewNotVisible       = 942
	errInsufficientPrivileg = 1031
)

// isRoleReadDenied reports whether Oracle refused the read for want of a
// privilege, rather than failing it for any other reason.
//
// The distinction is the whole point: a refusal is something to degrade
// around, and everything else is a fault to surface. Matching on the message
// text would blur the two -- and would break the moment a server answers in a
// language other than English -- so this asks the driver for the server's own
// error number.
func isRoleReadDenied(err error) bool {
	var oracleErr *network.OracleError
	if !errors.As(err, &oracleErr) {
		return false
	}
	switch oracleErr.ErrCode {
	case errViewNotVisible, errInsufficientPrivileg:
		return true
	default:
		return false
	}
}

// roleQuery reads the roles this server has that Ptah may own.
//
// ORACLE_MAINTAINED is the column that separates them from the engine's own.
// Measured on a fresh 23.26.2.0.0: DBA_ROLES holds 109 rows, of which 2 answer
// 'N', and both were the ones a test had just created. A name list would go
// stale with every release; this is a catalog fact, and it is the Oracle
// equivalent of what internal/reservedrole does for PostgreSQL and of
// sys.database_principals.is_fixed_role for SQL Server.
//
// DBA_ROLES rather than ALL_ROLES because Oracle has no ALL_ROLES. That is
// what makes the privilege question unavoidable here: there is no unprivileged
// view of the same rows to fall back on.
const roleQuery = `
SELECT r.role, r.authentication_type
FROM dba_roles r
WHERE r.oracle_maintained = 'N'
ORDER BY r.role`

// readRoles reads the roles Ptah may own on this server.
//
// Every attribute but two is absent, and reporting false for it is the only
// truth available rather than a loss: an Oracle role has no LOGIN, no
// SUPERUSER, no CREATEDB, no CREATEROLE and no REPLICATION, because in Oracle
// the thing that logs in and holds those is a USER and not a ROLE. The
// renderer says the same thing from the other side, refusing a declaration
// that asks a role for any of them.
//
// The two that are not false:
//
//   - Inherit, which is true for the same reason as on SQL Server. A grantee
//     of an Oracle role holds that role's privileges while the role is
//     enabled; there is no NOINHERIT to report.
//   - HasPassword, from AUTHENTICATION_TYPE. A role can be IDENTIFIED BY a
//     password, and PASSWORD is the value that says so. NONE is the ordinary
//     case and every role in the measurement above reported it.
func (r *Reader) readRoles(ctx context.Context) ([]types.DBRole, error) {
	rows, err := r.db.QueryContext(ctx, roleQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roles []types.DBRole
	for rows.Next() {
		var role types.DBRole
		var authentication sql.NullString
		if err := rows.Scan(&role.Name, &authentication); err != nil {
			return nil, err
		}
		role.Inherit = true
		role.HasPassword = strings.EqualFold(strings.TrimSpace(authentication.String), "PASSWORD")
		roles = append(roles, role)
	}
	return roles, rows.Err()
}

// grantQuery reads the object privileges granted on the inspected schema's
// objects.
//
// ALL_TAB_PRIVS rather than DBA_TAB_PRIVS, and that is not a smaller version
// of the same thing: ALL_TAB_PRIVS reports every grant on an object the
// connected account OWNS, so scoping it to the inspected schema returns the
// same rows a privileged account sees there. Measured on 23.26.2.0.0 as the
// ordinary owner and as SYSTEM, both answered with the same two rows.
//
// TYPE is filtered rather than mapped through, and the row that makes the
// point is one Oracle creates by itself: granting nothing at all still leaves
//
//	GRANTEE=PUBLIC  TABLE_NAME=PTAH  PRIVILEGE=INHERIT PRIVILEGES  TYPE=USER
//
// in ALL_TAB_PRIVS for every account. It is a grant on the USER object, which
// no declaration can name.
const grantQuery = `
SELECT p.grantee, p.privilege, p.table_name, p.grantable, p.type, p.grantor
FROM all_tab_privs p
WHERE p.table_schema = :1
  AND p.type IN ('TABLE', 'VIEW', 'SEQUENCE')
ORDER BY p.grantee, p.table_name, p.privilege`

// readGrants reads the object privileges held on this schema's objects.
//
// Three kinds of row are deliberately not read, and each would be planned as
// something to take away.
//
//   - A SYSTEM privilege (DBA_SYS_PRIVS): `GRANT CREATE SESSION TO r` names no
//     object, and goschema.Grant has no shape without one -- its target is a
//     table, a schema or a sequence. Reported, it would match no declaration
//     and be planned as a REVOKE of a privilege nobody could have declared.
//   - A role granted to a role (DBA_ROLE_PRIVS), for the same reason: Ptah
//     models privileges held by a role, not membership between two of them.
//   - A privilege on an object kind outside the TYPE filter above -- a
//     procedure, a package, a directory, a user -- which no declaration can
//     name either.
//
// Not reading them means Ptah leaves them alone, which is the only safe answer
// while nothing can declare them. It is not a claim that they do not exist,
// and a reader adding any of the three has to give the comparator a
// declaration shape to compare it against in the same change.
//
// WITH GRANT OPTION is read even though the renderer refuses to emit it,
// because the two answer different questions. Oracle refuses
// `GRANT ... TO <role> WITH GRANT OPTION` with ORA-01926 -- measured on
// 23.26.2.0.0 -- so a declaration asking for one cannot be rendered; a row
// already carrying GRANTABLE='YES' on some other grantee is a fact about the
// server, and dropping it would describe the server as something it is not.
func (r *Reader) readGrants(ctx context.Context) ([]types.DBGrant, error) {
	rows, err := r.db.QueryContext(ctx, grantQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []types.DBGrant
	for rows.Next() {
		var grant types.DBGrant
		var grantable, objectType string
		var grantor sql.NullString
		if err := rows.Scan(&grant.Role, &grant.Privilege, &grant.ObjectName,
			&grantable, &objectType, &grantor); err != nil {
			return nil, err
		}
		grant.Schema = r.schema
		grant.Privilege = strings.ToUpper(strings.TrimSpace(grant.Privilege))
		grant.ObjectType = grantObjectTypeFor(objectType)
		grant.WithOption = strings.EqualFold(strings.TrimSpace(grantable), "YES")
		grant.GrantedBy = strings.TrimSpace(grantor.String)
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// grantObjectTypeFor maps ALL_TAB_PRIVS.TYPE onto the object type the shared
// grant shape names.
//
// A VIEW becomes TABLE rather than staying VIEW, because that is the word the
// declared side carries for one: goschema.Grant spells every relation target
// OnTable, on every dialect, and PostgreSQL's information_schema reports a
// view grant the same way. Keying the two sides differently would make one
// grant into two -- a GRANT planned because the declaration matched nothing,
// and a REVOKE planned because the row matched nothing -- on every run of an
// unchanged schema, which is stokaro/ptah#1232 in a comparator that builds its
// own key.
//
// Only the three types [grantQuery] admits reach this function; anything else
// is not read at all, for the reason [Reader.readGrants] gives.
func grantObjectTypeFor(catalogType string) string {
	switch strings.ToUpper(strings.TrimSpace(catalogType)) {
	case "SEQUENCE":
		return "SEQUENCE"
	default:
		return "TABLE"
	}
}

// readRolesInto fills in the roles and grants, or records that this account was
// not permitted to look.
//
// Role management on Oracle needs a privileged account, and that is a
// privilege rather than a capability. Measured on 23.26.2.0.0 as the ordinary
// schema owner: DBA_ROLES answers ORA-00942, ALL_ROLES does not exist, and
// CREATE ROLE answers ORA-01031. A read that turned any of those into "this
// server has no roles" would be the same mistake stokaro/ptah#1898 collected,
// and a read that FAILED over them would mean an account with SELECT on its
// own schema could no longer describe that schema at all -- over a kind it
// cannot declare either (stokaro/ptah#1762).
//
// Recording Role as not described is what makes the degradation safe rather
// than silent, exactly as the MySQL and ClickHouse readers do. The comparator
// refuses to conclude "this role is missing" from a read that admits it did
// not look, so a declared role is reported as an undecided addition instead of
// planned from nothing.
//
// The pair travels together even though ALL_TAB_PRIVS stays readable when
// DBA_ROLES does not. A description holding grants but no roles would read as
// "these privileges belong to roles that are not there", and the grant
// comparator has no coverage to consult: it decides a REVOKE from live rows
// alone, so those rows must not arrive without the roles they belong to.
func (r *Reader) readRolesInto(ctx context.Context, schema *types.DBSchema) error {
	roles, err := r.readRoles(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read roles: %w", err)
		}
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}

	grants, err := r.readGrants(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read grants: %w", err)
		}
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}

	schema.Roles = roles
	schema.Grants = grants
	return nil
}
