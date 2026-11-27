package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
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
	code, ok := oracleErrorCode(err)
	if !ok {
		return false
	}
	switch code {
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
// equivalent of what internal/pgreservedrole does for PostgreSQL and of
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
//   - PasswordState, from AUTHENTICATION_TYPE. A role can be IDENTIFIED BY a
//     password, and PASSWORD is the value that says so. NONE, EXTERNAL,
//     GLOBAL, and APPLICATION are the documented non-password forms. A NULL
//     or an unknown future value cannot safely establish either state.
func (r *Reader) readRoles(ctx context.Context) ([]catalog.Role, error) {
	rows, err := r.db.QueryContext(ctx, roleQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roles []catalog.Role
	for rows.Next() {
		var role catalog.Role
		var authentication sql.NullString
		if err := rows.Scan(&role.Name, &authentication); err != nil {
			return nil, err
		}
		role.Inherit = true
		switch strings.ToUpper(strings.TrimSpace(authentication.String)) {
		case "PASSWORD":
			role.PasswordState = catalog.RolePasswordPresent
		case "NONE", "EXTERNAL", "GLOBAL", "APPLICATION":
			role.PasswordState = catalog.RolePasswordAbsent
		default:
			role.PasswordState = catalog.RolePasswordUnknown
		}
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
  AND p.type IN ('TABLE', 'VIEW', 'SEQUENCE', 'FUNCTION', 'PROCEDURE')
ORDER BY p.grantee, p.table_name, p.privilege`

// readGrants reads the object privileges held on this schema's objects.
//
// Three kinds of row are deliberately not read, and each would be planned as
// something to take away.
//
//   - A SYSTEM privilege (DBA_SYS_PRIVS): `GRANT CREATE SESSION TO r` names no
//     object, and schemamodel.Grant has no shape without one -- its target is a
//     table, a schema or a sequence. Reported, it would match no declaration
//     and be planned as a REVOKE of a privilege nobody could have declared.
//   - A role granted to a role (DBA_ROLE_PRIVS), for the same reason: Ptah
//     models privileges held by a role, not membership between two of them.
//   - A privilege on an object kind outside the TYPE filter above: a
//     package, a directory, a user. A function or a procedure is inside it:
//     `GRANT EXECUTE ON f TO r` names one the way it names a table, and the
//     declaration carries that grant as a table grant.
//
// Not reading them means Ptah leaves them alone, which is the safe answer for
// a row no declaration can name. It is not a claim that they do not exist,
// and a reader adding any of the three has to give the comparator a
// declaration shape to compare it against in the same change.
//
// A package is the exception, because a declaration can name one:
// `GRANT EXECUTE ON pkg TO r` in a SQL schema file is carried as a table
// grant, as a routine's is. The row it compares with is not read, so every
// apply plans that GRANT again and no plan reports the schema synced.
//
// WITH GRANT OPTION is read even though the renderer refuses to emit it,
// because the two answer different questions. Oracle refuses
// `GRANT ... TO <role> WITH GRANT OPTION` with ORA-01926 -- measured on
// 23.26.2.0.0 -- so a declaration asking for one cannot be rendered; a row
// already carrying GRANTABLE='YES' on some other grantee is a fact about the
// server, and dropping it would describe the server as something it is not.
func (r *Reader) readGrants(ctx context.Context) ([]catalog.Grant, error) {
	rows, err := r.db.QueryContext(ctx, grantQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		var grant catalog.Grant
		var grantable, objectType string
		var grantor sql.NullString
		if err := rows.Scan(&grant.Role, &grant.Privilege, &grant.ObjectName,
			&grantable, &objectType, &grantor); err != nil {
			return nil, err
		}
		grant.Privilege = strings.ToUpper(strings.TrimSpace(grant.Privilege))
		grant.ObjectType = grantObjectTypeFor(objectType)
		grant.WithOption = strings.EqualFold(strings.TrimSpace(grantable), "YES")
		grant.GrantedBy = grantor.String
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// grantObjectTypeFor maps ALL_TAB_PRIVS.TYPE onto the object type the shared
// grant shape names.
//
// A VIEW becomes TABLE rather than staying VIEW, because that is the word the
// declared side carries for one: schemamodel.Grant spells every relation target
// OnTable, on every dialect, and PostgreSQL's information_schema reports a
// view grant the same way. A FUNCTION and a PROCEDURE become TABLE for the
// same reason: Oracle's GRANT names one with no object kind, so the
// declaration carries it OnTable, and the comparison takes what ALL grants on
// it from the routine the catalog reads. Keying the two sides differently would make one
// grant into two -- a GRANT planned because the declaration matched nothing,
// and a REVOKE planned because the row matched nothing -- on every run of an
// unchanged schema, which is stokaro/ptah#1232 in a comparator that builds its
// own key.
//
// Only the types [grantQuery] admits reach this function; anything else is not
// read at all, for the reason [Reader.readGrants] gives.
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
func (r *Reader) readRolesInto(ctx context.Context, schema *catalog.Database) error {
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

	everywhere, partial, err := r.readWideGrants(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read system privileges: %w", err)
		}
		// Left nil: a declared revoke on a table is then planned, and the
		// server says whether the role held the privilege on it.
		return nil
	}
	schema.WideGrants = everywhere
	schema.WideGrantsPartial = partial
	return nil
}

// wideGrantQuery reads the system privileges that give a role or PUBLIC an
// object privilege on every table. Measured on 23.26.3: after
// `GRANT SELECT ANY TABLE TO r`, the owner's `REVOKE SELECT ON t FROM r` fails
// with ORA-01927 and a member of r keeps reading t. Each privilege listed is
// the object privilege of the same name followed by ANY TABLE. The rows are
// read only for the roles [roleQuery] reads, and for PUBLIC.
const wideGrantQuery = `
SELECT p.grantee, p.privilege
FROM dba_sys_privs p
WHERE p.privilege IN ('SELECT ANY TABLE', 'READ ANY TABLE', 'INSERT ANY TABLE', 'UPDATE ANY TABLE',
                      'DELETE ANY TABLE', 'ALTER ANY TABLE', 'FLASHBACK ANY TABLE')
  AND (p.grantee = 'PUBLIC' OR p.grantee IN (SELECT r.role FROM dba_roles r WHERE r.oracle_maintained = 'N'))
ORDER BY p.grantee, p.privilege`

// schemaGrantQuery reads the system privileges that give a role or PUBLIC an
// object privilege on every table of the schema the read describes. Measured
// on 23.26.3: after `GRANT SELECT ANY TABLE ON SCHEMA app TO r`, the owner's
// `REVOKE SELECT ON t FROM r` fails with ORA-01927 and a member of r keeps
// reading t, as under SELECT ANY TABLE. DBA_SCHEMA_PRIVS reports the grant as
// GRANTEE=R, PRIVILEGE=SELECT ANY TABLE, SCHEMA=APP. The privileges and the
// grantees are the ones [wideGrantQuery] reads.
const schemaGrantQuery = `
SELECT p.grantee, p.privilege
FROM dba_schema_privs p
WHERE p.schema = :1
  AND p.privilege IN ('SELECT ANY TABLE', 'READ ANY TABLE', 'INSERT ANY TABLE', 'UPDATE ANY TABLE',
                      'DELETE ANY TABLE', 'ALTER ANY TABLE', 'FLASHBACK ANY TABLE')
  AND (p.grantee = 'PUBLIC' OR p.grantee IN (SELECT r.role FROM dba_roles r WHERE r.oracle_maintained = 'N'))
ORDER BY p.grantee, p.privilege`

// readWideGrants reads the table privileges a role or PUBLIC holds on every
// table through a system privilege, one row each, and then those it holds on
// every table of this schema through a system privilege granted on the
// schema.
//
// The second read is asked only where the catalog has DBA_SCHEMA_PRIVS
// ([capability.CatalogSchemaPrivileges]). Oracle 21 has no such view and
// answers ORA-00942. That is also how Oracle 23 refuses an account that may
// read DBA_SYS_PRIVS and not DBA_SCHEMA_PRIVS, measured on 23.26.3 with the
// two views granted one by one as SYSDBA. A refused second read keeps what the
// first found and reports the read partial: a revoke on a table that SELECT ANY TABLE
// covers is still refused, and one on a table the plan creates is planned.
func (r *Reader) readWideGrants(ctx context.Context) ([]catalog.WideGrant, bool, error) {
	everywhere := make([]catalog.WideGrant, 0)
	err := r.scanWideGrants(ctx, wideGrantQuery, nil, func(grantee, privilege string) {
		everywhere = append(everywhere, catalog.WideGrant{
			Role:      grantee,
			Privilege: strings.TrimSuffix(privilege, " ANY TABLE"),
			Grant:     privilege,
		})
	})
	if err != nil {
		return nil, false, err
	}
	if !r.caps.Has(capability.CatalogSchemaPrivileges) {
		return everywhere, false, nil
	}
	var onSchema []catalog.WideGrant
	err = r.scanWideGrants(ctx, schemaGrantQuery, []any{r.schema}, func(grantee, privilege string) {
		onSchema = append(onSchema, catalog.WideGrant{
			Role:      grantee,
			Privilege: strings.TrimSuffix(privilege, " ANY TABLE"),
			Grant:     privilege + " ON SCHEMA " + r.schema,
			Schema:    r.schema,
		})
	})
	if err != nil {
		if !isRoleReadDenied(err) {
			return nil, false, err
		}
		return everywhere, true, nil
	}
	return append(everywhere, onSchema...), false, nil
}

// scanWideGrants runs a query that answers a grantee and a system privilege
// per row, and hands each row to add.
func (r *Reader) scanWideGrants(ctx context.Context, query string, args []any, add func(grantee, privilege string)) error {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var grantee, privilege string
		if err := rows.Scan(&grantee, &privilege); err != nil {
			return err
		}
		add(grantee, privilege)
	}
	return rows.Err()
}
