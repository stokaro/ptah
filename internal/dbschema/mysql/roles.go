package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
)

// readRoles reports the roles this server holds, and nothing else that lives in
// the same table.
//
// The refusal this replaces said Ptah "cannot read or compare" a MySQL-family
// role. The catalog says otherwise, measured on MySQL 8.4: a role is a row in
// mysql.user, and two columns separate it from an account that can log in.
//
//	user        | host | account_locked | password_expired
//	app_reader  | %    | Y              | Y
//	root        | %    | N              | N
//
// A role is created locked with its password expired and no authentication
// string; a user is not. Reading without that discriminator would report every
// account on the server as a role, and the first plan would then offer to drop
// them (stokaro/ptah#1762).
//
// mysql.user is server-wide rather than per-database, so what comes back is not
// scoped by the connected schema the way a table read is. That is a property of
// the object: a role is a server principal, and there is no per-database one to
// read instead.
func (r *Reader) readRoles(ctx context.Context) ([]catalog.Role, error) {
	predicate, err := r.rolePredicate(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT user
		FROM mysql.user
		WHERE `+predicate+`
		  AND user NOT LIKE 'mysql.%'
		ORDER BY user`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roles []catalog.Role
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		// Every attribute a Role can carry is a PostgreSQL one, and a
		// MySQL-family role carries none of them: CREATE ROLE takes a name and
		// nothing else. Reporting them false is not a default standing in for
		// an unread value -- it is what the object is.
		roles = append(roles, catalog.Role{Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return roles, nil
}

// rolePredicate picks the discriminator this server actually has.
//
// The two engines record the same fact differently, and asking the catalog
// which columns exist is what this reader already does elsewhere rather than
// branching on a dialect name.
//
// MariaDB 11.8 has mysql.user.is_role and NO account_locked column at all --
// `SELECT account_locked FROM mysql.user` is
// `ERROR 1054: Unknown column 'account_locked'`, so a query written for MySQL
// does not degrade there, it fails. MySQL 8.4 has no is_role and marks a role
// locked with its password expired and an empty authentication string.
func (r *Reader) rolePredicate(ctx context.Context) (string, error) {
	var hasIsRole int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'mysql' AND table_name = 'user' AND column_name = 'is_role'`).Scan(&hasIsRole)
	if err != nil {
		return "", err
	}
	if hasIsRole > 0 {
		return "is_role = 'Y'", nil
	}
	return "account_locked = 'Y' AND password_expired = 'Y' AND authentication_string = ''", nil
}

// readGrants reports the privileges the roles hold on the connected database.
//
// Both scopes MySQL accepts come from the information_schema privilege views;
// [grantQuery] says why those rather than the grant tables. A grant on another
// database is not reported, because the description this reader produces is of
// one database and a grant elsewhere is not part of it.
func (r *Reader) readGrants(ctx context.Context, dbName string) ([]catalog.Grant, error) {
	predicate, err := r.rolePredicate(ctx)
	if err != nil {
		return nil, err
	}
	rolePredicateOn := func(prefix string) string { return qualifyPredicate(predicate, prefix) }
	rows, err := r.db.QueryContext(ctx, grantQuery(rolePredicateOn("u.")), dbName, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		var role, objectSchema, object, privilege, grantable, objectType string
		if err := rows.Scan(&role, &objectSchema, &object, &privilege, &grantable, &objectType); err != nil {
			return nil, err
		}
		grant := catalog.Grant{
			Role:       role,
			Privilege:  strings.ToUpper(strings.TrimSpace(privilege)),
			ObjectType: objectType,
			WithOption: strings.EqualFold(strings.TrimSpace(grantable), "YES"),
		}
		// The schema and the table are kept apart. QualifiedTarget joins them,
		// and handing it a name that was already joined made it quote the whole
		// dotted string as one identifier -- `"mysrc`.`t"` -- which the server
		// refuses to parse.
		if strings.EqualFold(objectType, "SCHEMA") {
			grant.ObjectName = objectSchema
		} else {
			grant.Schema = objectSchema
			grant.ObjectName = object
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

// qualifyPredicate prefixes every column name in a role predicate, so the same
// discriminator can be used in a join where mysql.user carries an alias.
func qualifyPredicate(predicate, prefix string) string {
	for _, column := range []string{"is_role", "account_locked", "password_expired", "authentication_string"} {
		predicate = strings.ReplaceAll(predicate, column, prefix+column)
	}
	return predicate
}

// MySQL server error numbers for a read the account is not permitted to make.
//
//   - 1142 ER_TABLEACCESS_DENIED_ERROR: "SELECT command denied to user ... for
//     table 'user'", which is what an account without SELECT on mysql.user gets.
//   - 1143 ER_COLUMNACCESS_DENIED_ERROR, the column-level form of the same
//     refusal.
//   - 1044 ER_DBACCESS_DENIED_ERROR, refused at the database rather than the
//     table.
const (
	errTableAccessDenied    = 1142
	errColumnAccessDenied   = 1143
	errDatabaseAccessDenied = 1044
)

// isRoleReadDenied reports whether the server refused the read for want of a
// privilege, rather than failing it for any other reason.
//
// The distinction is the whole point: a refusal is something to degrade around,
// and everything else is a fault to surface. Matching on the message text would
// blur the two, so this asks the server for its own error number.
func isRoleReadDenied(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case errTableAccessDenied, errColumnAccessDenied, errDatabaseAccessDenied:
		return true
	default:
		return false
	}
}

// readRolesInto fills in the roles and grants, or records that this account was
// not permitted to look.
//
// The preset says whether the SERVER has roles. It says nothing about whether
// the connected ACCOUNT may read them, and those are different questions:
// mysql.user and mysql.tables_priv need a privilege that reading a table does
// not. Failing the whole read over that would mean an account with SELECT on
// its own schema could no longer describe that schema at all -- not because
// anything about its tables changed, but because a kind it may not even declare
// became unreadable (stokaro/ptah#1762).
//
// Recording Role as not described is what makes the degradation safe rather
// than silent, exactly as the ClickHouse reader does for system.roles. The
// comparator refuses to conclude "this role is missing" from a read that admits
// it did not look, so a declared role is reported as an undecided addition
// instead of planned from nothing. Nothing destructive follows either: role and
// grant removals are decided from live rows, and there are none.
func (r *Reader) readRolesInto(ctx context.Context, schema *catalog.Database, dbName string) error {
	roles, err := r.readRoles(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read roles: %w", err)
		}
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}
	schema.Roles = roles

	grants, err := r.readGrants(ctx, dbName)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read grants: %w", err)
		}
		// The pair travels together. A description holding roles but no grants
		// would read as "these roles have no privileges", which is a claim this
		// account could not check.
		schema.Roles = nil
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}
	schema.Grants = grants

	memberships, err := r.readRoleMemberships(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read role memberships: %w", err)
		}
		// The graph travels with the roles for the same reason the grants do: a
		// description holding roles and no memberships reads as "nobody holds
		// these", which is a claim this account could not check.
		schema.Roles = nil
		schema.Grants = nil
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}
	schema.RoleMemberships = memberships
	return nil
}

// membershipTable picks the table this server records the role graph in.
//
// The two engines record the same edge in different tables, and the catalog is
// asked which one exists rather than the dialect name -- the same rule
// rolePredicate follows. MySQL 8.4 has mysql.role_edges with FROM_USER and
// TO_USER; MariaDB 11.8 has mysql.roles_mapping with Role and User, and no
// role_edges at all, so a query written for one does not degrade on the other,
// it fails (stokaro/ptah#1950).
func (r *Reader) membershipTable(ctx context.Context) (query string, found bool, err error) {
	var hasRoleEdges int
	if err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'mysql' AND table_name = 'role_edges'`).Scan(&hasRoleEdges); err != nil {
		return "", false, err
	}
	if hasRoleEdges > 0 {
		return `
			SELECT FROM_USER AS role_name, TO_USER AS member_name,
			       WITH_ADMIN_OPTION = 'Y' AS admin_option
			FROM mysql.role_edges
			ORDER BY FROM_USER, TO_USER`, true, nil
	}

	var hasRolesMapping int
	if err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'mysql' AND table_name = 'roles_mapping'`).Scan(&hasRolesMapping); err != nil {
		return "", false, err
	}
	if hasRolesMapping > 0 {
		return `
			SELECT Role AS role_name, User AS member_name,
			       Admin_option = 'Y' AS admin_option
			FROM mysql.roles_mapping
			WHERE User <> ''
			ORDER BY Role, User`, true, nil
	}
	return "", false, nil
}

// readRoleMemberships reports the role-in-role edges this server holds.
//
// A server old enough to have neither table has no role graph to read, and
// answers an empty list rather than an error: roles arrived in MySQL 8.0 and
// MariaDB 10.0.5, and a 5.7 server is describing a world where the question
// does not exist.
func (r *Reader) readRoleMemberships(ctx context.Context) ([]catalog.RoleMembership, error) {
	query, found, err := r.membershipTable(ctx)
	if err != nil {
		return nil, err
	}
	memberships := make([]catalog.RoleMembership, 0)
	if !found {
		return memberships, nil
	}

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var membership catalog.RoleMembership
		if err := rows.Scan(&membership.Role, &membership.Member, &membership.AdminOption); err != nil {
			return nil, err
		}
		if membership.Role == "" || membership.Member == "" {
			continue
		}
		memberships = append(memberships, membership)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return memberships, nil
}

// grantQuery projects one row per privilege a role holds in one schema.
//
// It is a function rather than a string built at the call site so that a test
// can read the projection it actually runs. The four defects this replaced were
// all in the SQL and all invisible from outside: each produced a description
// that parsed, named real roles, and was wrong about what they hold.
//
// The grantee is `'user'@'host'` in these views, so the user is cut out of it
// to join against mysql.user, which is where the role discriminator lives.
func grantQuery(rolePredicate string) string {
	const granteeUser = `REPLACE(SUBSTRING_INDEX(granted.grantee, '@', 1), '''', '')`
	return `
		SELECT ` + granteeUser + ` AS role_name, object_schema, object_name, privilege,
			   grantable, object_type
		FROM (
			SELECT sp.GRANTEE AS grantee, sp.TABLE_SCHEMA AS object_schema,
				   '' AS object_name, sp.PRIVILEGE_TYPE AS privilege,
				   sp.IS_GRANTABLE AS grantable, 'SCHEMA' AS object_type
			FROM information_schema.SCHEMA_PRIVILEGES AS sp
			WHERE sp.TABLE_SCHEMA = ?
			UNION ALL
			SELECT tp.GRANTEE, tp.TABLE_SCHEMA, tp.TABLE_NAME, tp.PRIVILEGE_TYPE,
				   tp.IS_GRANTABLE, 'TABLE'
			FROM information_schema.TABLE_PRIVILEGES AS tp
			WHERE tp.TABLE_SCHEMA = ?
		) AS granted
		JOIN mysql.user AS u ON u.user = ` + granteeUser + `
		WHERE ` + rolePredicate + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY role_name, object_schema, object_name, privilege`
}
