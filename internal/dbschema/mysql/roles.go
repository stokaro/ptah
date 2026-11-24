package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/internal/mysqlgrants"
	"ptah.run/internal/mysqlname"
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
		// The configurable Role attributes are PostgreSQL ones, and CREATE ROLE
		// here takes a name and nothing else, so LOGIN, SUPERUSER and the rest
		// are false because the object cannot have them. The discriminator
		// above also establishes that the principal cannot authenticate, so
		// password absence is known rather than a zero value standing in for
		// unread data.
		//
		// Inherit is the one attribute that is true, for the reason the SQL
		// Server, Oracle and ClickHouse readers give. A role granted to a role
		// passes its privileges on whenever the outer role is active, and
		// there is no NOINHERIT to turn that off: `CREATE ROLE r NOINHERIT` is
		// ERROR 1064 on MySQL 8.4, 9.7 and 26.7 and on MariaDB 11.8 and 12.3,
		// and an account whose only active role holds SELECT through a second
		// role reads the table. A read of false would make every declared
		// role differ from itself, because a declaration defaults to true, and
		// the ALTER ROLE that difference asks for does not exist
		// (stokaro/ptah#3890).
		roles = append(roles, catalog.Role{
			Name:          name,
			Inherit:       true,
			PasswordState: catalog.RolePasswordAbsent,
		})
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
// The database, table and column scopes come from the information_schema
// privilege views; [grantQuery] says why those rather than the grant tables. A
// grant on another database is not reported, because the description this
// reader produces is of one database and a grant elsewhere is not part of it.
func (r *Reader) readGrants(ctx context.Context, dbName string) ([]catalog.Grant, error) {
	predicate, err := r.rolePredicate(ctx)
	if err != nil {
		return nil, err
	}
	rolePredicateOn := func(prefix string) string { return qualifyPredicate(predicate, prefix) }
	rows, err := r.db.QueryContext(ctx, grantQuery(rolePredicateOn("u.")), nameBinds(dbName, grantQueryArms)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		var role, objectSchema, object, column, privilege, grantable, objectType string
		if err := rows.Scan(&role, &objectSchema, &object, &column, &privilege, &grantable, &objectType); err != nil {
			return nil, err
		}
		grant := catalog.Grant{
			Role:       role,
			Privilege:  strings.ToUpper(strings.TrimSpace(privilege)),
			ObjectType: objectType,
			WithOption: strings.EqualFold(strings.TrimSpace(grantable), "YES"),
			Column:     column,
		}
		// The schema and the table are kept apart. QualifiedTarget joins them,
		// and handing it a name that was already joined made it quote the whole
		// dotted string as one identifier -- `"mysrc`.`t"` -- which the server
		// refuses to parse.
		//
		// The database is the one this read describes, spelled as the
		// description spells it, not as the row does: every row the query
		// returns is of that database, and on a server that folds names the
		// privilege views spell it in lower case. Measured on MariaDB 11.8.9
		// and 12.3.3 with lower_case_table_names=2, a session that selects R_LC
		// reads DATABASE() = R_LC and TABLE_SCHEMA = r_lc in the privilege
		// views, and grants keyed by r_lc are missing under R_LC and extra
		// under r_lc on every comparison.
		if strings.EqualFold(objectType, "SCHEMA") {
			grant.ObjectName = dbName
		} else {
			grant.Schema = dbName
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
func (r *Reader) readRolesInto(ctx context.Context, schema *catalog.Database, databases []string) error {
	roles, err := r.readRoles(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read roles: %w", err)
		}
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}
	schema.Roles = roles

	grants, err := r.readGrantsIn(ctx, databases)
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

	wide, err := r.readWideAccess(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read global privileges: %w", err)
		}
		// Left nil: the security analysis then says it could not measure a
		// role's reach, rather than measuring it without what is granted on
		// every database.
		wide = nil
	}
	schema.WideAccess = wide

	everywhere, err := r.readWideGrants(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read global privileges: %w", err)
		}
		// Left nil: a declared revoke on a table is then planned, and the
		// server says whether the role held the privilege on it.
		everywhere = nil
	}
	patterns, err := r.readPatternGrants(ctx, databases)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read grants on database patterns: %w", err)
		}
		patterns = nil
	}
	schema.WideGrants, schema.WideGrantsPartial = joinWideGrants(everywhere, patterns)
	return r.readConnectedRole(ctx, schema)
}

// joinWideGrants is the grants on every database and those on a database
// pattern, each nil where its read did not establish it. The join is nil where
// neither read did, and partial where one of them did not.
func joinWideGrants(everywhere, patterns []catalog.WideGrant) (joined []catalog.WideGrant, partial bool) {
	if everywhere == nil && patterns == nil {
		return nil, false
	}
	joined = make([]catalog.WideGrant, 0, len(everywhere)+len(patterns))
	return append(append(joined, everywhere...), patterns...), everywhere == nil || patterns == nil
}

// readPatternGrants reads the table privileges each role holds on each of
// databases through a grant on a database name pattern, such as GRANT SELECT
// ON `cs%`.*, one row per privilege and database the pattern reaches. The
// grant names no database the read describes, so the grants on the database
// leave it out, and a role whose member reads every table there would read as
// holding nothing.
//
// The grant is reported as wide, on the database it reaches: a REVOKE on one
// table takes none of it. Measured on MySQL 8.4.13 and MariaDB 11.8.9, after
// GRANT SELECT ON `wc%`.*, a member reads wc1.t and REVOKE SELECT ON wc1.t
// fails with Error 1147.
//
// MySQL with partial_revokes ON reads "%" and "_" in a database name as the
// characters themselves, so a pattern grant reaches no other database there:
// measured on MySQL 8.4.13, the member above is refused wc1.t. MariaDB has no
// such setting.
//
// The server uses the most specific database row that matches, as
// [mysqlgrants] describes, and with an active role MySQL's choice does not
// follow from specificity. So every row that matches is reported, and a revoke
// under a pattern that another row shadows is refused rather than planned.
//
// The query reads every database row of a role that is not the database's own,
// and [mysqlgrants.Options.MatchesDatabase] decides which of them reach it,
// as the server's privilege check does. SQL LIKE disagrees with that check on
// a multi-byte name and, on MySQL under NO_BACKSLASH_ESCAPES, on an escaped
// wildcard.
func (r *Reader) readPatternGrants(ctx context.Context, databases []string) ([]catalog.WideGrant, error) {
	options, err := mysqlgrants.ReadOptions(ctx, r.db)
	if err != nil {
		return nil, err
	}
	if options.LiteralDatabaseNames {
		return make([]catalog.WideGrant, 0), nil
	}
	predicate, err := r.rolePredicate(ctx)
	if err != nil {
		return nil, err
	}
	patterns := make([]catalog.WideGrant, 0)
	for _, database := range databases {
		inDatabase, err := r.readPatternGrantsOn(ctx, qualifyPredicate(predicate, "u."), database, options)
		if err != nil {
			return nil, err
		}
		patterns = append(patterns, inDatabase...)
	}
	return patterns, nil
}

// readPatternGrantsOn is [readPatternGrants] for one database. A privilege no
// grant on a table names, such as EVENT, is left out, as [readWideGrants]
// leaves it out.
func (r *Reader) readPatternGrantsOn(ctx context.Context, rolePredicate, database string, options mysqlgrants.Options) ([]catalog.WideGrant, error) {
	rows, err := r.db.QueryContext(ctx, patternGrantQuery(rolePredicate), nameBinds(database, patternGrantQueryPredicates)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var patterns []catalog.WideGrant
	for rows.Next() {
		var role, pattern, privilege string
		if err := rows.Scan(&role, &pattern, &privilege); err != nil {
			return nil, err
		}
		privilege = strings.ToUpper(strings.TrimSpace(privilege))
		if !namesATablePrivilege(privilege) || !options.MatchesDatabase(pattern, database) {
			continue
		}
		patterns = append(patterns, catalog.WideGrant{
			Role: role, Privilege: privilege, Grant: privilege + " ON " + quoteMySQLIdentifier(pattern) + ".*", Schema: database,
		})
	}
	return patterns, rows.Err()
}

// namesATablePrivilege reports whether privilege is one a GRANT on a table
// names, as [globalTablePrivileges] lists them.
func namesATablePrivilege(privilege string) bool {
	for _, held := range globalTablePrivileges {
		if held.privilege == privilege {
			return true
		}
	}
	return false
}

// patternGrantQuery projects one row per privilege a role holds through a grant
// on a database other than the one the binds name, whose own rows
// [grantQuery] reads. Which of the rows reach that database is decided in Go;
// see [Reader.readPatternGrants].
func patternGrantQuery(rolePredicate string) string {
	const granteeUser = `REPLACE(SUBSTRING_INDEX(sp.GRANTEE, '@', 1), '''', '')`
	return `
		SELECT ` + granteeUser + ` AS role_name, sp.TABLE_SCHEMA, sp.PRIVILEGE_TYPE
		FROM information_schema.SCHEMA_PRIVILEGES AS sp
		JOIN mysql.user AS u ON u.user = ` + granteeUser + `
		WHERE NOT ` + mysqlname.SameDatabase("sp.TABLE_SCHEMA", "?") + `
			  AND ` + rolePredicate + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY role_name, sp.TABLE_SCHEMA, sp.PRIVILEGE_TYPE`
}

// patternGrantQueryPredicates is the number of predicates [patternGrantQuery]
// binds the database name in, each twice.
const patternGrantQueryPredicates = 1

// globalTablePrivileges pairs each privilege column of mysql.user with the
// privilege a GRANT on a table names the same way. Held globally, each covers
// every table, and a REVOKE on one table cannot take it: measured on MySQL
// 8.4.13 and MariaDB 10.11.19 and 11.8.9, after `GRANT SELECT ON *.* TO r`,
// `REVOKE SELECT ON d.t FROM r` fails with Error 1147 and r keeps reading t.
// A column marked optional exists on one flavor only: Delete_history_priv is
// MariaDB's. Every other column exists on each supported server.
var globalTablePrivileges = []struct {
	column, privilege string
	optional          bool
}{
	{column: "Select_priv", privilege: "SELECT"}, {column: "Insert_priv", privilege: "INSERT"},
	{column: "Update_priv", privilege: "UPDATE"}, {column: "Delete_priv", privilege: "DELETE"},
	{column: "Create_priv", privilege: "CREATE"}, {column: "Drop_priv", privilege: "DROP"},
	{column: "References_priv", privilege: "REFERENCES"}, {column: "Index_priv", privilege: "INDEX"},
	{column: "Alter_priv", privilege: "ALTER"}, {column: "Create_view_priv", privilege: "CREATE VIEW"},
	{column: "Show_view_priv", privilege: "SHOW VIEW"}, {column: "Trigger_priv", privilege: "TRIGGER"},
	{column: "Delete_history_priv", privilege: "DELETE HISTORY", optional: true},
}

// readWideGrants reads the table privileges each role holds on every
// database, the `GRANT SELECT ON *.*` kind, one row per privilege.
//
// The columns of mysql.user are read rather than
// information_schema.USER_PRIVILEGES, which on MariaDB lists no role.
// Measured on MariaDB 10.11.19 and 11.8.9, a role holding `SELECT ON *.*` has
// no row there and Select_priv = 'Y' in mysql.user. PUBLIC is a role on
// MariaDB, so a grant to it is read as well. Delete_history_priv, which MySQL
// lacks, is left out where the server has none. Where any other column is
// missing the account sees only part of mysql.user, and the read returns nil:
// it does not establish the grants on every database.
func (r *Reader) readWideGrants(ctx context.Context) ([]catalog.WideGrant, error) {
	predicate, err := r.rolePredicate(ctx)
	if err != nil {
		return nil, err
	}
	present, err := r.userColumns(ctx)
	if err != nil {
		return nil, err
	}
	var columns []string
	var privileges []string
	for _, held := range globalTablePrivileges {
		switch {
		case present[strings.ToLower(held.column)]:
			columns = append(columns, held.column)
			privileges = append(privileges, held.privilege)
		case !held.optional:
			// information_schema lists only the columns the account may read,
			// so a missing one says the account sees part of mysql.user. A
			// read of the rest cannot say no role holds the privilege
			// everywhere.
			return nil, nil
		}
	}
	rows, err := r.db.QueryContext(ctx, wideGrantQuery(predicate, columns))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	everywhere := make([]catalog.WideGrant, 0)
	for rows.Next() {
		var role string
		held := make([]bool, len(columns))
		targets := []any{&role}
		for index := range held {
			targets = append(targets, &held[index])
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		for index, privilege := range privileges {
			if held[index] {
				everywhere = append(everywhere, catalog.WideGrant{Role: role, Privilege: privilege, Grant: privilege + " ON *.*"})
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return everywhere, nil
}

// wideGrantQuery projects, for each role holding one of the columns, the role
// and whether it holds each column, in order. The columns are names from
// [globalTablePrivileges], never input.
func wideGrantQuery(rolePredicate string, columns []string) string {
	held := make([]string, len(columns))
	for index, column := range columns {
		held[index] = column + " = 'Y'"
	}
	return `
		SELECT user, ` + strings.Join(held, ", ") + `
		FROM mysql.user
		WHERE ` + rolePredicate + `
			  AND user NOT LIKE 'mysql.%'
			  AND (` + strings.Join(held, " OR ") + `)
		ORDER BY user`
}

// userColumns is the set of the columns mysql.user has, in lower case.
func (r *Reader) userColumns(ctx context.Context) (map[string]bool, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'mysql' AND table_name = 'user'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	present := make(map[string]bool)
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		present[strings.ToLower(column)] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return present, nil
}

// readWideAccess reads the privileges each role holds on every database, the
// `GRANT SELECT ON *.*` kind. A description of one database leaves them out,
// because a statement that grants them is not part of that database, but the
// role still reaches every table in it: without this, a role holding global
// SELECT reads the whole database while the grants say it reads nothing.
//
// SELECT is read access, and INSERT, UPDATE or DELETE is write access. Other
// global privileges change no rows and are not reported. The privilege columns
// of mysql.user are read rather than information_schema.USER_PRIVILEGES,
// which on MariaDB lists no role.
func (r *Reader) readWideAccess(ctx context.Context) ([]catalog.WideAccess, error) {
	predicate, err := r.rolePredicate(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT user, Select_priv = 'Y', Insert_priv = 'Y' OR Update_priv = 'Y' OR Delete_priv = 'Y'
		FROM mysql.user
		WHERE `+predicate+`
			  AND user NOT LIKE 'mysql.%'
			  AND (Select_priv = 'Y' OR Insert_priv = 'Y' OR Update_priv = 'Y' OR Delete_priv = 'Y')
		ORDER BY user`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	access := make([]catalog.WideAccess, 0)
	for rows.Next() {
		held := catalog.WideAccess{Source: "global privileges"}
		if err := rows.Scan(&held.Role, &held.Read, &held.Write); err != nil {
			return nil, err
		}
		access = append(access, held)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return access, nil
}

// readConnectedRole records the role this connection runs as. CURRENT_USER()
// answers `user@host`, and a membership names the user alone; a user name can
// hold an `@`, and a host cannot.
func (r *Reader) readConnectedRole(ctx context.Context, schema *catalog.Database) error {
	var account string
	// No row leaves the role unknown, and a plan then covers every membership.
	if err := r.db.QueryRowContext(ctx, `SELECT CURRENT_USER()`).Scan(&account); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to read the connected account: %w", err)
	}
	schema.ConnectedRole = account
	if at := strings.LastIndexByte(account, '@'); at >= 0 {
		schema.ConnectedRole = account[:at]
	}
	return nil
}

// readGrantsIn reads the grants on each of databases, in that order.
func (r *Reader) readGrantsIn(ctx context.Context, databases []string) ([]catalog.Grant, error) {
	var grants []catalog.Grant
	for _, database := range databases {
		inDatabase, err := r.readGrants(ctx, database)
		if err != nil {
			return nil, err
		}
		grants = append(grants, inDatabase...)
	}
	return grants, nil
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
//
// A privilege limited to columns is a row of COLUMN_PRIVILEGES per column and
// no row of TABLE_PRIVILEGES: measured on MySQL 8.4.13 and MariaDB 11.8.9,
// `GRANT SELECT (name) ON t TO r` adds nothing to TABLE_PRIVILEGES. Without
// that view the read holds no such grant, and a plan for a declared one
// grants it again on every run.
func grantQuery(rolePredicate string) string {
	const granteeUser = `REPLACE(SUBSTRING_INDEX(granted.grantee, '@', 1), '''', '')`
	return `
		SELECT ` + granteeUser + ` AS role_name, object_schema, object_name, column_name,
			   privilege, grantable, object_type
		FROM (
			SELECT sp.GRANTEE AS grantee, sp.TABLE_SCHEMA AS object_schema,
				   '' AS object_name, '' AS column_name, sp.PRIVILEGE_TYPE AS privilege,
				   sp.IS_GRANTABLE AS grantable, 'SCHEMA' AS object_type
			FROM information_schema.SCHEMA_PRIVILEGES AS sp
			WHERE ` + mysqlname.SameDatabase("sp.TABLE_SCHEMA", "?") + `
			UNION ALL
			SELECT tp.GRANTEE, tp.TABLE_SCHEMA, tp.TABLE_NAME, '', tp.PRIVILEGE_TYPE,
				   tp.IS_GRANTABLE, 'TABLE'
			FROM information_schema.TABLE_PRIVILEGES AS tp
			WHERE ` + mysqlname.SameDatabase("tp.TABLE_SCHEMA", "?") + `
			UNION ALL
			SELECT cp.GRANTEE, cp.TABLE_SCHEMA, cp.TABLE_NAME, cp.COLUMN_NAME, cp.PRIVILEGE_TYPE,
				   cp.IS_GRANTABLE, 'TABLE'
			FROM information_schema.COLUMN_PRIVILEGES AS cp
			WHERE ` + mysqlname.SameDatabase("cp.TABLE_SCHEMA", "?") + `
		) AS granted
		JOIN mysql.user AS u ON u.user = ` + granteeUser + `
		WHERE ` + rolePredicate + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY role_name, object_schema, object_name, column_name, privilege`
}

// grantQueryArms is the number of privilege views [grantQuery] reads, each
// filtered on the database by one [mysqlname.SameDatabase] predicate.
const grantQueryArms = 3
