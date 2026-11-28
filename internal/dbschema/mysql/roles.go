package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
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
//
// A MySQL role is an account, and two roles can share a user name on two
// hosts. A declaration names a role by its name, which is the role on host %
// ([mysqlgrants.Options.RoleHost]), so a role on another host is a principal a
// declaration cannot name, and the read leaves it out as it leaves out a
// login. Read by its user alone, 'mdup'@'localhost' and 'mdup'@'%' were one
// role, and the grants of the first were printed as grants to the second:
// measured on MySQL 8.4.13 and 26.10.0, after `GRANT SELECT ON dup_probe.* TO
// 'mdup'@'localhost'`, a session with 'mdup'@'%' active is refused the table
// with Error 1142. A MariaDB role has no host, so the predicate needs none
// there.
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
	return "account_locked = 'Y' AND password_expired = 'Y' AND authentication_string = '' AND host = '%'", nil
}

// readGrants reports the direct privileges held by grantees a declaration can
// name on the connected database. A login is never added to the role list.
//
// The database, table and column scopes come from source: the information_schema
// privilege views on MySQL, where [grantQuery] says why those, and the grant
// tables on MariaDB, where [grantSource] says why those. A grant on another
// database is not reported, because the description this reader produces is
// of one database and a grant elsewhere is not part of it.
func (r *Reader) readGrants(ctx context.Context, dbName string, source grantSource) ([]catalog.Grant, error) {
	query, arms := grantQuery(readGrantee(source)), grantQueryArms
	if source.tables {
		query, arms = mariaDBGrantQuery(readGrantee(source), source.databaseColumns), 1
	}
	rows, err := r.db.QueryContext(ctx, query, nameBinds(dbName, arms)...)
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

// markLoginGrants marks each grant whose grantee is no role of roles as a
// login's ([catalog.Grant.Login]). On MySQL and MariaDB a grantee on host %
// is a role or a login, and the read reads both: a declared grant to a login
// it holds is then not planned again, and its rollback does not revoke what the
// login held before. Marked, a login's grant is left out of a description and
// out of what a comparison removes: measured on MySQL 8.4.13 and 26.10.0 and
// MariaDB 11.8.9 and 12.3.3, after CREATE USER app and GRANT SELECT, INSERT ON
// rva.* TO app, a comparison of rva with a database holding the same tables
// otherwise plans REVOKE INSERT ON rva.* FROM app, and `ptah db read` prints
// the GRANT with no statement creating app. MariaDB's PUBLIC is every
// account's grantee and no login. It reads as PUBLIC in upper case, and a
// login may be named public: MariaDB 11.8.9 and 12.3.3 accept CREATE USER
// 'public'@'%', so the name is matched exactly, as [withoutPublic] matches it.
func markLoginGrants(grants []catalog.Grant, roles []catalog.Role, mariaDB bool) []catalog.Grant {
	isRole := make(map[string]bool, len(roles))
	for _, role := range roles {
		isRole[role.Name] = true
	}
	for index := range grants {
		grantee := grants[index].Role
		everyAccount := mariaDB && grantee == publicGrantee
		grants[index].Login = !isRole[grantee] && !everyAccount
	}
	return grants
}

// readGrantee is the accounts whose privileges the read reads, as a predicate
// over mysql.user aliased u: the grantees a declaration names, every role and
// every login on host %. The grants on a database, the global privileges, the
// rows on database name patterns and the partial revokes are each read for
// the same accounts, so a login a role holds passes on everything the read
// knows of it. Measured on MySQL 8.4.13 and 26.10.0, after CREATE USER legacy
// and GRANT SELECT ON *.* TO legacy, a session with a role that holds legacy
// active reads every table, and after a REVOKE on one table from legacy,
// legacy still reads it.
func readGrantee(source grantSource) string {
	if source.tables {
		return mariaDBDeclaredGrantee
	}
	return "u.host = '%'"
}

// A MariaDB bare grantee names the role when one has that name, and otherwise
// the login on host %. A login that shares a role's name must not contribute
// its privileges to the role. Account names compare by bytes, including case.
const mariaDBDeclaredGrantee = `(u.is_role = 'Y' OR (u.host = '%' AND NOT EXISTS (
	SELECT 1 FROM mysql.user AS named_role
	WHERE named_role.is_role = 'Y' AND CAST(named_role.user AS BINARY) = CAST(u.user AS BINARY)
)))`

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
// instead of planned from nothing. A removal is never inferred from an unread
// grant; an explicit REVOKE is still planned so the server answers for it.
func (r *Reader) readRolesInto(ctx context.Context, account sessionAccount, schema *catalog.Database, databases []string) error {
	// The privilege views check SELECT at mysql's database scope before
	// listing another grantee. SELECT on mysql.user, or even on every grant
	// table separately, proves no such visibility. Keep the roles and grants
	// unknown together instead of accepting a filtered empty result.
	if !account.grants.OnDatabase("mysql").Holds("SELECT") {
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}
	roles, err := r.readRoles(ctx)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read roles: %w", err)
		}
		schema.NotDescribed = schema.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}
	roles = withoutPublic(roles, account.mariaDB)
	schema.Roles = roles
	names := &mysqlgrants.Names{MariaDB: account.mariaDB}
	for _, role := range roles {
		names.Name(mysqlgrants.Account{User: role.Name, Host: mysqlgrants.Options{MariaDB: account.mariaDB}.RoleHost()})
	}

	source, err := r.grantSourceOf(ctx, account)
	if err != nil {
		return fmt.Errorf("failed to read the grant tables: %w", err)
	}
	grants, err := r.readGrantsIn(ctx, databases, source)
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
	schema.Grants = markLoginGrants(grants, roles, account.mariaDB)

	memberships, err := r.readRoleMemberships(ctx, names)
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
	mandatory, err := r.readMandatoryRoles(ctx, names)
	if err != nil {
		return fmt.Errorf("failed to read mandatory roles: %w", err)
	}
	schema.PublicIsRole = !account.mariaDB && slices.ContainsFunc(roles, isPublicRole)
	schema.RoleMemberships = memberships
	mandatory.addTo(schema)

	everywhere, err := r.readWideGrants(ctx, source)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read global privileges: %w", err)
		}
		// Left nil: a declared revoke on a table is then planned, and the
		// server says whether the role held the privilege on it.
		everywhere = nil
	}
	rows, err := r.readDatabaseRows(ctx, source)
	if err != nil {
		if !isRoleReadDenied(err) {
			return fmt.Errorf("failed to read grants on database patterns: %w", err)
		}
		rows = nil
	}
	everywhere = rows.withoutPartialRevokes(everywhere, databases)
	schema.WideGrants, schema.WideGrantsPartial = joinWideGrants(everywhere, rows.patternGrants(databases))
	schema.WideAccess = wideAccessOf(everywhere, rows.databaseAccess(databases, roles, schema.RoleMemberships))
	schema.SchemaGrantsInWideAccess = schema.WideAccess != nil && rows.statesOwnRows()
	if err := r.readConnectedRole(ctx, schema, names); err != nil {
		return err
	}
	collisions := names.Collisions()
	refuseCollisions(schema, collisions)
	schema.UnreadRoles = unreadCauses(unreadRoles(schema, readLogins(names, collisions)), names, collisions)
	return nil
}

// refuseCollisions leaves out each principal name two accounts read as
// ([mysqlgrants.Names]), and records it as a role the read does not describe.
// A role named `nm@localhost` beside the login 'nm'@'localhost' is the shape:
// a membership of either reads the same, so neither the role's memberships nor
// its privileges can be told from the login's. The role, its grants and its
// access are left out, the connected role is left unknown where it is one of
// them, and the memberships stay, naming a principal the read did not read
// ([unreadRoles]). The rest of the server reads as it is, so one collision
// does not stop a read of a database that has nothing to do with either
// account.
func refuseCollisions(schema *catalog.Database, collisions []mysqlgrants.Collision) {
	for _, collision := range collisions {
		name := collision.Name
		schema.Roles = slices.DeleteFunc(schema.Roles, func(role catalog.Role) bool { return role.Name == name })
		schema.Grants = slices.DeleteFunc(schema.Grants, func(grant catalog.Grant) bool { return grant.Role == name })
		schema.WideGrants = slices.DeleteFunc(schema.WideGrants, func(grant catalog.WideGrant) bool { return grant.Role == name })
		schema.WideAccess = slices.DeleteFunc(schema.WideAccess, func(held catalog.WideAccess) bool {
			return held.Role == name || held.Subject == name
		})
		if schema.ConnectedRole == name {
			schema.ConnectedRole = ""
		}
		schema.NotDescribed = schema.NotDescribed.With(coverage.Object{
			Kind: coverage.Role, Name: name, Reason: coverage.Unresolved, Provenance: coverage.Observed,
		})
	}
}

// isPublicRole reports whether role is named PUBLIC, in any case. MySQL lets
// CREATE ROLE PUBLIC create one, and so does `PuBlIc`; both read as PUBLIC to
// a reader that folds the name as MariaDB's grantee is folded.
func isPublicRole(role catalog.Role) bool {
	return strings.EqualFold(role.Name, publicGrantee)
}

// unreadRoles is each principal a role schema describes holds, or PUBLIC
// holds where it stands for every account, whose privileges the read did not
// read: a role or a login on a host other than
// [mysqlgrants.Options.RoleHost], which MySQL lets a statement grant as a
// role, or a name two accounts read as. Measured on MySQL 26.10.0, after CREATE USER
// 'legacy'@'localhost' ACCOUNT LOCK and GRANT 'legacy'@'localhost' TO a role,
// a session with the role active holds what legacy holds. Its privileges
// reach the member, and the read did not read them, so a reach measured
// without them would report the member narrower than the server makes it: the
// security analysis says so for each role that holds one, and measures the
// others.
//
// A login on host % for which login reports true is read as a role is
// ([readGrantee]), so it is no such principal, and what it holds in turn
// reaches the role that holds it: the walk goes on through it.
func unreadRoles(schema *catalog.Database, login func(name string) bool) []string {
	described := make(map[string]bool, len(schema.Roles)+1)
	if !schema.PublicIsRole {
		described[publicGrantee] = true
	}
	for _, role := range schema.Roles {
		described[role.Name] = true
	}
	queue := slices.Sorted(maps.Keys(described))
	reached := maps.Clone(described)
	var unread []string
	hold := func(name string) {
		switch {
		case reached[name]:
		case login(name):
			reached[name] = true
			queue = append(queue, name)
		default:
			unread = append(unread, name)
		}
	}
	// Every account holds the mandatory roles a read names beside a role
	// named PUBLIC, through no membership it records.
	if every := schema.EveryAccountHoldsUnread; every != nil {
		for _, role := range every.Roles {
			hold(role)
		}
	}
	for len(queue) > 0 {
		member := queue[0]
		queue = queue[1:]
		for _, membership := range schema.RoleMemberships {
			if membership.Member == member {
				hold(membership.Role)
			}
		}
	}
	slices.Sort(unread)
	return slices.Compact(unread)
}

// readLogins reports whether a principal name is a login on host % whose
// privileges the read reads ([readGrantee]): the account names gave the name
// is on host %, and no other account reads as it. Only MySQL lets a
// statement grant a login as a role, and there a login on host % reads as its
// user alone.
func readLogins(names *mysqlgrants.Names, collisions []mysqlgrants.Collision) func(name string) bool {
	return func(name string) bool {
		account, named := names.Account(name)
		return named && account.Host == "%" &&
			!slices.ContainsFunc(collisions, func(collision mysqlgrants.Collision) bool { return collision.Name == name })
	}
}

// unreadCauses gives each principal of unread the cause the read did not
// read its privileges, from the account names gave it: a name two accounts
// read as, or an account on another host, whose privileges the read does not
// read at all.
func unreadCauses(unread []string, names *mysqlgrants.Names, collisions []mysqlgrants.Collision) []catalog.UnreadRole {
	if len(unread) == 0 {
		return nil
	}
	shared := make(map[string][]mysqlgrants.Account, len(collisions))
	for _, collision := range collisions {
		shared[collision.Name] = collision.Accounts
	}
	roles := make([]catalog.UnreadRole, 0, len(unread))
	for _, name := range unread {
		roles = append(roles, catalog.UnreadRole{Name: name, Cause: unreadCause(name, names, shared[name])})
	}
	return roles
}

// unreadCause is [unreadCauses] for one principal.
func unreadCause(name string, names *mysqlgrants.Names, shared []mysqlgrants.Account) string {
	if len(shared) > 0 {
		accounts := make([]string, 0, len(shared))
		for _, account := range shared {
			accounts = append(accounts, account.Grantee())
		}
		return "the accounts " + strings.Join(accounts, " and ") +
			" read as this one name, so the read leaves its privileges out"
	}
	account, named := names.Account(name)
	switch {
	case !named:
		return "the read found no account of this name"
	default:
		return "it is the account " + account.Grantee() + ", and the read reads the privileges of roles and of accounts on host % alone"
	}
}

// publicGrantee is the grantee MariaDB names PUBLIC, whose privileges every
// account holds. MySQL has none, and a read names by it what every account
// holds there: the mandatory roles ([Reader.readMandatoryRoles]).
const publicGrantee = "PUBLIC"

// withoutPublic leaves MariaDB's PUBLIC out of the roles a read describes.
// MariaDB keeps PUBLIC as a role row once something is granted to it, and the
// row stays after the grant is revoked. It is the grantee every account
// holds, not a role a statement can create: CREATE ROLE PUBLIC is Error 1959
// on MariaDB 11.8.9 and 12.3.3, so a role read of it is a checkpoint that
// cannot be applied, and a role with no members to the security analysis.
// Its grants and memberships are still read, as every account's. On MySQL a
// role may be named PUBLIC, and it is a role like any other.
func withoutPublic(roles []catalog.Role, mariaDB bool) []catalog.Role {
	return slices.DeleteFunc(roles, func(role catalog.Role) bool { return mariaDB && role.Name == publicGrantee })
}

// wideAccessOf is the access to the rows of every table that the grants on
// every database and the grants on a database name pattern give, which the
// security analysis reads to measure a role's reach. A description of one
// database leaves both out, because a statement that grants them is not part
// of that database, but the role still reaches every table there: without
// this, a role holding SELECT ON *.* or SELECT ON `ab%`.* reads the whole
// database while its grants say it reads nothing.
//
// A grant on every database gives each role that holds it one read and one
// write entry at most, each named by the privileges that give it, and one of
// each per database where a partial revoke leaves it only some of those a
// read describes ([databaseRows.withoutPartialRevokes]). patterns is
// [databaseRows.databaseAccess]. SELECT is read access, and INSERT, UPDATE and
// DELETE are write access, as [rowaccess.Of] classifies them; the other table
// privileges change no rows and give none.
//
// It is nil where either read did not establish its grants: the security
// analysis then says it could not measure a role's reach, rather than
// measuring it without them.
func wideAccessOf(everywhere []catalog.WideGrant, patterns []catalog.WideAccess) []catalog.WideAccess {
	if everywhere == nil || patterns == nil {
		return nil
	}
	type holding struct{ role, schema string }
	var order []holding
	held := make(map[holding][]string)
	for _, grant := range everywhere {
		key := holding{role: grant.Role, schema: grant.Schema}
		if _, seen := held[key]; !seen {
			order = append(order, key)
		}
		held[key] = append(held[key], grant.Privilege)
	}
	access := make([]catalog.WideAccess, 0, len(everywhere)+len(patterns))
	for _, key := range order {
		for _, entry := range accessOf(key.role, "*.*", held[key]) {
			entry.Schema = key.schema
			access = append(access, entry)
		}
	}
	return append(access, patterns...)
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

// granteeMatchesAccount matches a privilege view's account to mysql.user's
// alias u. Both engines surround the raw user and host with single quotes;
// they do not escape quotes or backslashes within either part. Comparing that
// complete spelling keeps an embedded @ intact and separates accounts on
// different hosts. A byte comparison also keeps case-distinct users apart.
// grantee is a column expression owned by this reader, never SQL input.
func granteeMatchesAccount(grantee string) string {
	return "CAST(" + grantee + " AS BINARY) = CAST(CONCAT('''', u.user, '''@''', u.host, '''') AS BINARY)"
}

// globalTablePrivileges pairs each privilege column of mysql.user with the
// privilege a GRANT on a table names the same way. Held globally, each covers
// every table, and a REVOKE on one table cannot take it: measured on MySQL
// 8.4.13 and MariaDB 10.11.19 and 11.8.9, after `GRANT SELECT ON *.* TO r`,
// `REVOKE SELECT ON d.t FROM r` fails with Error 1147 and r keeps reading t.
// A column marked optional exists on one flavor only: Delete_history_priv is
// MariaDB's. Every other column exists on each supported server.
var globalTablePrivileges = []globalTablePrivilege{
	{column: "Select_priv", privilege: "SELECT"}, {column: "Insert_priv", privilege: "INSERT"},
	{column: "Update_priv", privilege: "UPDATE"}, {column: "Delete_priv", privilege: "DELETE"},
	{column: "Create_priv", privilege: "CREATE"}, {column: "Drop_priv", privilege: "DROP"},
	{column: "References_priv", privilege: "REFERENCES"}, {column: "Index_priv", privilege: "INDEX"},
	{column: "Alter_priv", privilege: "ALTER"}, {column: "Create_view_priv", privilege: "CREATE VIEW"},
	{column: "Show_view_priv", privilege: "SHOW VIEW"}, {column: "Trigger_priv", privilege: "TRIGGER"},
	{column: "Delete_history_priv", privilege: "DELETE HISTORY", optional: true},
}

// globalTablePrivilege is one entry of [globalTablePrivileges].
type globalTablePrivilege struct {
	column, privilege string
	optional          bool
}

// readWideGrants reads the table privileges each account [readGrantee] names
// holds on every database, the `GRANT SELECT ON *.*` kind, one row per
// privilege: every role, and every login on host %.
//
// The columns of mysql.user are read rather than
// information_schema.USER_PRIVILEGES, which on MariaDB lists no role.
// Measured on MariaDB 10.11.19 and 11.8.9, a role holding `SELECT ON *.*` has
// no row there and Select_priv = 'Y' in mysql.user. PUBLIC is a role row on
// MariaDB, so a grant to it is read as well. Delete_history_priv, which MySQL
// lacks, is left out where the server has none. Where any other column is
// missing the account sees only part of mysql.user, and the read returns nil:
// it does not establish the grants on every database.
func (r *Reader) readWideGrants(ctx context.Context, source grantSource) ([]catalog.WideGrant, error) {
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
	rows, err := r.db.QueryContext(ctx, wideGrantQuery(readGrantee(source), columns))
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

// wideGrantQuery projects, for each account grantee names holding one of the
// columns, its user and whether it holds each column, in order. grantee is a
// predicate over mysql.user aliased u ([readGrantee]). The columns are names
// from [globalTablePrivileges], never input.
func wideGrantQuery(grantee string, columns []string) string {
	held := make([]string, len(columns))
	for index, column := range columns {
		held[index] = "u." + column + " = 'Y'"
	}
	return `
		SELECT u.user, ` + strings.Join(held, ", ") + `
		FROM mysql.user AS u
		WHERE ` + grantee + `
			  AND u.user NOT LIKE 'mysql.%'
			  AND (` + strings.Join(held, " OR ") + `)
		ORDER BY u.user`
}

// userColumns is the set of the columns mysql.user has, in lower case.
func (r *Reader) userColumns(ctx context.Context) (map[string]bool, error) {
	return r.grantTableColumns(ctx, "user")
}

// grantTableColumns is the set of the columns the grant table mysql.<table>
// has, in lower case. table is a name this package writes, never input.
func (r *Reader) grantTableColumns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'mysql' AND table_name = '`+table+`'`)
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

// readConnectedRole records the role this connection runs as, named the way a
// membership names it ([mysqlgrants.Names]), so a plan finds the memberships
// of the account that runs it. CURRENT_USER() answers `user@host`.
func (r *Reader) readConnectedRole(ctx context.Context, schema *catalog.Database, names *mysqlgrants.Names) error {
	var account string
	// No row leaves the role unknown, and a plan then covers every membership.
	if err := r.db.QueryRowContext(ctx, `SELECT CURRENT_USER()`).Scan(&account); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to read the connected account: %w", err)
	}
	if account == "" {
		return nil
	}
	schema.ConnectedRole = names.Name(mysqlgrants.SplitAccount(account))
	return nil
}

// readGrantsIn reads the grants on each of databases, in that order.
func (r *Reader) readGrantsIn(ctx context.Context, databases []string, source grantSource) ([]catalog.Grant, error) {
	var grants []catalog.Grant
	for _, database := range databases {
		inDatabase, err := r.readGrants(ctx, database, source)
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
//
// Each end of an edge is an account, a user and a host, and the query reads
// both. A member is a role or a login, and a login can share a role's user
// name on another host. MySQL records the role's host too; a MariaDB role has
// none, so its host reads as empty.
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
			SELECT FROM_USER AS role_name, FROM_HOST AS role_host, TO_USER AS member_name, TO_HOST AS member_host,
			       WITH_ADMIN_OPTION = 'Y' AS admin_option
			FROM mysql.role_edges
			ORDER BY FROM_USER, FROM_HOST, TO_USER, TO_HOST`, true, nil
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
			SELECT Role AS role_name, '' AS role_host, User AS member_name, Host AS member_host,
			       Admin_option = 'Y' AS admin_option
			FROM mysql.roles_mapping
			WHERE User <> ''
			ORDER BY Role, User, Host`, true, nil
	}
	return "", false, nil
}

// readRoleMemberships reports the role-in-role edges this server holds, each
// end named as names names its account.
//
// A server old enough to have neither table has no role graph to read, and
// answers an empty list rather than an error: roles arrived in MySQL 8.0 and
// MariaDB 10.0.5, and a 5.7 server is describing a world where the question
// does not exist.
func (r *Reader) readRoleMemberships(ctx context.Context, names *mysqlgrants.Names) ([]catalog.RoleMembership, error) {
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
		var role, member mysqlgrants.Account
		var membership catalog.RoleMembership
		if err := rows.Scan(&role.User, &role.Host, &member.User, &member.Host, &membership.AdminOption); err != nil {
			return nil, err
		}
		if role.User == "" || member.User == "" {
			continue
		}
		membership.Role, membership.Member = names.Name(role), names.Name(member)
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
// The grantee is an account, including its host. The complete spelling joins
// mysql.user, where the role discriminator lives, and that row supplies the
// role name without interpreting quotes or @ within it.
//
// A privilege limited to columns is a row of COLUMN_PRIVILEGES per column and
// no row of TABLE_PRIVILEGES: measured on MySQL 8.4.13 and MariaDB 11.8.9,
// `GRANT SELECT (name) ON t TO r` adds nothing to TABLE_PRIVILEGES. Without
// that view the read holds no such grant, and a plan for a declared one
// grants it again on every run.
func grantQuery(rolePredicate string) string {
	return `
		SELECT u.user AS role_name, object_schema, object_name, column_name,
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
		JOIN mysql.user AS u ON ` + granteeMatchesAccount("granted.grantee") + `
		WHERE ` + rolePredicate + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY role_name, object_schema, object_name, column_name, privilege`
}

// grantQueryArms is the number of privilege views [grantQuery] reads, each
// filtered on the database by one [mysqlname.SameDatabase] predicate.
const grantQueryArms = 3

// grantSource says where the grants on databases, tables and columns are
// read. The zero value reads the information_schema privilege views, as on
// MySQL.
//
// MariaDB's views list a privilege a role or a user holds through a role as
// its own: measured on MariaDB 11.8.9 and 12.3.3, after GRANT parent TO member
// with parent holding SELECT ON app.*, SCHEMA_PRIVILEGES lists SELECT on app
// for member too, while SHOW GRANTS FOR member lists only the membership and
// REVOKE SELECT ON app.* FROM member fails with Error 1141. The same holds in
// TABLE_PRIVILEGES and COLUMN_PRIVILEGES. A read of the views then describes
// grants member does not hold, a plan built on it revokes what the server has
// nothing of, and a declared grant member does not hold reads as held and is
// never made. MySQL's views list a grantee's own privileges only. So on
// MariaDB the grants are read from mysql.db, mysql.tables_priv and
// mysql.columns_priv, which hold each grantee's own rows.
type grantSource struct {
	// tables is true where the grants are read from the grant tables.
	tables bool
	// databaseColumns are the privilege columns of mysql.db the server has,
	// in the order of [mariaDBDatabasePrivileges].
	databaseColumns []globalTablePrivilege
}

// grantSourceOf is the source of the grants on the server account reads.
func (r *Reader) grantSourceOf(ctx context.Context, account sessionAccount) (grantSource, error) {
	if !account.mariaDB {
		return grantSource{}, nil
	}
	present, err := r.grantTableColumns(ctx, "db")
	if err != nil {
		return grantSource{}, err
	}
	source := grantSource{tables: true}
	for _, held := range mariaDBDatabasePrivileges {
		// A required column is named whether or not information_schema lists
		// it: a server that hides it from the account refuses the read for
		// want of a privilege, and the read degrades as it does for a refusal.
		if !held.optional || present[strings.ToLower(held.column)] {
			source.databaseColumns = append(source.databaseColumns, held)
		}
	}
	return source, nil
}

// mariaDBDatabasePrivileges pairs each privilege column of MariaDB's mysql.db
// with the privilege SCHEMA_PRIVILEGES and SHOW GRANTS name it by, measured on
// MariaDB 11.8.9 and 12.3.3 after GRANT ALL. Grant_priv is the grant option,
// not a privilege. Show_create_routine_priv exists from MariaDB 11.3 on, so
// it is optional.
var mariaDBDatabasePrivileges = []globalTablePrivilege{
	{column: "Select_priv", privilege: "SELECT"}, {column: "Insert_priv", privilege: "INSERT"},
	{column: "Update_priv", privilege: "UPDATE"}, {column: "Delete_priv", privilege: "DELETE"},
	{column: "Create_priv", privilege: "CREATE"}, {column: "Drop_priv", privilege: "DROP"},
	{column: "References_priv", privilege: "REFERENCES"}, {column: "Index_priv", privilege: "INDEX"},
	{column: "Alter_priv", privilege: "ALTER"}, {column: "Create_tmp_table_priv", privilege: "CREATE TEMPORARY TABLES"},
	{column: "Lock_tables_priv", privilege: "LOCK TABLES"}, {column: "Create_view_priv", privilege: "CREATE VIEW"},
	{column: "Show_view_priv", privilege: "SHOW VIEW"}, {column: "Create_routine_priv", privilege: "CREATE ROUTINE"},
	{column: "Alter_routine_priv", privilege: "ALTER ROUTINE"}, {column: "Execute_priv", privilege: "EXECUTE"},
	{column: "Event_priv", privilege: "EVENT"}, {column: "Trigger_priv", privilege: "TRIGGER"},
	{column: "Delete_history_priv", privilege: "DELETE HISTORY"},
	{column: "Show_create_routine_priv", privilege: "SHOW CREATE ROUTINE", optional: true},
}

// mariaDBSetPrivilege pairs a member of the SET a MariaDB grant table keeps
// a table's or a column's privileges in with the privilege TABLE_PRIVILEGES
// and COLUMN_PRIVILEGES name it by.
type mariaDBSetPrivilege struct {
	member, privilege string
}

// mariaDBTablePrivileges are the members of mysql.tables_priv.Table_priv,
// measured on MariaDB 11.8.9 and 12.3.3 after GRANT ALL on a table. Grant is
// the grant option, not a privilege.
var mariaDBTablePrivileges = []mariaDBSetPrivilege{
	{"Select", "SELECT"}, {"Insert", "INSERT"}, {"Update", "UPDATE"}, {"Delete", "DELETE"}, {"Create", "CREATE"},
	{"Drop", "DROP"}, {"References", "REFERENCES"}, {"Index", "INDEX"}, {"Alter", "ALTER"},
	{"Create View", "CREATE VIEW"}, {"Show view", "SHOW VIEW"}, {"Trigger", "TRIGGER"},
	{"Delete versioning rows", "DELETE HISTORY"},
}

// mariaDBColumnPrivileges are the members of mysql.columns_priv.Column_priv.
var mariaDBColumnPrivileges = []mariaDBSetPrivilege{
	{"Select", "SELECT"}, {"Insert", "INSERT"}, {"Update", "UPDATE"}, {"References", "REFERENCES"},
}

// mariaDBPrivilegeRows projects one row per privilege each grantee holds in
// the grant tables, the way the privilege views project it on MySQL: the
// grantee's user and host, the database, the table and the column, empty for
// a grant on a database or a whole table, the privilege, whether it is
// grantable, YES or NO, and SCHEMA or TABLE. A table's grant option makes
// each privilege on its columns grantable too. A row that holds the grant
// option and no privilege is USAGE, grantable, as the views and SHOW GRANTS
// list it; a table row whose columns hold privileges lists none, as
// TABLE_PRIVILEGES does not. Measured on MariaDB 11.8.9 and 12.3.3.
// [mariaDBDatabasePrivilegeRows] is the rows on databases alone.
//
// The text columns take the collation the views give them, so the database
// predicate and the order are the views': the grant tables compare as bytes.
// With lower_case_table_names=2 the views spell r_lc a database a session
// selected as R_LC ([mysqlname.SameDatabase]), and the views read the grant
// tables, so a byte comparison there would find no grant. No such server was
// measured here: on a Linux file system MariaDB 12.3.3 sets the value to 0.
func mariaDBPrivilegeRows(databaseColumns []globalTablePrivilege) string {
	arms := mariaDBDatabaseArms(databaseColumns)
	for _, held := range mariaDBTablePrivileges {
		arms = append(arms, `SELECT tp.User, tp.Host, `+viewText("tp.Db")+`, `+viewText("tp.Table_name")+`, '', '`+held.privilege+`',
			   IF(FIND_IN_SET('Grant', tp.Table_priv) > 0, 'YES', 'NO'), 'TABLE'
		FROM mysql.tables_priv AS tp WHERE FIND_IN_SET('`+held.member+`', tp.Table_priv) > 0`)
	}
	arms = append(arms, `SELECT tp.User, tp.Host, `+viewText("tp.Db")+`, `+viewText("tp.Table_name")+`, '', 'USAGE', 'YES', 'TABLE'
		FROM mysql.tables_priv AS tp WHERE tp.Table_priv = 'Grant' AND tp.Column_priv = ''`)
	for _, held := range mariaDBColumnPrivileges {
		arms = append(arms, `SELECT cp.User, cp.Host, `+viewText("cp.Db")+`, `+viewText("cp.Table_name")+`, `+viewText("cp.Column_name")+`,
			   '`+held.privilege+`', IF(FIND_IN_SET('Grant', COALESCE(tg.Table_priv, '')) > 0, 'YES', 'NO'), 'TABLE'
		FROM mysql.columns_priv AS cp
		LEFT JOIN mysql.tables_priv AS tg
			ON tg.User = cp.User AND tg.Host = cp.Host AND tg.Db = cp.Db AND tg.Table_name = cp.Table_name
		WHERE FIND_IN_SET('`+held.member+`', cp.Column_priv) > 0`)
	}
	return unionAll(arms)
}

// mariaDBDatabasePrivilegeRows is [mariaDBPrivilegeRows] on databases alone.
func mariaDBDatabasePrivilegeRows(databaseColumns []globalTablePrivilege) string {
	return unionAll(mariaDBDatabaseArms(databaseColumns))
}

// unionAll joins the arms of a query.
func unionAll(arms []string) string {
	return strings.Join(arms, "\n\t\t\tUNION ALL\n\t\t\t")
}

// mariaDBDatabaseArms are the arms of [mariaDBPrivilegeRows] on databases:
// one per privilege column of mysql.db, and one for a row holding the grant
// option alone.
func mariaDBDatabaseArms(databaseColumns []globalTablePrivilege) []string {
	arms := make([]string, 0, len(databaseColumns)+1)
	none := make([]string, 0, len(databaseColumns))
	for _, held := range databaseColumns {
		arms = append(arms, `SELECT d.User AS grantee_user, d.Host AS grantee_host, `+viewText("d.Db")+` AS object_schema,
				   '' AS object_name, '' AS column_name, '`+held.privilege+`' AS privilege,
				   IF(d.Grant_priv = 'Y', 'YES', 'NO') AS grantable, 'SCHEMA' AS object_type
			FROM mysql.db AS d WHERE d.`+held.column+` = 'Y'`)
		none = append(none, "d."+held.column+" <> 'Y'")
	}
	arms = append(arms, `SELECT d.User, d.Host, `+viewText("d.Db")+`, '', '', 'USAGE', 'YES', 'SCHEMA'
			FROM mysql.db AS d WHERE d.Grant_priv = 'Y' AND `+strings.Join(none, " AND "))
	return arms
}

// viewText is column of a grant table compared and sorted as the privilege
// views compare their text, which is utf8mb3_general_ci on MariaDB 11.8.9 and
// 12.3.3.
func viewText(column string) string {
	return "CONVERT(" + column + " USING utf8mb3) COLLATE utf8mb3_general_ci"
}

// mariaDBGrantQuery is [grantQuery] read from MariaDB's grant tables, as
// [grantSource] says why, with one [mysqlname.SameDatabase] predicate on the
// database.
func mariaDBGrantQuery(rolePredicate string, databaseColumns []globalTablePrivilege) string {
	return `
		SELECT u.user AS role_name, object_schema, object_name, column_name,
			   privilege, grantable, object_type
		FROM (
			` + mariaDBPrivilegeRows(databaseColumns) + `
		) AS granted
		JOIN mysql.user AS u ON u.user = granted.grantee_user AND u.host = granted.grantee_host
		WHERE ` + mysqlname.SameDatabase("granted.object_schema", "?") + `
			  AND ` + rolePredicate + `
			  AND u.user NOT LIKE 'mysql.%'
		ORDER BY role_name, object_schema, object_name, column_name, privilege`
}
