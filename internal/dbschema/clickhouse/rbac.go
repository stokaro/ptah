package clickhouse

import (
	"fmt"

	"go.5x5.cz/ptah/dbschema/types"
)

// grantsQuery reads the grants this description owns: rows held by a ROLE, on
// the connected database, at database or table scope.
//
// Four properties of this statement are load-bearing, and each was measured on
// live clickhouse-server 24.10.4.191 and 26.7.3.19 rather than read off a
// manual.
//
// The projection names only the eight columns 24.10 has. 26.7's system.grants
// has ten -- user_name, role_name, access_type, access_object, database, table,
// column, is_partial_revoke, grant_option, is_wildcard -- and 24.10 has that
// list without access_object and is_wildcard. A `SELECT *`, or a projection
// naming either of the two, passes everything written against 26.7 and fails
// outright on 24.10, a line internal/capabilityprobe/cells.go declares and CI
// starts. There is deliberately no version branch: the older line's column set
// is a subset of the newer one's, so the statement the oldest supported server
// accepts is accepted by every newer one, and a per-version projection would be
// a second thing to keep correct for an answer it could never give differently.
// [Reader.version] is carried for the diagnostic instead, see [Reader.onServer].
//
// A user's privileges are never read. system.grants holds users and roles in one
// table, discriminated by which of user_name and role_name is NULL. Without
// `user_name IS NULL AND role_name IS NOT NULL` a user's privileges enter the
// described set, and what a description carries is what a plan may revoke: where
// a user and a role share a name -- they are separate namespaces, so ClickHouse
// permits it -- Ptah would revoke from the user. This is the ownership boundary
// the PostgreSQL reader draws through pg_roles, drawn where ClickHouse puts it.
//
// Only the connected database is described. `database = ?` leaves out every
// other database's rows and, because a NULL compares equal to nothing, the
// global `*.*` rows as well. Those grants reach objects no declared schema
// describes; leaving them out of the read is what keeps them out of the
// revocable set, which is why the restriction is in the WHERE clause rather than
// applied to the answer.
//
// Column-scoped rows are left out for the same reason. `GRANT SELECT(id) ON
// db.t` records a row with column='id', a scope Ptah does not model --
// internal/clickhouserbac refuses a declared column privilege -- and reporting
// it without its column would describe a privilege the role does not hold.
//
// ifNull renders each projected string as a non-nullable String, so the scan
// targets are plain Go strings. The filters above already guarantee role_name
// and database are non-NULL; table is genuinely NULL for a database-wide grant,
// and the empty string is how internal/clickhouserbac's Scope spells that scope.
// toString is applied to access_type because the catalog stores the privilege as
// an enum; the name is what it reads back as, upper-cased whatever case the
// GRANT was written in.
//
// The alias on every projected expression is a name of its own rather than the
// source column's, because ClickHouse reads `toString(x) AS x` as a cyclic
// alias and refuses the statement.
const grantsQuery = `
	SELECT
		ifNull(role_name, '') AS grantee,
		toString(access_type) AS privilege,
		ifNull(database, '')  AS database_name,
		ifNull(table, '')     AS table_name,
		is_partial_revoke,
		grant_option
	FROM system.grants
	WHERE user_name IS NULL
	  AND role_name IS NOT NULL
	  AND database = ?
	  AND ifNull(` + columnScopeColumn + `, '') = ''
	ORDER BY grantee, table_name, privilege
`

// columnScopeColumn is system.grants' column-scope column, quoted because
// `column` is a word an unquoted identifier position should not have to argue
// about.
const columnScopeColumn = "`column`"

// rolesQuery reads every role the server has.
//
// system.roles is exactly (name, id, storage) on both measured lines. A
// ClickHouse role carries no attributes at all -- no LOGIN, PASSWORD,
// SUPERUSER, CREATEDB, CREATEROLE, INHERIT or REPLICATION, and
// `CREATE ROLE ... COMMENT 'x'` is a syntax error (Code 62) -- so a name and its
// storage is the whole of what there is to read. The id is not read because
// nothing in Ptah's model can hold it.
//
// The read is over system.roles alone. system.users is never queried, here or
// anywhere else in this reader: it is where ClickHouse keeps auth_params, and
// `SHOW CREATE USER` renders `IDENTIFIED WITH ...`. A description that never
// asks cannot leak a credential into a plan, a log or a diff.
const rolesQuery = `
	SELECT name, storage
	FROM system.roles
	ORDER BY name
`

// configuredRoleStorage is the system.roles.storage value of a role defined in
// the server's configuration files rather than by SQL.
//
// Measured on 26.7.3.19: a role created with CREATE ROLE reports
// 'local_directory', and the manual's other value is 'users_xml' for a role
// declared in users.xml. That container held no users_xml role, so what a
// DROP ROLE against one answers is unmeasured -- which is the argument for
// keeping such a role out of the description rather than for relying on the
// server to refuse. Ptah manages what SQL owns.
const configuredRoleStorage = "users_xml"

// readRBACInto fills the description's roles and grants.
//
// Grants are read first because the roles the description defines are derived
// from them: a role is described exactly when a grant this read describes names
// it, which is the rule the PostgreSQL reader states as "a role is described
// exactly when some other statement in the same description can name it". Taking
// the set from the described grants themselves rather than re-deriving it in SQL
// is what keeps the two from disagreeing about scope -- there is one definition
// of "described", and it is the grant list.
func (r *Reader) readRBACInto(dbName string, schema *types.DBSchema) error {
	grants, err := r.readGrants(dbName)
	if err != nil {
		return err
	}
	described, outOfScope, err := r.readRoles(grants)
	if err != nil {
		return err
	}
	schema.Grants = grants
	schema.Roles = described
	schema.RolesOutOfScope = outOfScope
	return nil
}

// readGrants reads the grants of one database.
//
// A row that subtracts rather than adds is reported rather than dropped:
// `GRANT SELECT ON db.* TO r` followed by `REVOKE SELECT ON db.t FROM r` leaves
// two rows, the second with is_partial_revoke = 1, and dropping it would
// describe a role whose effective privileges are quietly narrower than the
// description says. Ptah plans no such shape; see types.DBGrant.IsPartialRevoke
// for why reporting it is still the reader's job.
func (r *Reader) readGrants(dbName string) ([]types.DBGrant, error) {
	rows, err := r.db.Query(grantsQuery, dbName)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read grants%s: %w", r.onServer(), err)
	}
	defer rows.Close()

	var grants []types.DBGrant
	for rows.Next() {
		var (
			role, privilege, database, table string
			partialRevoke, grantOption       uint8
		)
		if err := rows.Scan(&role, &privilege, &database, &table, &partialRevoke, &grantOption); err != nil {
			return nil, fmt.Errorf("clickhouse: scan grant%s: %w", r.onServer(), err)
		}
		grants = append(grants, types.DBGrant{
			Role:            role,
			Privilege:       privilege,
			ObjectType:      grantObjectType(table),
			Schema:          database,
			ObjectName:      table,
			WithOption:      grantOption != 0,
			IsPartialRevoke: partialRevoke != 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: read grants%s: %w", r.onServer(), err)
	}
	return grants, nil
}

// grantObjectType names the kind of object a live grant row applies to.
//
// ClickHouse has no object-type keyword -- the shape of the two-part pattern IS
// the object type, `db`.`t` against `db`.* -- so the kind is read back off the
// table column rather than off a keyword. Ptah models a ClickHouse database as a
// schema, which is what makes a database-wide grant a SCHEMA grant here.
//
// The database lands in DBGrant.Schema for both kinds, unlike the PostgreSQL
// reader, which leaves Schema empty on a schema grant and puts the schema name
// in ObjectName. internal/clickhouserbac's ScopeOfLive reads the scope out of
// (Schema, ObjectName) in exactly that shape, so a database-wide row keeps its
// database in Schema and reports an empty ObjectName.
func grantObjectType(table string) string {
	if table == "" {
		return "SCHEMA"
	}
	return "TABLE"
}

// readRoles reads system.roles and partitions it into the roles this
// description defines and the roles it deliberately does not.
//
// Two reasons put a role in the second list, and they are different facts:
//
//   - It holds no grant this read described. The description would otherwise
//     create a role nothing else in it refers to, and ClickHouse roles are
//     server-wide, so on a shared server it would name every other tenant's
//     roles as well. That is the rule stokaro/ptah#1267 established for
//     PostgreSQL, applied to the catalog ClickHouse keeps roles in.
//   - It is defined in the server's configuration rather than by SQL
//     (see [configuredRoleStorage]). SQL does not own it, so Ptah does not
//     describe it -- even when it holds a described grant.
//
// The lists partition system.roles, which is the property a comparator depends
// on: "not described" and "not present" stay different answers, so nothing plans
// a CREATE ROLE for a role the server already has. Granting to a role that does
// not exist fails at Code 511 (UNKNOWN_ROLE), so that distinction is what stands
// between a plan and a refused statement.
func (r *Reader) readRoles(grants []types.DBGrant) (described, outOfScope []types.DBRole, err error) {
	rows, err := r.db.Query(rolesQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse: read roles%s: %w", r.onServer(), err)
	}
	defer rows.Close()

	granted := grantedRoleNames(grants)
	for rows.Next() {
		var name, storage string
		if err := rows.Scan(&name, &storage); err != nil {
			return nil, nil, fmt.Errorf("clickhouse: scan role%s: %w", r.onServer(), err)
		}
		role := liveRole(name)
		if storage == configuredRoleStorage || !granted[name] {
			outOfScope = append(outOfScope, role)
			continue
		}
		described = append(described, role)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("clickhouse: read roles%s: %w", r.onServer(), err)
	}
	return described, outOfScope, nil
}

// liveRole renders one system.roles row as the shared role model.
//
// Every attribute types.DBRole carries beyond the name is a PostgreSQL notion
// ClickHouse has no column for, so they stay false -- and internal/clickhouserbac
// refuses a declaration that sets one, rather than dropping it, so the two ends
// agree that the attributes do not exist.
//
// Inherit is the exception and is reported true because it is true: a ClickHouse
// role always inherits from the roles granted to it, there is no NOINHERIT to
// read, and the annotation parser defaults a declared role to inherit=true. A
// live read of false would make every role differ from its own declaration.
func liveRole(name string) types.DBRole {
	return types.DBRole{Name: name, Inherit: true}
}

// grantedRoleNames is the set of roles the described grants name.
func grantedRoleNames(grants []types.DBGrant) map[string]bool {
	names := make(map[string]bool, len(grants))
	for _, grant := range grants {
		names[grant.Role] = true
	}
	return names
}

// onServer names the server version in a diagnostic, when the reader was told
// one.
//
// The RBAC reads are the one place in this reader where a failure is more likely
// to be a fact about the version than about the database: they name catalog
// columns, and system.grants grew two of them between the two release lines this
// repository declares. A bare "unknown identifier" sends a reader looking at the
// schema; the version is what points at the catalog instead. A reader built
// without a version prints no clause rather than an empty pair of quotes.
func (r *Reader) onServer() string {
	if r.version == "" {
		return ""
	}
	return fmt.Sprintf(" on server %q", r.version)
}
