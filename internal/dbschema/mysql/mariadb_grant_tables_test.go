package mysql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// inheritingPrivileges answers the role questions of a reader as MariaDB
// 12.3.3 answers them for two roles, member and parent, where parent is
// granted to member, parent holds SELECT on app and on the pattern a%, and
// member holds DELETE on app.t2. The privilege views list parent's grants
// under member as well, as MariaDB lists them; the grant tables list each
// role's own. Every other question goes to next.
func inheritingPrivileges(next dbtest.QueryHandler) dbtest.QueryHandler {
	grantColumns := []string{"role_name", "object_schema", "object_name", "column_name", "privilege", "grantable", "object_type"}
	rowColumns := []string{"role_name", "object_schema", "privilege"}
	answers := []struct {
		marker string
		result dbtest.QueryResult
	}{
		{marker: "column_name = 'is_role'", result: dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{int64(1)}}}},
		{marker: "granted.object_schema, granted.privilege", result: dbtest.QueryResult{Columns: rowColumns, Rows: [][]driver.Value{
			{"parent", "a%", "SELECT"},
			{"parent", "app", "SELECT"},
		}}},
		{marker: "sp.TABLE_SCHEMA, sp.PRIVILEGE_TYPE", result: dbtest.QueryResult{Columns: rowColumns, Rows: [][]driver.Value{
			{"member", "a%", "SELECT"},
			{"member", "app", "SELECT"},
			{"parent", "a%", "SELECT"},
			{"parent", "app", "SELECT"},
		}}},
		{marker: "FROM mysql.tables_priv", result: dbtest.QueryResult{Columns: grantColumns, Rows: [][]driver.Value{
			{"member", "app", "t2", "", "DELETE", "NO", "TABLE"},
			{"parent", "app", "", "", "SELECT", "NO", "SCHEMA"},
		}}},
		{marker: "information_schema.SCHEMA_PRIVILEGES", result: dbtest.QueryResult{Columns: grantColumns, Rows: [][]driver.Value{
			{"member", "app", "", "", "SELECT", "NO", "SCHEMA"},
			{"member", "app", "t2", "", "DELETE", "NO", "TABLE"},
			{"parent", "app", "", "", "SELECT", "NO", "SCHEMA"},
		}}},
		{marker: "Select_priv = 'Y',", result: dbtest.QueryResult{Columns: []string{"user", "read", "write"}}},
		{marker: "table_name = 'role_edges'", result: dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{int64(0)}}}},
		{marker: "table_name = 'roles_mapping'", result: dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{int64(1)}}}},
		{marker: "FROM mysql.roles_mapping", result: dbtest.QueryResult{
			Columns: []string{"role_name", "role_host", "member_name", "member_host", "admin_option"},
			Rows:    [][]driver.Value{{"parent", "", "member", "", int64(0)}},
		}},
		{marker: "FROM mysql.user", result: dbtest.QueryResult{Columns: []string{"user"}, Rows: [][]driver.Value{{"member"}, {"parent"}}}},
	}
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		for _, answer := range answers {
			if strings.Contains(query, answer.marker) {
				return answer.result, nil
			}
		}
		return next(query, args)
	}
}

// TestReader_ReadsTheOwnGrantsOfAMariaDBRole_HappyPath describes each MariaDB
// role with the grants SHOW GRANTS lists for it. MariaDB's privilege views
// list what a role holds through another role as its own: measured on MariaDB
// 11.8.9 and 12.3.3, with parent granted to member, SCHEMA_PRIVILEGES lists
// parent's SELECT on app under member too, and REVOKE SELECT ON app.* FROM
// member fails with Error 1141. Read from the views, member holds a grant it
// does not, both on the database and on the pattern.
func TestReader_ReadsTheOwnGrantsOfAMariaDBRole_HappyPath(t *testing.T) {
	c := qt.New(t)
	session := dbtest.MySQLSession(dbtest.MariaDBVersion, dbtest.MySQLAllGrants,
		inheritingPrivileges(visibleCatalog{selected: "app", tables: []string{"t2"}}.answer))
	db := dbtest.Open(t, session)

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "member", Privilege: "DELETE", ObjectType: "TABLE", Schema: "app", ObjectName: "t2"},
		{Role: "parent", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "app"},
	})
	c.Assert(got.WideGrants, qt.DeepEquals, []catalog.WideGrant{
		{Role: "parent", Privilege: "SELECT", Grant: "SELECT ON `a%`.*", Schema: "app"},
	})
	c.Assert(got.RoleMemberships, qt.DeepEquals, []catalog.RoleMembership{{Role: "parent", Member: "member"}})
}
