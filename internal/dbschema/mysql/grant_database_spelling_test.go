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

// foldedPrivileges answers the role questions of a reader as MariaDB answers
// them with lower_case_table_names=2: the session selected R_LC and
// DATABASE() says so, while the privilege views spell the database r_lc. Every
// other question goes to next.
func foldedPrivileges(next dbtest.QueryHandler) dbtest.QueryHandler {
	answers := []struct {
		marker string
		result dbtest.QueryResult
	}{
		{marker: "column_name = 'is_role'", result: dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{int64(1)}}}},
		{marker: "sp.TABLE_SCHEMA, sp.PRIVILEGE_TYPE", result: dbtest.QueryResult{Columns: []string{"role_name", "TABLE_SCHEMA", "PRIVILEGE_TYPE"}}},
		{marker: "information_schema.SCHEMA_PRIVILEGES", result: dbtest.QueryResult{
			Columns: []string{"role_name", "object_schema", "object_name", "column_name", "privilege", "grantable", "object_type"},
			Rows: [][]driver.Value{
				{"lc2_r", "r_lc", "", "", "SELECT", "NO", "SCHEMA"},
				{"lc2_r", "r_lc", "items", "", "SELECT", "NO", "TABLE"},
				{"lc2_r", "r_lc", "items", "name", "INSERT", "NO", "TABLE"},
			},
		}},
		{marker: "Select_priv = 'Y',", result: dbtest.QueryResult{Columns: []string{"user", "read", "write"}}},
		{marker: "FROM mysql.user", result: dbtest.QueryResult{Columns: []string{"user"}, Rows: [][]driver.Value{{"lc2_r"}}}},
		{marker: "table_name = 'role_edges'", result: dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{int64(1)}}}},
		{marker: "FROM mysql.role_edges", result: dbtest.QueryResult{Columns: []string{"role_name", "member_name", "admin_option"}}},
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

// TestReader_QualifiesGrantsWithTheDatabaseItDescribes_HappyPath reads the
// grants of a database under the name the description gives it, not the
// spelling the privilege views use. The description of R_LC holds no table
// under r_lc, so a grant qualified with r_lc names an object the description
// does not hold, and every comparison reports it missing under one spelling and
// extra under the other.
func TestReader_QualifiesGrantsWithTheDatabaseItDescribes_HappyPath(t *testing.T) {
	c := qt.New(t)
	session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
		foldedPrivileges(visibleCatalog{selected: "R_LC", tables: []string{"items"}}.answer))
	db := dbtest.Open(t, session)

	got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "lc2_r", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "R_LC"},
		{Role: "lc2_r", Privilege: "SELECT", ObjectType: "TABLE", Schema: "R_LC", ObjectName: "items"},
		{Role: "lc2_r", Privilege: "INSERT", ObjectType: "TABLE", Schema: "R_LC", ObjectName: "items", Column: "name"},
	})
}
