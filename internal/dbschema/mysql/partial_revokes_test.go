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

// partiallyRevoked answers a reader as MySQL 8.4.13 answers it with
// partial_revokes ON for three roles: pv2 holds SELECT and INSERT on every
// database with SELECT revoked from qpr, pvo holds SELECT and UPDATE on every
// database with both revoked from qpr and UPDATE from qpo, and pvu holds
// SELECT on every database. Every other question goes to next.
func partiallyRevoked(next dbtest.QueryHandler) dbtest.QueryHandler {
	columns := []string{
		"Select_priv", "Insert_priv", "Update_priv", "Delete_priv", "Create_priv", "Drop_priv",
		"References_priv", "Index_priv", "Alter_priv", "Create_view_priv", "Show_view_priv", "Trigger_priv",
	}
	present := make([][]driver.Value, 0, len(columns))
	for _, column := range columns {
		present = append(present, []driver.Value{column})
	}
	held := func(role string, privileges ...string) []driver.Value {
		row := []driver.Value{role}
		for _, column := range columns {
			row = append(row, strings.Contains(strings.Join(privileges, ","), column))
		}
		return row
	}
	answers := []struct {
		marker string
		result dbtest.QueryResult
	}{
		{marker: "SHOW VARIABLES WHERE Variable_name IN ('partial_revokes'", result: dbtest.QueryResult{
			Columns: []string{"Variable_name", "Value"},
			Rows:    [][]driver.Value{{"lower_case_table_names", "0"}, {"partial_revokes", "ON"}},
		}},
		{marker: "column_name = 'is_role'", result: dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{int64(0)}}}},
		{marker: "JSON_TABLE(u.User_attributes", result: dbtest.QueryResult{
			Columns: []string{"role_name", "database_name", "name"},
			Rows:    [][]driver.Value{{"pv2", "qpr", "SELECT"}, {"pvo", "qpo", "UPDATE"}, {"pvo", "qpr", "SELECT"}, {"pvo", "qpr", "UPDATE"}},
		}},
		{marker: "Insert_priv = 'Y', Update_priv = 'Y'", result: dbtest.QueryResult{
			Columns: append([]string{"user"}, columns...),
			Rows: [][]driver.Value{
				held("pv2", "Select_priv", "Insert_priv"), held("pvo", "Select_priv", "Update_priv"), held("pvu", "Select_priv"),
			},
		}},
		{marker: "SELECT column_name", result: dbtest.QueryResult{Columns: []string{"column_name"}, Rows: present}},
		{marker: "information_schema.SCHEMA_PRIVILEGES", result: dbtest.QueryResult{
			Columns: []string{"role_name", "object_schema", "object_name", "column_name", "privilege", "grantable", "object_type"},
		}},
		{marker: "table_name = 'role_edges'", result: dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{int64(1)}}}},
		{marker: "FROM mysql.role_edges", result: dbtest.QueryResult{
			Columns: []string{"role_name", "role_host", "member_name", "member_host", "admin_option"},
		}},
		{marker: "FROM mysql.user", result: dbtest.QueryResult{Columns: []string{"user"}, Rows: [][]driver.Value{{"pv2"}, {"pvo"}, {"pvu"}}}},
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

// TestReader_TakesPartialRevokesFromGrantsOnEveryDatabase_HappyPath reads a
// grant on every database less what a partial revoke takes from a database
// the read describes. Measured on MySQL 8.4.13 and 26.10.0 with
// partial_revokes ON, a session with pv2 active is refused with Error 1142 on
// every table of qpr and reads qpo, while mysql.user says pv2 holds SELECT on
// every database. Read without the revoke, pv2 reaches all of qpr, and a
// revoke of SELECT on one of its tables is refused as one the global grant
// undoes.
func TestReader_TakesPartialRevokesFromGrantsOnEveryDatabase_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		selected   any
		databases  []string
		wantGrants []catalog.WideGrant
		wantAccess []catalog.WideAccess
	}{
		{
			name:     "a read of the database the revoke names",
			selected: "qpr",
			wantGrants: []catalog.WideGrant{
				{Role: "pv2", Privilege: "INSERT", Grant: "INSERT ON *.*"},
				{Role: "pvu", Privilege: "SELECT", Grant: "SELECT ON *.*"},
			},
			wantAccess: []catalog.WideAccess{
				{Role: "pv2", Write: true, Source: "INSERT ON *.*"},
				{Role: "pvu", Read: true, Source: "SELECT ON *.*"},
			},
		},
		{
			name:      "a read of the server",
			databases: []string{"qpo", "qpr"},
			wantGrants: []catalog.WideGrant{
				{Role: "pv2", Privilege: "SELECT", Grant: "SELECT ON *.*", Schema: "qpo"},
				{Role: "pv2", Privilege: "INSERT", Grant: "INSERT ON *.*"},
				{Role: "pvo", Privilege: "SELECT", Grant: "SELECT ON *.*", Schema: "qpo"},
				{Role: "pvu", Privilege: "SELECT", Grant: "SELECT ON *.*"},
			},
			wantAccess: []catalog.WideAccess{
				{Role: "pv2", Read: true, Source: "SELECT ON *.*", Schema: "qpo"},
				{Role: "pv2", Write: true, Source: "INSERT ON *.*"},
				{Role: "pvo", Read: true, Source: "SELECT ON *.*", Schema: "qpo"},
				{Role: "pvu", Read: true, Source: "SELECT ON *.*"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants,
				visibleCatalog{selected: test.selected, databases: test.databases, tables: []string{"t1"}}.answer)
			db := dbtest.Open(t, partiallyRevoked(session))

			got, err := mysql.NewMySQLReaderWithCapabilities(db.SQL, "", capability.MySQL84()).ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got.WideGrants, qt.DeepEquals, test.wantGrants)
			c.Assert(got.WideAccess, qt.DeepEquals, test.wantAccess)
		})
	}
}
