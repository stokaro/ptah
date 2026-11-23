package mssql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mssql"
)

// answeringPermissions scripts the read of sys.database_permissions and
// answers every other catalog read with no rows.
func answeringPermissions(rows [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "sys.database_permissions") || !strings.Contains(query, "COL_NAME") {
			return dbtest.QueryResult{}, nil
		}
		return dbtest.QueryResult{
			Columns: []string{"grantee", "permission_name", "state_desc", "class_desc", "schema_name", "object_name", "column_name", "grantor"},
			Rows:    rows,
		}, nil
	}
}

// TestReader_ReadsColumnPermissions_HappyPath reads a permission on one column
// as a grant limited to that column, and a column REVOKE row under a table
// grant as what it is, a row that withholds the column: read as a grant, it
// would say the role holds SELECT on secret, which a member of the role is
// refused.
func TestReader_ReadsColumnPermissions_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringPermissions([][]driver.Value{
		{"reader", "SELECT", "GRANT", "OBJECT_OR_COLUMN", "dbo", "t", "", "dbo"},
		{"reader", "SELECT", "REVOKE", "OBJECT_OR_COLUMN", "dbo", "t", "secret", "dbo"},
		{"reader", "SELECT", "DENY", "OBJECT_OR_COLUMN", "dbo", "u", "name", "dbo"},
		{"reader", "UPDATE", "GRANT", "OBJECT_OR_COLUMN", "dbo", "u", "id", "dbo"},
		{"reader", "UPDATE", "GRANT_WITH_GRANT_OPTION", "OBJECT_OR_COLUMN", "dbo", "u", "name", "dbo"},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", GrantedBy: "dbo"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", Column: "secret", IsPartialRevoke: true, GrantedBy: "dbo"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "u", Column: "name", IsPartialRevoke: true, GrantedBy: "dbo"},
		{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "u", Column: "id", GrantedBy: "dbo"},
		{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "u", Column: "name", WithOption: true, GrantedBy: "dbo"},
	})
}

// TestReader_ReadsEachGrantorOfAPermission_HappyPath reads the row each
// grantor holds for one permission: the server keeps them apart, and a revoke
// of the grant option with CASCADE takes the rows its grantee granted.
func TestReader_ReadsEachGrantorOfAPermission_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringPermissions([][]driver.Value{
		{"q2", "SELECT", "GRANT_WITH_GRANT_OPTION", "OBJECT_OR_COLUMN", "dbo", "t48", "", "dbo"},
		{"q2v", "SELECT", "GRANT", "OBJECT_OR_COLUMN", "dbo", "t48", "", "dbo"},
		{"q2v", "SELECT", "GRANT", "OBJECT_OR_COLUMN", "dbo", "t48", "", "q2"},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "q2", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t48", WithOption: true, GrantedBy: "dbo"},
		{Role: "q2v", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t48", GrantedBy: "dbo"},
		{Role: "q2v", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t48", GrantedBy: "q2"},
	})
}
