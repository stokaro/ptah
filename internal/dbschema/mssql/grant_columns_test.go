package mssql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mssql"
)

// answeringPermissions scripts the read of sys.database_permissions and
// answers every other catalog read with no rows. A row starts with the
// grantee's principal_id; 0 is the fixed role public. It ends with the owner
// of the securable, which a row that is not about the owner leaves empty.
func answeringPermissions(rows [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "sys.database_permissions") || !strings.Contains(query, "COL_NAME") {
			return dbtest.QueryResult{}, nil
		}
		return dbtest.QueryResult{
			Columns: []string{"principal_id", "grantee", "permission_name", "state_desc", "class_desc", "schema_name", "object_name", "column_name", "grantor", "owner"},
			Rows:    rows,
		}, nil
	}
}

// TestReader_ReadsColumnPermissions_HappyPath reads a permission on one column
// as a grant limited to that column, and a column REVOKE row under a table
// grant as what it is, a row that withholds the column: read as a grant, it
// would say the role holds SELECT on secret, which a member of the role is
// refused. A DENY row is marked denied as well, and the REVOKE row is not.
func TestReader_ReadsColumnPermissions_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringPermissions([][]driver.Value{
		{int64(5), "reader", "SELECT", "GRANT", "OBJECT_OR_COLUMN", "dbo", "t", "", "dbo", ""},
		{int64(5), "reader", "SELECT", "REVOKE", "OBJECT_OR_COLUMN", "dbo", "t", "secret", "dbo", ""},
		{int64(5), "reader", "SELECT", "DENY", "OBJECT_OR_COLUMN", "dbo", "u", "name", "dbo", ""},
		{int64(5), "reader", "UPDATE", "GRANT", "OBJECT_OR_COLUMN", "dbo", "u", "id", "dbo", ""},
		{int64(5), "reader", "UPDATE", "GRANT_WITH_GRANT_OPTION", "OBJECT_OR_COLUMN", "dbo", "u", "name", "dbo", ""},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", GrantedBy: "dbo"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", Column: "secret", IsPartialRevoke: true, GrantedBy: "dbo"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "u", Column: "name", IsPartialRevoke: true, Denied: true, GrantedBy: "dbo"},
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
		{int64(5), "q2", "SELECT", "GRANT_WITH_GRANT_OPTION", "OBJECT_OR_COLUMN", "dbo", "t48", "", "dbo", ""},
		{int64(5), "q2v", "SELECT", "GRANT", "OBJECT_OR_COLUMN", "dbo", "t48", "", "dbo", ""},
		{int64(5), "q2v", "SELECT", "GRANT", "OBJECT_OR_COLUMN", "dbo", "t48", "", "q2", ""},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "q2", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t48", WithOption: true, GrantedBy: "dbo"},
		{Role: "q2v", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t48", GrantedBy: "dbo"},
		{Role: "q2v", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t48", GrantedBy: "q2"},
	})
}

// TestReader_ReadsTheFixedRolePublicByItsPrincipal_HappyPath names the
// principal with principal_id 0 PUBLIC, as a schema file records the fixed
// role, and every other principal by its own name, so a role a case-sensitive
// database holds as Public stays a role of its own. A name with trailing
// spaces is read: the server compares names with them ignored, so it is the
// principal the bare name addresses.
func TestReader_ReadsTheFixedRolePublicByItsPrincipal_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringPermissions([][]driver.Value{
		{int64(0), "public", "SELECT", "GRANT", "OBJECT_OR_COLUMN", "dbo", "t", "", "dbo", ""},
		{int64(7), "Public", "DELETE", "DENY", "OBJECT_OR_COLUMN", "dbo", "t", "", "dbo", ""},
		{int64(8), "r ", "UPDATE", "DENY", "OBJECT_OR_COLUMN", "dbo", "t", "", "dbo ", "dbo "},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "PUBLIC", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", GrantedBy: "dbo"},
		{Role: "Public", Privilege: "DELETE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", IsPartialRevoke: true, Denied: true, GrantedBy: "dbo"},
		{Role: "r", Privilege: "UPDATE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "t", IsPartialRevoke: true, Denied: true, GrantedBy: "dbo", ObjectOwner: "dbo"},
	})
}

// TestReader_RefusesAnAmbiguousPrincipal_FailurePath refuses a grantee other
// than the fixed role that a comparison would take for another principal: a
// schema reserves PUBLIC for the fixed role, and the SQL Server reader
// refuses leading and non-space trailing white space.
func TestReader_RefusesAnAmbiguousPrincipal_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		grantee string
		wantErr string
	}{
		{name: "PUBLIC", grantee: "PUBLIC", wantErr: `the database principal "PUBLIC" has the name a schema gives the fixed role public, .*`},
		{name: "PUBLIC after a space", grantee: " PUBLIC", wantErr: `the database principal " PUBLIC" has the name a schema gives the fixed role public, .*`},
		{name: "public after a space", grantee: " public", wantErr: `the database principal " public" uses surrounding white space that Ptah does not support on SQL Server; rename the principal`},
		{name: "a role before a tab", grantee: "r\t", wantErr: `the database principal "r\\t" uses surrounding white space that Ptah does not support on SQL Server; rename the principal`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, answeringPermissions([][]driver.Value{
				{int64(9), test.grantee, "DELETE", "DENY", "OBJECT_OR_COLUMN", "dbo", "t", "", "dbo", ""},
			}))

			live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `sqlserver: read grants: unsupported feature: `+test.wantErr)
			c.Assert(live, qt.IsNil)
		})
	}
}

// answeringRole scripts a database role and the connected role with the same
// name, and answers every other catalog read with no rows.
func answeringRole(name string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if query == "SELECT USER_NAME()" {
			return dbtest.QueryResult{Columns: []string{"name"}, Rows: [][]driver.Value{{name}}}, nil
		}
		if !strings.Contains(query, "p.type = 'R' AND p.is_fixed_role = 0") {
			return dbtest.QueryResult{}, nil
		}
		return dbtest.QueryResult{Columns: []string{"name"}, Rows: [][]driver.Value{{name}}}, nil
	}
}

func TestReader_SQLServerRoleIgnoresTrailingSpaces(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringRole("reader "))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Roles, qt.HasLen, 1)
	c.Assert(live.Roles[0].Name, qt.Equals, "reader")
	c.Assert(live.ConnectedRole, qt.Equals, "reader")
}

// TestReader_RefusesAnAmbiguousRole_FailurePath refuses a database role a
// comparison would take for another principal, with no grant to it: a role
// named PUBLIC, which a case-sensitive database accepts beside the fixed role
// public, and a role whose leading white space this reader does not support.
func TestReader_RefusesAnAmbiguousRole_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		role    string
		wantErr string
	}{
		{name: "PUBLIC", role: "PUBLIC", wantErr: `the database principal "PUBLIC" has the name a schema gives the fixed role public, .*`},
		{name: "a role after a space", role: " r", wantErr: `the database principal " r" uses surrounding white space that Ptah does not support on SQL Server; .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, answeringRole(test.role))

			live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `sqlserver: read roles: unsupported feature: `+test.wantErr)
			c.Assert(live, qt.IsNil)
		})
	}
}

// TestReader_ReadsTheOwnerOfEachSecurable_HappyPath reads the owner the server
// names for each row's securable: a procedure given to another principal is
// owned by it and not by the owner of its schema, and a grant run with CONTROL
// on it names that owner as its grantor.
func TestReader_ReadsTheOwnerOfEachSecurable_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringPermissions([][]driver.Value{
		{int64(5), "reader", "EXECUTE", "GRANT", "OBJECT_OR_COLUMN", "dbo", "p", "", "powner", "powner"},
		{int64(5), "reader", "SELECT", "GRANT", "SCHEMA", "", "app", "", "dbo", "app_owner"},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "p", GrantedBy: "powner", ObjectOwner: "powner"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "app", GrantedBy: "dbo", ObjectOwner: "app_owner"},
	})
}

// answeringUnmodeledPermissions scripts the read of the permissions on a
// securable class the schema does not model and answers every other catalog
// read with no rows. A row holds the class, its class_desc, the major_id, the
// permission, its state, the grantee, the type of a principal the
// permission is on, the schema of a type or an XML schema collection, and the
// securable's name.
func answeringUnmodeledPermissions(rows [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "pe.class NOT IN (0, 1, 3)") {
			return dbtest.QueryResult{}, nil
		}
		return dbtest.QueryResult{
			Columns: []string{"class", "class_desc", "major_id", "permission_name", "state_desc", "grantee", "principal_type", "schema_name", "name"},
			Rows:    rows,
		}, nil
	}
}

// TestReader_ReportsAPermissionOnAnUnmodeledClass_HappyPath keeps a permission
// on a role, a user, a type or a certificate out of the grants and reports it
// as the statement that gives it, by the securable's own name: read with the
// object or schema name of its major_id, a grant on a role reads as a grant on
// sysclones. A class the read cannot name is named by its major_id. A
// principal's trailing spaces, which the server ignores, are left out.
func TestReader_ReportsAPermissionOnAnUnmodeledClass_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringUnmodeledPermissions([][]driver.Value{
		{int64(4), "DATABASE_PRINCIPAL", int64(6), "ALTER", "GRANT", "app", "R", "", "auditor"},
		{int64(4), "DATABASE_PRINCIPAL", int64(9), "ALTER", "GRANT", "app", "A", "", "reporting"},
		{int64(4), "DATABASE_PRINCIPAL", int64(7), "IMPERSONATE", "GRANT", "app", "S", "", "worker"},
		{int64(4), "DATABASE_PRINCIPAL", int64(6), "VIEW DEFINITION", "DENY", "app ", "R", "", "auditor "},
		{int64(6), "TYPE", int64(257), "EXECUTE", "GRANT_WITH_GRANT_OPTION", "app", "", "dbo", "money_t"},
		{int64(25), "CERTIFICATE", int64(256), "CONTROL", "GRANT", "app", "", "", "signing"},
		{int64(34), "EXTERNAL_LANGUAGE", int64(65536), "EXECUTE EXTERNAL SCRIPT", "GRANT", "app", "", "", ""},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.Grants, qt.HasLen, 0)
	reason := "Ptah compares permissions on the database, a schema, an object or a column, " +
		"and no permission on another securable class, so no plan grants, denies or revokes it"
	c.Assert(live.UnmodeledObjects, qt.DeepEquals, []catalog.UnmodeledObject{
		{Kind: "permission", Name: "GRANT ALTER ON ROLE::auditor TO app", Reason: reason},
		{Kind: "permission", Name: "GRANT ALTER ON APPLICATION ROLE::reporting TO app", Reason: reason},
		{Kind: "permission", Name: "GRANT IMPERSONATE ON USER::worker TO app", Reason: reason},
		{Kind: "permission", Name: "DENY VIEW DEFINITION ON ROLE::auditor TO app", Reason: reason},
		{Kind: "permission", Name: "GRANT EXECUTE ON TYPE::dbo.money_t TO app WITH GRANT OPTION", Reason: reason},
		{Kind: "permission", Name: "GRANT CONTROL ON CERTIFICATE::signing TO app", Reason: reason},
		{Kind: "permission", Name: "GRANT EXECUTE EXTERNAL SCRIPT ON EXTERNAL_LANGUAGE with major_id 65536 TO app", Reason: reason},
	})
}
