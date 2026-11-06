package postgres

// White-box testing required: readOwnerPrivilegesForSchema is unexported, and the
// exported read reaches it only after every other catalog statement a fake would
// have to answer consistently. What is under test is how each row of the statement
// becomes a grant: a privilege the owner no longer holds has to read as a revoke,
// and a schema has no schema of its own. The statement's own answers are measured on
// live servers in integration/dbschema/postgres/owner_privileges_live_test.go.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
)

func TestReadOwnerPrivilegesForSchema_ReportsWhatTheOwnerHoldsAndWhatItRevoked(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{
			Columns: []string{"object_type", "object_name", "rolname", "privilege_type", "grantable"},
			Rows: [][]driver.Value{
				{"SCHEMA", "app", "owner", "CREATE", nil},
				{"SCHEMA", "app", "owner", "USAGE", false},
				{"SEQUENCE", "s", "owner", "USAGE", false},
				{"TABLE", "t", "owner", "SELECT", nil},
				{"TABLE", "t", "owner", "UPDATE", false},
				{"TABLE", "t", "owner", "INSERT", true},
			},
		}, nil
	})
	reader := NewPostgreSQLReader(db.SQL, "public")
	reader.SetSchemas([]string{"app"})

	grants, err := reader.readOwnerPrivilegesForSchema(c.Context(), "app")

	c.Assert(err, qt.IsNil)
	c.Assert(grants, qt.DeepEquals, []catalog.Grant{
		{Role: "owner", Privilege: "CREATE", ObjectType: "SCHEMA", ObjectName: "app", Implicit: true, IsPartialRevoke: true},
		{Role: "owner", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app", Implicit: true},
		{Role: "owner", Privilege: "USAGE", ObjectType: "SEQUENCE", Schema: "app", ObjectName: "s", Implicit: true},
		{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", Schema: "app", ObjectName: "t", Implicit: true, IsPartialRevoke: true},
		{Role: "owner", Privilege: "UPDATE", ObjectType: "TABLE", Schema: "app", ObjectName: "t", Implicit: true},
		{Role: "owner", Privilege: "INSERT", ObjectType: "TABLE", Schema: "app", ObjectName: "t", Implicit: true, WithOption: true},
	})
}

// TestGrantReads_LeaveTheOwnersEntryToTheOwnerRead pins the predicate each read of
// an ACL carries. A branch without it reports the owner's entry as grants, and a
// relation whose ACL was written then reads differently from one whose ACL is NULL.
func TestGrantReads_LeaveTheOwnersEntryToTheOwnerRead(t *testing.T) {
	var query string
	db := dbtest.Open(qt.New(t), func(sent string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		query = sent
		return dbtest.QueryResult{Columns: []string{"a", "b", "c", "d", "e", "f"}}, nil
	})
	reader := NewPostgreSQLReader(db.SQL, "public")

	t.Run("tables, views and materialized views", func(t *testing.T) {
		c := qt.New(t)

		_, err := reader.readTableGrantsForSchema(t.Context(), "public")

		c.Assert(err, qt.IsNil)
		c.Assert(strings.Count(query, "g.grantee <> pg_get_userbyid(c.relowner)"), qt.Equals, 1)
		c.Assert(strings.Count(query, "acl.grantee <> c.relowner"), qt.Equals, 2)
	})
	t.Run("sequences", func(t *testing.T) {
		c := qt.New(t)

		_, err := reader.readSequenceGrantsForSchema(t.Context(), "public")

		c.Assert(err, qt.IsNil)
		c.Assert(query, qt.Contains, "acl.grantee <> c.relowner")
	})
	t.Run("schemas", func(t *testing.T) {
		c := qt.New(t)

		_, err := reader.readSchemaGrantsForSchema(t.Context(), "public")

		c.Assert(err, qt.IsNil)
		c.Assert(query, qt.Contains, "acl.grantee <> n.nspowner")
	})
	t.Run("the role scoping", func(t *testing.T) {
		c := qt.New(t)

		clauses := strings.Join(reader.grantRolesInScopeClauses(), "\n")

		c.Assert(strings.Count(clauses, "WHERE acl.grantee <> c.relowner"), qt.Equals, 2)
		c.Assert(strings.Count(clauses, "WHERE acl.grantee <> s.nspowner"), qt.Equals, 2)
	})
}
