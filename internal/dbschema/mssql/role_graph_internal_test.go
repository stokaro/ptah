package mssql

// White-box testing required: readRoleMemberships and readObjectOwners are
// unexported, and the exported ReadSchema path reaches them only through a live
// server. What is under test is which catalog rows each one asks for and how it
// maps them, neither of which has another observation point.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestReadRoleMemberships_ExcludesTheRolesEveryDatabaseShips pins the role-side
// filter.
//
// db_owner and its siblings exist in every SQL Server database, and `public`
// holds every principal by definition, so reporting either would put a
// membership nobody wrote in front of every analysis. The member side is
// deliberately not filtered the same way (stokaro/ptah#1950).
func TestReadRoleMemberships_ExcludesTheRolesEveryDatabaseShips(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(query, qt.Contains, "sys.database_role_members")
		c.Assert(query, qt.Contains, "role_principal.is_fixed_role = 0")
		c.Assert(query, qt.Contains, "role_principal.name <> 'public'")
		return dbtest.QueryResult{
			Columns: []string{"role_name", "member_name"},
			Rows: [][]driver.Value{
				{"analyst", "alice"},
				{"reader", "alice"},
			},
		}, nil
	})
	reader := NewSQLServerReader(db.SQL, "dbo")

	memberships, err := reader.readRoleMemberships()

	c.Assert(err, qt.IsNil)
	c.Assert(memberships, qt.DeepEquals, []types.DBRoleMembership{
		{Role: "analyst", Member: "alice"},
		{Role: "reader", Member: "alice"},
	})
}

// TestReadObjectOwners_ResolvesTheOwnerAndWhetherItAuthenticates pins the two
// decisions this read makes.
//
// An object with no principal_id is owned by its schema's owner, which on an
// ordinary database is every object -- resolving through COALESCE rather than
// dropping those rows is the difference between an answer and an empty one.
// And OwnerCanLogin asks authentication_type_desc: measured on SQL Server 2025,
// dbo reports INSTANCE while `guest` and a WITHOUT LOGIN user both report NONE,
// so neither is somebody whose password could be held.
func TestReadObjectOwners_ResolvesTheOwnerAndWhetherItAuthenticates(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(query, qt.Contains, "COALESCE(o.principal_id, s.principal_id)")
		// Both halves of the UNION have to ask, not one: a projection that
		// defaulted the flag on either side would report every owner of that
		// half as somebody whose password could be held.
		c.Assert(strings.Count(query, "authentication_type_desc <> 'NONE'"), qt.Equals, 2)
		c.Assert(query, qt.Contains, "sys.schemas")
		return dbtest.QueryResult{
			Columns: []string{"kind", "schema_name", "object_name", "owner_name", "owner_can_login"},
			Rows: [][]driver.Value{
				{"U", "dbo", "users", "dbo", true},
				{"SO", "dbo", "order_seq", "dbo", true},
				{"schema", "", "reporting", "alice", false},
			},
		}, nil
	})
	reader := NewSQLServerReader(db.SQL, "dbo")

	owners, err := reader.readObjectOwners()

	c.Assert(err, qt.IsNil)
	// The catalog's type codes are mapped into Ptah's vocabulary, so a consumer
	// need not know what sys.objects.type 'SO' is.
	c.Assert(owners, qt.DeepEquals, []types.DBObjectOwner{
		{Kind: "table", Schema: "dbo", Name: "users", Owner: "dbo", OwnerCanLogin: true},
		{Kind: "sequence", Schema: "dbo", Name: "order_seq", Owner: "dbo", OwnerCanLogin: true},
		{Kind: "schema", Name: "reporting", Owner: "alice", OwnerCanLogin: false},
	})
}
