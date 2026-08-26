package postgres

// White-box testing required: readObjectOwners is unexported and the exported
// ReadSchema path reaches it only through a live server. What is under test is
// the shape of the call -- how many arguments a two-branch UNION over the same
// placeholders needs -- which has no observation point in the returned rows.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestReadObjectOwners_PassesOneArgumentPerSchema pins the argument count.
//
// Both halves of the UNION reuse $1..$n rather than continuing the numbering,
// so a placeholder appearing twice is still one parameter. Passing the schema
// list twice -- which is what a reader assembling the query in two halves
// naturally does -- answers `expected 1 arguments, got 2` from the driver, and
// no unit test that scripts result rows can see it. This one asserts the call
// rather than the answer (stokaro/ptah#1950).
func TestReadObjectOwners_PassesOneArgumentPerSchema(t *testing.T) {
	c := qt.New(t)
	var seen []driver.NamedValue
	db := dbtest.Open(c, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(query, qt.Contains, "pg_class")
		c.Assert(query, qt.Contains, "rolcanlogin")
		seen = args
		return dbtest.QueryResult{
			Columns: []string{"kind", "schema_name", "object_name", "owner_name", "owner_can_login"},
			Rows: [][]driver.Value{
				{"r", "public", "users", "app_user", true},
				{"S", "public", "users_id_seq", "app_user", true},
				{"schema", "", "public", "pg_database_owner", false},
			},
		}, nil
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	owners, err := reader.readObjectOwners(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(seen, qt.HasLen, 1)
	// The catalog letters are mapped into the vocabulary the rest of Ptah
	// speaks, so a consumer need not know what relkind 'S' is.
	c.Assert(owners, qt.DeepEquals, []catalog.ObjectOwner{
		{Kind: "table", Schema: "public", Name: "users", Owner: "app_user", OwnerCanLogin: true},
		{Kind: "sequence", Schema: "public", Name: "users_id_seq", Owner: "app_user", OwnerCanLogin: true},
		{Kind: "schema", Name: "public", Owner: "pg_database_owner"},
	})
}

// TestReadObjectOwners_ReadsEveryInspectedSchema is the control on the argument
// count: one schema is the case where passing the list twice and passing it
// once differ by exactly one argument, so a second row proves the count follows
// the list rather than a constant.
func TestReadObjectOwners_ReadsEveryInspectedSchema(t *testing.T) {
	c := qt.New(t)
	var seen []driver.NamedValue
	db := dbtest.Open(c, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		c.Assert(strings.Count(query, "$2") > 0, qt.IsTrue)
		seen = args
		return dbtest.QueryResult{
			Columns: []string{"kind", "schema_name", "object_name", "owner_name", "owner_can_login"},
		}, nil
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
	reader.schemas = []string{"public", "app"}

	owners, err := reader.readObjectOwners(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(seen, qt.HasLen, 2)
	c.Assert(owners, qt.HasLen, 0)
}
