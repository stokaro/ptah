package postgres

// White-box testing required: readSequenceGrantsForSchema is unexported, and
// the exported read reaches it only after every other catalog statement a fake
// would have to answer consistently. What is under test is that every row the
// statement answers reaches the read, the grant on a sequence a column owns
// beside the one on a standalone sequence, and nothing exported shows a row the
// read drops.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
)

// TestReadSequenceGrantsForSchema_KeepsTheSequenceAColumnOwns reads a grant on
// items_id_seq, which a serial column owns and which the sequence read leaves
// to its column, beside a grant on the standalone order_seq.
func TestReadSequenceGrantsForSchema_KeepsTheSequenceAColumnOwns(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{
			Columns: []string{"grantee", "privilege_type", "schema_name", "object_name", "with_option", "grantor"},
			Rows: [][]driver.Value{
				{"app_reader", "USAGE", "app", "items_id_seq", false, "owner"},
				{"app_reader", "USAGE", "app", "order_seq", false, "owner"},
			},
		}, nil
	})
	reader := NewPostgreSQLReader(db.SQL, "public")
	reader.SetSchemas([]string{"app"})

	grants, err := reader.readSequenceGrantsForSchema(c.Context(), "app")

	c.Assert(err, qt.IsNil)
	c.Assert(grants, qt.DeepEquals, []catalog.Grant{
		{Role: "app_reader", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "items_id_seq", Schema: "app", GrantedBy: "owner"},
		{Role: "app_reader", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "order_seq", Schema: "app", GrantedBy: "owner"},
	})
}
