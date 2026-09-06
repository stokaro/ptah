package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// evolvedEnumRows is what PostgreSQL sends for a type whose members were added
// out of order. The rows arrive sorted by a float4 position the reader does not
// ask for -- 0, 1, 1.5, 2 for `queued`, `sent` plus a BEFORE and a preceding
// insert -- so the only thing the reader can go on is the order itself.
var evolvedEnumRows = [][]driver.Value{
	{"delivery_state", "draft"},
	{"delivery_state", "queued"},
	{"delivery_state", "processing"},
	{"delivery_state", "sent"},
}

// enumCatalog answers a full schema read, capturing the enum query on the way
// so the projection can be asserted separately from the values it produces.
func enumCatalog(capturedSQL *string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "SELECT EXISTS") {
			return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}}, nil
		}
		if strings.Contains(query, "has_table_privilege") {
			return dbtest.QueryResult{Columns: []string{"has_table_privilege"}, Rows: [][]driver.Value{{false}}}, nil
		}
		if strings.Contains(query, "pg_enum") {
			*capturedSQL = strings.Join(strings.Fields(query), " ")
			return dbtest.QueryResult{
				Columns: []string{"enum_name", "enum_value"},
				Rows:    evolvedEnumRows,
			}, nil
		}
		return dbtest.QueryResult{Columns: []string{"a", "b", "c", "d", "e", "f", "g", "h"}}, nil
	}
}

// TestReadSchemaContext_EnumQueryOrdersByASortOrderItDoesNotProjectHappyPath is
// the direct guard for stokaro/ptah#2719.
//
// pg_enum.enumsortorder is a float4. Initial members take 1..n and a value
// added with BEFORE or AFTER takes a position between its neighbors, so
// `ADD VALUE 'processing' BEFORE 'sent'` on a two-member type produces 1, 1.5,
// 2. The reader selected that column and scanned it into an int purely to
// discard it, and the fractional position failed the scan -- taking the whole
// schema read down for a type PostgreSQL considers ordinary:
//
//	failed to scan enum: sql: Scan error on column index 2,
//	name "enumsortorder": converting driver.Value type float64 ("1.5")
//	to a int: invalid syntax
//
// The fix is that the value never crosses the wire, so the assertion is on the
// query: ordered BY the column, and not projecting it. A fixture alone would
// not hold that -- re-adding the column and widening both the scan destination
// and the fixture passes every other test in this package, and reintroduces the
// dependency on a value this reader has no use for.
func TestReadSchemaContext_EnumQueryOrdersByASortOrderItDoesNotProjectHappyPath(t *testing.T) {
	c := qt.New(t)

	var enumSQL string
	db := dbtest.Open(c, enumCatalog(&enumSQL))
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	_, err := reader.ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)

	projection, _, found := strings.Cut(enumSQL, " FROM ")
	c.Assert(found, qt.IsTrue, qt.Commentf("enum query: %s", enumSQL))
	c.Assert(projection, qt.Not(qt.Contains), "enumsortorder")

	_, ordering, found := strings.Cut(enumSQL, " ORDER BY ")
	c.Assert(found, qt.IsTrue, qt.Commentf("enum query: %s", enumSQL))
	c.Assert(ordering, qt.Contains, "enumsortorder")
}

// TestReadSchemaContext_EvolvedEnumKeepsTheRowOrderHappyPath is the other half.
// Dropping the column moves the whole ordering promise onto the ORDER BY, so a
// reader that reordered what arrived would now have nothing to correct it.
func TestReadSchemaContext_EvolvedEnumKeepsTheRowOrderHappyPath(t *testing.T) {
	c := qt.New(t)

	var enumSQL string
	db := dbtest.Open(c, enumCatalog(&enumSQL))
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	schema, err := reader.ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.Enums, qt.HasLen, 1)
	c.Assert(schema.Enums[0].Name, qt.Equals, "delivery_state")
	c.Assert(schema.Enums[0].Values, qt.DeepEquals,
		[]string{"draft", "queued", "processing", "sent"})
}
