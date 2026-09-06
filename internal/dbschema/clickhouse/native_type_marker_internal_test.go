package clickhouse

// White-box testing required: the column read is package-local and the marker
// it sets is not reachable through an exported API.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// A type read from ClickHouse's catalog is marked as the target's own --
// stokaro/ptah#2142.
//
// The portable mapping turns a declared DATETIME into DateTime64(3), because a
// schema written for several engines means a timestamp with subsecond
// precision. Applied to a type ClickHouse itself reported, that is a different
// column: DateTime is second precision and four bytes wide, DateTime64(3) is
// millisecond precision and eight. Without the marker a description of this
// database replayed widened.
func TestReadColumnsByTable_MarksTheTypeAsTheTargetsOwn(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringClickHouseTypes)
	reader := NewClickHouseReader(db.SQL, "app")

	columns, err := reader.readColumnsByTable(t.Context(), "app")

	c.Assert(err, qt.IsNil)
	events := columns["events"]
	c.Assert(events, qt.HasLen, 2)
	for _, column := range events {
		c.Assert(column.TypeIsDeclaredText, qt.IsTrue,
			qt.Commentf("column %q", column.Name))
	}
	// The type itself is carried unchanged, which is what the marker protects.
	c.Assert(events[1].DataType, qt.Equals, "DateTime")
}

func answeringClickHouseTypes(_ string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{
		Columns: []string{
			"table", "name", "type", "default_kind", "default_expression",
			"position", "comment", "is_in_primary_key",
		},
		Rows: [][]driver.Value{
			{"events", "id", "Int32", "", "", int64(1), "", uint8(1)},
			{"events", "created_at", "DateTime", "", "", int64(2), "", uint8(0)},
		},
	}, nil
}
