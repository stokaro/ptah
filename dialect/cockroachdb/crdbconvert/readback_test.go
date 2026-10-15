package crdbconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dialect/cockroachdb/crdbconvert"
	"ptah.run/internal/cockroachtype"
)

// TestReadBack_ColumnTypeIsTheDefaultWidth pins CockroachDB's read-back column
// type to the type a column takes at the default integer width.
func TestReadBack_ColumnTypeIsTheDefaultWidth(t *testing.T) {
	for _, declared := range []string{"INT", "INTEGER", "BIGINT", "TEXT"} {
		t.Run(declared, func(t *testing.T) {
			c := qt.New(t)

			stored, ok := crdbconvert.ReadBack{}.ColumnType(declared)

			c.Assert(ok, qt.IsTrue)
			c.Assert(stored, qt.Equals, cockroachtype.Resolve(declared, 0))
		})
	}
}

func TestReadBack_ReportsNoIndexDetail(t *testing.T) {
	c := qt.New(t)

	method, ok := crdbconvert.ReadBack{}.IndexMethod("btree")

	c.Assert(ok, qt.IsFalse)
	c.Assert(method, qt.Equals, "")
	c.Assert(crdbconvert.ReadBack{}.UniqueIndexDetail(catalog.Index{Comment: "x"}), qt.IsFalse)
}
