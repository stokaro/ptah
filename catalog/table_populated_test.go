package catalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// TestTablePopulated_HappyPath counts a table as populated when its
// statistics count a row, and when the database reports no usable statistics,
// and as empty for the zero value.
func TestTablePopulated_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name  string
		table catalog.Table
		want  bool
	}{
		{name: "rows counted", table: catalog.Table{EstimatedRows: 3}, want: true},
		{name: "statistics unknown", table: catalog.Table{RowStatsUnknown: true}, want: true},
		{name: "empty", table: catalog.Table{}, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.table.Populated(), qt.Equals, test.want)
		})
	}
}
