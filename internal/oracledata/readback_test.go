package oracledata_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/oracledata"
	"ptah.run/internal/oracletype"
)

// TestReadBack_ColumnTypeIsTheTypeMap pins Oracle's read-back column type to
// the type map the renderer creates the column with.
func TestReadBack_ColumnTypeIsTheTypeMap(t *testing.T) {
	for _, declared := range []string{"TEXT", "BIGINT", "BOOLEAN", "VARCHAR(20)", "TIMESTAMP"} {
		t.Run(declared, func(t *testing.T) {
			c := qt.New(t)

			stored, ok := oracledata.ReadBack{}.ColumnType(declared)

			c.Assert(ok, qt.IsTrue)
			c.Assert(stored, qt.Equals, oracletype.Map(declared))
		})
	}
}

func TestReadBack_ReportsNoIndexDetail(t *testing.T) {
	c := qt.New(t)

	method, ok := oracledata.ReadBack{}.IndexMethod("BITMAP")

	c.Assert(ok, qt.IsFalse)
	c.Assert(method, qt.Equals, "")
	c.Assert(oracledata.ReadBack{}.UniqueIndexDetail(catalog.Index{Comment: "x"}), qt.IsFalse)
}
