package dataorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dataorder"
)

// TestColumnTypes reads the declared types the row renderer needs, from the
// fields of one table and no other.
func TestColumnTypes(t *testing.T) {
	t.Run("the declared type of each column of the table", func(t *testing.T) {
		c := qt.New(t)
		db := &schemamodel.Database{
			Tables: []schemamodel.Table{
				{StructName: "Flag", Name: "flags"},
				{StructName: "Event", Name: "events"},
			},
			Fields: []schemamodel.Field{
				{StructName: "Flag", Name: "code", Type: "VARCHAR(32)"},
				{StructName: "Flag", Name: "seen", Type: "TIMESTAMP"},
				{StructName: "Event", Name: "seen", Type: "DATE"},
			},
		}
		c.Assert(dataorder.ColumnTypes(db, db.Tables[0]), qt.DeepEquals, map[string]string{
			"code": "VARCHAR(32)",
			"seen": "TIMESTAMP",
		})
	})

	t.Run("a nil schema has no types", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(dataorder.ColumnTypes(nil, schemamodel.Table{StructName: "Flag"}), qt.IsNil)
	})
}
