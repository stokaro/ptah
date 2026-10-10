package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_IndexInvisible writes an index hidden from the optimizer as
// `invisible="true"`, which the annotation parser reads back
// (stokaro/ptah#3853).
func TestRender_IndexInvisible(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Order", FieldName: "ID", Name: "id", Type: "INT", Primary: true},
			{StructName: "Order", FieldName: "Total", Name: "total", Type: "INT"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Order", Name: "k_total", TableName: "orders", Fields: []string{"total"},
			Comment: "lookup", Invisible: true, KeyBlockSize: 8,
		}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", string(files[0].Data))

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains, `invisible="true"`)
	c.Assert(reparsed.Indexes, qt.HasLen, 1)
	c.Assert(reparsed.Indexes[0].Invisible, qt.IsTrue)
	c.Assert(reparsed.Indexes[0].KeyBlockSize, qt.Equals, uint64(8))
	c.Assert(reparsed.Indexes[0].Comment, qt.Equals, "lookup")
}
