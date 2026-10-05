package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

func TestRender_ColumnStoreRoundTrip(t *testing.T) {
	c := qt.New(t)
	spec := &ast.YDBColumnTableSpec{HashColumns: []string{"id"}, Partitions: 8, TTL: &ast.YDBTieredTTLSpec{Column: "at", Tiers: []ast.YDBTTLTierSpec{{Interval: "P1D", ExternalSource: "/local/archive"}, {Interval: "P7D"}}}}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events", PrimaryKey: []string{"id"}, YDBColumnTable: spec}},
		Fields: []schemamodel.Field{{StructName: "Event", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}, {StructName: "Event", FieldName: "At", Name: "at", Type: "TIMESTAMP"}},
	}
	files, err := goschematogo.Render(db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource("schema.go", string(files[0].Data))
	c.Assert(err, qt.IsNil)
	c.Assert(reparsed.Tables, qt.HasLen, 1)
	c.Assert(reparsed.Tables[0].YDBColumnTable, qt.DeepEquals, spec)
}
