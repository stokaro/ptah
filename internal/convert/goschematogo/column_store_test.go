package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

func TestRender_ColumnStoreRoundTrip(t *testing.T) {
	c := qt.New(t)
	spec := &ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 8, TTL: &ydbschema.TieredTTL{Column: "at", Tiers: []ydbschema.TTLTier{{Interval: "P1D", ExternalSource: "/local/archive"}, {Interval: "P7D"}}}}}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events", PrimaryKey: []string{"id"}, Facets: must.Must(schemaext.NewFacets(spec))}},
		Fields: []schemamodel.Field{{StructName: "Event", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}, {StructName: "Event", FieldName: "At", Name: "at", Type: "TIMESTAMP"}},
	}
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", string(files[0].Data))
	c.Assert(err, qt.IsNil)
	c.Assert(reparsed.Tables, qt.HasLen, 1)
	got, held, err := schemaext.FacetAs[*ydbschema.DesiredColumnStore](reparsed.Tables[0].Facets, ydbschema.ColumnStoreKind)
	c.Assert(err, qt.IsNil)
	c.Assert(held, qt.IsTrue)
	c.Assert(got, qt.DeepEquals, spec)
}
