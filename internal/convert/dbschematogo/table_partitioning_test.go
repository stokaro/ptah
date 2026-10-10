package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// A YDB table's settings, as the reader reports them, reach the declaration
// built from the database as the YDB owner's declared facet, sharing nothing
// with the read, so `ptah introspect` writes them back and a comparison of
// the two finds nothing to change.
func TestConvert_CarriesATablesPartitioning(t *testing.T) {
	c := qt.New(t)
	settings := ydbschema.TablePartitioning{ByLoad: new(true), MinPartitions: 4, KeyBloomFilter: new(true)}
	observed := &ydbschema.ObservedTablePartitioning{TablePartitioning: settings.Clone()}
	facets := must.Must(must.Must(schemaext.NewFacets(observed)).WithTargetScope(ydbschema.TablePartitioningKind, platform.YDB))
	database := &catalog.Database{Tables: []catalog.Table{{Name: "items", Type: "TABLE", Facets: facets,
		Columns: []catalog.Column{{Name: "id", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "NO",
			IsPrimaryKey: true, OrdinalPosition: 1}}}}}

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), database, platform.YDB, must.Must(builtin.New())))

	c.Assert(converted.Tables, qt.HasLen, 1)
	declared, found, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](converted.Tables[0].Facets, ydbschema.TablePartitioningKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(declared.TablePartitioning, qt.DeepEquals, settings)
	*declared.ByLoad = false
	c.Assert(*observed.ByLoad, qt.IsTrue, qt.Commentf("the declaration shares no pointer with the description"))
}
