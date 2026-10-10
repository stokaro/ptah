package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_CarriesTheTimescaleStateTheCatalogKept pins WHICH values the
// conversion carries into a declaration.
//
// A down migration is built from this description, and the two definitions a
// TimescaleDB server can answer with are not interchangeable: pg_get_viewdef
// answers the rewritten one, which selects from the materialization hypertable
// in a schema the extension owns, and rebuilding an aggregate from it would
// name a relation no declaration may touch (stokaro/ptah#1026). A hypertable
// keeps the interval the catalog reported, the column it partitions on, and
// nothing it cannot declare.
func TestConvert_CarriesTheTimescaleStateTheCatalogKept(t *testing.T) {
	c := qt.New(t)
	const definition = "SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings"

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
		Tables: []catalog.Table{{
			Schema: "public", Name: "readings", Columns: []catalog.Column{{Name: "time", DataType: "timestamp with time zone"}},
			Facets: must.Must(schemaext.NewFacets(&tsschema.ObservedHypertable{Column: "time", ColumnType: "timestamp with time zone", ChunkInterval: "7 days", Dimensions: 1})),
		}},
		FeatureObjects: must.Must(schemaext.NewObjects(tsschema.ObservedContinuousAggregateObject("public", "hourly", tsschema.ObservedContinuousAggregate{
			Definition: definition, MaterializedOnly: new(true), HypertableSchema: "public", HypertableName: "readings",
		}))),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}, "postgres", must.Must(builtin.New())))

	c.Assert(converted.Tables, qt.HasLen, 1)
	hypertable, found := must.Must2(schemaext.FacetAs[*tsschema.DesiredHypertable](converted.Tables[0].Facets, tsschema.HypertableKind))
	c.Assert(found, qt.IsTrue)
	c.Assert(hypertable, qt.DeepEquals, &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "7 days"})
	c.Assert(must.Must(converted.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{
		tsschema.DesiredContinuousAggregateObject("public", "hourly", tsschema.DesiredContinuousAggregate{Body: definition, MaterializedOnly: new(true)}),
	})
	c.Assert(converted.FeatureCoverage.Representation(), qt.Equals, schemaext.Desired)
}
