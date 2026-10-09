package schemadiff_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

func TestComparisonDecodesIndexPropertiesBeforePreparation(t *testing.T) {
	c := qt.New(t)
	selected := must.Must(engine.New(engine.Provider{
		ID: "example.org/index-source", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs: chschema.IndexCodecs(), Properties: []engine.PropertySource{{
			Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Definitions: chsource.IndexDefinitions(), Service: chsource.IndexService{},
		}},
	}))
	stop := errors.New("captured preparation input")
	var received schemapreparation.Request
	runtime := comparisonPreparation{Runtime: selected, prepare: func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		received = request.Clone()
		return schemapreparation.Result{}, stop
	}}
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64"}},
		Indexes: []schemamodel.Index{{Name: "by_id", StructName: "Event", Fields: []string{"id"}, Type: "set(100)",
			Overrides: map[string]map[string]string{"clickhouse": {"granularity": "18446744073709551615"}},
		}},
	}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, &catalog.Database{}, "clickhouse", runtime)
	c.Assert(err, qt.ErrorIs, stop)
	c.Assert(diff, qt.IsNil)
	c.Assert(received.Tables, qt.HasLen, 1)
	c.Assert(received.Tables[0].Desired.Indexes, qt.HasLen, 1)
	index := received.Tables[0].Desired.Indexes[0]
	value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](index.Facets, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, (&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 18446744073709551615}).Desired())
	c.Assert(index.Type, qt.Equals, "")
	c.Assert(index.Overrides, qt.HasLen, 0)
	c.Assert(source.Indexes[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(source.Indexes[0].Type, qt.Equals, "set(100)")
	c.Assert(source.Indexes[0].Overrides["clickhouse"]["granularity"], qt.Equals, "18446744073709551615")
}
