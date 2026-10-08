package chschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func TestSelectedProviderPreservesTableFacetsThroughEnvelopes(t *testing.T) {
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/clickhouse", Codecs: chschema.Codecs()}))
	registry := runtime.Codecs()
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{"observed", schemaext.Observed, tableFixture()},
		{"desired", schemaext.Desired, tableFixture().Desired()},
		{"omitted", schemaext.Desired, &chschema.DesiredTable{}},
		{"default", schemaext.Desired, &chschema.DesiredTable{PrimaryKey: chschema.Setting{State: chschema.Default}}},
		{"explicit empty", schemaext.Desired, &chschema.DesiredTable{PrimaryKey: chschema.Setting{State: chschema.Explicit}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			facets := must.Must(schemaext.NewFacets(test.value))
			facets = must.Must(facets.WithTargetScope(chschema.TableKind, "clickhouse"))
			encoded, err := registry.EncodeFacets(t.Context(), test.representation, facets)
			c.Assert(err, qt.IsNil)
			decoded, err := registry.DecodeFacets(t.Context(), test.representation, encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.Equal(facets), qt.IsTrue)
			c.Assert(decoded.TargetScope(chschema.TableKind), qt.DeepEquals, []string{"clickhouse"})
			values := must.Must(decoded.Values())
			c.Assert(values, qt.HasLen, 1)
			c.Assert(values[0].Equal(test.value), qt.IsTrue)
		})
	}
}
