package schemaproperties_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
)

func indexPropertyRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/index-properties", Targets: []engine.Target{{Name: "clickhouse", Aliases: []string{"ch"}}, {Name: "other"}},
		Codecs: chschema.IndexCodecs(), Properties: []engine.PropertySource{{
			Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Definitions: chsource.IndexDefinitions(), Service: chsource.IndexService{},
		}},
	}))
}

func TestIndexPropertiesConsumeClaimedTypeWithoutMutatingSource(t *testing.T) {
	c := qt.New(t)
	runtime := indexPropertyRuntime()
	source := &schemamodel.Database{Indexes: []schemamodel.Index{
		{Name: "by.id", Type: "set(100)", Fields: []string{"id"}, Overrides: map[string]map[string]string{
			"ch": {"granularity": "18446744073709551615", "future": "retained"}, "other": {"type": "foreign"},
		}},
		{Name: "unmanaged"},
	}}
	decoded, err := schemaproperties.DecodeIndexes(t.Context(), source, "CH", runtime)
	c.Assert(err, qt.IsNil)
	value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](decoded.Indexes[0].Facets, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, (&chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}).Desired())
	c.Assert(decoded.Indexes[0].Type, qt.Equals, "")
	c.Assert(decoded.Indexes[0].Overrides, qt.DeepEquals, map[string]map[string]string{"ch": {"future": "retained"}, "other": {"type": "foreign"}})
	c.Assert(decoded.Indexes[0].Facets.TargetScope(chschema.IndexKind), qt.DeepEquals, []string{"clickhouse"})
	c.Assert(decoded.Indexes[1].Facets.IsZero(), qt.IsTrue)
	c.Assert(source.Indexes[0].Type, qt.Equals, "set(100)")
	c.Assert(source.Indexes[0].Overrides["ch"]["granularity"], qt.Equals, "18446744073709551615")
	c.Assert(source.Indexes[0].Facets.IsZero(), qt.IsTrue)
	decoded.Indexes[0].Fields[0] = "changed"
	c.Assert(source.Indexes[0].Fields, qt.DeepEquals, []string{"id"})

	exported, err := schemaproperties.EncodeIndexes(t.Context(), decoded, "ch", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(exported.Indexes[0].Type, qt.Equals, "")
	c.Assert(exported.Indexes[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(exported.Indexes[0].Overrides["clickhouse"], qt.DeepEquals, map[string]string{"type": "set(100)", "granularity": "18446744073709551615"})
	reread, err := schemaproperties.DecodeIndexes(t.Context(), exported, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(reread.Indexes[0].Facets.Equal(decoded.Indexes[0].Facets), qt.IsTrue)
	exported.Indexes[0].Overrides["ch"]["future"] = "changed"
	c.Assert(decoded.Indexes[0].Overrides["ch"]["future"], qt.Equals, "retained")
}

func TestDecodeIndexesRefusesCompetingDeclarationsAndPartialResults(t *testing.T) {
	for _, test := range []struct {
		name  string
		index schemamodel.Index
		want  error
	}{
		{"common type and scoped type", schemamodel.Index{Type: "minmax", Overrides: map[string]map[string]string{"ch": {"type": "set(100)"}}}, schemaext.ErrDuplicate},
		{"target aliases", schemamodel.Index{Overrides: map[string]map[string]string{"ch": {"granularity": "1"}, "clickhouse": {"granularity": "1"}}}, schemaext.ErrDuplicate},
		{"typed and common", schemamodel.Index{Type: "minmax", Facets: must.Must(schemaext.NewFacets(&chschema.DesiredIndex{}))}, schemaext.ErrDuplicate},
		{"typed and empty scoped", schemamodel.Index{Facets: must.Must(schemaext.NewFacets(&chschema.DesiredIndex{})), Overrides: map[string]map[string]string{"ch": {"type": ""}}}, schemaext.ErrDuplicate},
		{"invalid number", schemamodel.Index{Overrides: map[string]map[string]string{"ch": {"granularity": "invalid"}}}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &schemamodel.Database{Indexes: []schemamodel.Index{{Name: "first", Type: "minmax"}, test.index}}
			got, err := schemaproperties.DecodeIndexes(t.Context(), source, "clickhouse", indexPropertyRuntime())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(got, qt.IsNil)
			c.Assert(source.Indexes[0].Type, qt.Equals, "minmax")
			c.Assert(source.Indexes[0].Facets.IsZero(), qt.IsTrue)
			c.Assert(source.Indexes[1], qt.DeepEquals, test.index)
		})
	}
}

func TestEncodeIndexesRefusesLossAndCompetingSettings(t *testing.T) {
	values := must.Must(schemaext.NewFacets(&chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Default}}))
	foreign := must.Must(values.WithTargetScope(chschema.IndexKind, "other"))
	excluded := must.Must(foreign.ForTarget(must.Must(schemaext.NewTargetSelection("clickhouse"))))
	for _, test := range []struct {
		name  string
		index schemamodel.Index
		want  error
	}{
		{"foreign binding", schemamodel.Index{Facets: foreign}, ptaherr.ErrUnsupportedFeature},
		{"excluded value", schemamodel.Index{Facets: excluded}, ptaherr.ErrUnsupportedFeature},
		{"empty value", schemamodel.Index{Facets: must.Must(schemaext.NewFacets(&chschema.DesiredIndex{}))}, ptaherr.ErrUnsupportedFeature},
		{"common type", schemamodel.Index{Facets: values, Type: "minmax"}, schemaext.ErrDuplicate},
		{"scoped property", schemamodel.Index{Facets: values, Overrides: map[string]map[string]string{"clickhouse": {"granularity": ""}}}, schemaext.ErrDuplicate},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &schemamodel.Database{Indexes: []schemamodel.Index{{Name: "first", Facets: values}, test.index}}
			got, err := schemaproperties.EncodeIndexes(t.Context(), source, "ch", indexPropertyRuntime())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(got, qt.IsNil)
			c.Assert(source.Indexes[0].Facets.Equal(values), qt.IsTrue)
			c.Assert(source.Indexes[1], qt.DeepEquals, test.index)
		})
	}
}

func TestIndexPropertyOwnershipLeavesUnclaimedCommonTypeIntact(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{Indexes: []schemamodel.Index{{Name: "by.id", Type: "btree", Overrides: map[string]map[string]string{"clickhouse": {"granularity": "8"}}}}}
	decoded, err := schemaproperties.DecodeIndexes(t.Context(), source, "other", indexPropertyRuntime())
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Indexes, qt.DeepEquals, source.Indexes)
	decoded.Indexes[0].Overrides["clickhouse"]["granularity"] = "changed"
	c.Assert(source.Indexes[0].Overrides["clickhouse"]["granularity"], qt.Equals, "8")
}
