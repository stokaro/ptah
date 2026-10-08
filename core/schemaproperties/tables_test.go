package schemaproperties_test

import (
	"context"
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

func sourceRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "ptah.run/clickhouse", Targets: []engine.Target{{Name: "clickhouse", Aliases: []string{"ch"}}, {Name: "other"}},
		Codecs: chschema.Codecs(), Properties: []engine.PropertySource{{
			Target: "clickhouse", Format: schemaext.TablePlatformProperties, Definitions: chsource.Definitions(), Service: chsource.Service{},
		}},
	}))
}

func TestTables_PreserveIntentAndUnclaimedProperties(t *testing.T) {
	c := qt.New(t)
	runtime := sourceRuntime()
	db := &schemamodel.Database{Tables: []schemamodel.Table{
		{Name: "events", Overrides: map[string]map[string]string{
			"ch":    {"order_by.state": "default", "primary_key": "", "future": "retained"},
			"other": {"engine": "foreign"},
		}},
		{Name: "untouched"},
	}}
	decoded, err := schemaproperties.DecodeTables(c.Context(), db, "CH", runtime)
	c.Assert(err, qt.IsNil)
	value, found, err := schemaext.FacetAs[*chschema.DesiredTable](decoded.Tables[0].Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, &chschema.DesiredTable{OrderBy: chschema.Setting{State: chschema.Default}, PrimaryKey: chschema.Setting{State: chschema.Explicit}})
	c.Assert(decoded.Tables[0].Facets.TargetScope(chschema.TableKind), qt.DeepEquals, []string{"clickhouse"})
	c.Assert(decoded.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{"ch": {"future": "retained"}, "other": {"engine": "foreign"}})
	c.Assert(decoded.Tables[1].Facets.IsZero(), qt.IsTrue)
	c.Assert(db.Tables[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(db.Tables[0].Overrides["ch"], qt.HasLen, 3)

	exported, err := schemaproperties.EncodeTables(c.Context(), decoded, "ch", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(exported.Tables[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(exported.Tables[0].Overrides["clickhouse"], qt.DeepEquals, map[string]string{"order_by.state": "default", "primary_key": ""})
	reread, err := schemaproperties.DecodeTables(c.Context(), exported, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(reread.Tables[0].Facets.Equal(decoded.Tables[0].Facets), qt.IsTrue)
	exported.Tables[0].Overrides["ch"]["future"] = "changed"
	c.Assert(decoded.Tables[0].Overrides["ch"]["future"], qt.Equals, "retained")
}

func TestDecodeTables_RefusesConflictsAndInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name string
		db   *schemamodel.Database
		want error
	}{
		{"nil schema", nil, schemaext.ErrInvalidValue},
		{"alias conflict", &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Overrides: map[string]map[string]string{"ch": {"engine": ""}, "clickhouse": {"engine": ""}}}}}, schemaext.ErrDuplicate},
		{"typed conflict", &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Facets: must.Must(schemaext.NewFacets(&chschema.DesiredTable{})), Overrides: map[string]map[string]string{"clickhouse": {"engine": ""}}}}}, schemaext.ErrDuplicate},
		{"invalid later table", &schemamodel.Database{Tables: []schemamodel.Table{
			{Name: "first", Overrides: map[string]map[string]string{"clickhouse": {"engine": "Memory"}}},
			{Name: "second", Overrides: map[string]map[string]string{"clickhouse": {"engine.state": "invalid"}}},
		}}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := schemaproperties.DecodeTables(c.Context(), test.db, "clickhouse", sourceRuntime())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestEncodeTables_RefusesLossAndMixedDeclarations(t *testing.T) {
	value := &chschema.DesiredTable{Engine: chschema.Setting{State: chschema.Explicit, Value: "Memory"}}
	unscoped := must.Must(schemaext.NewFacets(value))
	foreign := must.Must(unscoped.WithTargetScope(chschema.TableKind, "other"))
	excluded := must.Must(foreign.ForTarget(must.Must(schemaext.NewTargetSelection("clickhouse"))))
	for _, test := range []struct {
		name      string
		facets    schemaext.Facets
		overrides map[string]map[string]string
		want      error
	}{
		{"excluded", excluded, nil, ptaherr.ErrUnsupportedFeature},
		{"foreign", foreign, nil, ptaherr.ErrUnsupportedFeature},
		{"mixed empty property", unscoped, map[string]map[string]string{"ch": {"engine": ""}}, schemaext.ErrDuplicate},
		{"empty facet", must.Must(schemaext.NewFacets(&chschema.DesiredTable{})), nil, ptaherr.ErrUnsupportedFeature},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Facets: test.facets, Overrides: test.overrides}}}
			got, err := schemaproperties.EncodeTables(c.Context(), db, "clickhouse", sourceRuntime())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestTables_RequireSelectedRuntimeAndLiveContext(t *testing.T) {
	c := qt.New(t)
	runtime := sourceRuntime()
	db := &schemamodel.Database{}
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	decoded, err := schemaproperties.DecodeTables(canceled, db, "clickhouse", runtime)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(decoded, qt.IsNil)
	encoded, err := schemaproperties.EncodeTables(canceled, db, "clickhouse", runtime)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(encoded, qt.IsNil)
	decoded, err = schemaproperties.DecodeTables(c.Context(), db, "missing", runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(decoded, qt.IsNil)
	decoded, err = schemaproperties.DecodeTables(c.Context(), db, "clickhouse", nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(decoded, qt.IsNil)
}

func TestTables_KnownTargetWithoutPropertyFormatsPreservesSource(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Overrides: map[string]map[string]string{"other": {"engine": "value"}}}}}
	got, err := schemaproperties.DecodeTables(c.Context(), db, "other", sourceRuntime())
	c.Assert(err, qt.IsNil)
	c.Assert(got.Tables[0].Overrides, qt.DeepEquals, db.Tables[0].Overrides)
}
