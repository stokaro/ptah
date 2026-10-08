package featurereport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/dbmlrender"
	"ptah.run/internal/schemastats"
)

const tuningKind schemaext.Kind = "example.org/tuning"

type tuningValue struct{ Count int }

func (*tuningValue) Kind() schemaext.Kind     { return tuningKind }
func (v *tuningValue) Clone() schemaext.Value { return &tuningValue{Count: v.Count} }
func (v *tuningValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*tuningValue)
	return ok && w != nil && *v == *w
}

type tuningReporter struct {
	metric string
	calls  int
	sizes  []int
	fail   error
}

func (r *tuningReporter) ReportValues(_ context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	r.calls++
	r.sizes = append(r.sizes, len(request.Values))
	if r.fail != nil {
		return nil, r.fail
	}
	result := make([]schemaext.ValueReport, len(request.Values))
	for i, value := range request.Values {
		result[i] = schemaext.ValueReport{Kind: tuningKind, Counts: []schemaext.MetricCount{{Name: r.metric, Value: value.(*tuningValue).Count}}}
	}
	return result, nil
}

func tuningProvider(reporter *tuningReporter) engine.Provider {
	encode := func(value schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(value.(*tuningValue).Count)
	}
	codec := schemaext.Codec{Prototype: &tuningValue{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"type":"integer"}`), Encode: encode, Canonical: encode,
		Clone: func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*tuningValue).Clone(), nil },
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			count, err := schemaext.DecodeJSON[int](data)
			return &tuningValue{Count: count}, err
		},
	}
	return engine.Provider{ID: "example.org/reporting", Codecs: []schemaext.Codec{codec}, Reporting: []engine.Reporting{{
		Representation: schemaext.Desired, Service: reporter,
		Definitions: []schemaext.ReportDefinition{{Kind: tuningKind, DisplayName: "tuning settings",
			Metrics: []schemaext.MetricDefinition{{Name: reporter.metric, Help: "Captured tuning units"}}}},
	}}}
}

func tuningFixture(c *qt.C) (*schemamodel.Database, *catalog.Database) {
	c.Helper()
	facets := func(count int) schemaext.Facets {
		result, err := schemaext.NewFacets(&tuningValue{Count: count})
		c.Assert(err, qt.IsNil)
		return result
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	parent := builder.TableParts("app", "events")
	child := objectidentity.ID{Kind: objectidentity.Kind(tuningKind), Schema: parent.Schema, Parent: parent.Name, Name: parent.Name}
	objects, err := schemaext.NewObjects(
		schemaext.Object{Ref: builder.SchemaScopedParts(objectidentity.Kind(tuningKind), "app", "standalone"), Value: &tuningValue{Count: 5}},
		schemaext.Object{Ref: child, Value: &tuningValue{Count: 6}},
	)
	c.Assert(err, qt.IsNil)
	db := &schemamodel.Database{
		Facets: facets(1), FeatureObjects: objects,
		Tables:  []schemamodel.Table{{Schema: "app", Name: "events", StructName: "Event", Facets: facets(2)}},
		Fields:  []schemamodel.Field{{StructName: "Event", Name: "id", Type: "integer", Facets: facets(3)}},
		Indexes: []schemamodel.Index{{StructName: "Event", Name: "lookup", Fields: []string{"id"}, Facets: facets(4)}},
	}
	observed := &catalog.Database{
		Tables:  []catalog.Table{{Schema: "app", Name: "events", Columns: []catalog.Column{{Name: "id", DataType: "integer"}}}},
		Indexes: []catalog.Index{{Schema: "app", TableName: "events", Name: "lookup", Columns: []string{"id"}}},
	}
	return db, observed
}

func TestConsumers_ReportAnExternalModelWithoutTargetRegistration(t *testing.T) {
	c := qt.New(t)
	reporter := &tuningReporter{metric: "tuning_units"}
	runtime, err := engine.New(tuningProvider(reporter))
	c.Assert(err, qt.IsNil)
	db, observed := tuningFixture(c)
	stats, err := schemastats.Collect(t.Context(), db, "", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(stats.Metrics[len(stats.Metrics)-1], qt.DeepEquals, schemastats.Metric{Name: "tuning_units", Help: "Captured tuning units", Value: 21})

	document, err := dbmlrender.Render(t.Context(), db, dbmlrender.Options{}, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(document.Omitted, qt.DeepEquals, []string{"tuning settings (6)"})
	filtered, err := dbmlrender.Render(t.Context(), db, dbmlrender.Options{ExcludeTables: []string{"events"}}, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(filtered.Omitted, qt.DeepEquals, []string{"tuning settings (2)"})

	var warnings bytes.Buffer
	report, err := atlasreport.NewSchemaInspectReport(t.Context(), db, observed, catalog.ServerInfo{Dialect: "postgres"}, &warnings, atlasreport.SchemaInspectReportOptions{}, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(warnings.Len(), qt.Equals, 0)
	_, err = report.MarshalJSON()
	c.Assert(err, qt.IsNil)
	c.Assert(warnings.String(), qt.Equals,
		"warning: JSON schema inspection leaves out tuning settings (2)\n"+
			"warning: JSON schema inspection leaves out tuning settings (2) from table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out tuning settings (1) from column \"id\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out tuning settings (1) from index \"lookup\" of table \"app.events\"\n")
	warnings.Reset()
	_, err = atlasreport.RenderSchemaInspect(`{{ json (index (index .Realm.Schemas 0).Tables 0) }}`, report)
	c.Assert(err, qt.IsNil)
	c.Assert(warnings.String(), qt.Not(qt.Contains), "tuning settings (2)\n")
	c.Assert(warnings.String(), qt.Contains, "tuning settings (2) from table")
	c.Assert(reporter.sizes, qt.DeepEquals, []int{6, 6, 2, 6})
}

func TestConsumers_RefuseMissingReportersAndProviderFailures(t *testing.T) {
	failure := errors.New("provider unavailable")
	missing := tuningProvider(&tuningReporter{metric: "tuning_units"})
	missing.Reporting = nil
	for _, test := range []struct {
		name     string
		provider engine.Provider
		want     error
	}{
		{name: "provider failure", provider: tuningProvider(&tuningReporter{metric: "tuning_units", fail: failure}), want: failure},
		{name: "missing handler", provider: missing, want: ptaherr.ErrUnsupportedFeature},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := engine.New(test.provider)
			c.Assert(err, qt.IsNil)
			db, observed := tuningFixture(c)
			stats, err := schemastats.Collect(t.Context(), db, "", runtime)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(stats, qt.DeepEquals, schemastats.Stats{})
			document, err := dbmlrender.Render(t.Context(), db, dbmlrender.Options{}, runtime)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(document, qt.DeepEquals, dbmlrender.Result{})
			// Omitting the diagnostics writer must not bypass feature validation.
			report, err := atlasreport.NewSchemaInspectReport(t.Context(), db, observed, catalog.ServerInfo{Dialect: "postgres"}, nil, atlasreport.SchemaInspectReportOptions{}, runtime)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(report, qt.IsNil)
		})
	}
}

func TestStats_RefusesCollidingOrOverflowingFeatureCounts(t *testing.T) {
	for _, test := range []struct {
		metric string
		want   error
	}{
		{metric: "tables", want: schemaext.ErrDuplicate}, {metric: "tuning_units", want: schemaext.ErrInvalidValue},
	} {
		t.Run(test.metric, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := engine.New(tuningProvider(&tuningReporter{metric: test.metric}))
			c.Assert(err, qt.IsNil)
			db, _ := tuningFixture(c)
			db.Facets, err = schemaext.NewFacets(&tuningValue{Count: math.MaxInt})
			c.Assert(err, qt.IsNil)
			stats, err := schemastats.Collect(t.Context(), db, "", runtime)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(stats, qt.DeepEquals, schemastats.Stats{})
		})
	}
}
