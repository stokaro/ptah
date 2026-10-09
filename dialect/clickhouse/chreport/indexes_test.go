package chreport_test

import (
	"context"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func TestIndexReportsUseFrozenLabelsAndRetainDesiredIntent(t *testing.T) {
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{"unmanaged", schemaext.Desired, &chschema.DesiredIndex{}},
		{"default request", schemaext.Desired, &chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Default}}},
		{"observation", schemaext.Observed, &chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			definitions := chreport.IndexDefinitions()
			runtime := must.Must(engine.New(engine.Provider{
				ID: "example.org/index-reports", Codecs: chschema.IndexCodecs(),
				Reporting: []engine.Reporting{{Representation: test.representation, Definitions: definitions, Service: chreport.IndexService{}}},
			}))
			definitions[0].DisplayName = "mutated"
			definitions[0].Metrics[0].Name = "mutated"
			report, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(report.Definitions, qt.DeepEquals, chreport.IndexDefinitions())
			c.Assert(report.Values, qt.DeepEquals, []schemaext.ValueReport{{Kind: chschema.IndexKind, Counts: []schemaext.MetricCount{{Name: "clickhouse_index_settings", Value: 1}}}})
		})
	}
}

func TestIndexReportsRefuseInvalidBatches(t *testing.T) {
	for _, invalid := range []schemaext.Value{
		&chschema.ObservedIndex{IndexType: "minmax", Granularity: 1},
		(*chschema.DesiredIndex)(nil), &chschema.DesiredTable{},
		&chschema.DesiredIndex{Granularity: chschema.GranularitySetting{State: chschema.Explicit}},
		&chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Default, Value: "minmax"}},
	} {
		c := qt.New(t)
		report, err := (chreport.IndexService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Desired, Values: []schemaext.Value{&chschema.DesiredIndex{}, invalid}})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(report, qt.IsNil)
	}
}

func TestIndexReportsValidateEmptyRequests(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := (chreport.IndexService{}).ReportValues(ctx, schemaext.ReportingRequest{Representation: schemaext.Desired})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report, qt.IsNil)
	var missingContext context.Context
	report, err = (chreport.IndexService{}).ReportValues(missingContext, schemaext.ReportingRequest{Representation: schemaext.Desired})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.IsNil)
	report, err = (chreport.IndexService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Change})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.IsNil)
}
