package chreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

// Each captured schedule counts one refreshable view, in either
// representation, under labels a caller cannot change.
func TestRefreshReportsCountRefreshableViews(t *testing.T) {
	schedule := chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{"declaration", schemaext.Desired, &chschema.DesiredRefresh{Schedule: schedule}},
		{"observation", schemaext.Observed, &chschema.ObservedRefresh{Schedule: schedule}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			definitions := chreport.RefreshDefinitions()
			runtime := must.Must(engine.New(engine.Provider{
				ID: "example.org/refresh-reports", Codecs: chschema.RefreshCodecs(),
				Reporting: []engine.Reporting{{Representation: test.representation, Definitions: definitions, Service: chreport.RefreshService{}}},
			}))
			definitions[0].Metrics[0].Name = "mutated"

			report, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Definitions, qt.DeepEquals, chreport.RefreshDefinitions())
			c.Assert(report.Definitions[0].DisplayName, qt.Equals, "ClickHouse refresh schedule")
			c.Assert(report.Values, qt.DeepEquals, []schemaext.ValueReport{{Kind: chschema.RefreshKind, Counts: []schemaext.MetricCount{{Name: "clickhouse_refreshable_materialized_views", Value: 1}}}})
		})
	}
}

func TestRefreshReports_FailurePath(t *testing.T) {
	schedule := chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		valid, invalid schemaext.Value
	}{
		{"an observation among declarations", schemaext.Desired, &chschema.DesiredRefresh{Schedule: schedule}, &chschema.ObservedRefresh{Schedule: schedule}},
		{"a declaration among observations", schemaext.Observed, &chschema.ObservedRefresh{Schedule: schedule}, &chschema.DesiredRefresh{Schedule: schedule}},
		{"a typed nil", schemaext.Desired, &chschema.DesiredRefresh{Schedule: schedule}, (*chschema.DesiredRefresh)(nil)},
		{"an empty schedule", schemaext.Observed, &chschema.ObservedRefresh{Schedule: schedule}, &chschema.ObservedRefresh{}},
		{"another model", schemaext.Desired, &chschema.DesiredRefresh{Schedule: schedule}, &chschema.DesiredIndex{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report, err := (chreport.RefreshService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.valid, test.invalid}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(report, qt.IsNil)
		})
	}
}
