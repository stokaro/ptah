package tsreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsreport"
	"ptah.run/dialect/timescaledb/tsschema"
)

// TestReportValues_CountsEachModel pins the metric each value counts under,
// which is the name inventory and export-loss reports print.
func TestReportValues_CountsEachModel(t *testing.T) {
	c := qt.New(t)

	reports, err := tsreport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed, Values: []schemaext.Value{
		&tsschema.ObservedHypertable{Column: "time", Dimensions: 1},
		&tsschema.ObservedContinuousAggregate{Definition: "SELECT 1"},
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: tsschema.HypertableKind, Counts: []schemaext.MetricCount{{Name: "hypertables", Value: 1}}},
		{Kind: tsschema.ContinuousAggregateKind, Counts: []schemaext.MetricCount{{Name: "continuous_aggregates", Value: 1}}},
	})
	c.Assert(tsreport.Definitions()[0].DisplayName, qt.Equals, "hypertables")
	c.Assert(tsreport.Definitions()[1].DisplayName, qt.Equals, "continuous aggregates")
}

// TestReportValues_RefusesAValueOfTheOtherRepresentation pins that a report
// validates what it counts: a declaration in an observed batch, and an invalid
// value, are refused rather than counted.
func TestReportValues_RefusesAValueOfTheOtherRepresentation(t *testing.T) {
	tests := []struct {
		name  string
		value schemaext.Value
	}{
		{name: "a declaration in an observed batch", value: &tsschema.DesiredHypertable{Column: "time"}},
		{name: "an observation naming no dimension", value: &tsschema.ObservedHypertable{Dimensions: 1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reports, err := tsreport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(reports, qt.IsNil)
		})
	}
}
