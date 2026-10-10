package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbschema"
)

func TestTTLReportValues_CountsEachCapturedTTL(t *testing.T) {
	policy := ydbschema.TTL{Column: "ts", Interval: "P30D"}
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "a declaration", representation: schemaext.Desired, value: &ydbschema.DesiredTTL{Policy: policy}},
		{name: "an observation", representation: schemaext.Observed, value: &ydbschema.ObservedTTL{Policy: policy}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reports, err := ydbreport.TTLService{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{{Kind: ydbschema.TTLKind, Counts: []schemaext.MetricCount{{Name: "ydb_ttl_tables", Value: 1}}}})
		})
	}
}

func TestTTLReportValues_RefusesAMismatchedValue(t *testing.T) {
	c := qt.New(t)

	reports, err := ydbreport.TTLService{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed, Values: []schemaext.Value{&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}}},
	})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(reports, qt.IsNil)
	c.Assert(ydbreport.TTLDefinitions()[0].Kind, qt.Equals, ydbschema.TTLKind)
}
