package crdbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbreport"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

func TestReportValues_CountsEachCapturedPolicy(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "a declaration", representation: schemaext.Desired, value: &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 day"}}},
		{name: "an observation", representation: schemaext.Observed, value: &crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 day"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reports, err := crdbreport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{{Kind: crdbschema.RowTTLKind, Counts: []schemaext.MetricCount{{Name: "cockroachdb_row_ttl_tables", Value: 1}}}})
		})
	}
}

func TestReportValues_RefusesAMismatchedValue(t *testing.T) {
	c := qt.New(t)

	reports, err := crdbreport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed, Values: []schemaext.Value{&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 day"}}},
	})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(reports, qt.IsNil)
	c.Assert(crdbreport.Definitions()[0].Kind, qt.Equals, crdbschema.RowTTLKind)
}
