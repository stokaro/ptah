package spannerreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerreport"
	"ptah.run/dialect/spanner/spannerschema"
)

func TestReportValues_CountsEachCapturedPolicy(t *testing.T) {
	policy := spannerschema.Policy{Column: "created_at", Interval: "30 days"}
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "a declaration", representation: schemaext.Desired, value: &spannerschema.DesiredRowDeletion{Policy: policy}},
		{name: "an observation", representation: schemaext.Observed, value: &spannerschema.ObservedRowDeletion{Policy: policy}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reports, err := spannerreport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{{Kind: spannerschema.RowDeletionKind, Counts: []schemaext.MetricCount{{Name: "spanner_row_deletion_policy_tables", Value: 1}}}})
		})
	}
}

func TestReportValues_RefusesAMismatchedValue(t *testing.T) {
	c := qt.New(t)

	reports, err := spannerreport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed, Values: []schemaext.Value{&spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: "ts", Interval: "1 days"}}},
	})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(reports, qt.IsNil)
	c.Assert(spannerreport.Definitions()[0].Kind, qt.Equals, spannerschema.RowDeletionKind)
}
