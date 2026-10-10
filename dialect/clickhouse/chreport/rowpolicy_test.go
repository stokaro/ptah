package chreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chreport"
	"ptah.run/dialect/clickhouse/chschema"
)

// Each captured policy counts once in its representation, and a value of the
// other representation is refused rather than counted.
func TestRowPolicyReporting(t *testing.T) {
	c := qt.New(t)
	service := chreport.RowPolicyService{}

	desired, err := service.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Desired,
		Values: []schemaext.Value{&chschema.DesiredRowPolicy{}}})
	c.Assert(err, qt.IsNil)
	mismatched, err := service.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed,
		Values: []schemaext.Value{&chschema.DesiredRowPolicy{}}})

	c.Assert(desired, qt.DeepEquals, []schemaext.ValueReport{{Kind: chschema.RowPolicyKind,
		Counts: []schemaext.MetricCount{{Name: "clickhouse_row_policies", Value: 1}}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(mismatched, qt.IsNil)
	c.Assert(chreport.RowPolicyDefinitions()[0].DisplayName, qt.Equals, "ClickHouse row policies")
}
