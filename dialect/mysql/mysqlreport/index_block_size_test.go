package mysqlreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlreport"
	"ptah.run/dialect/mysql/mysqlschema"
)

// A read observes every index, so only a hint held is counted; the metric is
// reported for every value.
func TestIndexBlockSizeService_CountsEachHint(t *testing.T) {
	c := qt.New(t)

	reports, err := mysqlreport.IndexBlockSizeService{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed, Values: []schemaext.Value{
			&mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true},
			&mysqlschema.ObservedIndexBlockSize{},
		}})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: mysqlschema.IndexBlockSizeKind, Counts: []schemaext.MetricCount{{Name: mysqlreport.IndexBlockSizeMetric, Value: 1}}},
		{Kind: mysqlschema.IndexBlockSizeKind, Counts: []schemaext.MetricCount{{Name: mysqlreport.IndexBlockSizeMetric, Value: 0}}},
	})
}

func TestIndexBlockSizeService_RefusesTheOtherRepresentation(t *testing.T) {
	c := qt.New(t)

	reports, err := mysqlreport.IndexBlockSizeService{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Desired, Values: []schemaext.Value{&mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8}}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*MySQL index block size report has mismatched value .*`)
	c.Assert(reports, qt.IsNil)
}
