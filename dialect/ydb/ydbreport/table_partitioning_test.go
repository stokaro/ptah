package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestTablePartitioningReportValues_CountsATableStatingSettings counts each
// value once where it states a setting, and none where it states nothing.
func TestTablePartitioningReportValues_CountsATableStatingSettings(t *testing.T) {
	c := qt.New(t)

	reports, err := ydbreport.TablePartitioningService{}.ReportValues(t.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed, Values: []schemaext.Value{
			&ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{KeyBloomFilter: new(false)}},
			&ydbschema.ObservedTablePartitioning{},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: ydbschema.TablePartitioningKind, Counts: []schemaext.MetricCount{{Name: "ydb_table_partitioning", Value: 1}}},
		{Kind: ydbschema.TablePartitioningKind, Counts: []schemaext.MetricCount{{Name: "ydb_table_partitioning", Value: 0}}},
	})
	c.Assert(ydbreport.TablePartitioningDefinitions()[0].DisplayName, qt.Equals, "table partitioning, read replicas and key bloom filters")
}

// TestTablePartitioningReportValues_FailurePath refuses a value of the other
// representation and an invalid one, with no partial report.
func TestTablePartitioningReportValues_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReportingRequest
	}{
		{name: "a mismatched value", request: schemaext.ReportingRequest{Representation: schemaext.Desired,
			Values: []schemaext.Value{&ydbschema.ObservedTablePartitioning{}}}},
		{name: "an invalid value", request: schemaext.ReportingRequest{Representation: schemaext.Desired,
			Values: []schemaext.Value{&ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ReadReplicas: "x"}}}}},
		{name: "a change representation", request: schemaext.ReportingRequest{Representation: schemaext.Change}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reports, err := ydbreport.TablePartitioningService{}.ReportValues(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(reports, qt.IsNil)
		})
	}
}
