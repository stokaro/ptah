package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbreport"
)

var (
	reportedMirror = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	reportedIngest = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
)

// TestReplicationReport_CountsEachObject counts each declared or observed
// replication and transfer once, under its own metric.
func TestReplicationReport_CountsEachObject(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		values         []schemaext.Value
	}{
		{name: "desired", representation: schemaext.Desired, values: []schemaext.Value{
			&ydbreplication.DesiredReplication{Spec: reportedMirror}, &ydbreplication.DesiredTransfer{Spec: reportedIngest}}},
		{name: "observed", representation: schemaext.Observed, values: []schemaext.Value{
			&ydbreplication.ObservedReplication{Spec: reportedMirror}, &ydbreplication.ObservedTransfer{Spec: reportedIngest}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.ReplicationService{}).ReportValues(t.Context(),
				schemaext.ReportingRequest{Representation: test.representation, Values: test.values})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []schemaext.ValueReport{
				{Kind: ydbreplication.ReplicationKind, Counts: []schemaext.MetricCount{{Name: "async_replications", Value: 1}}},
				{Kind: ydbreplication.TransferKind, Counts: []schemaext.MetricCount{{Name: "transfers", Value: 1}}},
			})
		})
	}
}

// TestReplicationReport_FailurePath refuses a value of the other
// representation, an invalid value and a representation neither kind has,
// with no partial report.
func TestReplicationReport_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReportingRequest
	}{
		{name: "a declaration among observations", request: schemaext.ReportingRequest{Representation: schemaext.Observed,
			Values: []schemaext.Value{&ydbreplication.ObservedTransfer{Spec: reportedIngest}, &ydbreplication.DesiredTransfer{Spec: reportedIngest}}}},
		{name: "a declaration without items", request: schemaext.ReportingRequest{Representation: schemaext.Desired,
			Values: []schemaext.Value{&ydbreplication.DesiredReplication{Spec: ydbreplication.ReplicationSpec{Connection: reportedMirror.Connection}}}}},
		{name: "a change representation", request: schemaext.ReportingRequest{Representation: schemaext.Change,
			Values: []schemaext.Value{&ydbreplication.DesiredTransfer{Spec: reportedIngest}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.ReplicationService{}).ReportValues(t.Context(), test.request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}
