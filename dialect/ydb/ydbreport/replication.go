package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
)

// ReplicationService reports async replications and transfers.
type ReplicationService struct{}

// ReplicationDefinitions declares the inventory metrics for both kinds.
func ReplicationDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{
		{Kind: ydbreplication.ReplicationKind, DisplayName: "async replications",
			Metrics: []schemaext.MetricDefinition{{Name: "async_replications", Help: "Async replications"}}},
		{Kind: ydbreplication.TransferKind, DisplayName: "transfers",
			Metrics: []schemaext.MetricDefinition{{Name: "transfers", Help: "Transfers"}}},
	}
}

// ReportValues validates each value through its codec and counts it.
func (ReplicationService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	codecs := make(map[schemaext.Kind]schemaext.Codec)
	for _, candidate := range ydbreplication.Codecs() {
		if candidate.Representation == request.Representation {
			codecs[candidate.Prototype.Kind()] = candidate
		}
	}
	if len(codecs) == 0 {
		return nil, fmt.Errorf("%w: replication reporting requires desired or observed state", schemaext.ErrInvalidValue)
	}
	metrics := map[schemaext.Kind]string{ydbreplication.ReplicationKind: "async_replications", ydbreplication.TransferKind: "transfers"}
	result := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if value == nil {
			return nil, fmt.Errorf("%w: replication reporting requires a value", schemaext.ErrInvalidValue)
		}
		codec, ok := codecs[value.Kind()]
		if !ok {
			return nil, fmt.Errorf("%w: unexpected replication object %T", schemaext.ErrInvalidValue, value)
		}
		if _, err := codec.Clone(value); err != nil {
			return nil, err
		}
		result = append(result, schemaext.ValueReport{Kind: value.Kind(),
			Counts: []schemaext.MetricCount{{Name: metrics[value.Kind()], Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
