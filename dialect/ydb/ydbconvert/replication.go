package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
)

// AsyncReplicationService converts async replications between a declaration
// and an observation. Every setting is kept as written and no default is
// filled in. A projected observation carries no state, since only a read
// establishes one, and a declaration made from an observation leaves the
// state out, since no declaration sets it.
type AsyncReplicationService struct{}

// TransferService converts transfers between a declaration and an
// observation, as [AsyncReplicationService] converts replications.
type TransferService struct{}

// ConvertFeatures returns an independent batch, in input order.
func (AsyncReplicationService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "async replication", ydbreplication.ReplicationCodecs(),
		func(value *ydbreplication.DesiredReplication) *ydbreplication.ObservedReplication {
			return value.Observed("")
		},
		(*ydbreplication.ObservedReplication).Desired)
}

// ConvertFeatures returns an independent batch, in input order.
func (TransferService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "transfer", ydbreplication.TransferCodecs(),
		func(value *ydbreplication.DesiredTransfer) *ydbreplication.ObservedTransfer {
			return value.Observed("")
		},
		(*ydbreplication.ObservedTransfer).Desired)
}
