package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
)

// PoolService reverses captured resource pool settings. It restores configuration;
// queries already run or rejected under the forward settings cannot be undone.
type PoolService struct{}

// ReverseChanges returns complete inverse operands and projected forward state.
// Missing observations are never replaced by defaults or live database reads.
func (PoolService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "resource pool", capability.ResourcePools, reverseResourcePool)
}

func reverseResourcePool(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbworkload.ValidateIdentity(record.Subject, ydbworkload.PoolKind); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.ResourcePool)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a resource pool change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if record.Subject.Name.Source == ydbworkload.DefaultPool && (change.Before == nil || change.After == nil) {
		return schemaext.Reversal{}, fmt.Errorf("%w: the default pool cannot be created or removed", schemaext.ErrInvalidValue)
	}
	if change.Before != nil {
		if err := ydbworkload.ValidatePoolRef(record.Subject, change.Before.Spec); err != nil {
			return schemaext.Reversal{}, err
		}
	}
	if change.After != nil {
		if err := ydbworkload.ValidatePoolRef(record.Subject, change.After.Spec); err != nil {
			return schemaext.Reversal{}, err
		}
	}
	if change.Before != nil && change.After != nil && (ydbworkload.PoolsEqual(change.After.Spec, change.Before.Spec)) {
		return schemaext.Reversal{}, fmt.Errorf("%w: resource pool operands contain no change", schemaext.ErrInvalidValue)
	}
	reverse := &ydbdiff.ResourcePool{After: change.Before.Desired(), Before: change.After.Observed()}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbworkload.PoolKind}
	if reverse.Before != nil {
		projection.Value = reverse.Before.Clone()
	}
	strategy := "restore captured resource pool settings"
	if change.Before == nil {
		strategy = "remove the created resource pool"
	}
	if change.After == nil {
		strategy = "recreate the removed resource pool"
	}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reverse},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy,
		Limitations: []string{"Restoring workload configuration cannot undo queries run, queued, or rejected under the forward settings."}}, nil
}
