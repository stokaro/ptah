package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ClassifierService reverses captured resource pool classifier settings. It restores configuration;
// queries already run or rejected under the forward settings cannot be undone.
type ClassifierService struct{}

// ReverseChanges returns complete inverse operands and projected forward state.
// Missing observations are never replaced by defaults or live database reads.
func (ClassifierService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "resource pool classifier", capability.ResourcePools, reverseResourcePoolClassifier)
}

func reverseResourcePoolClassifier(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbworkload.ValidateIdentity(record.Subject, ydbworkload.ClassifierKind); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.ResourcePoolClassifier)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a resource pool classifier change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}

	if change.Before != nil && change.After != nil && (change.After.Spec == change.Before.Spec) {
		return schemaext.Reversal{}, fmt.Errorf("%w: resource pool classifier operands contain no change", schemaext.ErrInvalidValue)
	}
	reverse := &ydbdiff.ResourcePoolClassifier{After: change.Before.Desired(), Before: change.After.Observed()}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbworkload.ClassifierKind}
	if reverse.Before != nil {
		projection.Value = reverse.Before.Clone()
	}
	strategy := "restore captured resource pool classifier settings"
	if change.Before == nil {
		strategy = "remove the created resource pool classifier"
	}
	if change.After == nil {
		strategy = "recreate the removed resource pool classifier"
	}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reverse},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy,
		Limitations: []string{"Restoring workload configuration cannot undo queries run, queued, or rejected under the forward settings."}}, nil
}
