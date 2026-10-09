package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
)

// CoordinationService restores captured configuration and reports runtime state
// lost by dropping a node. It does not recover semaphores or rate limiter resources.
type CoordinationService struct{}

// ReverseChanges projects partial ALTER semantics before constructing inverses.
// The projection is a prediction of stored settings, never inspection evidence.
func (CoordinationService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "coordination", capability.CoordinationNodes, reverseCoordination)
}

func reverseCoordination(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbcoordination.ValidateRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.CoordinationNode)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a coordination change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if change.Before != nil && change.After != nil && ydbcoordination.Changes(change.After.Spec, change.Before.Spec).IsZero() {
		return schemaext.Reversal{}, fmt.Errorf("%w: coordination operands contain no change", schemaext.ErrInvalidValue)
	}
	reverse := &ydbdiff.CoordinationNode{After: change.Before.Desired()}
	if change.After != nil {
		reverse.Before = change.After.Observed()
		if change.Before != nil {
			// ALTER sets only changed settings. Untouched raw settings remain
			// as observed, while resets to defaults become explicit stored values.
			reverse.Before.Spec = ydbcoordination.Merge(change.Before.Spec, ydbcoordination.Changes(change.After.Spec, change.Before.Spec))
		}
	}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbcoordination.Kind}
	if reverse.Before != nil {
		projection.Value = reverse.Before.Clone()
	}
	strategy, limitations := coordinationRecovery(change)
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reverse},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy, Limitations: limitations}, nil
}

func coordinationRecovery(change *ydbdiff.CoordinationNode) (string, []string) {
	switch {
	case change.Before == nil:
		return "drop the created coordination node", []string{"Dropping the new node discards its semaphores and rate limiter resources."}
	case change.After == nil:
		return "recreate the dropped coordination node", []string{"Recreating the configuration cannot recover semaphores or rate limiter resources lost when the node was dropped."}
	default:
		return "restore coordination settings in place", nil
	}
}
