package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// StreamingService restores captured query configuration and reports lost
// checkpoint state. Reset permission follows a body change into its reverse.
type StreamingService struct{}

// ReverseChanges projects complete stored settings before constructing inverses.
// The projection is a prediction of stored settings, never inspection evidence.
func (StreamingService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "streaming", capability.StreamingQueries, reverseStreaming)
}

func reverseStreaming(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbstreaming.ValidateIdentity(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.StreamingQuery)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a streaming change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if change.Before != nil && change.After != nil && ydbstreaming.Equal(change.After.Spec, change.Before.Spec) {
		return schemaext.Reversal{}, fmt.Errorf("%w: streaming operands contain no change", schemaext.ErrInvalidValue)
	}
	reverse := &ydbdiff.StreamingQuery{After: change.Before.Desired()}
	if change.After != nil {
		reverse.Before = change.After.Observed()
		if reverse.After != nil {
			if err := ydbstreaming.ValidateAlter(record.Subject.String(), change.After.Spec, change.Before.Spec,
				ydbstreaming.AlterOptions{AllowStateReset: change.After.AllowStateReset}); err != nil {
				return schemaext.Reversal{}, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
			}
			reverse.After.AllowStateReset = change.After.AllowStateReset
		}
	}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbstreaming.Kind}
	if reverse.Before != nil {
		projection.Value = reverse.Before.Clone()
	}
	strategy, limitations := streamingRecovery(change)
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reverse},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy, Limitations: limitations}, nil
}

func streamingRecovery(change *ydbdiff.StreamingQuery) (string, []string) {
	switch {
	case change.Before == nil:
		return "drop the created streaming query", []string{ydbstreaming.CheckpointLoss}
	case change.After == nil:
		return "recreate the dropped streaming query", []string{ydbstreaming.CheckpointLoss}
	case !ydbstreaming.SameBody(change.Before.Spec.Text, change.After.Spec.Text):
		return "restore the query body in place", []string{ydbstreaming.CheckpointLoss}
	default:
		return "restore streaming execution settings in place", nil
	}
}
