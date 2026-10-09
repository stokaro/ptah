package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
)

// WorkloadStreamingService plans workload routing and streaming changes in one
// owner batch. Queries stop before workload mutations and resume after them,
// just as they do around captured common operations. No host implementation or
// mutable graph crosses the owner boundary. The zero value is ready for use.
type WorkloadStreamingService struct{}

// PlanFeatures preserves input-order receipts and diagnostic indexes across
// the coupled families. A refusal by either family discards the whole batch.
func (WorkloadStreamingService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := workloadScope(ctx, request); err != nil {
		return featureplan.Result{}, err
	}
	workload, streaming := request, request
	workload.Changes, streaming.Changes = nil, nil
	var workloadIndexes, streamingIndexes []int
	for index, change := range request.Changes {
		switch change.Value.(type) {
		case *ydbdiff.ResourcePool, *ydbdiff.ResourcePoolClassifier:
			workload.Changes = append(workload.Changes, change)
			workloadIndexes = append(workloadIndexes, index)
		case *ydbdiff.StreamingQuery:
			streaming.Changes = append(streaming.Changes, change)
			streamingIndexes = append(streamingIndexes, index)
		default:
			return featureplan.Result{}, fmt.Errorf("%w: unexpected workload or streaming change %T", schemaext.ErrInvalidValue, change.Value)
		}
	}
	workloadResult, err := (WorkloadService{}).PlanFeatures(ctx, workload)
	if err != nil {
		return featureplan.Result{}, err
	}
	if len(workloadResult.Diagnostics) > 0 {
		return workloadRemapDiagnostics(workloadResult, workloadIndexes), nil
	}
	streaming.CommonSteps = slices.Clone(request.CommonSteps)
	for _, contribution := range workloadResult.Contributions {
		for _, step := range contribution.Steps {
			streaming.CommonSteps = append(streaming.CommonSteps, featureplan.CommonStep{
				ID: step.ID, Effects: slices.Clone(step.Effects), Transaction: step.Transaction, Impact: step.Impact,
			})
		}
	}
	streamingResult, err := (StreamingService{}).PlanFeatures(ctx, streaming)
	if err != nil {
		return featureplan.Result{}, err
	}
	if len(streamingResult.Diagnostics) > 0 {
		return workloadRemapDiagnostics(streamingResult, streamingIndexes), nil
	}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes)),
		Contributions: append(workloadResult.Contributions, streamingResult.Contributions...)}
	for index, plan := range workloadResult.Changes {
		result.Changes[workloadIndexes[index]] = plan
	}
	for index, plan := range streamingResult.Changes {
		result.Changes[streamingIndexes[index]] = plan
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func workloadRemapDiagnostics(result featureplan.Result, indexes []int) featureplan.Result {
	for index, diagnostic := range result.Diagnostics {
		if diagnostic.Change != nil {
			result.Diagnostics[index].Change = new(indexes[*diagnostic.Change])
		}
	}
	return result
}
