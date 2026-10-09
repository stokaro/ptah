package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
)

// PlanDeclarations creates workload routing before starting declared streaming
// queries. Object receipts and refusal indexes retain the caller's order.
func (WorkloadStreamingService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := workloadScope(ctx, featureplan.Request{Target: request.Target, Identifiers: request.Identifiers}); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	workload, streaming := request, request
	workload.Objects, streaming.Objects = nil, nil
	var workloadIndexes, streamingIndexes []int
	for index, object := range request.Objects {
		switch object.Value.(type) {
		case *ydbworkload.DesiredPool, *ydbworkload.DesiredClassifier:
			workload.Objects = append(workload.Objects, object)
			workloadIndexes = append(workloadIndexes, index)
		case *ydbstreaming.Desired:
			streaming.Objects = append(streaming.Objects, object)
			streamingIndexes = append(streamingIndexes, index)
		default:
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: unexpected workload or streaming declaration %T", schemaext.ErrInvalidValue, object.Value)
		}
	}
	workloadResult, err := (WorkloadService{}).PlanDeclarations(ctx, workload)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	if len(workloadResult.Diagnostics) > 0 {
		return workloadRemapDeclarationDiagnostics(workloadResult, workloadIndexes), nil
	}
	streaming.CommonSteps = slices.Clone(request.CommonSteps)
	for _, contribution := range workloadResult.Contributions {
		for _, step := range contribution.Steps {
			streaming.CommonSteps = append(streaming.CommonSteps, featureplan.CommonStep{
				ID: step.ID, Effects: slices.Clone(step.Effects), Transaction: step.Transaction, Impact: step.Impact,
			})
		}
	}
	streamingResult, err := (StreamingService{}).PlanDeclarations(ctx, streaming)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	if len(streamingResult.Diagnostics) > 0 {
		return workloadRemapDeclarationDiagnostics(streamingResult, streamingIndexes), nil
	}
	result := featureplan.DeclarationResult{Complete: true, Declarations: make([]featureplan.DeclarationPlan, len(request.Objects)),
		Contributions: append(workloadResult.Contributions, streamingResult.Contributions...)}
	for index, plan := range workloadResult.Declarations {
		result.Declarations[workloadIndexes[index]] = plan
	}
	for index, plan := range streamingResult.Declarations {
		result.Declarations[streamingIndexes[index]] = plan
	}
	if err := ctx.Err(); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	return result, nil
}

func workloadRemapDeclarationDiagnostics(result featureplan.DeclarationResult, indexes []int) featureplan.DeclarationResult {
	for index, diagnostic := range result.Diagnostics {
		if diagnostic.Object != nil {
			result.Diagnostics[index].Object = new(indexes[*diagnostic.Object])
		}
	}
	return result
}
