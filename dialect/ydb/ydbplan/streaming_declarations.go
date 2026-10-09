package ydbplan

import (
	"context"
	"fmt"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// PlanDeclarations derives CREATE operations from authored query definitions.
// The create operands express the requested command, not an observation about
// a database. They share the owner's operation and graph rules with migrations.
func (s StreamingService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	planning := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers,
		Capabilities: request.Capabilities, CommonSteps: request.CommonSteps,
		Changes: make([]schemaext.ChangeRecord, len(request.Objects))}
	for i, object := range request.Objects {
		value, ok := object.Value.(*ydbstreaming.Desired)
		if !ok || value == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired streaming query", schemaext.ErrInvalidValue)
		}
		planning.Changes[i] = schemaext.ChangeRecord{Subject: object.Ref,
			Value: &ydbdiff.StreamingQuery{After: value.Clone().(*ydbstreaming.Desired)}}
	}
	reply, err := s.PlanFeatures(ctx, planning)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: reply.Complete, Contributions: reply.Contributions}
	for _, diagnostic := range reply.Diagnostics {
		diagnostic = diagnostic.Clone()
		diagnostic.Problem.Kind = string(ydbstreaming.Kind)
		result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Problem: diagnostic.Problem, Object: diagnostic.Change})
	}
	for _, receipt := range reply.Changes {
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: receipt.Subject,
			Strategy: "create the declared configuration with YDB streaming-query statements", Steps: receipt.Steps})
	}
	return result, nil
}
