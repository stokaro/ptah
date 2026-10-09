package ydbplan

import (
	"context"
	"fmt"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
)

// PlanDeclarations derives CREATE operations from authored node definitions.
// The create operands express the requested command, not an observation about
// a database. They share the owner's operation and graph rules with migrations.
func (s CoordinationService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	planning := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers,
		Capabilities: request.Capabilities, CommonSteps: request.CommonSteps,
		Changes: make([]schemaext.ChangeRecord, len(request.Objects))}
	for i, object := range request.Objects {
		value, ok := object.Value.(*ydbcoordination.Desired)
		if !ok || value == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired coordination node", schemaext.ErrInvalidValue)
		}
		planning.Changes[i] = schemaext.ChangeRecord{Subject: object.Ref,
			Value: &ydbdiff.CoordinationNode{After: new(*value)}}
	}
	reply, err := s.PlanFeatures(ctx, planning)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: reply.Complete, Contributions: reply.Contributions}
	for _, diagnostic := range reply.Diagnostics {
		diagnostic = diagnostic.Clone()
		diagnostic.Problem.Kind = string(ydbcoordination.Kind)
		result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Problem: diagnostic.Problem, Object: diagnostic.Change})
	}
	for _, receipt := range reply.Changes {
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: receipt.Subject,
			Strategy: "create the declared configuration through the coordination service", Steps: receipt.Steps})
	}
	return result, nil
}
