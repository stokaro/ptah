package ydbplan

import (
	"context"
	"fmt"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
)

// PlanDeclarations derives one CREATE SECRET per declared secret, with the
// value its variable holds when the statement runs. A declaration that asks
// for a rotation is created all the same: a schema rendered from scratch has
// no earlier value to replace. The operations share the owner's graph rules
// with migrations, so a secret precedes the data sources that read it and a
// declared table at its path is refused.
func (s SecretService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	planning := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers,
		Capabilities: request.Capabilities, CommonSteps: request.CommonSteps,
		Changes: make([]schemaext.ChangeRecord, len(request.Objects))}
	for i, object := range request.Objects {
		value, ok := object.Value.(*ydbsecret.Desired)
		if !ok || value == nil {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: expected a desired secret", schemaext.ErrInvalidValue)
		}
		after := *value
		after.Rotate = false
		planning.Changes[i] = schemaext.ChangeRecord{Subject: object.Ref, Value: &ydbdiff.Secret{After: &after}}
	}
	reply, err := s.PlanFeatures(ctx, planning)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: reply.Complete, Contributions: reply.Contributions}
	for _, diagnostic := range reply.Diagnostics {
		diagnostic = diagnostic.Clone()
		diagnostic.Problem.Kind = string(ydbsecret.Kind)
		result.Diagnostics = append(result.Diagnostics, featureplan.DeclarationDiagnostic{Problem: diagnostic.Problem, Object: diagnostic.Change})
	}
	for _, receipt := range reply.Changes {
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: receipt.Subject,
			Strategy: "create the declared secret with the value its variable holds", Steps: receipt.Steps})
	}
	return result, nil
}
