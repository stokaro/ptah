package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

func (r *Runtime) validateDeclarationReply(ctx context.Context, service int, request featureplan.DeclarationRequest, reply featureplan.DeclarationResult) (featureplan.DeclarationResult, error) {
	if err := reply.ValidateOutcome(request); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	if len(reply.Diagnostics) != 0 {
		return snapshotDeclarationDiagnostics(request, reply.Diagnostics)
	}
	if len(reply.Declarations) != len(request.Objects) {
		return featureplan.DeclarationResult{}, fmt.Errorf("%w: declaration planning changed the result count", schemaext.ErrInvalidValue)
	}
	owner := r.declarationServices[service]
	tables := declarationTables(request)
	parents := make([]objectidentity.ID, 0, len(tables))
	for _, table := range tables {
		parents = append(parents, table.Subject)
	}
	contributions, steps, err := r.snapshotPlanningContributions(ctx, owner.owner, owner.OperationKinds, parents, reply.Contributions)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	for _, common := range request.CommonSteps {
		if steps[common.ID] {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: declaration reused a common step identity", schemaext.ErrInvalidValue)
		}
	}
	result := featureplan.DeclarationResult{Complete: true, Contributions: contributions, Declarations: slices.Clone(reply.Declarations)}
	covered := make(map[plangraph.StepID]bool)
	for i, receipt := range result.Declarations {
		if receipt.Subject != request.Objects[i].Ref || !reversalText(receipt.Strategy) {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: declaration changed its subject or omitted its strategy", schemaext.ErrInvalidValue)
		}
		seen := make(map[plangraph.StepID]bool)
		for _, step := range receipt.Steps {
			if !steps[step] || seen[step] {
				return featureplan.DeclarationResult{}, fmt.Errorf("%w: declaration names a missing or duplicate step", schemaext.ErrInvalidValue)
			}
			seen[step], covered[step] = true, true
		}
		result.Declarations[i].Steps = slices.Clone(receipt.Steps)
	}
	if len(covered) != len(steps) {
		return featureplan.DeclarationResult{}, fmt.Errorf("%w: declaration planning emitted an unaccounted step", schemaext.ErrInvalidValue)
	}
	return result, ctx.Err()
}

func snapshotDeclarationDiagnostics(request featureplan.DeclarationRequest, diagnostics []featureplan.DeclarationDiagnostic) (featureplan.DeclarationResult, error) {
	result := featureplan.DeclarationResult{Complete: true, Diagnostics: make([]featureplan.DeclarationDiagnostic, len(diagnostics))}
	for i, diagnostic := range diagnostics {
		if diagnostic.Object != nil && schemaext.Kind(diagnostic.Problem.Kind) != request.Objects[*diagnostic.Object].Value.Kind() {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: declaration diagnostic changed its input kind", schemaext.ErrInvalidValue)
		}
		result.Diagnostics[i] = diagnostic.Clone()
	}
	return result, nil
}
