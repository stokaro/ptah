package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// planningReply binds only an SDK-validated complete result. The selected engine
// runtime checks model snapshots and receipts again before the host graph joins it.
func (t *target) planningReply(request *protocol.PlanFeaturesRequest, reply *protocol.PlannedFeatures) (featureplan.Result, error) {
	result := featureplan.Result{Complete: reply.GetComplete()}
	for _, contribution := range reply.GetContributions() {
		bound := plangraph.Contribution[featureplan.Operation]{Owner: contribution.GetOwner()}
		for _, step := range contribution.GetSteps() {
			operation, err := t.planningOperation(request, step.GetOperation())
			if err != nil {
				return featureplan.Result{}, err
			}
			metadata := step.GetMetadata()
			transaction := map[protocol.PlanningMetadata_Transaction]plangraph.Transaction{
				protocol.PlanningMetadata_UNKNOWN:   plangraph.TransactionUnknown,
				protocol.PlanningMetadata_ALLOWED:   plangraph.TransactionAllowed,
				protocol.PlanningMetadata_REQUIRED:  plangraph.TransactionRequired,
				protocol.PlanningMetadata_FORBIDDEN: plangraph.TransactionForbidden,
			}[metadata.GetTransaction()]
			placement := map[protocol.PlanningMetadata_Placement]plangraph.Placement{
				protocol.PlanningMetadata_DEFAULT: plangraph.PlacementDefault,
				protocol.PlanningMetadata_EARLY:   plangraph.PlacementEarly,
			}[metadata.GetPlacement()]
			bound.Steps = append(bound.Steps, plangraph.Step[featureplan.Operation]{
				ID:          boundPlanningStepID(step.GetId()),
				Payload:     operation,
				Effects:     boundPlanningEffects(metadata.GetEffects()),
				Transaction: transaction,
				Impact:      boundPlanningImpact(metadata.GetImpact(), metadata.GetReason()),
				Placement:   placement,
			})
		}
		for _, dependency := range contribution.GetDependencies() {
			bound.Dependencies = append(bound.Dependencies, plangraph.Dependency{Before: boundPlanningStepID(dependency.GetBefore()), After: boundPlanningStepID(dependency.GetAfter())})
		}
		result.Contributions = append(result.Contributions, bound)
	}
	for _, receipt := range reply.GetChanges() {
		result.Changes = append(result.Changes, featureplan.ChangePlan{
			Subject:  boundIdentity(receipt.GetSubject()),
			Kind:     schemaext.Kind(receipt.GetKind()),
			Strategy: receipt.GetStrategy(),
			Steps:    boundPlanningStepIDs(receipt.GetSteps()),
		})
	}
	for _, parent := range reply.GetParents() {
		receipt := parent.GetReceipt()
		result.Parents = append(result.Parents, featureplan.ParentPlan{
			Subject:  boundIdentity(receipt.GetSubject()),
			Kind:     schemaext.Kind(receipt.GetKind()),
			Action:   boundPlanningParentAction(parent.GetAction()),
			Strategy: receipt.GetStrategy(),
			Steps:    boundPlanningStepIDs(receipt.GetSteps()),
		})
	}
	for _, rewrite := range reply.GetRewrites() {
		result.Rewrites = append(result.Rewrites, plangraph.Rewrite{Sources: boundPlanningStepIDs(rewrite.GetSources()), Replacement: boundPlanningStepID(rewrite.GetReplacement())})
	}
	for _, diagnostic := range reply.GetDiagnostics() {
		result.Diagnostics = append(result.Diagnostics, boundPlanningDiagnostic(diagnostic))
	}
	return result, nil
}

func (t *target) planningOperation(request *protocol.PlanFeaturesRequest, operation *protocol.PlanningOperation) (featureplan.Operation, error) {
	contract := t.host.models[modelKey{kind: schemaext.Kind(operation.GetValue().GetValue().GetKind()), representation: schemaext.Operation}]
	if contract == nil || contract.engines[request.GetEngine()] == nil {
		return featureplan.Operation{}, fmt.Errorf("%w: planning operation model is unavailable", schemaext.ErrInvalidValue)
	}
	model, err := contract.resolved(modelWire{Format: 1, Engine: request.GetEngine(), Features: slices.Clone(request.GetTargetFeatures())}, operation.GetValue())
	if err != nil {
		return featureplan.Operation{}, err
	}
	roles := map[protocol.NativeOperation_Role]ast.ExtensionRole{protocol.NativeOperation_STATEMENT: ast.StatementExtension, protocol.NativeOperation_ALTER_TABLE: ast.AlterExtension}
	phases := map[protocol.PlanningOperation_Phase]featureplan.Phase{
		protocol.PlanningOperation_DEFAULT:      featureplan.PhaseDefault,
		protocol.PlanningOperation_DEPENDENT:    featureplan.PhaseDependent,
		protocol.PlanningOperation_PREREQUISITE: featureplan.PhasePrerequisite,
		protocol.PlanningOperation_TYPE:         featureplan.PhaseType,
		protocol.PlanningOperation_LIFECYCLE:    featureplan.PhaseLifecycle,
	}
	return featureplan.Operation{
		Role:    roles[operation.GetRole()],
		Parent:  boundIdentity(operation.GetParent()),
		Payload: &resolvedOperation{model: model},
		Notes:   slices.Clone(operation.GetNotes()),
		Phase:   phases[operation.GetPhase()],
	}, nil
}

func boundPlanningDiagnostic(diagnostic *protocol.PlanningDiagnostic) featureplan.Diagnostic {
	codes := map[protocol.PlanningDiagnostic_Code]schemavalidation.Code{
		protocol.PlanningDiagnostic_INVALID_SCHEMA:      schemavalidation.InvalidSchema,
		protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE: schemavalidation.UnsupportedFeature,
		protocol.PlanningDiagnostic_OMITTED_DECLARATION: schemavalidation.OmittedDeclaration,
	}
	result := featureplan.Diagnostic{
		Problem: schemavalidation.Diagnostic{Code: codes[diagnostic.GetCode()], Kind: diagnostic.GetKind(), Object: diagnostic.GetObject(), Feature: diagnostic.GetFeature(), Message: diagnostic.GetMessage()},
	}
	switch index := diagnostic.GetInput().(type) {
	case *protocol.PlanningDiagnostic_Change:
		result.Change = new(int(index.Change))
	case *protocol.PlanningDiagnostic_Parent:
		result.Parent = new(int(index.Parent))
	}
	return result
}
