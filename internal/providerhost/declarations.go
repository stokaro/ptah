package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

type modelDeclarationPlanner struct {
	target      *target
	declaration *protocol.ModelDeclarations
}

func (t *target) registerDeclarations(result *engine.Provider) {
	for _, declaration := range t.engine.GetDeclarations() {
		result.Declarations = append(result.Declarations, engine.DeclarationPlanning{
			Target: t.engine.GetId(), Kinds: boundKinds(declaration.GetKinds()), OperationKinds: boundKinds(declaration.GetOperationKinds()),
			Service: modelDeclarationPlanner{target: t, declaration: declaration},
		})
	}
}

// PlanDeclarations captures authored values without supplying current state.
// The runtime validates receipts before the host schedules the returned graph.
func (s modelDeclarationPlanner) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := ctx.Err(); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	remote, err := s.declarationRequest(request)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	reply, err := s.target.host.client.PlanDeclarations(ctx, remote)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result, err := s.target.declarationReply(remote, reply)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	return result, nil
}

func (s modelDeclarationPlanner) declarationRequest(request featureplan.DeclarationRequest) (*protocol.PlanDeclarationsRequest, error) {
	if request.Target != s.target.engine.GetId() {
		return nil, fmt.Errorf("%w: declaration target changed", schemaext.ErrInvalidValue)
	}
	if err := onlyFields(request, "Target", "Identifiers", "Capabilities", "Objects", "Tables", "Types", "CommonSteps"); err != nil {
		return nil, err
	}
	identifiers, err := comparisonIdentifiers(request.Identifiers)
	if err != nil {
		return nil, err
	}
	if err := request.Capabilities.Validate(); err != nil {
		return nil, err
	}
	remote := &protocol.PlanDeclarationsRequest{Engine: request.Target, DeclarationId: s.declaration.GetId(), Identifiers: identifiers, Capabilities: make(map[string]bool), Types: planningTypes(request.Types)}
	for name, value := range request.Capabilities.All() {
		remote.Capabilities[string(name)] = value
	}
	capture := planningCapture{
		comparisonCapture: comparisonCapture{target: s.target, request: &protocol.CompareModelsRequest{Engine: request.Target, TargetFeatures: featureSet(s.target.engine.GetFeatures())}},
		identifiers:       request.Identifiers,
	}
	for i, object := range request.Objects {
		value, err := capture.value(schemaext.Desired, object.Value, strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		remote.Objects = append(remote.Objects, &protocol.ComparisonRecord{Kind: string(object.Value.Kind()), Subject: comparisonIdentity(object.Ref), Value: value, Targets: slices.Clone(object.Targets)})
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, table := range request.Tables {
		subject := builder.TableParts(table.Table.Schema, table.Table.Name)
		declaration, err := capture.declaration(subject, table)
		if err != nil {
			return nil, err
		}
		remote.Tables = append(remote.Tables, &protocol.ValidationTable{Subject: comparisonIdentity(subject), Declaration: declaration})
	}
	for _, step := range request.CommonSteps {
		captured, err := capture.commonStep(step)
		if err != nil {
			return nil, err
		}
		remote.CommonSteps = append(remote.CommonSteps, captured)
	}
	remote.TargetFeatures = slices.Clone(capture.request.GetTargetFeatures())
	return remote, nil
}

func (t *target) declarationReply(request *protocol.PlanDeclarationsRequest, reply *protocol.PlannedDeclarations) (featureplan.DeclarationResult, error) {
	// Both contracts carry the same operation and graph vocabulary. SDK
	// declaration validation already accounted for every contributed step.
	bound, err := t.planningReply(&protocol.PlanFeaturesRequest{Engine: request.Engine, TargetFeatures: request.TargetFeatures}, &protocol.PlannedFeatures{Complete: reply.Complete, Contributions: reply.Contributions})
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	result := featureplan.DeclarationResult{Complete: reply.Complete, Contributions: bound.Contributions}
	for _, receipt := range reply.GetDeclarations() {
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: boundIdentity(receipt.GetSubject()), Strategy: receipt.GetStrategy(), Steps: boundPlanningStepIDs(receipt.GetSteps())})
	}
	for _, diagnostic := range reply.GetDiagnostics() {
		problem := diagnostic.GetProblem()
		code, err := schemaDiagnosticCode(problem.GetCode())
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		captured := featureplan.DeclarationDiagnostic{Problem: schemavalidation.Diagnostic{Code: code, Kind: problem.GetKind(), Object: problem.GetObject(), Feature: problem.GetFeature(), Message: problem.GetMessage()}}
		if diagnostic.Object != nil {
			captured.Object = new(int(diagnostic.GetObject()))
		}
		result.Diagnostics = append(result.Diagnostics, captured)
	}
	return result, nil
}
