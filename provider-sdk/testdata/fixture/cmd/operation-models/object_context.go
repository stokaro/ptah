package main

import (
	"context"
	"fmt"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const contextObjectKind = "example.org/fixture/dependent-object"

func configureObjectContext(description *protocol.Description) {
	description.MinContract = &protocol.ContractVersion{Major: provider.Revision, Minor: 7}
	description.Features = append(description.Features, provider.PlanningObjectContextFeature, provider.PlanningLifecycleFeature, provider.TextUnitsFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.PlanningObjectContextFeature, provider.PlanningLifecycleFeature, provider.TextUnitsFeature)
	for _, representation := range []protocol.ValueInput_Representation{protocol.ValueInput_DESIRED, protocol.ValueInput_OBSERVED} {
		model := proto.CloneOf(engine.ValueModels[len(engine.ValueModels)-1])
		model.Kind, model.Representation = contextObjectKind, representation
		engine.ValueModels = append(engine.ValueModels, model)
	}
	engine.Planning = append(engine.Planning, &protocol.ModelPlanning{Id: "object-context", ContextKinds: []string{contextObjectKind}, OperationKinds: []string{operationKind}})
}

func normalizeContextObject(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	if input.GetValue().GetOwner().GetKind() != contextObjectKind {
		return nil, fmt.Errorf("dependent object has the wrong owner kind")
	}
	operation := proto.CloneOf(input)
	operation.Representation, operation.Value.Kind = protocol.ValueInput_OPERATION, operationKind
	operation.Value.Owner.Kind = "table"
	value, err := normalize(operation)
	if err != nil {
		return nil, err
	}
	value.Value.Kind = contextObjectKind
	value.Value.Owner.Kind, value.NormalizedOwner.Kind = contextObjectKind, contextObjectKind
	value.OperationSemantics = nil
	return value, nil
}

// An unchanged native object needs a step whenever the common plan removes
// its input. The fixture models that step as creation of a marker table.
func (p *owner) planObjectContext(ctx context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	result := &protocol.PlannedFeatures{Complete: true}
	contribution := &protocol.PlanningContribution{Owner: "example.org/fixture"}
	for i, subject := range provider.PlanningObjectSubjects(request) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if subject.GetName().GetSource() == "refused" {
			return &protocol.PlannedFeatures{Complete: true, Diagnostics: []*protocol.PlanningDiagnostic{{
				Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE, Kind: contextObjectKind, Message: "Dependency assessment refused",
				Input: &protocol.PlanningDiagnostic_ContextObject{ContextObject: uint32(i)},
			}}}, nil
		}
		receipt := &protocol.PlanningReceipt{Subject: proto.CloneOf(subject), Kind: contextObjectKind, Strategy: "Retain unchanged dependent object"}
		if subject.GetName().GetSource() == "omitted" {
			continue
		}
		for j, common := range request.GetCommonSteps() {
			for _, effect := range common.GetMetadata().GetEffects() {
				if effect.GetAction() != protocol.PlanningEffect_DROP || effect.GetSubject().GetKind() != "table" {
					continue
				}
				envelope := proto.CloneOf(p.operationEnvelope)
				envelope.Owner = &protocol.Reference{Engine: request.GetEngine(), Kind: "table", Schema: subject.GetSchema().GetSource(), Name: subject.GetName().GetSource()}
				envelope.Payload = []byte(`{"action":"create"}`)
				value, err := normalize(&protocol.ValueInput{Id: "context/" + strconv.Itoa(i) + "/" + strconv.Itoa(j), Representation: protocol.ValueInput_OPERATION, Value: envelope})
				if err != nil {
					return nil, err
				}
				step := &protocol.PlanningStep{
					Id:        &protocol.StepIdentity{Owner: contribution.Owner, Name: value.Id},
					Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT, Phase: protocol.PlanningOperation_LIFECYCLE},
					Metadata: &protocol.PlanningMetadata{
						Transaction: protocol.PlanningMetadata_FORBIDDEN, Impact: value.OperationSemantics.Impact, Reason: value.OperationSemantics.Reason,
						Effects: []*protocol.PlanningEffect{{Subject: proto.CloneOf(effect.Subject), Action: protocol.PlanningEffect_READ}},
					},
				}
				contribution.Steps = append(contribution.Steps, step)
				receipt.Steps = append(receipt.Steps, proto.CloneOf(step.Id))
			}
		}
		result.Objects = append(result.Objects, receipt)
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []*protocol.PlanningContribution{contribution}
	}
	return result, nil
}
