package main

import (
	"context"
	"fmt"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const declarationKind = "example.org/fixture/authored-object"

func configureDeclarations(description *protocol.Description) {
	description.Features = append(description.Features, provider.SchemaDeclarationsFeature, provider.ValidateSchemaFeature, provider.TextUnitsFeature, provider.PlanningLifecycleFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.SchemaDeclarationsFeature, provider.ValidateSchemaFeature, provider.TextUnitsFeature, provider.PlanningLifecycleFeature)
	desired := proto.CloneOf(engine.ValueModels[len(engine.ValueModels)-1])
	desired.Kind, desired.Representation = declarationKind, protocol.ValueInput_DESIRED
	engine.ValueModels = append(engine.ValueModels, desired)
	engine.Declarations = []*protocol.ModelDeclarations{{Id: "authored-objects", Kinds: []string{declarationKind}, OperationKinds: []string{operationKind}}}
}

func normalizeDeclaration(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	if input.GetValue().GetOwner().GetKind() != declarationKind {
		return nil, fmt.Errorf("authored object has the wrong owner kind")
	}
	operation := proto.CloneOf(input)
	operation.Representation, operation.Value.Kind = protocol.ValueInput_OPERATION, operationKind
	operation.Value.Owner.Kind = "table"
	value, err := normalize(operation)
	if err != nil {
		return nil, err
	}
	value.Value.Kind = declarationKind
	value.Value.Owner.Kind, value.NormalizedOwner.Kind = declarationKind, declarationKind
	value.OperationSemantics = nil
	return value, nil
}

func (*owner) ValidateSchema(ctx context.Context, request *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, table := range request.GetTables() {
		if table.GetDeclaration().GetName() == "invalid" {
			return &protocol.SchemaValidation{Complete: true, Diagnostics: []*protocol.SchemaDiagnostic{{Code: protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE,
				Kind: "table", Object: "invalid",
				Message: "Fixture refuses this authored table"}}}, nil
		}
	}
	return &protocol.SchemaValidation{Complete: true}, nil
}

func (p *owner) PlanDeclarations(ctx context.Context, request *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error) {
	result := &protocol.PlannedDeclarations{Complete: true}
	contribution := &protocol.PlanningContribution{Owner: "example.org/fixture"}
	for i, object := range request.GetObjects() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := object.GetSubject().GetName().GetSource()
		if name == "refused" {
			return &protocol.PlannedDeclarations{Complete: true, Diagnostics: []*protocol.DeclarationDiagnostic{{Object: new(uint32(i)), Problem: &protocol.SchemaDiagnostic{Code: protocol.SchemaDiagnostic_UNSUPPORTED_FEATURE,
				Kind: declarationKind, Object: name,
				Message: "Fixture refuses this authored object"}}}}, nil
		}
		envelope := proto.CloneOf(p.operationEnvelope)
		envelope.Payload = append([]byte(nil), object.GetValue().GetValue().GetPayload()...)
		envelope.Owner = proto.CloneOf(object.GetValue().GetValue().GetOwner())
		envelope.Owner.Kind = "table"
		value, err := normalize(&protocol.ValueInput{Id: "declaration/" + strconv.Itoa(i), Representation: protocol.ValueInput_OPERATION, Value: envelope})
		if err != nil {
			return nil, err
		}
		step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: contribution.Owner, Name: value.Id}, Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT},
			Metadata: &protocol.PlanningMetadata{Transaction: protocol.PlanningMetadata_FORBIDDEN,
				Impact: value.OperationSemantics.Impact, Reason: value.OperationSemantics.Reason,
				Effects: []*protocol.PlanningEffect{{Subject: proto.CloneOf(object.Subject), Action: protocol.PlanningEffect_CREATE}}}}
		contribution.Steps = append(contribution.Steps, step)
		if name != "omitted" {
			result.Declarations = append(result.Declarations, &protocol.DeclarationReceipt{Subject: proto.CloneOf(object.Subject), Strategy: "Create the authored object", Steps: []*protocol.StepIdentity{proto.CloneOf(step.Id)}})
		}
		if name == "cycle" {
			contribution.Dependencies = append(contribution.Dependencies, &protocol.PlanningDependency{Before: proto.CloneOf(step.Id), After: proto.CloneOf(step.Id)})
		}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = []*protocol.PlanningContribution{contribution}
	}
	return result, nil
}

func (p *owner) planAuthoredParent(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable, common *protocol.CommonPlanningStep, index int) (*protocol.PlanningStep, *protocol.PlanningRewrite, error) {
	if table.GetCurrent() != nil {
		return nil, nil, fmt.Errorf("authored parent received observed state")
	}
	desired := table.GetDesired()
	if desired.GetName() == "unclaimed" {
		return nil, nil, nil
	}
	if desired.GetName() == "captured" && (len(desired.GetIndexes()) != 1 || len(desired.GetTriggers()) != 1 || desired.GetColumns()[0].GetComment() != "retained") {
		return nil, nil, fmt.Errorf("authored parent lost its complete declaration")
	}
	envelope := proto.CloneOf(p.operationEnvelope)
	envelope.Owner = &protocol.Reference{Engine: request.Engine, Kind: "table", Schema: desired.Schema, Name: desired.Name}
	envelope.Payload = []byte(`{"action":"create"}`)
	value, err := normalize(&protocol.ValueInput{Id: "parent/" + strconv.Itoa(index), Representation: protocol.ValueInput_OPERATION, Value: envelope})
	if err != nil {
		return nil, nil, err
	}
	metadata := proto.CloneOf(common.Metadata)
	metadata.Transaction, metadata.Impact, metadata.Reason = protocol.PlanningMetadata_FORBIDDEN, value.OperationSemantics.Impact, value.OperationSemantics.Reason
	step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: "example.org/fixture", Name: value.Id},
		Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT, Phase: protocol.PlanningOperation_LIFECYCLE},
		Metadata: metadata}
	return step, &protocol.PlanningRewrite{Sources: []*protocol.StepIdentity{proto.CloneOf(common.Id)}, Replacement: proto.CloneOf(step.Id)}, nil
}
