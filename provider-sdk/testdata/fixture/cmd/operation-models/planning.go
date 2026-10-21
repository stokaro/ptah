package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func normalizeChange(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	if input.GetValue().GetKind() != changeKind {
		return nil, fmt.Errorf("unknown change")
	}
	operation := proto.CloneOf(input)
	operation.Representation = protocol.ValueInput_OPERATION
	operation.Value.Kind = operationKind
	normalized, err := normalize(operation)
	if err != nil {
		return nil, err
	}
	facts := normalized.OperationSemantics
	normalized.Value.Kind = changeKind
	normalized.ChangeSemantics = &protocol.ChangeSemantics{Impact: facts.Impact, Reason: facts.Reason, Access: facts.Access, AccessReason: facts.AccessReason,
		ReplacesOwner: new(false), RemovesState: new(facts.Impact == protocol.ChangeSemantics_DESTRUCTIVE)}
	normalized.OperationSemantics = nil
	return normalized, nil
}

// PlanFeatures is offline. Executable table operations are derived from the
// captured change, and only the host can order them against its common graph.
func (*owner) PlanFeatures(ctx context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	transactional, known := request.GetCapabilities()["transactional_ddl"]
	if !known || transactional {
		return &protocol.PlannedFeatures{Complete: true, Diagnostics: []*protocol.PlanningDiagnostic{{Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE, Kind: "table", Message: "Fixture requires explicit nontransactional DDL"}}}, nil
	}
	contribution := &protocol.PlanningContribution{Owner: "example.org/fixture"}
	result := &protocol.PlannedFeatures{Complete: true}
	for i, change := range request.GetChanges() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		step, err := planTableChange(change, strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		contribution.Steps = append(contribution.Steps, step)
		result.Changes = append(result.Changes, &protocol.PlanningReceipt{Subject: proto.CloneOf(change.GetSubject()), Kind: changeKind, Strategy: "Apply the captured table action", Steps: []*protocol.StepIdentity{proto.CloneOf(step.Id)}})
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = append(result.Contributions, contribution)
	}
	for _, table := range request.GetTables() {
		if table.GetAction() == protocol.PlanningTable_CONTEXT {
			continue
		}
		for _, kind := range request.GetParentKinds() {
			result.Parents = append(result.Parents, &protocol.PlanningParentReceipt{Action: table.GetAction(), Receipt: &protocol.PlanningReceipt{
				Subject: proto.CloneOf(table.GetSubject()), Kind: kind, Strategy: "Accept fixture common table operation"}})
		}
	}
	return result, nil
}

func planTableChange(change *protocol.PlanningChange, id string) (*protocol.PlanningStep, error) {
	envelope := proto.CloneOf(change.GetValue().GetValue())
	envelope.Kind = operationKind
	value, err := normalize(&protocol.ValueInput{Id: "table/" + id, Value: envelope, Representation: protocol.ValueInput_OPERATION})
	if err != nil {
		return nil, err
	}
	var op operation
	if err := json.Unmarshal(value.Value.Payload, &op); err != nil {
		return nil, err
	}
	role, action := protocol.NativeOperation_STATEMENT, protocol.PlanningEffect_CREATE
	var parent *protocol.ComparisonIdentity
	switch op.Action {
	case "drop":
		action = protocol.PlanningEffect_DROP
	case "rename":
		role, action = protocol.NativeOperation_ALTER_TABLE, protocol.PlanningEffect_ALTER
		parent = proto.CloneOf(change.GetSubject())
	}
	transaction := protocol.PlanningMetadata_FORBIDDEN
	if op.Transaction == "required" {
		transaction = protocol.PlanningMetadata_REQUIRED
	}
	return &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: "example.org/fixture", Name: "table/" + id},
		Operation: &protocol.PlanningOperation{Value: value, Role: role, Parent: parent},
		Metadata: &protocol.PlanningMetadata{Transaction: transaction, Impact: value.OperationSemantics.Impact, Reason: value.OperationSemantics.Reason,
			Effects: []*protocol.PlanningEffect{{Subject: proto.CloneOf(change.GetSubject()), Action: action}}}}, nil
}
