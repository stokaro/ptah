package main

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// planOwnedEdit requires the complete desired and observed column definitions.
// Its rename operation makes dispatch visible without interpreting the host diff.
func (p *owner) planOwnedEdit(table *protocol.PlanningTable) ([]*protocol.PlanningStep, error) {
	if table.GetAction() != protocol.PlanningTable_ALTER ||
		len(table.GetCurrent().GetColumns()) != 1 ||
		len(table.GetDesired().GetColumns()) != 1 ||
		table.Current.Columns[0].GetDataType() != "INTEGER" ||
		table.Desired.Columns[0].GetType() != "TEXT" {
		return nil, fmt.Errorf("owned edit lost its complete column context")
	}
	name := table.Desired.Name
	if name == "refused" {
		return nil, fmt.Errorf("fixture refuses this owned column edit")
	}
	if name == "noop" {
		return nil, nil
	}
	envelope := proto.CloneOf(p.operationEnvelope)
	envelope.Owner = &protocol.Reference{Engine: "fixture", Kind: "table", Schema: table.Desired.Schema, Name: name}
	envelope.Payload = []byte(`{"action":"rename","to":"accepted"}`)
	value, err := normalize(&protocol.ValueInput{Id: "owned-edit", Representation: protocol.ValueInput_OPERATION, Value: envelope})
	if err != nil {
		return nil, err
	}
	return []*protocol.PlanningStep{{Id: &protocol.StepIdentity{Owner: "example.org/fixture", Name: "owned-edit"},
		Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_ALTER_TABLE, Parent: proto.CloneOf(table.Subject)},
		Metadata: &protocol.PlanningMetadata{Transaction: protocol.PlanningMetadata_FORBIDDEN,
			Impact: value.OperationSemantics.Impact, Reason: value.OperationSemantics.Reason,
			Effects: []*protocol.PlanningEffect{{Subject: proto.CloneOf(table.Subject), Action: protocol.PlanningEffect_ALTER}}}}}, nil
}
