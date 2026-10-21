package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// A parent assessment can replace a common operand without a native change.
// This fixture deliberately supports only plain column additions.
func (p *owner) planColumnRewrites(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable) ([]*protocol.PlanningStep, []*protocol.PlanningRewrite, error) {
	var steps []*protocol.PlanningStep
	var rewrites []*protocol.PlanningRewrite
	for i, common := range request.GetCommonSteps() {
		column := common.GetAddedColumn()
		if column == nil || !proto.Equal(common.GetParent(), table.GetSubject()) {
			continue
		}
		if table.GetAction() != protocol.PlanningTable_ALTER || table.GetDesired() == nil {
			return nil, nil, fmt.Errorf("column rewrite requires an altered table declaration")
		}
		plain := &protocol.PlanningColumn{Name: column.GetName(), Type: column.GetType(), Nullable: column.GetNullable()}
		if !proto.Equal(column, plain) {
			return nil, nil, fmt.Errorf("unsupported added column properties")
		}
		payload, err := json.Marshal(operation{Action: "add-column", Column: &addedColumn{Name: plain.Name, Type: plain.Type, Nullable: new(plain.Nullable)}})
		if err != nil {
			return nil, nil, err
		}
		id := "column/" + strconv.Itoa(i)
		envelope := proto.CloneOf(p.operationEnvelope)
		envelope.Payload = payload
		envelope.Owner = &protocol.Reference{Engine: request.GetEngine(), Kind: "table", Schema: table.GetDesired().GetSchema(), Name: table.GetDesired().GetName()}
		value, err := normalize(&protocol.ValueInput{Id: id, Representation: protocol.ValueInput_OPERATION, Value: envelope})
		if err != nil {
			return nil, nil, err
		}
		metadata := proto.CloneOf(common.GetMetadata())
		metadata.Transaction = protocol.PlanningMetadata_FORBIDDEN
		metadata.Impact, metadata.Reason = value.GetOperationSemantics().GetImpact(), value.GetOperationSemantics().GetReason()
		step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: "example.org/fixture", Name: id}, Metadata: metadata,
			Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_ALTER_TABLE, Parent: proto.CloneOf(table.GetSubject())}}
		steps = append(steps, step)
		rewrites = append(rewrites, &protocol.PlanningRewrite{Sources: []*protocol.StepIdentity{proto.CloneOf(common.GetId())}, Replacement: proto.CloneOf(step.Id)})
	}
	return steps, rewrites, nil
}
