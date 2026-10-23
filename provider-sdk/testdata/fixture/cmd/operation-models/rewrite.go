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
		if !proto.Equal(common.GetParent(), table.GetSubject()) {
			continue
		}
		if column == nil {
			if p.ownedLifecycle && table.GetAction() == protocol.PlanningTable_DROP && droppedTable(common) {
				step, rewrite, err := p.planTableDrop(request, table, common, i)
				if err != nil {
					return nil, nil, err
				}
				steps, rewrites = append(steps, step), append(rewrites, rewrite)
				continue
			}
			if dropped := droppedColumn(common); p.ownedLifecycle && dropped != "" {
				step, rewrite, err := p.planColumnDrop(request, table, common, dropped, i)
				if err != nil {
					return nil, nil, err
				}
				steps, rewrites = append(steps, step), append(rewrites, rewrite)
			}
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

// droppedColumn names the column a common step drops, or is empty when the
// step drops none. A column step reads its table and drops one column of it.
func droppedColumn(common *protocol.CommonPlanningStep) string {
	for _, effect := range common.GetMetadata().GetEffects() {
		if effect.GetAction() == protocol.PlanningEffect_DROP && effect.GetSubject().GetKind() == "column" {
			return effect.GetSubject().GetName().GetSource()
		}
	}
	return ""
}

func (p *owner) planColumnDrop(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable, common *protocol.CommonPlanningStep, column string, index int) (*protocol.PlanningStep, *protocol.PlanningRewrite, error) {
	if table.GetAction() != protocol.PlanningTable_ALTER || table.GetDesired() == nil {
		return nil, nil, fmt.Errorf("column rewrite requires an altered table declaration")
	}
	payload, err := json.Marshal(operation{Action: "drop-column", DropColumn: column})
	if err != nil {
		return nil, nil, err
	}
	id := "column/" + strconv.Itoa(index)
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
	return step, &protocol.PlanningRewrite{Sources: []*protocol.StepIdentity{proto.CloneOf(common.GetId())}, Replacement: proto.CloneOf(step.Id)}, nil
}

// droppedTable reports whether a common step drops its table, the only effect a
// table removal carries.
func droppedTable(common *protocol.CommonPlanningStep) bool {
	for _, effect := range common.GetMetadata().GetEffects() {
		if effect.GetAction() == protocol.PlanningEffect_DROP && effect.GetSubject().GetKind() == "table" {
			return true
		}
	}
	return false
}

func (p *owner) planTableDrop(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable, common *protocol.CommonPlanningStep, index int) (*protocol.PlanningStep, *protocol.PlanningRewrite, error) {
	if table.GetCurrent() == nil {
		return nil, nil, fmt.Errorf("table removal requires an observed table")
	}
	payload, err := json.Marshal(operation{Action: "drop"})
	if err != nil {
		return nil, nil, err
	}
	id := "table-drop/" + strconv.Itoa(index)
	envelope := proto.CloneOf(p.operationEnvelope)
	envelope.Payload = payload
	envelope.Owner = &protocol.Reference{Engine: request.GetEngine(), Kind: "table", Schema: table.GetCurrent().GetSchema(), Name: table.GetCurrent().GetName()}
	value, err := normalize(&protocol.ValueInput{Id: id, Representation: protocol.ValueInput_OPERATION, Value: envelope})
	if err != nil {
		return nil, nil, err
	}
	metadata := proto.CloneOf(common.GetMetadata())
	metadata.Transaction = protocol.PlanningMetadata_FORBIDDEN
	metadata.Impact, metadata.Reason = value.GetOperationSemantics().GetImpact(), value.GetOperationSemantics().GetReason()
	step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: "example.org/fixture", Name: id}, Metadata: metadata,
		Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT}}
	return step, &protocol.PlanningRewrite{Sources: []*protocol.StepIdentity{proto.CloneOf(common.GetId())}, Replacement: proto.CloneOf(step.Id)}, nil
}
