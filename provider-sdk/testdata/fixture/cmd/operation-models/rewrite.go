package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// A parent assessment can replace a common operand without a native change.
// This fixture deliberately supports only plain column additions.
func (p *owner) planColumnRewrites(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable) ([]*protocol.PlanningStep, []*protocol.PlanningRewrite, error) {
	if added := addedNotNull(request, table); p.rebuilds && added != "" {
		return p.planRebuild(request, table, added)
	}
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

// addedNotNull names the first NOT NULL column the common steps add to table,
// or is empty when they add none. SQLite refuses to add such a column in
// place.
func addedNotNull(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable) string {
	for _, common := range request.GetCommonSteps() {
		if column := common.GetAddedColumn(); column != nil && !column.GetNullable() && proto.Equal(common.GetParent(), table.GetSubject()) {
			return column.GetName()
		}
	}
	return ""
}

// planRebuild replaces every column step of table with one rebuild that
// writes the declared table, when the policy asks for rebuilds. Without the
// request it refuses the change in the policy's words, so the operator learns
// how to ask; a host that sends no policy gets the refusal without them.
func (p *owner) planRebuild(request *protocol.PlanFeaturesRequest, table *protocol.PlanningTable, added string) ([]*protocol.PlanningStep, []*protocol.PlanningRewrite, error) {
	policy := request.GetPolicy()
	if !policy.GetAllowTableRebuild() {
		ask := "a rebuild of the table"
		if policy != nil {
			ask += "; ask for one with " + policy.GetTableRebuildRequest()
		}
		return nil, nil, fmt.Errorf("adding NOT NULL column %q to table %q needs %s", added, table.GetSubject().GetName().GetSource(), ask)
	}
	declared, observed := table.GetDesired(), table.GetCurrent()
	if table.GetAction() != protocol.PlanningTable_ALTER || declared == nil || observed == nil {
		return nil, nil, fmt.Errorf("a rebuild requires both captured table states")
	}
	held := make(map[string]bool, len(observed.GetColumns()))
	for _, column := range observed.GetColumns() {
		held[column.GetName()] = true
	}
	// The copy list is required, so a rebuild that keeps no column still
	// writes an empty one.
	rebuild := &rebuiltTable{Copy: make([]string, 0, len(declared.GetColumns()))}
	for _, column := range declared.GetColumns() {
		primary := column.GetPrimary() || slices.Contains(declared.GetPrimaryKey(), column.GetName())
		rebuild.Columns = append(rebuild.Columns, rebuiltColumn{Name: column.GetName(), Type: strings.ToUpper(column.GetType()), Nullable: column.GetNullable() && !primary, PrimaryKey: primary})
		if held[column.GetName()] {
			rebuild.Copy = append(rebuild.Copy, column.GetName())
		}
	}
	payload, err := json.Marshal(operation{Action: "rebuild", Rebuild: rebuild})
	if err != nil {
		return nil, nil, err
	}
	envelope := proto.CloneOf(p.operationEnvelope)
	envelope.Payload = payload
	envelope.Owner = &protocol.Reference{Engine: request.GetEngine(), Kind: "table", Schema: declared.GetSchema(), Name: declared.GetName()}
	id := "rebuild/" + declared.GetName()
	value, err := normalize(&protocol.ValueInput{Id: id, Representation: protocol.ValueInput_OPERATION, Value: envelope})
	if err != nil {
		return nil, nil, err
	}
	metadata := &protocol.PlanningMetadata{Transaction: protocol.PlanningMetadata_FORBIDDEN, Impact: value.GetOperationSemantics().GetImpact(), Reason: value.GetOperationSemantics().GetReason(),
		Effects: []*protocol.PlanningEffect{{Subject: proto.CloneOf(table.GetSubject()), Action: protocol.PlanningEffect_ALTER}}}
	rewrite := &protocol.PlanningRewrite{}
	for _, common := range request.GetCommonSteps() {
		if !proto.Equal(common.GetParent(), table.GetSubject()) {
			continue
		}
		rewrite.Sources = append(rewrite.Sources, proto.CloneOf(common.GetId()))
		for _, effect := range common.GetMetadata().GetEffects() {
			if !proto.Equal(effect.GetSubject(), table.GetSubject()) {
				metadata.Effects = append(metadata.Effects, proto.CloneOf(effect))
			}
		}
	}
	step := &protocol.PlanningStep{Id: &protocol.StepIdentity{Owner: "example.org/fixture", Name: id}, Metadata: metadata,
		Operation: &protocol.PlanningOperation{Value: value, Role: protocol.NativeOperation_STATEMENT}}
	rewrite.Replacement = proto.CloneOf(step.Id)
	return []*protocol.PlanningStep{step}, []*protocol.PlanningRewrite{rewrite}, nil
}
