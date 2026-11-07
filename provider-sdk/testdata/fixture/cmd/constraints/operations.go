package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// statementsOperation is the payload of a statements operation.
type statementsOperation struct {
	Statements []string `json:"statements"`
}

// statements is the normalized operation that runs statements on table.
func (o *owner) statements(engine, table string, statements []string) (*protocol.NormalizedValue, error) {
	payload, err := json.Marshal(statementsOperation{Statements: statements})
	if err != nil {
		return nil, err
	}
	envelope := proto.CloneOf(o.operation)
	envelope.Payload = payload
	envelope.Owner = &protocol.Reference{Engine: engine, Kind: "table", Name: table}
	return normalizeStatements(&protocol.ValueInput{Id: "rebuild/" + table, Representation: protocol.ValueInput_OPERATION, Value: envelope})
}

// normalizeStatements checks a statements operation of a table and writes its
// canonical form.
func normalizeStatements(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	canonical, err := modeljson.Canonical(input.GetValue().GetPayload())
	if err != nil {
		return nil, err
	}
	var operation statementsOperation
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&operation); err != nil {
		return nil, err
	}
	owner := input.GetValue().GetOwner()
	if len(operation.Statements) == 0 || owner.GetKind() != "table" || owner.GetName() == "" || owner.GetSchema() != "" {
		return nil, fmt.Errorf("a statements operation runs statements on an unqualified table")
	}
	value := proto.CloneOf(input.GetValue())
	value.Payload = canonical
	return &protocol.NormalizedValue{Id: input.GetId(), Value: value, NormalizedOwner: proto.CloneOf(owner), CanonicalFormat: "constraint-statements-v1",
		CanonicalProjection: bytes.Clone(canonical), OperationSemantics: &protocol.OperationSemantics{Impact: protocol.ChangeSemantics_BEHAVIORAL,
			Reason: "Rebuild table " + owner.GetName(), Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "A rebuild changes no grants"}}, nil
}

// NormalizeValues normalizes statements operations and leaves every other
// value to the backend.
func (o *owner) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range request.GetValues() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if input.GetRepresentation() == protocol.ValueInput_OPERATION && input.GetValue().GetKind() == statementsKind {
			value, err := normalizeStatements(input)
			if err != nil {
				return nil, err
			}
			result.Values = append(result.Values, value)
			continue
		}
		values, err := o.Provider.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
			Values: []*protocol.ValueInput{input}})
		if err != nil {
			return nil, err
		}
		result.Values = append(result.Values, values.GetValues()...)
	}
	return result, nil
}

// Render passes a statements operation on as a native unit, which Execute
// runs, and refuses every other change.
func (o *owner) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		native := change.GetNative()
		if native.GetValue().GetValue().GetKind() != statementsKind {
			return nil, fmt.Errorf("the constraints fixture renders its statements operations only")
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Native{Native: proto.CloneOf(native.GetValue().GetValue())}})
	}
	return result, nil
}

// Execute runs the statements of each native unit as one SQL unit.
func (o *owner) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	translated := proto.CloneOf(request)
	for _, unit := range translated.GetUnits() {
		if err := provider.ValidateUnit(unit); err != nil {
			return &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED, DiagnosticCode: "fixture.native-refused"}, nil
		}
		native := unit.GetNative()
		if native == nil {
			continue
		}
		value, err := normalizeStatements(&protocol.ValueInput{Representation: protocol.ValueInput_OPERATION, Value: native})
		if err != nil || native.GetKind() != statementsKind {
			return &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED, DiagnosticCode: "fixture.native-refused"}, nil
		}
		var operation statementsOperation
		if err := json.Unmarshal(value.GetValue().GetPayload(), &operation); err != nil {
			return nil, err
		}
		unit.Body = &protocol.Unit_Sql{Sql: strings.Join(operation.Statements, " ")}
	}
	return o.Provider.Execute(ctx, translated)
}
