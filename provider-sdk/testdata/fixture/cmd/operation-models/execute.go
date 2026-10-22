package main

import (
	"context"
	"encoding/json"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// Execute translates native operations inside the owning process. Every unit is
// validated before the SQLite backend can write the first one.
func (p *owner) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	translated := proto.CloneOf(request)
	for _, unit := range translated.GetUnits() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := provider.ValidateUnit(unit); err != nil {
			return nativeRefusal(), nil
		}
		if unit.GetNative() == nil {
			continue
		}
		sql, err := p.executableNative(unit.GetNative(), request.GetTransactionId())
		if err != nil {
			return nativeRefusal(), nil
		}
		unit.Body = &protocol.Unit_Sql{Sql: sql}
	}
	return p.Provider.Execute(ctx, translated)
}

func nativeRefusal() *protocol.Execution {
	return &protocol.Execution{Outcome: protocol.Execution_NOT_STARTED, DiagnosticCode: "fixture.native-refused"}
}

func (p *owner) executableNative(value *protocol.Envelope, transactionID string) (string, error) {
	expected := proto.CloneOf(p.operationEnvelope)
	expected.Owner, expected.Payload = value.GetOwner(), value.GetPayload()
	if !p.nativeUnits || !proto.Equal(value, expected) {
		return "", fmt.Errorf("unknown native execution model")
	}
	normalized, err := normalize(&protocol.ValueInput{Representation: protocol.ValueInput_OPERATION, Value: value})
	if err != nil {
		return "", err
	}
	var op operation
	if err := json.Unmarshal(normalized.GetValue().GetPayload(), &op); err != nil {
		return "", err
	}
	if op.Transaction == "required" && transactionID == "" {
		return "", fmt.Errorf("native operation requires a transaction")
	}
	native := &protocol.NativeOperation{Value: normalized, Role: protocol.NativeOperation_STATEMENT}
	if op.Action == "rename" || op.Action == "add-column" {
		native.Role, native.Parent = protocol.NativeOperation_ALTER_TABLE, normalized.GetNormalizedOwner()
	}
	return renderNative(native)
}
