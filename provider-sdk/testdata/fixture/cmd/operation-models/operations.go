package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
	"strings"
)

type operation struct {
	Action string  `json:"action"`
	To     *string `json:"to,omitempty"`
}

func normalize(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	if input.GetRepresentation() != protocol.ValueInput_OPERATION || input.GetValue().GetKind() != operationKind {
		return nil, fmt.Errorf("unknown operation")
	}
	canonical, err := modeljson.Canonical(input.GetValue().GetPayload())
	if err != nil {
		return nil, err
	}
	var op operation
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&op); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &fields); err != nil {
		return nil, err
	}
	if _, present := fields["to"]; present && op.Action != "rename" {
		return nil, fmt.Errorf("only rename accepts a target")
	}
	impact := protocol.ChangeSemantics_ADDITIVE
	switch op.Action {
	case "create":
		if op.To != nil {
			return nil, fmt.Errorf("create cannot rename")
		}
	case "drop":
		if op.To != nil {
			return nil, fmt.Errorf("drop cannot rename")
		}
		impact = protocol.ChangeSemantics_DESTRUCTIVE
	case "rename":
		if op.To == nil || *op.To == "" || strings.ContainsRune(*op.To, 0) {
			return nil, fmt.Errorf("rename requires a target name")
		}
		impact = protocol.ChangeSemantics_BEHAVIORAL
	default:
		return nil, fmt.Errorf("unknown action")
	}
	ref := input.GetValue().GetOwner()
	if ref.GetKind() != "table" || ref.GetName() == "" || ref.GetDatabase() != "" || ref.GetCatalog() != "" || len(ref.GetPath()) != 0 || len(ref.GetOverload()) != 0 || (ref.GetSchema() != "" && ref.GetSchema() != "main") || strings.ContainsRune(ref.GetName(), 0) {
		return nil, fmt.Errorf("fixture operation requires a common table")
	}
	value := proto.CloneOf(input.GetValue())
	value.Payload = canonical
	return &protocol.NormalizedValue{Id: input.GetId(), Value: value, NormalizedOwner: proto.CloneOf(ref), CanonicalFormat: "table-operation-v1", CanonicalProjection: bytes.Clone(canonical), OperationSemantics: &protocol.OperationSemantics{Impact: impact, Reason: "Table " + op.Action, Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "The operation changes no grants"}}, nil
}

func (p *owner) NormalizeValues(ctx context.Context, r *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range r.GetValues() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if input.GetRepresentation() == protocol.ValueInput_OPERATION {
			value, err := normalize(input)
			if err != nil {
				return nil, err
			}
			result.Values = append(result.Values, value)
			continue
		}
		values, err := p.Provider.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: r.GetEngine(), TargetFeatures: r.GetTargetFeatures(), Values: []*protocol.ValueInput{input}})
		if err != nil {
			return nil, err
		}
		result.Values = append(result.Values, values.GetValues()...)
	}
	return result, nil
}

func (p *owner) Render(ctx context.Context, r *protocol.RenderRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, change := range r.GetChanges() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if change.GetNative() == nil {
			rendered, err := p.Provider.Render(ctx, &protocol.RenderRequest{Engine: r.GetEngine(), TargetFeatures: r.GetTargetFeatures(), Changes: []*protocol.Change{change}})
			if err != nil {
				return nil, err
			}
			result.Units = append(result.Units, rendered.GetUnits()...)
			continue
		}
		readOnly, known := r.GetCapabilities()["fixture.read-only"]
		if !known || readOnly {
			return nil, fmt.Errorf("fixture requires a known writable target")
		}
		sql, err := renderNative(change.GetNative())
		if err != nil {
			return nil, err
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: sql}})
	}
	return result, nil
}

func renderNative(native *protocol.NativeOperation) (string, error) {
	captured := native.GetValue()
	value, err := normalize(&protocol.ValueInput{Id: captured.GetId(), Representation: protocol.ValueInput_OPERATION, Value: captured.GetValue()})
	if err != nil {
		return "", err
	}
	if !proto.Equal(value, captured) {
		return "", fmt.Errorf("operation semantics changed")
	}
	var op operation
	if err := json.Unmarshal(value.GetValue().GetPayload(), &op); err != nil {
		return "", err
	}
	subject := value.GetValue().GetOwner()
	name := quote(subject.GetName())
	switch op.Action {
	case "rename":
		if native.GetRole() != protocol.NativeOperation_ALTER_TABLE || !proto.Equal(native.GetParent(), subject) {
			return "", fmt.Errorf("rename requires its actual table parent")
		}
		return "ALTER TABLE " + name + " RENAME TO " + quote(*op.To) + ";", nil
	case "create", "drop":
		if native.GetRole() != protocol.NativeOperation_STATEMENT {
			return "", fmt.Errorf("standalone table operation required")
		}
		if op.Action == "create" {
			return "CREATE TABLE " + name + " (id INTEGER NOT NULL PRIMARY KEY);", nil
		}
		return "DROP TABLE " + name + ";", nil
	default:
		return "", fmt.Errorf("unknown action")
	}
}
func quote(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
