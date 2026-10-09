package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
)

type coordinationWire struct {
	Schema string          `json:"schema"`
	Name   string          `json:"name"`
	Change json.RawMessage `json:"change"`
}

// CoordinationCodec records the explicit standalone operation wire. It carries
// all operands and no capability grants, database handles, or host callbacks.
func CoordinationCodec() schemaext.Codec {
	change := ydbdiff.CoordinationCodec()
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := coordinationOperation(payload)
		if err != nil {
			return nil, err
		}
		operands, err := change.Encode(&value.Change)
		if err != nil {
			return nil, err
		}
		return json.Marshal(coordinationWire{Schema: value.Schema, Name: value.Name, Change: operands})
	}
	return schemaext.Codec{Prototype: &CoordinationNode{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["schema","name","change"],"additionalProperties":false,`+
			`"properties":{"schema":{"type":"string"},"name":{"type":"string","minLength":1},"change":%s}}`, change.Definition)),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := coordinationOperation(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: decodeCoordinationOperation,
	}
}

func decodeCoordinationOperation(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if len(fields) != 3 {
		return nil, fmt.Errorf("%w: coordination operation requires exactly schema, name, and change", schemaext.ErrInvalidValue)
	}
	for _, key := range []string{"schema", "name", "change"} {
		if len(fields[key]) == 0 || string(fields[key]) == "null" {
			return nil, fmt.Errorf("%w: coordination operation requires %s", schemaext.ErrInvalidValue, key)
		}
	}
	wire, err := schemaext.DecodeJSON[coordinationWire](data)
	if err != nil {
		return nil, err
	}
	operands, err := ydbdiff.CoordinationCodec().Decode(wire.Change)
	if err != nil {
		return nil, err
	}
	change, ok := operands.(*ydbdiff.CoordinationNode)
	if !ok || change == nil {
		return nil, fmt.Errorf("%w: unexpected coordination operands %T", schemaext.ErrInvalidValue, operands)
	}
	value := &CoordinationNode{Schema: wire.Schema, Name: wire.Name, Change: *change}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

func coordinationOperation(payload schemaext.Payload) (*CoordinationNode, error) {
	value, ok := payload.(*CoordinationNode)
	if !ok {
		return nil, fmt.Errorf("%w: expected a coordination operation, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}
