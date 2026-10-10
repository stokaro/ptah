package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// pathChangeOperation is a statement on the object at one scheme path that
// carries the change between the object's two operands.
type pathChangeOperation interface {
	ast.ExtensionPayload
	Validate() error
}

// pathChangeWire is the version-one wire those statements share. The change
// is written by the family's own operand codec.
type pathChangeWire struct {
	Schema string          `json:"schema"`
	Name   string          `json:"name"`
	Change json.RawMessage `json:"change"`
}

// pathChangeCodec describes the wire of one family's path statements. split
// and join convert between the statement and its path and operands, and
// family names the statement in errors. The codec refuses missing, null and
// unknown fields, and validates every statement it encodes, clones or
// decodes.
func pathChangeCodec[P pathChangeOperation, T any, C interface {
	*T
	schemaext.Payload
}](family string, prototype P, operands schemaext.Codec, split func(P) (schema, name string, change C), join func(schema, name string, change T) P,
) schemaext.Codec {
	validated := func(payload schemaext.Payload) (P, error) {
		value, ok := payload.(P)
		if !ok {
			return value, fmt.Errorf("%w: expected a %s operation, got %T", schemaext.ErrInvalidValue, family, payload)
		}
		if err := value.Validate(); err != nil {
			return value, err
		}
		return value, nil
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := validated(payload)
		if err != nil {
			return nil, err
		}
		schema, name, change := split(value)
		encoded, err := operands.Encode(change)
		if err != nil {
			return nil, err
		}
		return json.Marshal(pathChangeWire{Schema: schema, Name: name, Change: encoded})
	}
	decode := func(data json.RawMessage) (schemaext.Payload, error) {
		fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
		if err != nil {
			return nil, err
		}
		if len(fields) != 3 {
			return nil, fmt.Errorf("%w: %s operation requires exactly schema, name, and change", schemaext.ErrInvalidValue, family)
		}
		for _, key := range []string{"schema", "name", "change"} {
			if len(fields[key]) == 0 || string(fields[key]) == "null" {
				return nil, fmt.Errorf("%w: %s operation requires %s", schemaext.ErrInvalidValue, family, key)
			}
		}
		wire, err := schemaext.DecodeJSON[pathChangeWire](data)
		if err != nil {
			return nil, err
		}
		decoded, err := operands.Decode(wire.Change)
		if err != nil {
			return nil, err
		}
		change, ok := decoded.(C)
		if !ok || change == nil {
			return nil, fmt.Errorf("%w: unexpected %s operands %T", schemaext.ErrInvalidValue, family, decoded)
		}
		value, err := validated(join(wire.Schema, wire.Name, *change))
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return schemaext.Codec{Prototype: prototype, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["schema","name","change"],"additionalProperties":false,`+
			`"properties":{"schema":{"type":"string"},"name":{"type":"string","minLength":1},"change":%s}}`, operands.Definition)),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := validated(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: decode,
	}
}
