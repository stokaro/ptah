package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// SecretCodec describes the version-one operation wire: the statement, the
// separate directory and leaf names, and the variable a value comes from. It
// refuses missing, null and unknown fields, so no wire form can carry a value.
func SecretCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := secretPayload(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return schemaext.Codec{Prototype: &Secret{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["operation","schema","name","value_env"],"additionalProperties":false,"properties":{` +
			`"operation":{"enum":["create","rotate","drop"]},"schema":{"type":"string"},"name":{"type":"string","minLength":1},"value_env":{"type":"string"}}}`),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := secretPayload(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: decodeSecret,
	}
}

func secretPayload(payload schemaext.Payload) (*Secret, error) {
	value, ok := payload.(*Secret)
	if !ok {
		return nil, fmt.Errorf("%w: expected a secret operation, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, &schemaext.InvalidModelError{Kind: SecretKind, Representation: schemaext.Operation, Message: err.Error()}
	}
	return value, nil
}

func decodeSecret(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	for _, field := range []string{"operation", "schema", "name", "value_env"} {
		if len(fields[field]) == 0 {
			return nil, fmt.Errorf("%w: secret operation requires %s", schemaext.ErrInvalidValue, field)
		}
	}
	for field, value := range fields {
		if string(value) == "null" {
			return nil, fmt.Errorf("%w: secret operation field %s cannot be null", schemaext.ErrInvalidValue, field)
		}
	}
	value, err := schemaext.DecodeJSON[*Secret](data)
	if err != nil {
		return nil, err
	}
	return secretPayload(value)
}
