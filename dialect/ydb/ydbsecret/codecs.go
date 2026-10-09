package ydbsecret

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// Codecs returns the explicit version-one desired and observed model
// descriptors. No wire form carries a secret's value: a declaration names its
// variable, and an observation is empty.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{desiredCodec(), observedCodec()}
}

// wireDesired names every field a declaration may carry. Unknown fields,
// including a value, are refused on decode.
type wireDesired struct {
	ValueEnv   string `json:"value_env,omitempty"`
	StructName string `json:"struct_name,omitempty"`
	Rotate     bool   `json:"rotate,omitempty"`
}

func desiredCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := desiredPayload(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(wireDesired(*value))
	}
	return schemaext.Codec{
		Prototype: &Desired{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{` +
			`"value_env":{"type":"string","minLength":1},"struct_name":{"type":"string"},"rotate":{"type":"boolean"}}}`),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := desiredPayload(payload)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			if err := wireFields(data, "value_env", "struct_name", "rotate"); err != nil {
				return nil, err
			}
			wire, err := schemaext.DecodeJSON[wireDesired](data)
			if err != nil {
				return nil, err
			}
			value := Desired(wire)
			return desiredPayload(&value)
		},
	}
}

func observedCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		if _, err := observedPayload(payload); err != nil {
			return nil, err
		}
		return json.RawMessage(`{}`), nil
	}
	return schemaext.Codec{
		Prototype: &Observed{}, Representation: schemaext.Observed, Version: 1,
		Definition: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{}}`),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := observedPayload(payload)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			if err := wireFields(data); err != nil {
				return nil, err
			}
			return &Observed{}, nil
		},
	}
}

// wireFields refuses a document that is not an object, a field outside
// allowed and a null field, so an omitted setting and an explicit null never
// decode to the same declaration.
func wireFields(data json.RawMessage, allowed ...string) error {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("%w: a secret requires an object", schemaext.ErrInvalidValue)
	}
	for field, value := range fields {
		if !slices.Contains(allowed, field) {
			return fmt.Errorf("%w: unexpected secret field %q", schemaext.ErrInvalidValue, field)
		}
		if string(value) == "null" {
			return fmt.Errorf("%w: secret field %q cannot be null", schemaext.ErrInvalidValue, field)
		}
	}
	return nil
}

func desiredPayload(payload schemaext.Payload) (*Desired, error) {
	if err := schemaext.ValidatePayload(payload); err != nil {
		return nil, err
	}
	value, ok := payload.(*Desired)
	if !ok {
		return nil, fmt.Errorf("%w: expected a desired secret, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, &schemaext.InvalidModelError{Kind: Kind, Representation: schemaext.Desired, Message: err.Error()}
	}
	return value, nil
}

func observedPayload(payload schemaext.Payload) (*Observed, error) {
	if err := schemaext.ValidatePayload(payload); err != nil {
		return nil, err
	}
	value, ok := payload.(*Observed)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed secret, got %T", schemaext.ErrInvalidValue, payload)
	}
	return value, nil
}

// Coverage records a source's claim about the secret namespace. Enrollment is
// limited to this model's own definition, whatever else a runtime registers.
func Coverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	var owned []schemaext.OwnedCodec
	for _, codec := range Codecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, definition := range registry.Definitions() {
		if definition.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: definition, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: secret coverage requires desired or observed state", schemaext.ErrInvalidValue)
}
