package ydbstreaming

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// Codecs returns explicit version-one desired and observed model descriptors.
// The wire preserves unset fields; target defaults are not applied by a codec.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{modelCodec(&Desired{}, schemaext.Desired), modelCodec(&Observed{}, schemaext.Observed)}
}

// The transport shape names configuration explicitly. It contains no common
// schema or AST struct and cannot capture source/target coverage implicitly.
type wireQuery struct {
	Spec Spec `json:"spec"`
}

type wireDesiredQuery struct {
	Spec            Spec   `json:"spec"`
	StructName      string `json:"struct_name,omitempty"`
	AllowStateReset bool   `json:"allow_state_reset"`
}

func modelCodec(prototype schemaext.Value, representation schemaext.Representation) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		spec, err := schemaSpec(payload, representation)
		if err != nil {
			return nil, err
		}
		if value, ok := payload.(*Desired); ok {
			return json.Marshal(wireDesiredQuery{Spec: spec, StructName: value.StructName, AllowStateReset: value.AllowStateReset})
		}
		return json.Marshal(wireQuery{Spec: spec})
	}
	properties := `"spec":{"type":"object","required":["text"],"additionalProperties":false,"properties":{"text":{"type":"string"},"run":{"type":"boolean"},"resource_pool":{"type":"string"}}}`
	required := `"spec"`
	if representation == schemaext.Desired {
		properties += `,"struct_name":{"type":"string"},"allow_state_reset":{"type":"boolean"}`
		required += `,"allow_state_reset"`
	}

	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":[` + required + `],"additionalProperties":false,"properties":{` + properties + `}}`),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if _, err := schemaSpec(payload, representation); err != nil {
				return nil, err
			}
			return payload.(schemaext.Value).Clone(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if err := validateWireQuery(fields, representation); err != nil {
				return nil, err
			}
			if err := validateWireSpec(fields["spec"]); err != nil {
				return nil, err
			}
			wire, err := schemaext.DecodeJSON[wireDesiredQuery](data)
			if err != nil {
				return nil, err
			}
			if err := Validate(wire.Spec); err != nil {
				return nil, &schemaext.InvalidModelError{Kind: Kind, Representation: representation, Message: err.Error()}
			}
			if representation == schemaext.Desired {
				return &Desired{Spec: wire.Spec, StructName: wire.StructName, AllowStateReset: wire.AllowStateReset}, nil
			}
			return &Observed{Spec: wire.Spec}, nil
		},
	}
}

func validateWireQuery(fields map[string]json.RawMessage, representation schemaext.Representation) error {
	if len(fields["spec"]) == 0 || string(fields["spec"]) == "null" {
		return fmt.Errorf("%w: streaming query requires spec", schemaext.ErrInvalidValue)
	}
	if representation == schemaext.Desired && len(fields["allow_state_reset"]) == 0 {
		return fmt.Errorf("%w: streaming query requires allow_state_reset", schemaext.ErrInvalidValue)
	}
	for field, value := range fields {
		if field == "spec" {
			continue
		}
		if (field != "struct_name" && field != "allow_state_reset") || representation != schemaext.Desired || string(value) == "null" {
			return fmt.Errorf("%w: unexpected streaming field %q", schemaext.ErrInvalidValue, field)
		}
	}
	return nil
}

func validateWireSpec(data json.RawMessage) error {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return err
	}
	if len(fields["text"]) == 0 {
		return fmt.Errorf("%w: streaming query requires text", schemaext.ErrInvalidValue)
	}
	for field, value := range fields {
		switch field {
		case "text", "run", "resource_pool":
		default:
			return fmt.Errorf("%w: unknown streaming setting %q", schemaext.ErrInvalidValue, field)
		}
		if string(value) == "null" {
			return fmt.Errorf("%w: streaming setting %q cannot be null", schemaext.ErrInvalidValue, field)
		}
	}
	return nil
}

func schemaSpec(payload schemaext.Payload, representation schemaext.Representation) (Spec, error) {
	if err := schemaext.ValidatePayload(payload); err != nil {
		return Spec{}, err
	}
	var spec Spec
	switch value := payload.(type) {
	case *Desired:
		if !utf8.ValidString(value.StructName) {
			return Spec{}, fmt.Errorf("%w: streaming query holder must be valid UTF-8", schemaext.ErrInvalidValue)
		}
		if representation != schemaext.Desired {
			return Spec{}, fmt.Errorf("%w: expected observed streaming query", schemaext.ErrInvalidValue)
		}
		spec = value.Spec
	case *Observed:
		if representation != schemaext.Observed {
			return Spec{}, fmt.Errorf("%w: expected desired streaming query", schemaext.ErrInvalidValue)
		}
		spec = value.Spec
	default:
		return Spec{}, fmt.Errorf("%w: expected streaming query, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := Validate(spec); err != nil {
		return Spec{}, &schemaext.InvalidModelError{Kind: Kind, Representation: representation, Message: err.Error()}
	}
	return spec, nil
}

// Coverage records this source's streaming-query namespace claim. Enrollment
// is limited to this model's precise definition, independent of runtime growth.
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
	return schemaext.Coverage{}, fmt.Errorf("%w: streaming coverage requires desired or observed state", schemaext.ErrInvalidValue)
}
