package ydbcoordination

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// Codecs returns explicit version-one desired and observed model descriptors.
// The wire preserves unset fields; target defaults are not applied by a codec.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{modelCodec(&Desired{}, schemaext.Desired), modelCodec(&Observed{}, schemaext.Observed)}
}

// The transport shape names configuration explicitly. It contains no common
// schema or AST struct and cannot capture source/target coverage implicitly.
type wireNode struct {
	Spec Spec `json:"spec"`
}

type wireDesiredNode struct {
	Spec       Spec   `json:"spec"`
	StructName string `json:"struct_name,omitempty"`
}

func modelCodec(prototype schemaext.Value, representation schemaext.Representation) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		spec, err := schemaSpec(payload, representation)
		if err != nil {
			return nil, err
		}
		if value, ok := payload.(*Desired); ok {
			return json.Marshal(wireDesiredNode{Spec: spec, StructName: value.StructName})
		}
		return json.Marshal(wireNode{Spec: spec})
	}
	properties := `"spec":{"type":"object","additionalProperties":false,"properties":{
			"self_check_period_millis":{"type":"integer","minimum":0,"maximum":4294967295},"session_grace_period_millis":{"type":"integer","minimum":0,"maximum":4294967295},
			"read_consistency_mode":{"enum":["","strict","relaxed"]},"attach_consistency_mode":{"enum":["","strict","relaxed"]},"rate_limiter_counters_mode":{"enum":["","aggregated","detailed"]}}}`
	if representation == schemaext.Desired {
		properties += `,"struct_name":{"type":"string"}`
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["spec"],"additionalProperties":false,"properties":{` + properties + `}}`),
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
			if err := validateWireNode(fields, representation); err != nil {
				return nil, err
			}
			if err := validateWireSpec(fields["spec"]); err != nil {
				return nil, err
			}
			wire, err := schemaext.DecodeJSON[wireDesiredNode](data)
			if err != nil {
				return nil, err
			}
			if err := Validate(wire.Spec); err != nil {
				return nil, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
			}
			if representation == schemaext.Desired {
				return &Desired{Spec: wire.Spec, StructName: wire.StructName}, nil
			}
			return &Observed{Spec: wire.Spec}, nil
		},
	}
}

func validateWireNode(fields map[string]json.RawMessage, representation schemaext.Representation) error {
	if len(fields["spec"]) == 0 || string(fields["spec"]) == "null" {
		return fmt.Errorf("%w: coordination node requires spec", schemaext.ErrInvalidValue)
	}
	for field, value := range fields {
		if field == "spec" {
			continue
		}
		if field != "struct_name" || representation != schemaext.Desired || string(value) == "null" {
			return fmt.Errorf("%w: unexpected coordination field %q", schemaext.ErrInvalidValue, field)
		}
	}
	return nil
}

func validateWireSpec(data json.RawMessage) error {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return err
	}
	for field, value := range fields {
		switch field {
		case "self_check_period_millis", "session_grace_period_millis", "read_consistency_mode", "attach_consistency_mode", "rate_limiter_counters_mode":
		default:
			return fmt.Errorf("%w: unknown coordination setting %q", schemaext.ErrInvalidValue, field)
		}
		if string(value) == "null" {
			return fmt.Errorf("%w: coordination setting %q cannot be null", schemaext.ErrInvalidValue, field)
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
		if representation != schemaext.Desired {
			return Spec{}, fmt.Errorf("%w: expected observed coordination node", schemaext.ErrInvalidValue)
		}
		spec = value.Spec
	case *Observed:
		if representation != schemaext.Observed {
			return Spec{}, fmt.Errorf("%w: expected desired coordination node", schemaext.ErrInvalidValue)
		}
		spec = value.Spec
	default:
		return Spec{}, fmt.Errorf("%w: expected coordination node, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := Validate(spec); err != nil {
		return Spec{}, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return spec, nil
}

// Coverage records this source's coordination-node namespace claim. Enrollment
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
	return schemaext.Coverage{}, fmt.Errorf("%w: coordination coverage requires desired or observed state", schemaext.ErrInvalidValue)
}
