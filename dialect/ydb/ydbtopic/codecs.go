package ydbtopic

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// Codecs returns the explicit version-one desired and observed model
// descriptors. The wire keeps every setting as written, unset ones left out;
// a codec resolves no default.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{modelCodec(&Desired{}, schemaext.Desired), modelCodec(&Observed{}, schemaext.Observed)}
}

// wireDesired and wireObserved name every field a topic may carry. Unknown
// fields, at any depth, are refused on decode.
type wireDesired struct {
	Spec       Spec   `json:"spec"`
	StructName string `json:"struct_name,omitempty"`
}

type wireObserved struct {
	Spec Spec `json:"spec"`
}

// SpecDefinition is the JSON schema of a topic's settings and consumers, in
// the form the codecs write them.
const SpecDefinition = `{"type":"object","additionalProperties":false,"properties":{` +
	`"min_active_partitions":{"type":"integer","minimum":1},"max_active_partitions":{"type":"integer","minimum":1},` +
	`"auto_partitioning_strategy":{"type":"string","minLength":1},` +
	`"auto_partitioning_up_utilization_percent":{"type":"integer","minimum":1,"maximum":100},` +
	`"auto_partitioning_down_utilization_percent":{"type":"integer","minimum":1,"maximum":100},` +
	`"auto_partitioning_stabilization_window":{"type":"string","minLength":1},"retention_period":{"type":"string","minLength":1},` +
	`"partition_write_speed_bytes_per_second":{"type":"integer","minimum":1},"partition_write_burst_bytes":{"type":"integer","minimum":1},` +
	`"supported_codecs":{"type":"array","items":{"type":"string"}},` +
	`"consumers":{"type":"array","items":` + ConsumerDefinition + `}}}`

// ConsumerDefinition is the JSON schema of one consumer, which a changefeed's
// topic carries too.
const ConsumerDefinition = `{"type":"object","required":["name"],"additionalProperties":false,"properties":{` +
	`"name":{"type":"string","minLength":1},"important":{"type":"boolean"},"read_from":{"type":"string","minLength":1},` +
	`"supported_codecs":{"type":"array","items":{"type":"string"}},"availability_period":{"type":"string","minLength":1}}}`

func modelCodec(prototype schemaext.Value, representation schemaext.Representation) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := topicPayload(payload, representation)
		if err != nil {
			return nil, err
		}
		if desired, ok := value.(*Desired); ok {
			return json.Marshal(wireDesired(*desired))
		}
		return json.Marshal(wireObserved(*value.(*Observed)))
	}
	properties := `"spec":` + SpecDefinition
	if representation == schemaext.Desired {
		properties += `,"struct_name":{"type":"string"}`
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["spec"],"additionalProperties":false,"properties":{` + properties + `}}`),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := topicPayload(payload, representation)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			if err := RefuseNulls(data); err != nil {
				return nil, err
			}
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if _, present := fields["spec"]; !present {
				return nil, fmt.Errorf("%w: a topic requires spec", schemaext.ErrInvalidValue)
			}
			var value schemaext.Value
			if representation == schemaext.Desired {
				wire, err := schemaext.DecodeJSON[wireDesired](data)
				if err != nil {
					return nil, err
				}
				value = &Desired{Spec: wire.Spec, StructName: wire.StructName}
			} else {
				wire, err := schemaext.DecodeJSON[wireObserved](data)
				if err != nil {
					return nil, err
				}
				value = &Observed{Spec: wire.Spec}
			}
			return topicPayload(value, representation)
		},
	}
}

// RefuseNulls refuses a JSON document that holds a null anywhere, so an
// omitted setting and an explicit null never decode to the same topic.
func RefuseNulls(data json.RawMessage) error {
	var document any
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("%w: decode topic: %v", schemaext.ErrInvalidValue, err)
	}
	if document == nil || holdsNull(document) {
		return fmt.Errorf("%w: a topic field cannot be null", schemaext.ErrInvalidValue)
	}
	return nil
}

func holdsNull(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case map[string]any:
		for _, field := range typed {
			if holdsNull(field) {
				return true
			}
		}
	case []any:
		if slices.ContainsFunc(typed, holdsNull) {
			return true
		}
	}
	return false
}

func topicPayload(payload schemaext.Payload, representation schemaext.Representation) (schemaext.Value, error) {
	if err := schemaext.ValidatePayload(payload); err != nil {
		return nil, err
	}
	switch value := payload.(type) {
	case *Desired:
		if representation != schemaext.Desired {
			return nil, fmt.Errorf("%w: expected an observed topic", schemaext.ErrInvalidValue)
		}
		if err := value.Validate(); err != nil {
			return nil, &schemaext.InvalidModelError{Kind: Kind, Representation: schemaext.Desired, Message: err.Error()}
		}
		return value, nil
	case *Observed:
		if representation != schemaext.Observed {
			return nil, fmt.Errorf("%w: expected a desired topic", schemaext.ErrInvalidValue)
		}
		if err := Validate(value.Spec); err != nil {
			return nil, &schemaext.InvalidModelError{Kind: Kind, Representation: schemaext.Observed, Message: err.Error()}
		}
		return value, nil
	default:
		return nil, fmt.Errorf("%w: expected a topic, got %T", schemaext.ErrInvalidValue, payload)
	}
}

// The reasons a read records a topic it lists and does not describe, which a
// source written from the read carries as a limit rather than a declaration:
// a topic on a target without the topics capability, and a queue of the older
// persistent queue kind, which Ptah reads on no line.
const (
	UnsupportedReason = "target capability topics is unavailable, so Ptah leaves the topic unmanaged"
	QueueGroupReason  = "a persistent queue group, which Ptah does not read"
)

// Coverage records a source's claim about the topic namespace. Enrollment is
// limited to this model's own definition, whatever else a runtime registers.
func Coverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return schemaext.OwnedCoverage("ptah.run/ydb", Codecs(), Kind, representation, knowledge, subjects)
}
