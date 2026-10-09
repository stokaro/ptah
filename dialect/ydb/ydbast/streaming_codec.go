package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
)

type streamingSpecWire struct {
	Text         string `json:"text"`
	Run          *bool  `json:"run,omitempty"`
	ResourcePool string `json:"resource_pool,omitempty"`
}

type streamingWire struct {
	Operation       StreamingOperation `json:"operation"`
	Schema          string             `json:"schema"`
	Name            string             `json:"name"`
	Creation        StreamingCreation  `json:"creation"`
	Spec            streamingSpecWire  `json:"spec"`
	Previous        streamingSpecWire  `json:"previous"`
	AllowStateReset bool               `json:"allow_state_reset"`
}

// StreamingCodec describes the version-one operation wire with separate path
// components and both operands. It refuses missing, null, and unknown fields;
// optional Run is omitted when unset. No target capabilities travel in the wire.
func StreamingCodec() schemaext.Codec {
	spec := `{"type":"object","required":["text"],"additionalProperties":false,"properties":{"text":{"type":"string"},"run":{"type":"boolean"},"resource_pool":{"type":"string"}}}`
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := streamingPayload(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(streamingWire{Operation: value.Operation, Schema: value.Schema, Name: value.Name,
			Creation: value.Creation, Spec: streamingSpecWire(value.Spec), Previous: streamingSpecWire(value.Previous), AllowStateReset: value.AllowStateReset})
	}
	return schemaext.Codec{Prototype: &StreamingQuery{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["operation","schema","name","creation","spec","previous","allow_state_reset"],"additionalProperties":false,"properties":{
		"operation":{"enum":["create","alter","drop"]},"schema":{"type":"string"},"name":{"type":"string","minLength":1},
		"creation":{"type":"object","required":["or_replace","if_not_exists"],"additionalProperties":false,"properties":{"or_replace":{"type":"boolean"},"if_not_exists":{"type":"boolean"}}},
		"spec":%s,"previous":%s,"allow_state_reset":{"type":"boolean"}}}`, spec, spec)),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := streamingPayload(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: decodeStreaming,
	}
}

func streamingPayload(payload schemaext.Payload) (*StreamingQuery, error) {
	value, ok := payload.(*StreamingQuery)
	if !ok {
		return nil, fmt.Errorf("%w: expected a streaming operation, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, &schemaext.InvalidModelError{Kind: StreamingQueryKind, Representation: schemaext.Operation, Message: err.Error()}
	}
	return value, nil
}

func decodeStreaming(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := streamingFields(data, "operation", "schema", "name", "creation", "spec", "previous", "allow_state_reset")
	if err != nil {
		return nil, err
	}
	if _, err := streamingFields(fields["creation"], "or_replace", "if_not_exists"); err != nil {
		return nil, err
	}
	for _, key := range []string{"spec", "previous"} {
		if _, err := streamingFields(fields[key], "text"); err != nil {
			return nil, err
		}
	}
	wire, err := schemaext.DecodeJSON[streamingWire](data)
	if err != nil {
		return nil, err
	}
	value := &StreamingQuery{Operation: wire.Operation, Schema: wire.Schema, Name: wire.Name, Creation: wire.Creation,
		Spec: ydbstreaming.Spec(wire.Spec), Previous: ydbstreaming.Spec(wire.Previous), AllowStateReset: wire.AllowStateReset}
	return streamingPayload(value)
}

func streamingFields(data json.RawMessage, required ...string) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	for _, field := range required {
		if len(fields[field]) == 0 {
			return nil, fmt.Errorf("%w: streaming operation requires %s", schemaext.ErrInvalidValue, field)
		}
	}
	for field, value := range fields {
		if string(value) == "null" {
			return nil, fmt.Errorf("%w: streaming operation field %s cannot be null", schemaext.ErrInvalidValue, field)
		}
	}
	return fields, nil
}
