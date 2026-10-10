package tsschema

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

const (
	desiredHypertableDefinition = `{"type":"object","required":["column"],"additionalProperties":false,"properties":{` +
		`"column":{"type":"string","minLength":1},"chunk_interval":{"type":"string"},"if_not_exists":{"type":"boolean"},"comment":{"type":"string"}}}`
	observedHypertableDefinition = `{"type":"object","required":["column","dimensions"],"additionalProperties":false,"properties":{` +
		`"column":{"type":"string","minLength":1},"column_type":{"type":"string"},"chunk_interval":{"type":"string"},"dimensions":{"type":"integer","minimum":1}}}`
	desiredAggregateDefinition = `{"type":"object","required":["body"],"additionalProperties":false,"properties":{` +
		`"body":{"type":"string","minLength":1},"materialized_only":{"type":"boolean"},"comment":{"type":"string"},"struct_name":{"type":"string"},` +
		`"normalized":{"type":"object","required":["body"],"additionalProperties":false,"properties":{"body":{"type":"string"}}}}}`
	observedAggregateDefinition = `{"type":"object","required":["definition"],"additionalProperties":false,"properties":{` +
		`"definition":{"type":"string","minLength":1},"materialized_only":{"type":"boolean"},"hypertable_schema":{"type":"string"},"hypertable_name":{"type":"string"}}}`
)

// Codecs returns the version-one desired and observed codecs of both models.
// Each call returns independent definitions. The codecs validate
// representation invariants; they resolve no defaults and claim no support.
func Codecs() []schemaext.Codec {
	return append(HypertableCodecs(), ContinuousAggregateCodecs()...)
}

// HypertableCodecs returns the desired and observed hypertable codecs, in that
// order.
func HypertableCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredHypertable{}, schemaext.Desired, desiredHypertableDefinition, validateDesiredHypertablePayload, decodeDesiredHypertable),
		modelCodec(&ObservedHypertable{}, schemaext.Observed, observedHypertableDefinition, validateObservedHypertablePayload, decodeObservedHypertable),
	}
}

// ContinuousAggregateCodecs returns the desired and observed aggregate codecs,
// in that order.
func ContinuousAggregateCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredContinuousAggregate{}, schemaext.Desired, desiredAggregateDefinition, validateDesiredAggregatePayload, decodeDesiredAggregate),
		modelCodec(&ObservedContinuousAggregate{}, schemaext.Observed, observedAggregateDefinition, validateObservedAggregatePayload, decodeObservedAggregate),
	}
}

func modelCodec(prototype schemaext.Value, representation schemaext.Representation, definition string,
	validate func(schemaext.Payload) error, decode func(json.RawMessage) (schemaext.Payload, error),
) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		if err := validate(payload); err != nil {
			return nil, err
		}
		return json.Marshal(payload)
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: json.RawMessage(definition),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if err := validate(payload); err != nil {
				return nil, err
			}
			value, ok := payload.(schemaext.Value)
			if !ok {
				return nil, fmt.Errorf("%w: expected a TimescaleDB value, got %T", schemaext.ErrInvalidValue, payload)
			}
			return schemaext.CloneValue(value)
		},
		Encode: encode, Canonical: encode, Decode: decode,
	}
}

func validateDesiredHypertablePayload(payload schemaext.Payload) error {
	value, ok := payload.(*DesiredHypertable)
	if !ok {
		return fmt.Errorf("%w: expected a desired hypertable, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateDesiredHypertable(value)
}

func validateObservedHypertablePayload(payload schemaext.Payload) error {
	value, ok := payload.(*ObservedHypertable)
	if !ok {
		return fmt.Errorf("%w: expected an observed hypertable, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateObservedHypertable(value)
}

func validateDesiredAggregatePayload(payload schemaext.Payload) error {
	value, ok := payload.(*DesiredContinuousAggregate)
	if !ok {
		return fmt.Errorf("%w: expected a desired continuous aggregate, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateDesiredContinuousAggregate(value)
}

func validateObservedAggregatePayload(payload schemaext.Payload) error {
	value, ok := payload.(*ObservedContinuousAggregate)
	if !ok {
		return fmt.Errorf("%w: expected an observed continuous aggregate, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateObservedContinuousAggregate(value)
}

func decodeDesiredHypertable(data json.RawMessage) (schemaext.Payload, error) {
	if _, err := wireObject(data, "hypertable", []string{"column", "chunk_interval", "if_not_exists", "comment"}, []string{"column"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*DesiredHypertable](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesiredHypertable(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedHypertable(data json.RawMessage) (schemaext.Payload, error) {
	if _, err := wireObject(data, "hypertable", []string{"column", "column_type", "chunk_interval", "dimensions"}, []string{"column", "dimensions"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*ObservedHypertable](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObservedHypertable(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeDesiredAggregate(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := wireObject(data, "continuous aggregate", []string{"body", "materialized_only", "comment", "struct_name", "normalized"}, []string{"body"})
	if err != nil {
		return nil, err
	}
	if normalized, found := fields["normalized"]; found {
		if _, err := wireObject(normalized, "normalized body", []string{"body"}, []string{"body"}); err != nil {
			return nil, err
		}
	}
	value, err := schemaext.DecodeJSON[*DesiredContinuousAggregate](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesiredContinuousAggregate(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedAggregate(data json.RawMessage) (schemaext.Payload, error) {
	if _, err := wireObject(data, "continuous aggregate", []string{"definition", "materialized_only", "hypertable_schema", "hypertable_name"}, []string{"definition"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*ObservedContinuousAggregate](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObservedContinuousAggregate(value); err != nil {
		return nil, err
	}
	return value, nil
}
