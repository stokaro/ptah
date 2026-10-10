package chschema

import (
	_ "embed" // Embed the exact definition bound to data-skipping values.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed index-codecs.json
var indexDefinition []byte

// IndexWireDefinition returns an independent description of both index models.
func IndexWireDefinition() json.RawMessage { return slices.Clone(indexDefinition) }

// IndexCodecs returns strict versioned codecs for desired and observed indexes.
// Registration understands the model but grants no target or server support.
func IndexCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		valueCodec(&DesiredIndex{}, schemaext.Desired, IndexWireDefinition(), decodeDesiredIndex, validateDesiredIndexPayload),
		valueCodec(&ObservedIndex{}, schemaext.Observed, IndexWireDefinition(), decodeObservedIndex, validateObservedIndexPayload),
	}
}

// valueCodec is the strict codec of one attached model value: validate guards
// every encoding and clone, and decode refuses what the model would not hold.
func valueCodec(prototype schemaext.Value, representation schemaext.Representation, definition json.RawMessage,
	decode func(json.RawMessage) (schemaext.Payload, error), validate func(schemaext.Payload) error,
) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) {
		if err := validate(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return schemaext.Codec{Prototype: prototype, Representation: representation, Version: 1, Definition: definition,
		Clone: func(value schemaext.Payload) (schemaext.Payload, error) {
			if err := validate(value); err != nil {
				return nil, err
			}
			return schemaext.CloneValue(value.(schemaext.Value))
		},
		Encode: encode, Decode: decode, Canonical: encode,
	}
}

func validateDesiredIndexPayload(payload schemaext.Payload) error {
	v, ok := payload.(*DesiredIndex)
	if !ok {
		return fmt.Errorf("%w: expected a desired ClickHouse index, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateDesiredIndex(v)
}

func validateObservedIndexPayload(payload schemaext.Payload) error {
	v, ok := payload.(*ObservedIndex)
	if !ok {
		return fmt.Errorf("%w: expected an observed ClickHouse index, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateObservedIndex(v)
}

func decodeDesiredIndex(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := wireObject(data, "index", []string{"index_type", "granularity"}, nil)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"index_type", "granularity"} {
		if value, present := fields[name]; present {
			if _, err := wireObject(value, "index "+name, []string{"state", "value"}, []string{"state"}); err != nil {
				return nil, err
			}
		}
	}
	value, err := schemaext.DecodeJSON[*DesiredIndex](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesiredIndex(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedIndex(data json.RawMessage) (schemaext.Payload, error) {
	names := []string{"index_type", "granularity"}
	if _, err := wireObject(data, "index", names, names); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*ObservedIndex](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObservedIndex(value); err != nil {
		return nil, err
	}
	return value, nil
}
