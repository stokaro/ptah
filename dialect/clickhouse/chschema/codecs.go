package chschema

import (
	"bytes"
	_ "embed" // Embed the definition that identifies this wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed table-codecs.json
var tableDefinition []byte

// WireDefinition returns an independent description of the table wire model.
// The definition covers both representations and their omission semantics.
func WireDefinition() json.RawMessage { return slices.Clone(tableDefinition) }

// Codecs returns the versioned table codecs. Each call returns independent
// definitions. These pure local codecs validate representation invariants;
// they do not resolve defaults, normalize SQL, or claim server support.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		tableCodec(&DesiredTable{}, schemaext.Desired, decodeDesired, validateDesiredPayload),
		tableCodec(&ObservedTable{}, schemaext.Observed, decodeObserved, validateObservedPayload),
	}
}

func tableCodec(prototype schemaext.Value, representation schemaext.Representation,
	decode func(json.RawMessage) (schemaext.Payload, error), validate func(schemaext.Payload) error,
) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) {
		if err := validate(value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: WireDefinition(),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if err := validate(payload); err != nil {
				return nil, err
			}
			value, ok := payload.(schemaext.Value)
			if !ok {
				return nil, fmt.Errorf("%w: expected a ClickHouse table value", schemaext.ErrInvalidValue)
			}
			return schemaext.CloneValue(value)
		},
		Encode: encode, Decode: decode, Canonical: encode,
	}
}

func validateDesiredPayload(payload schemaext.Payload) error {
	v, ok := payload.(*DesiredTable)
	if !ok {
		return fmt.Errorf("%w: expected a desired ClickHouse table, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateDesired(v)
}

func validateObservedPayload(payload schemaext.Payload) error {
	v, ok := payload.(*ObservedTable)
	if !ok {
		return fmt.Errorf("%w: expected an observed ClickHouse table, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateObserved(v)
}

func decodeDesired(data json.RawMessage) (schemaext.Payload, error) {
	properties, err := wireObject(data, "table", tablePropertyNames(), nil)
	if err != nil {
		return nil, err
	}
	for name, value := range properties {
		if _, err := wireObject(value, "table", []string{"state", "value"}, []string{"state"}); err != nil {
			return nil, fmt.Errorf("ClickHouse %s: %w", name, err)
		}
	}
	v, err := schemaext.DecodeJSON[*DesiredTable](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeObserved(data json.RawMessage) (schemaext.Payload, error) {
	names := tablePropertyNames()
	if _, err := wireObject(data, "table", names, names); err != nil {
		return nil, err
	}
	v, err := schemaext.DecodeJSON[*ObservedTable](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObserved(v); err != nil {
		return nil, err
	}
	return v, nil
}

func tablePropertyNames() []string {
	var names []string
	for _, property := range new(DesiredTable).properties() {
		names = append(names, property.name)
	}
	return names
}

// JSON null is not an omitted declaration or an inspected absence. Inspect keys
// as well as types: encoding/json otherwise accepts case-insensitive field names.
func wireObject(data json.RawMessage, model string, allowed, required []string) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("%w: expected a non-null ClickHouse %s object", schemaext.ErrInvalidValue, model)
	}
	for name, value := range fields {
		if !slices.Contains(allowed, name) {
			return nil, fmt.Errorf("%w: unknown ClickHouse %s property %q", schemaext.ErrInvalidValue, model, name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%w: ClickHouse %s property %q cannot be null", schemaext.ErrInvalidValue, model, name)
		}
	}
	for _, name := range required {
		if _, exists := fields[name]; !exists {
			return nil, fmt.Errorf("%w: missing ClickHouse %s property %q", schemaext.ErrInvalidValue, model, name)
		}
	}
	return fields, nil
}
