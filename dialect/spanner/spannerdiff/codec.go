package spannerdiff

import (
	"bytes"
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
)

// Codecs returns the row deletion policy change codec. Each operand uses the exact
// desired or observed wire model. An omitted operand is a known absence; null
// is refused, and a change must carry at least one operand.
func Codecs() []schemaext.Codec {
	definition := json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed row deletion policy; omitted when the table had none",`+
		`"after":"optional desired row deletion policy; omitted to remove it","constraint":"at least one operand is present"}`,
		spannerschema.WireDefinition()))
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := changeValue(payload)
		if err != nil {
			return nil, err
		}
		fields := make(map[string]json.RawMessage)
		for _, codec := range spannerschema.Codecs() {
			key, value := operand(v, codec.Representation)
			if value == nil {
				continue
			}
			encoded, err := codec.Encode(value)
			if err != nil {
				return nil, err
			}
			fields[key] = encoded
		}
		return json.Marshal(fields)
	}
	return []schemaext.Codec{{
		Prototype: &RowDeletion{}, Representation: schemaext.Change, Version: 1, Definition: definition,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := changeValue(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneChange(), nil
		},
		Encode: encode, Canonical: encode, Decode: decodeChange,
	}}
}

func operand(v *RowDeletion, representation schemaext.Representation) (string, schemaext.Payload) {
	if representation == schemaext.Observed {
		if v.Before == nil {
			return "before", nil
		}
		return "before", v.Before
	}
	if v.After == nil {
		return "after", nil
	}
	return "after", v.After
}

func changeValue(payload schemaext.Payload) (*RowDeletion, error) {
	v, ok := payload.(*RowDeletion)
	if !ok {
		return nil, fmt.Errorf("%w: expected Spanner row deletion policy change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := Validate(v); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeChange(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	for name, value := range fields {
		if name != "before" && name != "after" {
			return nil, fmt.Errorf("%w: unknown Spanner row deletion policy change property %q", schemaext.ErrInvalidValue, name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%w: Spanner row deletion policy change property %q cannot be null", schemaext.ErrInvalidValue, name)
		}
	}
	v := &RowDeletion{}
	for _, codec := range spannerschema.Codecs() {
		key, _ := operand(v, codec.Representation)
		raw, found := fields[key]
		if !found {
			continue
		}
		value, err := codec.Decode(raw)
		if err != nil {
			return nil, err
		}
		switch typed := value.(type) {
		case *spannerschema.ObservedRowDeletion:
			v.Before = typed
		case *spannerschema.DesiredRowDeletion:
			v.After = typed
		}
	}
	if _, err := changeValue(v); err != nil {
		return nil, err
	}
	return v, nil
}
