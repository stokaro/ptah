package crdbdiff

import (
	"bytes"
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// Codecs returns the row-level TTL change codec. Each operand uses the exact
// desired or observed wire model. An omitted operand is a known absence; null
// is refused, and a change must carry at least one operand.
func Codecs() []schemaext.Codec {
	definition := json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed row-level TTL; omitted when the table had none",`+
		`"after":"optional desired row-level TTL; omitted to remove the policy","constraint":"at least one operand is present"}`,
		crdbschema.WireDefinition()))
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := changeValue(payload)
		if err != nil {
			return nil, err
		}
		fields := make(map[string]json.RawMessage)
		for _, codec := range crdbschema.Codecs() {
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
		Prototype: &RowTTL{}, Representation: schemaext.Change, Version: 1, Definition: definition,
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

func operand(v *RowTTL, representation schemaext.Representation) (string, schemaext.Payload) {
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

func changeValue(payload schemaext.Payload) (*RowTTL, error) {
	v, ok := payload.(*RowTTL)
	if !ok {
		return nil, fmt.Errorf("%w: expected CockroachDB row-level TTL change, got %T", schemaext.ErrInvalidValue, payload)
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
			return nil, fmt.Errorf("%w: unknown CockroachDB row-level TTL change property %q", schemaext.ErrInvalidValue, name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%w: CockroachDB row-level TTL change property %q cannot be null", schemaext.ErrInvalidValue, name)
		}
	}
	v := &RowTTL{}
	for _, codec := range crdbschema.Codecs() {
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
		case *crdbschema.ObservedRowTTL:
			v.Before = typed
		case *crdbschema.DesiredRowTTL:
			v.After = typed
		}
	}
	if _, err := changeValue(v); err != nil {
		return nil, err
	}
	return v, nil
}
