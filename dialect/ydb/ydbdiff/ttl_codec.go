package ydbdiff

import (
	"bytes"
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TTLCodec returns the TTL change codec. Each operand uses the exact
// desired or observed wire model. An omitted operand is a known absence; null
// is refused, and a change must carry at least one operand.
func TTLCodec() schemaext.Codec {
	definition := json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"optional observed TTL; omitted when the table had none",`+
		`"after":"optional desired TTL; omitted to remove it","constraint":"at least one operand is present"}`,
		ydbschema.TTLWireDefinition()))
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := ttlChangeValue(payload)
		if err != nil {
			return nil, err
		}
		fields := make(map[string]json.RawMessage)
		for _, codec := range ydbschema.TTLCodecs() {
			key, value := ttlOperand(v, codec.Representation)
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
	return schemaext.Codec{
		Prototype: &TTL{}, Representation: schemaext.Change, Version: 1, Definition: definition,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := ttlChangeValue(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneChange(), nil
		},
		Encode: encode, Canonical: encode, Decode: decodeTTLChange,
	}
}

func ttlOperand(v *TTL, representation schemaext.Representation) (string, schemaext.Payload) {
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

func ttlChangeValue(payload schemaext.Payload) (*TTL, error) {
	v, ok := payload.(*TTL)
	if !ok {
		return nil, fmt.Errorf("%w: expected YDB TTL change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateTTL(v); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeTTLChange(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	for name, value := range fields {
		if name != "before" && name != "after" {
			return nil, fmt.Errorf("%w: unknown YDB TTL change property %q", schemaext.ErrInvalidValue, name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%w: YDB TTL change property %q cannot be null", schemaext.ErrInvalidValue, name)
		}
	}
	v := &TTL{}
	for _, codec := range ydbschema.TTLCodecs() {
		key, _ := ttlOperand(v, codec.Representation)
		raw, found := fields[key]
		if !found {
			continue
		}
		value, err := codec.Decode(raw)
		if err != nil {
			return nil, err
		}
		switch typed := value.(type) {
		case *ydbschema.ObservedTTL:
			v.Before = typed
		case *ydbschema.DesiredTTL:
			v.After = typed
		}
	}
	if _, err := ttlChangeValue(v); err != nil {
		return nil, err
	}
	return v, nil
}
