package chast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

func indexCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := indexValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	return schemaext.Codec{
		Prototype: &AddSkippingIndex{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["name","expression","index_type","granularity"],
			"additionalProperties":false,"properties":{"name":{"type":"string","minLength":1},"expression":{"type":"string","minLength":1},"index_type":{"type":"string"},
			"granularity":{"type":"integer","minimum":0}},"defaults":{"index_type":"minmax","granularity":1}}`),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := indexValue(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneExtension(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if len(fields) != 4 {
				return nil, fmt.Errorf("%w: ClickHouse ADD INDEX requires every operand", schemaext.ErrInvalidValue)
			}
			for _, name := range []string{"name", "expression", "index_type", "granularity"} {
				if len(fields[name]) == 0 || string(fields[name]) == "null" {
					return nil, fmt.Errorf("%w: ClickHouse ADD INDEX requires %s", schemaext.ErrInvalidValue, name)
				}
			}
			v, err := schemaext.DecodeJSON[*AddSkippingIndex](data)
			if err != nil {
				return nil, err
			}
			if err := v.Validate(); err != nil {
				return nil, err
			}
			return v, nil
		},
	}
}

func dropIndexCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := dropIndexValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	return schemaext.Codec{
		Prototype: &DropSkippingIndex{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["name"],"additionalProperties":false,"properties":{"name":{"type":"string","minLength":1}}}`),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := dropIndexValue(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneExtension(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if len(fields) != 1 || len(fields["name"]) == 0 || string(fields["name"]) == "null" {
				return nil, fmt.Errorf("%w: ClickHouse DROP INDEX requires exactly name", schemaext.ErrInvalidValue)
			}
			v, err := schemaext.DecodeJSON[*DropSkippingIndex](data)
			if err != nil {
				return nil, err
			}
			return dropIndexValue(v)
		},
	}
}

func dropIndexValue(payload schemaext.Payload) (*DropSkippingIndex, error) {
	v, ok := payload.(*DropSkippingIndex)
	if !ok {
		return nil, fmt.Errorf("%w: expected ClickHouse DROP INDEX, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return v, nil
}

func indexValue(payload schemaext.Payload) (*AddSkippingIndex, error) {
	v, ok := payload.(*AddSkippingIndex)
	if !ok {
		return nil, fmt.Errorf("%w: expected ClickHouse ADD INDEX, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return v, nil
}
