package chast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
)

// Codecs returns fresh version-one operation descriptors. The wire shape is an
// explicit owner-defined operand, never a serialized common Go AST. Invalid or
// unsupported transitions fail before encoding and after decoding.
func Codecs() []schemaext.Codec {
	change := chdiff.Codecs()[0]
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := storageValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return []schemaext.Codec{{
		Prototype: &AlterStorage{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"change":%s,"columns":{"type":"array","items":{"type":"object","required":["name","type"],`+
			`"additionalProperties":false,"properties":{"name":{"type":"string","minLength":1},"type":{"type":"string","minLength":1},`+
			`"value":{"enum":["DEFAULT","MATERIALIZED","ALIAS"]},"expression":{"type":"string"},"comment":{"type":"string"}}}}}`, change.Definition)),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := storageValue(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			_, columns := fields["columns"]
			if fields["change"] == nil || len(fields) != 1 && (len(fields) != 2 || !columns) {
				return nil, fmt.Errorf("%w: ClickHouse storage operation requires change and allows only columns beside it", schemaext.ErrInvalidValue)
			}
			decoded, err := change.Decode(fields["change"])
			if err != nil {
				return nil, err
			}
			value := &AlterStorage{Change: *decoded.(*chdiff.Table)}
			if columns {
				if value.Columns, err = schemaext.DecodeJSON[[]Column](fields["columns"]); err != nil {
					return nil, err
				}
				if len(value.Columns) == 0 {
					return nil, fmt.Errorf("%w: ClickHouse storage operation names an empty column list", schemaext.ErrInvalidValue)
				}
			}
			if _, err := storageValue(value); err != nil {
				return nil, err
			}
			return value, nil
		},
	}, indexCodec(), dropIndexCodec(), refreshCodec(), rowPolicyCodec(), rowPolicyCommandCodec()}
}

func storageValue(payload schemaext.Payload) (*AlterStorage, error) {
	v, ok := payload.(*AlterStorage)
	if !ok || v == nil {
		return nil, fmt.Errorf("%w: expected ClickHouse storage operation", schemaext.ErrInvalidValue)
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return v, nil
}
