package chast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

// Codecs returns fresh version-one operation descriptors. The wire shape is an
// explicit owner-defined operand, never a serialized common Go AST. Invalid or
// unsupported transitions fail before encoding and after decoding.
func Codecs() []schemaext.Codec {
	change := chdiff.Codecs()[0]
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := ttlValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return []schemaext.Codec{{
		Prototype: &AlterTTL{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"change":%s}`, change.Definition)),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := ttlValue(payload)
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
			if len(fields) != 1 || fields["change"] == nil {
				return nil, fmt.Errorf("%w: ClickHouse TTL operation requires exactly change", schemaext.ErrInvalidValue)
			}
			decoded, err := change.Decode(fields["change"])
			if err != nil {
				return nil, err
			}
			value, err := ttlValue(&AlterTTL{Change: *decoded.(*chdiff.Table)})
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}, indexCodec(), dropIndexCodec(), refreshCodec(), rowPolicyCodec()}
}

func ttlValue(payload schemaext.Payload) (*AlterTTL, error) {
	v, ok := payload.(*AlterTTL)
	if !ok || v == nil {
		return nil, fmt.Errorf("%w: expected ClickHouse TTL operation", schemaext.ErrInvalidValue)
	}
	if err := chsql.ValidateTTLChange(&v.Change); err != nil {
		return nil, err
	}
	return v, nil
}
