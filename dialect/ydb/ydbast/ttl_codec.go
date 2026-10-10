package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
)

// TTLCodec returns a fresh version-one operation descriptor. The wire shape is the
// owner's change operand, never a serialized common Go AST. A transition with
// invalid or equivalent operands fails before encoding and after decoding.
func TTLCodec() schemaext.Codec {
	change := ydbdiff.TTLCodec()
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := alterTTLValue(payload)
		if err != nil {
			return nil, err
		}
		encoded, err := change.Encode(&value.Change)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]json.RawMessage{"change": encoded})
	}
	return schemaext.Codec{
		Prototype: &AlterTTL{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"change":%s}`, change.Definition)),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := alterTTLValue(payload)
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
				return nil, fmt.Errorf("%w: YDB TTL operation requires exactly change", schemaext.ErrInvalidValue)
			}
			decoded, err := change.Decode(fields["change"])
			if err != nil {
				return nil, err
			}
			typed, ok := decoded.(*ydbdiff.TTL)
			if !ok {
				return nil, fmt.Errorf("%w: YDB TTL operation decoded %T", schemaext.ErrInvalidValue, decoded)
			}
			value, err := alterTTLValue(&AlterTTL{Change: *typed})
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}
}

func alterTTLValue(payload schemaext.Payload) (*AlterTTL, error) {
	v, ok := payload.(*AlterTTL)
	if !ok || v == nil {
		return nil, fmt.Errorf("%w: expected YDB TTL operation", schemaext.ErrInvalidValue)
	}
	if err := ValidateTTLChange(&v.Change); err != nil {
		return nil, err
	}
	return v, nil
}
