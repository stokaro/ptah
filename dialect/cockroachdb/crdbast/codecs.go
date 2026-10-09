package crdbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/internal/ttlsql"
)

// Codecs returns fresh version-one operation descriptors. The wire shape is the
// owner's change operand, never a serialized common Go AST. A transition with
// invalid or equivalent operands fails before encoding and after decoding.
func Codecs() []schemaext.Codec {
	change := crdbdiff.Codecs()[0]
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := alterValue(payload)
		if err != nil {
			return nil, err
		}
		encoded, err := change.Encode(&value.Change)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]json.RawMessage{"change": encoded})
	}
	return []schemaext.Codec{{
		Prototype: &AlterRowTTL{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"change":%s}`, change.Definition)),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := alterValue(payload)
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
				return nil, fmt.Errorf("%w: CockroachDB row-level TTL operation requires exactly change", schemaext.ErrInvalidValue)
			}
			decoded, err := change.Decode(fields["change"])
			if err != nil {
				return nil, err
			}
			typed, ok := decoded.(*crdbdiff.RowTTL)
			if !ok {
				return nil, fmt.Errorf("%w: CockroachDB row-level TTL operation decoded %T", schemaext.ErrInvalidValue, decoded)
			}
			value, err := alterValue(&AlterRowTTL{Change: *typed})
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}}
}

func alterValue(payload schemaext.Payload) (*AlterRowTTL, error) {
	v, ok := payload.(*AlterRowTTL)
	if !ok || v == nil {
		return nil, fmt.Errorf("%w: expected CockroachDB row-level TTL operation", schemaext.ErrInvalidValue)
	}
	if err := ttlsql.ValidateChange(&v.Change); err != nil {
		return nil, err
	}
	return v, nil
}
