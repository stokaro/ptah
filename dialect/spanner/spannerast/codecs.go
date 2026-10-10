package spannerast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerdiff"
)

// Codecs returns fresh version-one operation descriptors. The wire shape is the
// owner's change operand, never a serialized common Go AST. A transition with
// invalid or equivalent operands fails before encoding and after decoding.
func Codecs() []schemaext.Codec {
	change := spannerdiff.Codecs()[0]
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
		Prototype: &AlterRowDeletion{}, Representation: schemaext.Operation, Version: 1,
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
				return nil, fmt.Errorf("%w: Spanner row deletion policy operation requires exactly change", schemaext.ErrInvalidValue)
			}
			decoded, err := change.Decode(fields["change"])
			if err != nil {
				return nil, err
			}
			typed, ok := decoded.(*spannerdiff.RowDeletion)
			if !ok {
				return nil, fmt.Errorf("%w: Spanner row deletion policy operation decoded %T", schemaext.ErrInvalidValue, decoded)
			}
			value, err := alterValue(&AlterRowDeletion{Change: *typed})
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}}
}

func alterValue(payload schemaext.Payload) (*AlterRowDeletion, error) {
	v, ok := payload.(*AlterRowDeletion)
	if !ok || v == nil {
		return nil, fmt.Errorf("%w: expected Spanner row deletion policy operation", schemaext.ErrInvalidValue)
	}
	if err := ValidateChange(&v.Change); err != nil {
		return nil, err
	}
	return v, nil
}
