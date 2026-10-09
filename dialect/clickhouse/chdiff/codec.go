package chdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Codecs returns the table-change codec. Its operands use the exact desired
// and observed table wire models, including refusal of missing or null fields.
// Canonical encoding preserves SQL text and does not resolve defaults.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{directionalCodec(&Table{}, "table", chschema.WireDefinition(), chschema.Codecs(), Validate,
		func(before, after schemaext.Payload) *Table {
			v := &Table{}
			v.Before, _ = before.(*chschema.ObservedTable)
			v.After, _ = after.(*chschema.DesiredTable)
			return v
		})}
}

// IndexCodecs returns the versioned index-change codec. Both operands use the
// strict index wire models, and the desired operand must be fully resolved.
// Canonical encoding retains type spelling and the full uint64 granularity.
func IndexCodecs() []schemaext.Codec {
	return []schemaext.Codec{directionalCodec(&Index{}, "index", chschema.IndexWireDefinition(), chschema.IndexCodecs(), ValidateIndex,
		func(before, after schemaext.Payload) *Index {
			v := &Index{}
			v.Before, _ = before.(*chschema.ObservedIndex)
			v.After, _ = after.(*chschema.DesiredIndex)
			return v
		})}
}

// directionalCodec encodes a change as its complete before and after operands,
// each through the owner's model codec for its representation. Decoding
// requires exactly both operands and validates the assembled change.
func directionalCodec[T schemaext.ChangeValue](prototype T, noun string, operands json.RawMessage, models []schemaext.Codec,
	validate func(T) error, assemble func(before, after schemaext.Payload) T,
) schemaext.Codec {
	value := func(payload schemaext.Payload) (T, error) {
		var zero T
		v, ok := payload.(T)
		if !ok {
			return zero, fmt.Errorf("%w: expected ClickHouse %s change, got %T", schemaext.ErrInvalidValue, noun, payload)
		}
		if err := validate(v); err != nil {
			return zero, err
		}
		return v, nil
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := value(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"required non-null observed %s","after":"required non-null resolved desired %s"}`, operands, noun, noun)),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := value(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneChange(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if len(fields) != 2 || fields["before"] == nil || fields["after"] == nil {
				return nil, fmt.Errorf("%w: ClickHouse %s change requires exactly before and after", schemaext.ErrInvalidValue, noun)
			}
			decoded := make(map[schemaext.Representation]schemaext.Payload, len(models))
			for _, codec := range models {
				key := "after"
				if codec.Representation == schemaext.Observed {
					key = "before"
				}
				operand, err := codec.Decode(fields[key])
				if err != nil {
					return nil, err
				}
				decoded[codec.Representation] = operand
			}
			v, err := value(assemble(decoded[schemaext.Observed], decoded[schemaext.Desired]))
			if err != nil {
				return nil, err
			}
			return v, nil
		},
	}
}
