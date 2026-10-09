package chdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// IndexCodecs returns the versioned index-change codec. Both operands use the
// strict index wire models, and the desired operand must be fully resolved.
// Canonical encoding retains type spelling and the full uint64 granularity.
func IndexCodecs() []schemaext.Codec {
	definition := json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"required non-null observed index","after":"required non-null resolved desired index"}`, chschema.IndexWireDefinition()))
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := indexValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	return []schemaext.Codec{{
		Prototype: &Index{}, Representation: schemaext.Change, Version: 1, Definition: definition,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := indexValue(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneChange(), nil
		},
		Encode: encode, Canonical: encode, Decode: decodeIndex,
	}}
}

func indexValue(payload schemaext.Payload) (*Index, error) {
	v, ok := payload.(*Index)
	if !ok {
		return nil, fmt.Errorf("%w: expected ClickHouse index change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateIndex(v); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeIndex(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if len(fields) != 2 || fields["before"] == nil || fields["after"] == nil {
		return nil, fmt.Errorf("%w: ClickHouse index change requires exactly before and after", schemaext.ErrInvalidValue)
	}
	v := &Index{}
	for _, codec := range chschema.IndexCodecs() {
		key := "after"
		if codec.Representation == schemaext.Observed {
			key = "before"
		}
		value, err := codec.Decode(fields[key])
		if err != nil {
			return nil, err
		}
		switch typed := value.(type) {
		case *chschema.ObservedIndex:
			v.Before = typed
		case *chschema.DesiredIndex:
			v.After = typed
		}
	}
	return indexValue(v)
}
