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
	definition := json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"required non-null observed table","after":"required non-null resolved desired table"}`, chschema.WireDefinition()))
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := tableValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	return []schemaext.Codec{{
		Prototype: &Table{}, Representation: schemaext.Change, Version: 1, Definition: definition,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := tableValue(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneChange(), nil
		},
		Encode: encode, Canonical: encode, Decode: decodeTable,
	}}
}

func tableValue(payload schemaext.Payload) (*Table, error) {
	v, ok := payload.(*Table)
	if !ok {
		return nil, fmt.Errorf("%w: expected ClickHouse table change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := Validate(v); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeTable(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if len(fields) != 2 || fields["before"] == nil || fields["after"] == nil {
		return nil, fmt.Errorf("%w: ClickHouse table change requires exactly before and after", schemaext.ErrInvalidValue)
	}
	v := &Table{}
	for _, codec := range chschema.Codecs() {
		key := "after"
		if codec.Representation == schemaext.Observed {
			key = "before"
		}
		value, err := codec.Decode(fields[key])
		if err != nil {
			return nil, err
		}
		switch typed := value.(type) {
		case *chschema.ObservedTable:
			v.Before = typed
		case *chschema.DesiredTable:
			v.After = typed
		}
	}
	return tableValue(v)
}
