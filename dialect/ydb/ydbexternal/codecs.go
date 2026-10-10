package ydbexternal

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// Codecs returns the explicit version-one desired and observed model
// descriptors of both kinds. The wire keeps every setting as written; a
// codec resolves no path and no default.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredSource{}, schemaext.Desired, SourceKind, sourceDefinition),
		modelCodec(&ObservedSource{}, schemaext.Observed, SourceKind, sourceDefinition),
		modelCodec(&DesiredTable{}, schemaext.Desired, TableKind, tableDefinition),
		modelCodec(&ObservedTable{}, schemaext.Observed, TableKind, tableDefinition),
	}
}

const optionsDefinition = `{"type":"object","additionalProperties":{"type":"string"}}`

// sourceDefinition and tableDefinition are the JSON schemas of the two specs,
// in the form the codecs write them.
const (
	sourceDefinition = `{"type":"object","required":["source_type","auth_method"],"additionalProperties":false,"properties":{` +
		`"source_type":{"type":"string","minLength":1},"location":{"type":"string"},"auth_method":{"type":"string","minLength":1},` +
		`"options":` + optionsDefinition + `}}`
	tableDefinition = `{"type":"object","required":["data_source","location","columns"],"additionalProperties":false,"properties":{` +
		`"data_source":{"type":"string","minLength":1},"location":{"type":"string","minLength":1},` +
		`"columns":{"type":"array","minItems":1,"items":{"type":"object","required":["name","type"],"additionalProperties":false,` +
		`"properties":{"name":{"type":"string","minLength":1},"type":{"type":"string","minLength":1},"not_null":{"type":"boolean"}}}},` +
		`"options":` + optionsDefinition + `}}`
)

func modelCodec(prototype schemaext.Value, representation schemaext.Representation, kind schemaext.Kind, spec string) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := externalPayload(payload, representation, kind)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	properties := `"spec":` + spec
	if representation == schemaext.Desired {
		properties += `,"struct_name":{"type":"string"}`
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["spec"],"additionalProperties":false,"properties":{` + properties + `}}`),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := externalPayload(payload, representation, kind)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if raw, present := fields["spec"]; !present || string(raw) == "null" {
				return nil, fmt.Errorf("%w: an external object requires spec", schemaext.ErrInvalidValue)
			}
			var value schemaext.Value
			switch prototype.(type) {
			case *DesiredSource:
				value, err = decodeInto[DesiredSource](data)
			case *ObservedSource:
				value, err = decodeInto[ObservedSource](data)
			case *DesiredTable:
				value, err = decodeInto[DesiredTable](data)
			default:
				value, err = decodeInto[ObservedTable](data)
			}
			if err != nil {
				return nil, err
			}
			return externalPayload(value, representation, kind)
		},
	}
}

func decodeInto[T any, P interface {
	*T
	schemaext.Value
}](data json.RawMessage) (schemaext.Value, error) {
	value, err := schemaext.DecodeJSON[T](data)
	if err != nil {
		return nil, err
	}
	return P(&value), nil
}

// externalPayload accepts a value of kind in representation that passes its
// validation.
func externalPayload(payload schemaext.Payload, representation schemaext.Representation, kind schemaext.Kind) (schemaext.Value, error) {
	if err := schemaext.ValidatePayload(payload); err != nil {
		return nil, err
	}
	value, ok := payload.(schemaext.Value)
	if !ok || value.Kind() != kind {
		return nil, fmt.Errorf("%w: expected a %s %s, got %T", schemaext.ErrInvalidValue, representation, kind, payload)
	}
	var err error
	switch typed := value.(type) {
	case *DesiredSource:
		if representation == schemaext.Desired {
			err = typed.Validate()
		} else {
			err = fmt.Errorf("%w: expected an observed data source", schemaext.ErrInvalidValue)
		}
	case *ObservedSource:
		if representation == schemaext.Observed {
			err = typed.Spec.Validate()
		} else {
			err = fmt.Errorf("%w: expected a desired data source", schemaext.ErrInvalidValue)
		}
	case *DesiredTable:
		if representation == schemaext.Desired {
			err = typed.Validate()
		} else {
			err = fmt.Errorf("%w: expected an observed external table", schemaext.ErrInvalidValue)
		}
	case *ObservedTable:
		if representation == schemaext.Observed {
			err = typed.Spec.Validate()
		} else {
			err = fmt.Errorf("%w: expected a desired external table", schemaext.ErrInvalidValue)
		}
	}
	if err != nil {
		return nil, &schemaext.InvalidModelError{Kind: kind, Representation: representation, Message: err.Error()}
	}
	return value, nil
}

// UnsupportedReason is why a read records an external object it lists and
// does not describe on a target without the external_data_sources
// capability.
const UnsupportedReason = "target capability external_data_sources is unavailable, so Ptah leaves the object unmanaged"

// SourceCoverage records a source's claim about the data source namespace.
func SourceCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(SourceKind, representation, knowledge, subjects)
}

// TableCoverage records a source's claim about the external table namespace.
func TableCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedCoverage(TableKind, representation, knowledge, subjects)
}

// ownedCoverage builds this package's model registry once; see
// [schemaext.OwnedCoverageSource].
var ownedCoverage = schemaext.OwnedCoverageSource("ptah.run/ydb", Codecs)
