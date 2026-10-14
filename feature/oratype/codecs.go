package oratype

import (
	"encoding/json"

	"ptah.run/core/schemaext"
)

const (
	textDefinition      = `{"type":"string","minLength":1}`
	attributeDefinition = `{"type":"object","required":["name","type"],"additionalProperties":false,"properties":{` +
		`"name":` + textDefinition + `,"type":` + textDefinition + `}}`
	attributesDefinition = `{"type":"array","minItems":1,"items":` + attributeDefinition + `}`

	desiredDomainDefinition = `{"type":"object","required":["base_type"],"additionalProperties":false,"properties":{` +
		`"base_type":` + textDefinition + `,"not_null":{"const":true},"default":` + textDefinition + `,` +
		`"default_expr":` + textDefinition + `,"check":` + textDefinition + `,"comment":` + textDefinition + `,` +
		`"struct_name":` + textDefinition + `}}`
	observedDomainDefinition = `{"type":"object","required":["base_type"],"additionalProperties":false,"properties":{` +
		`"base_type":` + textDefinition + `,"not_null":{"const":true},"default":` + textDefinition + `,` +
		`"check":` + textDefinition + `}}`
	desiredObjectTypeDefinition = `{"type":"object","required":["attributes"],"additionalProperties":false,"properties":{` +
		`"attributes":` + attributesDefinition + `,"comment":` + textDefinition + `,"struct_name":` + textDefinition + `}}`
	observedObjectTypeDefinition = `{"type":"object","required":["attributes"],"additionalProperties":false,"properties":{` +
		`"attributes":` + attributesDefinition + `}}`
)

// The wire shapes. A key the encoder leaves out while it is empty is NonEmpty.
var (
	desiredDomainShape = schemaext.ObjectShape{Name: "domain",
		Allowed:  []string{"base_type", "not_null", "default", "default_expr", "check", "comment", "struct_name"},
		Required: []string{"base_type"},
		NonEmpty: []string{"base_type", "not_null", "default", "default_expr", "check", "comment", "struct_name"}}
	observedDomainShape = schemaext.ObjectShape{Name: "domain",
		Allowed:  []string{"base_type", "not_null", "default", "check"},
		Required: []string{"base_type"},
		NonEmpty: []string{"base_type", "not_null", "default", "check"}}
	desiredObjectTypeShape = schemaext.ObjectShape{Name: "object type",
		Allowed: []string{"attributes", "comment", "struct_name"}, Required: []string{"attributes"},
		NonEmpty: []string{"attributes", "comment", "struct_name"}}
	observedObjectTypeShape = schemaext.ObjectShape{Name: "object type",
		Allowed: []string{"attributes"}, Required: []string{"attributes"}, NonEmpty: []string{"attributes"}}
	attributeShape = schemaext.ObjectShape{Name: "object type attribute",
		Allowed: []string{"name", "type"}, Required: []string{"name", "type"}}
)

// Codecs returns the version-one desired and observed codecs of the two
// models, a domain's first, each desired before observed. Each call returns
// independent definitions. A decoder accepts only the spelling the encoder
// writes, so an omitted value written out, such as an empty comment, is
// refused. Every refusal is a [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue].
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredDomain]{
			Prototype: &DesiredDomain{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredDomainDefinition),
			Shape: objectWire(desiredDomainShape), Validate: ValidateDesiredDomain,
		}.Codec(),
		schemaext.ModelCodec[*ObservedDomain]{
			Prototype: &ObservedDomain{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedDomainDefinition),
			Shape: objectWire(observedDomainShape), Validate: ValidateObservedDomain,
		}.Codec(),
		schemaext.ModelCodec[*DesiredObjectType]{
			Prototype: &DesiredObjectType{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(desiredObjectTypeDefinition), Shape: attributesWire(desiredObjectTypeShape),
			Validate: ValidateDesiredObjectType,
		}.Codec(),
		schemaext.ModelCodec[*ObservedObjectType]{
			Prototype: &ObservedObjectType{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(observedObjectTypeDefinition), Shape: attributesWire(observedObjectTypeShape),
			Validate: ValidateObservedObjectType,
		}.Codec(),
	}
}

func objectWire(shape schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, shape)
		return err
	}
}

func attributesWire(shape schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, shape)
		if err != nil {
			return err
		}
		return eachObject(fields["attributes"], attributeShape)
	}
}

// eachObject checks every element of a JSON array against shape. An absent
// array checks nothing.
func eachObject(data json.RawMessage, shape schemaext.ObjectShape) error {
	if data == nil {
		return nil
	}
	elements, err := schemaext.DecodeJSON[[]json.RawMessage](data)
	if err != nil {
		return err
	}
	for _, element := range elements {
		if _, err := schemaext.DecodeObject(element, shape); err != nil {
			return err
		}
	}
	return nil
}
