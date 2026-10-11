package pgtype

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
		`"struct_name":` + textDefinition + `,"normalized":{"type":"object","additionalProperties":false,"properties":{` +
		`"check":` + textDefinition + `,"default":` + textDefinition + `}}}}`
	observedDomainDefinition = `{"type":"object","required":["base_type"],"additionalProperties":false,"properties":{` +
		`"base_type":` + textDefinition + `,"not_null":{"const":true},"default":` + textDefinition + `,` +
		`"check":` + textDefinition + `,"checks":{"type":"array","minItems":1,"items":{"type":"object","required":["name","expression"],` +
		`"additionalProperties":false,"properties":{"name":` + textDefinition + `,"expression":` + textDefinition + `}}},` +
		`"comment":` + textDefinition + `}}`
	desiredCompositeDefinition = `{"type":"object","required":["attributes"],"additionalProperties":false,"properties":{` +
		`"attributes":` + attributesDefinition + `,"comment":` + textDefinition + `,"struct_name":` + textDefinition + `}}`
	observedCompositeDefinition = `{"type":"object","required":["attributes"],"additionalProperties":false,"properties":{` +
		`"attributes":` + attributesDefinition + `,"comment":` + textDefinition + `}}`
	rangeAttributesDefinition = `"subtype_opclass":` + textDefinition + `,"collation":` + textDefinition + `,` +
		`"canonical":` + textDefinition + `,"subtype_diff":` + textDefinition + `,"comment":` + textDefinition
	desiredRangeDefinition = `{"type":"object","required":["subtype"],"additionalProperties":false,"properties":{` +
		`"subtype":` + textDefinition + `,` + rangeAttributesDefinition + `,` +
		`"cleared":{"type":"array","minItems":1,"uniqueItems":true,"items":{"enum":["subtype_opclass","collation","canonical","subtype_diff"]}},` +
		`"struct_name":` + textDefinition + `}}`
	observedRangeDefinition = `{"type":"object","required":["subtype"],"additionalProperties":false,"properties":{` +
		`"subtype":` + textDefinition + `,` + rangeAttributesDefinition + `}}`
)

// The wire shapes. A key the encoder leaves out while it is empty is
// NonEmpty, unless it holds a pointer, whose empty value is a value.
var (
	desiredDomainShape = schemaext.ObjectShape{Name: "domain",
		Allowed:  []string{"base_type", "not_null", "default", "default_expr", "check", "comment", "struct_name", "normalized"},
		Required: []string{"base_type"},
		NonEmpty: []string{"base_type", "not_null", "default", "default_expr", "check", "comment", "struct_name"}}
	normalizedDomainShape = schemaext.ObjectShape{Name: "normalized domain",
		Allowed: []string{"check", "default"}, NonEmpty: []string{"check", "default"}}
	observedDomainShape = schemaext.ObjectShape{Name: "domain",
		Allowed:  []string{"base_type", "not_null", "default", "check", "checks", "comment"},
		Required: []string{"base_type"},
		NonEmpty: []string{"base_type", "not_null", "default", "check", "checks", "comment"}}
	domainCheckShape = schemaext.ObjectShape{Name: "domain check",
		Allowed: []string{"name", "expression"}, Required: []string{"name", "expression"}}
	desiredCompositeShape = schemaext.ObjectShape{Name: "composite type",
		Allowed: []string{"attributes", "comment", "struct_name"}, Required: []string{"attributes"},
		NonEmpty: []string{"attributes", "comment", "struct_name"}}
	observedCompositeShape = schemaext.ObjectShape{Name: "composite type",
		Allowed: []string{"attributes", "comment"}, Required: []string{"attributes"}, NonEmpty: []string{"attributes", "comment"}}
	attributeShape = schemaext.ObjectShape{Name: "composite type attribute",
		Allowed: []string{"name", "type"}, Required: []string{"name", "type"}}
	desiredRangeShape = schemaext.ObjectShape{Name: "range type",
		Allowed:  []string{"subtype", "subtype_opclass", "collation", "canonical", "subtype_diff", "cleared", "comment", "struct_name"},
		Required: []string{"subtype"},
		NonEmpty: []string{"subtype", "subtype_opclass", "collation", "canonical", "subtype_diff", "cleared", "comment", "struct_name"}}
	observedRangeShape = schemaext.ObjectShape{Name: "range type",
		Allowed:  []string{"subtype", "subtype_opclass", "collation", "canonical", "subtype_diff", "comment"},
		Required: []string{"subtype"},
		NonEmpty: []string{"subtype", "subtype_opclass", "collation", "canonical", "subtype_diff", "comment"}}
)

// Codecs returns the version-one desired and observed codecs of the three
// models, a domain's first, then a composite type's and a range type's, each
// desired before observed. Each call returns independent definitions. The
// codecs validate representation invariants; they resolve no defaults and
// claim no support. A decoder accepts only the spelling the encoder writes,
// so an omitted value written out, such as an empty comment, is refused. A
// range type's cleared attributes are encoded sorted. Every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredDomain]{
			Prototype: &DesiredDomain{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredDomainDefinition),
			Shape: desiredDomainWire, Validate: ValidateDesiredDomain,
		}.Codec(),
		schemaext.ModelCodec[*ObservedDomain]{
			Prototype: &ObservedDomain{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedDomainDefinition),
			Shape: observedDomainWire, Validate: ValidateObservedDomain,
		}.Codec(),
		schemaext.ModelCodec[*DesiredComposite]{
			Prototype: &DesiredComposite{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredCompositeDefinition),
			Shape: compositeWire(desiredCompositeShape), Validate: ValidateDesiredComposite,
		}.Codec(),
		schemaext.ModelCodec[*ObservedComposite]{
			Prototype: &ObservedComposite{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedCompositeDefinition),
			Shape: compositeWire(observedCompositeShape), Validate: ValidateObservedComposite,
		}.Codec(),
		schemaext.ModelCodec[*DesiredRange]{
			Prototype: &DesiredRange{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredRangeDefinition),
			Shape: objectWire(desiredRangeShape), Validate: ValidateDesiredRange, Canonical: canonicalDesiredRange,
		}.Codec(),
		schemaext.ModelCodec[*ObservedRange]{
			Prototype: &ObservedRange{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedRangeDefinition),
			Shape: objectWire(observedRangeShape), Validate: ValidateObservedRange,
		}.Codec(),
	}
}

func objectWire(shape schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, shape)
		return err
	}
}

func desiredDomainWire(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, desiredDomainShape)
	if err != nil {
		return err
	}
	if normalized, found := fields["normalized"]; found {
		_, err = schemaext.DecodeObject(normalized, normalizedDomainShape)
	}
	return err
}

func observedDomainWire(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, observedDomainShape)
	if err != nil {
		return err
	}
	return eachObject(fields["checks"], domainCheckShape)
}

func compositeWire(shape schemaext.ObjectShape) func(json.RawMessage) error {
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

func canonicalDesiredRange(value *DesiredRange) *DesiredRange {
	canonical := value.Copy()
	canonical.Cleared = sortedNames(canonical.Cleared)
	if len(canonical.Cleared) == 0 {
		canonical.Cleared = nil
	}
	return canonical
}
