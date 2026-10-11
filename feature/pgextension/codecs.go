package pgextension

import (
	"encoding/json"

	"ptah.run/core/schemaext"
)

const (
	desiredDefinition = `{"type":"object","additionalProperties":false,"properties":{` +
		`"schema":{"type":"string","minLength":1},"version":{"type":"string","minLength":1},` +
		`"comment":{"type":"string","minLength":1},"if_not_exists":{"const":true},` +
		`"provides":{"type":"array","minItems":1,"uniqueItems":true,"items":{"type":"string","minLength":1}}}}`
	observedDefinition = `{"type":"object","required":["schema","version","relocatable"],"additionalProperties":false,"properties":{` +
		`"schema":{"type":"string"},"version":{"type":"string"},"relocatable":{"type":"boolean"},` +
		`"comment":{"type":"string"},"provides":{"type":"array","minItems":1,"uniqueItems":true,"items":{"type":"string","minLength":1}}}}`
)

// The wire shapes. A key the encoder leaves out while it is empty is
// NonEmpty, unless it holds a pointer, whose empty value is a value.
var (
	desiredShape = schemaext.ObjectShape{Name: "extension",
		Allowed:  []string{"schema", "version", "comment", "if_not_exists", "provides"},
		NonEmpty: []string{"schema", "version", "comment", "if_not_exists", "provides"}}
	observedShape = schemaext.ObjectShape{Name: "extension",
		Allowed:  []string{"schema", "version", "relocatable", "comment", "provides"},
		Required: []string{"schema", "version", "relocatable"}, NonEmpty: []string{"provides"}}
)

// Codecs returns the version-one desired and observed codecs of the
// extension model, in that order. Each call returns independent definitions.
// The codecs validate representation invariants; they resolve no defaults and
// claim no support. A decoder accepts only the spelling the encoder writes,
// so an omitted value written out, such as an empty version, is refused. The
// codecs write the members sorted and once each. Every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredExtension]{
			Prototype: &DesiredExtension{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredDefinition),
			Shape: shape(desiredShape), Validate: ValidateDesired, Canonical: canonicalDesired,
		}.Codec(),
		schemaext.ModelCodec[*ObservedExtension]{
			Prototype: &ObservedExtension{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedDefinition),
			Shape: shape(observedShape), Validate: ValidateObserved, Canonical: canonicalObserved,
		}.Codec(),
	}
}

func shape(object schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, object)
		return err
	}
}

func canonicalObserved(value *ObservedExtension) *ObservedExtension {
	canonical := value.Copy()
	canonical.Provides = sortedMembers(canonical.Provides)
	if len(canonical.Provides) == 0 {
		canonical.Provides = nil
	}
	return canonical
}

func canonicalDesired(value *DesiredExtension) *DesiredExtension {
	canonical := value.Copy()
	canonical.Provides = sortedMembers(canonical.Provides)
	if len(canonical.Provides) == 0 {
		canonical.Provides = nil
	}
	return canonical
}
