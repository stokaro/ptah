package mssqlschema

import (
	"encoding/json"
	"reflect"
	"strings"

	"ptah.run/core/schemaext"
)

const (
	// text is a string holding something besides white space: SQL Server
	// compares names without their trailing spaces, so a blank name equals
	// the empty one, and a blank argument is no expression.
	text           = `{"type":"string","pattern":"\\S"}`
	nameDefinition = `{"type":"object","required":["schema","name"],"additionalProperties":false,"properties":{` +
		`"schema":` + text + `,"name":` + text + `}}`
	predicateDefinition = `{"type":"object","required":["type","function","table"],"additionalProperties":false,"properties":{` +
		`"type":{"enum":["FILTER","BLOCK"]},"function":` + nameDefinition + `,` +
		`"arguments":{"type":"array","minItems":1,"items":` + text + `},"table":` + nameDefinition + `,` +
		`"operation":{"enum":["AFTER INSERT","AFTER UPDATE","BEFORE UPDATE","BEFORE DELETE"]}}}`

	desiredDefinition = `{"type":"object","required":["predicates"],"additionalProperties":false,"properties":{` +
		`"predicates":{"type":"array","items":` + predicateDefinition + `},` +
		`"enabled":{"type":"boolean"},"schema_binding":{"type":"boolean"},"not_for_replication":{"const":true},` +
		`"struct_name":{"type":"string","minLength":1}}}`
	observedDefinition = `{"type":"object","required":["predicates","enabled","schema_binding","not_for_replication"],` +
		`"additionalProperties":false,"properties":{` +
		`"predicates":{"type":"array","items":` + predicateDefinition + `},` +
		`"enabled":{"type":"boolean"},"schema_binding":{"type":"boolean"},"not_for_replication":{"type":"boolean"}}}`
)

// The wire shapes come from the JSON tags of the model types, which are what
// the encoder writes, so the keys are spelled in one place.
var (
	desiredShape   = wireShape[DesiredSecurityPolicy]("policy")
	observedShape  = wireShape[ObservedSecurityPolicy]("policy")
	predicateShape = wireShape[Predicate]("predicate")
	nameShape      = wireShape[ObjectName]("name")
)

// Codecs returns the version-one desired and observed security policy codecs,
// in that order. Each call returns independent definitions. Both encode with
// the types' MarshalJSON, so equal policies encode to the same bytes. A decoder accepts only the spelling the encoder writes: an omitted
// value written out, such as an empty struct name or argument list, is
// refused. Every refusal is a [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue]. The codecs validate representation invariants;
// they resolve no defaults and claim no support.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredSecurityPolicy]{
			Prototype: &DesiredSecurityPolicy{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredDefinition),
			Shape: policyShape(desiredShape), Validate: ValidateDesiredSecurityPolicy,
		}.Codec(),
		schemaext.ModelCodec[*ObservedSecurityPolicy]{
			Prototype: &ObservedSecurityPolicy{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedDefinition),
			Shape: policyShape(observedShape), Validate: ValidateObservedSecurityPolicy,
		}.Codec(),
	}
}

// MarshalJSON writes the canonical encoding: the predicates in their canonical
// order, and no predicate as an empty list rather than null. Equal policies
// encode to the same bytes, alone or inside a change or an operation. It does
// not validate; the codecs do.
func (v DesiredSecurityPolicy) MarshalJSON() ([]byte, error) {
	type wire DesiredSecurityPolicy
	return json.Marshal(wire(*canonicalDesired(&v)))
}

// MarshalJSON writes the canonical encoding, as
// [DesiredSecurityPolicy.MarshalJSON] does.
func (v ObservedSecurityPolicy) MarshalJSON() ([]byte, error) {
	type wire ObservedSecurityPolicy
	return json.Marshal(wire(*canonicalObserved(&v)))
}

func canonicalDesired(value *DesiredSecurityPolicy) *DesiredSecurityPolicy {
	canonical := value.Copy()
	canonical.Predicates = canonicalPredicates(canonical.Predicates)
	return canonical
}

func canonicalObserved(value *ObservedSecurityPolicy) *ObservedSecurityPolicy {
	canonical := value.Copy()
	canonical.Predicates = canonicalPredicates(canonical.Predicates)
	return canonical
}

// canonicalPredicates orders an owned copy in place and spells no predicate
// as an empty list, which both definitions require.
func canonicalPredicates(predicates []Predicate) []Predicate {
	if predicates == nil {
		return make([]Predicate, 0)
	}
	sortPredicates(predicates)
	return predicates
}

// policyShape checks the policy object's keys, each predicate's and each
// name's. Which type, operation and names a predicate holds is the
// validators'.
func policyShape(shape schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, shape)
		if err != nil {
			return err
		}
		predicates, err := schemaext.DecodeJSON[[]json.RawMessage](fields["predicates"])
		if err != nil {
			return err
		}
		for _, raw := range predicates {
			predicate, err := schemaext.DecodeObject(raw, predicateShape)
			if err != nil {
				return err
			}
			for _, key := range []string{"function", "table"} {
				name := nameShape
				name.Name = "predicate " + key
				if _, err := schemaext.DecodeObject(predicate[key], name); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// wireShape derives the strict shape of T's JSON object from its tags. Every
// tagged field is allowed. One without omitempty is required. One with
// omitempty is NonEmpty unless it is a pointer: its encoder never writes its
// empty value, while a pointer's false is a value.
func wireShape[T any](name string) schemaext.ObjectShape {
	shape := schemaext.ObjectShape{Name: name}
	for field := range reflect.TypeFor[T]().Fields() {
		key, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		shape.Allowed = append(shape.Allowed, key)
		switch {
		case options != "omitempty":
			shape.Required = append(shape.Required, key)
		case field.Type.Kind() != reflect.Pointer:
			shape.NonEmpty = append(shape.NonEmpty, key)
		}
	}
	return shape
}
