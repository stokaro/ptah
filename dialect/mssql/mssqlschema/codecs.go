package mssqlschema

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

const (
	nameDefinition = `{"type":"object","required":["schema","name"],"additionalProperties":false,"properties":{` +
		`"schema":{"type":"string","minLength":1},"name":{"type":"string","minLength":1}}}`
	predicateDefinition = `{"type":"object","required":["type","function","table"],"additionalProperties":false,"properties":{` +
		`"type":{"enum":["FILTER","BLOCK"]},"function":` + nameDefinition + `,` +
		`"arguments":{"type":"array","items":{"type":"string","minLength":1}},"table":` + nameDefinition + `,` +
		`"operation":{"enum":["AFTER INSERT","AFTER UPDATE","BEFORE UPDATE","BEFORE DELETE"]}}}`

	desiredDefinition = `{"type":"object","required":["predicates"],"additionalProperties":false,"properties":{` +
		`"predicates":{"type":"array","items":` + predicateDefinition + `},` +
		`"enabled":{"type":"boolean"},"schema_binding":{"type":"boolean"},"not_for_replication":{"type":"boolean"},` +
		`"struct_name":{"type":"string"}}}`
	observedDefinition = `{"type":"object","required":["predicates","enabled","schema_binding","not_for_replication"],` +
		`"additionalProperties":false,"properties":{` +
		`"predicates":{"type":"array","items":` + predicateDefinition + `},` +
		`"enabled":{"type":"boolean"},"schema_binding":{"type":"boolean"},"not_for_replication":{"type":"boolean"}}}`
)

// Codecs returns the version-one desired and observed security policy codecs,
// in that order. Each call returns independent definitions. Both encode the
// predicates in their canonical order, so equal policies encode to the same
// bytes, and a policy with no predicate encodes an empty list rather than
// null. The codecs validate representation invariants; they resolve no
// defaults and claim no support.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredSecurityPolicy{}, schemaext.Desired, desiredDefinition, ValidateDesiredSecurityPolicy, canonicalDesired,
			[]string{"predicates", "enabled", "schema_binding", "not_for_replication", "struct_name"}, []string{"predicates"}),
		modelCodec(&ObservedSecurityPolicy{}, schemaext.Observed, observedDefinition, ValidateObservedSecurityPolicy, canonicalObserved,
			[]string{"predicates", "enabled", "schema_binding", "not_for_replication"},
			[]string{"predicates", "enabled", "schema_binding", "not_for_replication"}),
	}
}

// modelCodec builds a codec for one representation. Every boundary validates
// first: a clone, an encoding and a decoding of an invalid value are refused,
// so no codec passes one on. canonical returns the value as it is encoded.
func modelCodec[T schemaext.Value](prototype T, representation schemaext.Representation, definition string,
	validate func(T) error, canonical func(T) T, allowed, required []string,
) schemaext.Codec {
	validated := func(payload schemaext.Payload) (T, error) {
		value, ok := payload.(T)
		if !ok {
			var zero T
			return zero, fmt.Errorf("%w: expected %T, got %T", schemaext.ErrInvalidValue, prototype, payload)
		}
		return value, validate(value)
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := validated(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(canonical(value))
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: json.RawMessage(definition),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := validated(payload)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			if err := policyShape(data, schemaext.ObjectShape{Name: "policy", Allowed: allowed, Required: required}); err != nil {
				return nil, err
			}
			value, err := schemaext.DecodeJSON[T](data)
			if err != nil {
				return nil, err
			}
			if err := validate(value); err != nil {
				return nil, err
			}
			return value, nil
		},
	}
}

func canonicalDesired(value *DesiredSecurityPolicy) *DesiredSecurityPolicy {
	canonical := value.Copy()
	canonical.Predicates = sortedPredicates(canonical.Predicates)
	if canonical.Predicates == nil {
		canonical.Predicates = make([]Predicate, 0)
	}
	return canonical
}

func canonicalObserved(value *ObservedSecurityPolicy) *ObservedSecurityPolicy {
	canonical := value.Copy()
	canonical.Predicates = sortedPredicates(canonical.Predicates)
	if canonical.Predicates == nil {
		canonical.Predicates = make([]Predicate, 0)
	}
	return canonical
}

// policyShape checks the policy object's keys, each predicate's and each
// name's, and that no present string value is empty. Which type, operation and
// names a predicate holds is the validators'.
func policyShape(data json.RawMessage, shape schemaext.ObjectShape) error {
	fields, err := decodeObject(data, shape)
	if err != nil {
		return err
	}
	predicates, err := schemaext.DecodeJSON[[]json.RawMessage](fields["predicates"])
	if err != nil {
		return err
	}
	for _, raw := range predicates {
		predicate, err := decodeObject(raw, schemaext.ObjectShape{Name: "predicate",
			Allowed: []string{"type", "function", "arguments", "table", "operation"}, Required: []string{"type", "function", "table"}},
			"type", "operation")
		if err != nil {
			return err
		}
		for _, key := range []string{"function", "table"} {
			if _, err := decodeObject(predicate[key], schemaext.ObjectShape{Name: "predicate " + key,
				Allowed: []string{"schema", "name"}, Required: []string{"schema", "name"}}, "schema", "name"); err != nil {
				return err
			}
		}
	}
	return nil
}
