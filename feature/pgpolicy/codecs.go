package pgpolicy

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

const (
	commandDefinition     = `{"enum":["ALL","SELECT","INSERT","UPDATE","DELETE"]}`
	compositionDefinition = `{"enum":["permissive","restrictive"]}`
	expressionDefinition  = `{"type":"string","minLength":1}`

	desiredPolicyDefinition = `{"type":"object","additionalProperties":false,"properties":{` +
		`"command":` + commandDefinition + `,` +
		`"roles":{"type":"array","minItems":1,"items":{"type":"object","additionalProperties":false,"minProperties":1,"maxProperties":1,"properties":{` +
		`"keyword":{"enum":["PUBLIC","CURRENT_ROLE","CURRENT_USER","SESSION_USER"]},"name":{"type":"string","minLength":1}}}},` +
		`"using":` + expressionDefinition + `,"with_check":` + expressionDefinition + `,` +
		`"composition":` + compositionDefinition + `,"comment":{"type":"string"},"struct_name":{"type":"string"}}}`
	observedPolicyDefinition = `{"type":"object","required":["command","roles","composition"],"additionalProperties":false,"properties":{` +
		`"command":` + commandDefinition + `,` +
		`"roles":{"type":"array","minItems":1,"items":{"type":"object","additionalProperties":false,"minProperties":1,"maxProperties":1,"properties":{` +
		`"keyword":{"enum":["PUBLIC"]},"name":{"type":"string","minLength":1}}}},` +
		`"using":` + expressionDefinition + `,"with_check":` + expressionDefinition + `,` +
		`"composition":` + compositionDefinition + `,"comment":{"type":"string"}}}`
	desiredTableStateDefinition = `{"type":"object","required":["enabled","forced"],"additionalProperties":false,"properties":{` +
		`"enabled":{"type":"boolean"},"forced":{"type":"boolean"},"comment":{"type":"string"},"struct_name":{"type":"string"}}}`
	observedTableStateDefinition = `{"type":"object","required":["enabled","forced"],"additionalProperties":false,"properties":{` +
		`"enabled":{"type":"boolean"},"forced":{"type":"boolean"}}}`
)

// Codecs returns the version-one desired and observed codecs of both models.
// Each call returns independent definitions. The codecs validate
// representation invariants; they resolve no defaults and claim no support.
func Codecs() []schemaext.Codec {
	return append(PolicyCodecs(), TableStateCodecs()...)
}

// PolicyCodecs returns the desired and observed policy codecs, in that order.
// Both encode a policy's role list in its canonical order, keywords before
// names, so equal policies encode to the same bytes.
func PolicyCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredPolicy{}, schemaext.Desired, desiredPolicyDefinition, ValidateDesiredPolicy, canonicalDesiredPolicy, desiredPolicyShape),
		modelCodec(&ObservedPolicy{}, schemaext.Observed, observedPolicyDefinition, ValidateObservedPolicy, canonicalObservedPolicy, observedPolicyShape),
	}
}

// TableStateCodecs returns the desired and observed table-state codecs, in
// that order.
func TableStateCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredTableState{}, schemaext.Desired, desiredTableStateDefinition, ValidateDesiredTableState, identity[*DesiredTableState], desiredTableStateShape),
		modelCodec(&ObservedTableState{}, schemaext.Observed, observedTableStateDefinition, ValidateObservedTableState, identity[*ObservedTableState], observedTableStateShape),
	}
}

// model is the payload type a codec owns.
type model interface {
	schemaext.Value
}

// modelCodec builds a codec for one model. Every boundary validates first:
// a clone, an encoding and a decoding of an invalid value are refused, so no
// codec passes one on. canonical returns the value as it is encoded, which
// orders a role list.
func modelCodec[T model](prototype T, representation schemaext.Representation, definition string,
	validate func(T) error, canonical func(T) T, shape func(json.RawMessage) error,
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
			if err := shape(data); err != nil {
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

func identity[T any](value T) T { return value }

func canonicalDesiredPolicy(value *DesiredPolicy) *DesiredPolicy {
	canonical := value.Copy()
	canonical.Roles = sortedRoles(canonical.Roles)
	return canonical
}

func canonicalObservedPolicy(value *ObservedPolicy) *ObservedPolicy {
	canonical := value.Copy()
	canonical.Roles = sortedRoles(canonical.Roles)
	return canonical
}

func desiredPolicyShape(data json.RawMessage) error {
	return policyShape(data, schemaext.ObjectShape{Name: "policy",
		Allowed: []string{"command", "roles", "using", "with_check", "composition", "comment", "struct_name"}})
}

func observedPolicyShape(data json.RawMessage) error {
	return policyShape(data, schemaext.ObjectShape{Name: "policy",
		Allowed:  []string{"command", "roles", "using", "with_check", "composition", "comment"},
		Required: []string{"command", "roles", "composition"}})
}

// policyShape checks the policy object's keys and each role selector's, and
// that no present value is empty. Whether a selector holds a keyword or a name
// and which one is the validators'.
func policyShape(data json.RawMessage, shape schemaext.ObjectShape) error {
	fields, err := decodeObject(data, shape, "command", "composition")
	if err != nil {
		return err
	}
	roles, found := fields["roles"]
	if !found {
		return nil
	}
	selectors, err := schemaext.DecodeJSON[[]json.RawMessage](roles)
	if err != nil {
		return err
	}
	for _, selector := range selectors {
		if _, err := decodeObject(selector, schemaext.ObjectShape{Name: "role selector", Allowed: []string{"keyword", "name"}}, "keyword", "name"); err != nil {
			return err
		}
	}
	return nil
}

func desiredTableStateShape(data json.RawMessage) error {
	_, err := decodeObject(data, schemaext.ObjectShape{Name: "table state",
		Allowed: []string{"enabled", "forced", "comment", "struct_name"}, Required: []string{"enabled", "forced"}})
	return err
}

func observedTableStateShape(data json.RawMessage) error {
	_, err := decodeObject(data, schemaext.ObjectShape{Name: "table state",
		Allowed: []string{"enabled", "forced"}, Required: []string{"enabled", "forced"}})
	return err
}
