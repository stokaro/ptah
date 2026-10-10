package pgpolicy

import (
	"encoding/json"

	"ptah.run/core/schemaext"
)

const (
	commandDefinition       = `{"enum":["ALL","SELECT","INSERT","UPDATE","DELETE"]}`
	compositionDefinition   = `{"enum":["permissive","restrictive"]}`
	expressionDefinition    = `{"type":"string","minLength":1}`
	observedRolesDefinition = `{"type":"array","minItems":1,"items":{"type":"object","additionalProperties":false,"minProperties":1,"maxProperties":1,"properties":{` +
		`"keyword":{"enum":["PUBLIC"]},"name":{"type":"string","minLength":1}}}}`
	normalizedDefinition = `{"type":"object","required":["roles"],"additionalProperties":false,"properties":{` +
		`"roles":` + observedRolesDefinition + `,"using":` + expressionDefinition + `,"with_check":` + expressionDefinition + `}}`

	desiredPolicyDefinition = `{"type":"object","additionalProperties":false,"properties":{` +
		`"command":` + commandDefinition + `,` +
		`"roles":{"type":"array","minItems":1,"items":{"type":"object","additionalProperties":false,"minProperties":1,"maxProperties":1,"properties":{` +
		`"keyword":{"enum":["PUBLIC","CURRENT_ROLE","CURRENT_USER","SESSION_USER"]},"name":{"type":"string","minLength":1}}}},` +
		`"using":` + expressionDefinition + `,"with_check":` + expressionDefinition + `,` +
		`"composition":` + compositionDefinition + `,"comment":{"type":"string"},"struct_name":{"type":"string"},` +
		`"normalized":` + normalizedDefinition + `}}`
	observedPolicyDefinition = `{"type":"object","required":["command","roles","composition"],"additionalProperties":false,"properties":{` +
		`"command":` + commandDefinition + `,` +
		`"roles":` + observedRolesDefinition + `,` +
		`"using":` + expressionDefinition + `,"with_check":` + expressionDefinition + `,` +
		`"composition":` + compositionDefinition + `,"comment":{"type":"string"}}}`
	desiredTableStateDefinition = `{"type":"object","required":["enabled","forced"],"additionalProperties":false,"properties":{` +
		`"enabled":{"type":"boolean"},"forced":{"type":"boolean"},"comment":{"type":"string"},"struct_name":{"type":"string"}}}`
	observedTableStateDefinition = `{"type":"object","required":["enabled","forced"],"additionalProperties":false,"properties":{` +
		`"enabled":{"type":"boolean"},"forced":{"type":"boolean"}}}`
)

// Codecs returns the version-one desired and observed codecs of both models.
// Each call returns independent definitions. The codecs validate
// representation invariants; they resolve no defaults and claim no support. A
// decoder accepts only the spelling the encoder writes, so an omitted value
// written out, such as an empty comment, is refused. Every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
func Codecs() []schemaext.Codec {
	return append(PolicyCodecs(), TableStateCodecs()...)
}

// PolicyCodecs returns the desired and observed policy codecs, in that order.
// Both encode a policy's role list in its canonical order, keywords before
// names, so equal policies encode to the same bytes.
func PolicyCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredPolicy]{
			Prototype: &DesiredPolicy{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredPolicyDefinition),
			Shape: desiredPolicyShape, Validate: ValidateDesiredPolicy, Canonical: canonicalDesiredPolicy,
		}.Codec(),
		schemaext.ModelCodec[*ObservedPolicy]{
			Prototype: &ObservedPolicy{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedPolicyDefinition),
			Shape: observedPolicyShape, Validate: ValidateObservedPolicy, Canonical: canonicalObservedPolicy,
		}.Codec(),
	}
}

// TableStateCodecs returns the desired and observed table-state codecs, in
// that order.
func TableStateCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredTableState]{
			Prototype: &DesiredTableState{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(desiredTableStateDefinition),
			Shape: objectShape(desiredTableStateShape), Validate: ValidateDesiredTableState,
		}.Codec(),
		schemaext.ModelCodec[*ObservedTableState]{
			Prototype: &ObservedTableState{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(observedTableStateDefinition),
			Shape: objectShape(observedTableStateShape), Validate: ValidateObservedTableState,
		}.Codec(),
	}
}

// The wire shapes. A key the encoder leaves out while it is empty is NonEmpty,
// unless it holds a pointer, whose empty value is a value.
var (
	desiredPolicyShapeOf = schemaext.ObjectShape{Name: "policy",
		Allowed:  []string{"command", "roles", "using", "with_check", "composition", "comment", "struct_name", "normalized"},
		NonEmpty: []string{"command", "roles", "composition", "comment", "struct_name"}}
	observedPolicyShapeOf = schemaext.ObjectShape{Name: "policy",
		Allowed:  []string{"command", "roles", "using", "with_check", "composition", "comment"},
		Required: []string{"command", "roles", "composition"}, NonEmpty: []string{"comment"}}
	normalizedPolicyShape = schemaext.ObjectShape{Name: "normalized policy",
		Allowed: []string{"roles", "using", "with_check"}, Required: []string{"roles"}}
	roleSelectorShape = schemaext.ObjectShape{Name: "role selector",
		Allowed: []string{"keyword", "name"}, NonEmpty: []string{"keyword", "name"}}
	desiredTableStateShape = schemaext.ObjectShape{Name: "table state",
		Allowed: []string{"enabled", "forced", "comment", "struct_name"}, Required: []string{"enabled", "forced"},
		NonEmpty: []string{"comment", "struct_name"}}
	observedTableStateShape = schemaext.ObjectShape{Name: "table state",
		Allowed: []string{"enabled", "forced"}, Required: []string{"enabled", "forced"}}
)

func objectShape(shape schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, shape)
		return err
	}
}

func canonicalDesiredPolicy(value *DesiredPolicy) *DesiredPolicy {
	canonical := value.Copy()
	canonical.Roles = sortedRoles(canonical.Roles)
	if canonical.Normalized != nil {
		canonical.Normalized.Roles = sortedRoles(canonical.Normalized.Roles)
	}
	return canonical
}

func canonicalObservedPolicy(value *ObservedPolicy) *ObservedPolicy {
	canonical := value.Copy()
	canonical.Roles = sortedRoles(canonical.Roles)
	return canonical
}

func desiredPolicyShape(data json.RawMessage) error {
	fields, err := policyShape(data, desiredPolicyShapeOf)
	if err != nil {
		return err
	}
	normalized, found := fields["normalized"]
	if !found {
		return nil
	}
	_, err = policyShape(normalized, normalizedPolicyShape)
	return err
}

func observedPolicyShape(data json.RawMessage) error {
	_, err := policyShape(data, observedPolicyShapeOf)
	return err
}

// policyShape checks the policy object's keys and each role selector's. Whether
// a selector holds a keyword or a name and which one is the validators'.
func policyShape(data json.RawMessage, shape schemaext.ObjectShape) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeObject(data, shape)
	if err != nil {
		return nil, err
	}
	roles, found := fields["roles"]
	if !found {
		return fields, nil
	}
	selectors, err := schemaext.DecodeJSON[[]json.RawMessage](roles)
	if err != nil {
		return nil, err
	}
	for _, selector := range selectors {
		if _, err := schemaext.DecodeObject(selector, roleSelectorShape); err != nil {
			return nil, err
		}
	}
	return fields, nil
}
