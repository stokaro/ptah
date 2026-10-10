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
		modelCodec(&DesiredPolicy{}, schemaext.Desired, desiredPolicyDefinition, encodeDesiredPolicy, decodeDesiredPolicy),
		modelCodec(&ObservedPolicy{}, schemaext.Observed, observedPolicyDefinition, encodeObservedPolicy, decodeObservedPolicy),
	}
}

// TableStateCodecs returns the desired and observed table-state codecs, in
// that order.
func TableStateCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredTableState{}, schemaext.Desired, desiredTableStateDefinition, encodeDesiredTableState, decodeDesiredTableState),
		modelCodec(&ObservedTableState{}, schemaext.Observed, observedTableStateDefinition, encodeObservedTableState, decodeObservedTableState),
	}
}

// modelCodec builds a codec whose clone and encodings validate the payload
// first, so no codec boundary passes an invalid value.
func modelCodec(prototype schemaext.Value, representation schemaext.Representation, definition string,
	encode func(schemaext.Payload) (json.RawMessage, error), decode func(json.RawMessage) (schemaext.Payload, error),
) schemaext.Codec {
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: json.RawMessage(definition),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if _, err := encode(payload); err != nil {
				return nil, err
			}
			value, ok := payload.(schemaext.Value)
			if !ok {
				return nil, fmt.Errorf("%w: expected a row-security value, got %T", schemaext.ErrInvalidValue, payload)
			}
			return schemaext.CloneValue(value)
		},
		Encode: encode, Canonical: encode, Decode: decode,
	}
}

func encodeDesiredPolicy(payload schemaext.Payload) (json.RawMessage, error) {
	value, ok := payload.(*DesiredPolicy)
	if !ok {
		return nil, fmt.Errorf("%w: expected a desired policy, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateDesiredPolicy(value); err != nil {
		return nil, err
	}
	canonical := value.Copy()
	canonical.Roles = sortedRoles(canonical.Roles)
	return json.Marshal(canonical)
}

func encodeObservedPolicy(payload schemaext.Payload) (json.RawMessage, error) {
	value, ok := payload.(*ObservedPolicy)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed policy, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateObservedPolicy(value); err != nil {
		return nil, err
	}
	canonical := value.Copy()
	canonical.Roles = sortedRoles(canonical.Roles)
	return json.Marshal(canonical)
}

func encodeDesiredTableState(payload schemaext.Payload) (json.RawMessage, error) {
	value, ok := payload.(*DesiredTableState)
	if !ok {
		return nil, fmt.Errorf("%w: expected a desired row-security table state, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateDesiredTableState(value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func encodeObservedTableState(payload schemaext.Payload) (json.RawMessage, error) {
	value, ok := payload.(*ObservedTableState)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed row-security table state, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateObservedTableState(value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

var (
	desiredPolicyKeys  = []string{"command", "roles", "using", "with_check", "composition", "comment", "struct_name"}
	observedPolicyKeys = []string{"command", "roles", "using", "with_check", "composition", "comment"}
)

func decodeDesiredPolicy(data json.RawMessage) (schemaext.Payload, error) {
	if err := policyWire(data, desiredPolicyKeys, nil); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*DesiredPolicy](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesiredPolicy(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedPolicy(data json.RawMessage) (schemaext.Payload, error) {
	if err := policyWire(data, observedPolicyKeys, []string{"command", "roles", "composition"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*ObservedPolicy](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObservedPolicy(value); err != nil {
		return nil, err
	}
	return value, nil
}

// policyWire checks the policy object's keys and each role selector's.
func policyWire(data json.RawMessage, allowed, required []string) error {
	fields, err := wireObject(data, "policy", allowed, required)
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
		if _, err := wireObject(selector, "role selector", []string{"keyword", "name"}, nil); err != nil {
			return err
		}
	}
	return nil
}

func decodeDesiredTableState(data json.RawMessage) (schemaext.Payload, error) {
	if _, err := wireObject(data, "table state", []string{"enabled", "forced", "comment", "struct_name"}, []string{"enabled", "forced"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*DesiredTableState](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesiredTableState(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedTableState(data json.RawMessage) (schemaext.Payload, error) {
	if _, err := wireObject(data, "table state", []string{"enabled", "forced"}, []string{"enabled", "forced"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*ObservedTableState](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObservedTableState(value); err != nil {
		return nil, err
	}
	return value, nil
}
