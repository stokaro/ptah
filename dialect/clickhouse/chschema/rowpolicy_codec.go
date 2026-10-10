package chschema

import (
	_ "embed" // Embed the exact definition bound to row policies.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed rowpolicy-codecs.json
var rowPolicyDefinition []byte

// RowPolicyWireDefinition returns an independent description of both row
// policy models.
func RowPolicyWireDefinition() json.RawMessage { return slices.Clone(rowPolicyDefinition) }

// RowPolicyCodecs returns strict versioned codecs for desired and observed row
// policies, in that order. Both encode each role list in byte order, so equal
// policies encode to the same bytes. Registration understands the model but
// grants no target or server support.
func RowPolicyCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		rowPolicyCodec(&DesiredRowPolicy{}, schemaext.Desired, decodeDesiredRowPolicy, validateDesiredRowPolicyPayload, canonicalDesiredRowPolicy),
		rowPolicyCodec(&ObservedRowPolicy{}, schemaext.Observed, decodeObservedRowPolicy, validateObservedRowPolicyPayload, canonicalObservedRowPolicy),
	}
}

// RowPolicyCoverage records knowledge of row policies for one representation:
// the whole kind, and any policy or table that departs from it. Known absence
// needs complete enumeration, so a read that could not see every policy of a
// database claims less than complete knowledge for what it could not see.
func RowPolicyCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range RowPolicyCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: "ptah.run/clickhouse", Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == RowPolicyKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: row policy coverage requires a schema representation", schemaext.ErrInvalidValue)
}

// rowPolicyCodec validates at every boundary, so a clone, an encoding and a
// decoding of an invalid value are refused, and encodes the canonical form.
func rowPolicyCodec[T schemaext.Value](prototype T, representation schemaext.Representation,
	decode func(json.RawMessage) (schemaext.Payload, error), validate func(schemaext.Payload) error, canonical func(T) T,
) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		if err := validate(payload); err != nil {
			return nil, err
		}
		value, _ := payload.(T)
		return json.Marshal(canonical(value))
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: RowPolicyWireDefinition(),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if err := validate(payload); err != nil {
				return nil, err
			}
			value, _ := payload.(T)
			return value.Clone(), nil
		},
		Encode: encode, Decode: decode, Canonical: encode,
	}
}

func canonicalDesiredRowPolicy(value *DesiredRowPolicy) *DesiredRowPolicy {
	canonical := value.Copy()
	canonical.Roles = canonical.Roles.canonical()
	return canonical
}

func canonicalObservedRowPolicy(value *ObservedRowPolicy) *ObservedRowPolicy {
	canonical := value.Copy()
	canonical.Roles = canonical.Roles.canonical()
	return canonical
}

func validateDesiredRowPolicyPayload(payload schemaext.Payload) error {
	v, ok := payload.(*DesiredRowPolicy)
	if !ok {
		return fmt.Errorf("%w: expected a desired ClickHouse row policy, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateDesiredRowPolicy(v)
}

func validateObservedRowPolicyPayload(payload schemaext.Payload) error {
	v, ok := payload.(*ObservedRowPolicy)
	if !ok {
		return fmt.Errorf("%w: expected an observed ClickHouse row policy, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateObservedRowPolicy(v)
}

func decodeDesiredRowPolicy(data json.RawMessage) (schemaext.Payload, error) {
	selection, err := rowPolicyShape(data, []string{"filter", "composition", "roles", "struct_name", "normalized_filter"}, nil)
	if err != nil {
		return nil, err
	}
	// A declaration that names nobody omits its roles: the encoder never
	// writes an empty selection for one.
	if selection != nil && len(selection) == 0 {
		return nil, fmt.Errorf("%w: ClickHouse row policy property %q cannot be empty; omit it for a policy that applies to nobody",
			schemaext.ErrInvalidValue, "roles")
	}
	value, err := schemaext.DecodeJSON[*DesiredRowPolicy](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesiredRowPolicy(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedRowPolicy(data json.RawMessage) (schemaext.Payload, error) {
	// An observation always states its selection, so one that names nobody is
	// written as an empty object.
	if _, err := rowPolicyShape(data, []string{"filter", "composition", "roles"}, []string{"composition", "roles"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*ObservedRowPolicy](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObservedRowPolicy(value); err != nil {
		return nil, err
	}
	return value, nil
}

// rowPolicyShape checks the policy object's keys and the role selection's, and
// refuses the spellings encoding/json would read as an omitted value: an
// empty string, an empty list, and all set to false. The encoder omits each of
// them, so none can be what the definition calls a value. It returns the
// selection's keys, nil when the policy has no roles property, so a caller can
// tell an empty selection from an absent one.
func rowPolicyShape(data json.RawMessage, allowed, required []string) (map[string]json.RawMessage, error) {
	fields, err := wireObject(data, "row policy", allowed, required)
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"filter", "composition", "struct_name", "normalized_filter"} {
		if string(fields[key]) == `""` {
			return nil, fmt.Errorf("%w: ClickHouse row policy property %q cannot be empty; omit it instead", schemaext.ErrInvalidValue, key)
		}
	}
	roles, found := fields["roles"]
	if !found {
		return nil, nil
	}
	selection, err := wireObject(roles, "row policy roles", []string{"all", "names", "except"}, nil)
	if err != nil {
		return nil, err
	}
	if string(selection["all"]) == "false" {
		return nil, fmt.Errorf("%w: ClickHouse row policy roles property %q is true or omitted", schemaext.ErrInvalidValue, "all")
	}
	for _, key := range []string{"names", "except"} {
		if string(selection[key]) == "[]" {
			return nil, fmt.Errorf("%w: ClickHouse row policy roles property %q cannot be empty; omit it instead", schemaext.ErrInvalidValue, key)
		}
	}
	return selection, nil
}
