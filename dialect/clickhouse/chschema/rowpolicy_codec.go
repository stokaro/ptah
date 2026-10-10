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
// policies, in that order. Both encode each role list in byte order, through
// [RoleSelection.MarshalJSON], so equal policies encode to the same bytes,
// alone or inside a change or an operation. Every refusal is a
// [schemaext.InvalidModelError]. Registration understands the model but grants
// no target or server support.
func RowPolicyCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredRowPolicy]{
			Prototype: &DesiredRowPolicy{}, Representation: schemaext.Desired, Version: 1, Definition: RowPolicyWireDefinition(),
			Shape: desiredRowPolicyShape, Validate: ValidateDesiredRowPolicy,
		}.Codec(),
		schemaext.ModelCodec[*ObservedRowPolicy]{
			Prototype: &ObservedRowPolicy{}, Representation: schemaext.Observed, Version: 1, Definition: RowPolicyWireDefinition(),
			Shape: observedRowPolicyShape, Validate: ValidateObservedRowPolicy,
		}.Codec(),
	}
}

// MarshalJSON writes the canonical encoding: each role list in byte order.
// It does not validate; the codecs do.
func (s RoleSelection) MarshalJSON() ([]byte, error) {
	type wire RoleSelection
	return json.Marshal(wire(s.canonical()))
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

func desiredRowPolicyShape(data json.RawMessage) error {
	selection, err := rowPolicyShape(data, []string{"filter", "composition", "roles", "struct_name", "normalized_filter"}, nil)
	if err != nil {
		return err
	}
	// A declaration that names nobody omits its roles: the encoder never
	// writes an empty selection for one.
	if selection != nil && len(selection) == 0 {
		return fmt.Errorf("%w: ClickHouse row policy property %q cannot be empty; omit it for a policy that applies to nobody",
			schemaext.ErrInvalidValue, "roles")
	}
	return nil
}

// observedRowPolicyShape checks an observation, which always states its
// selection, so one that names nobody is written as an empty object.
func observedRowPolicyShape(data json.RawMessage) error {
	_, err := rowPolicyShape(data, []string{"filter", "composition", "roles"}, []string{"composition", "roles"})
	return err
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
