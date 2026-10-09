package schemaext_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"ptah.run/core/schemaext"
)

type retention struct {
	Days uint32 `json:"days"`
}

// ExampleRegistry_DecodeCoverageHeader shows that a source without a feature
// account establishes no namespace authority and needs no generated directive.
func ExampleRegistry_DecodeCoverageHeader() {
	var registry schemaext.Registry
	known, found, err := registry.DecodeCoverageHeader(context.Background(), schemaext.Desired, `schema "main" {}`)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(found, known.IsZero())
	header, err := registry.EncodeCoverageHeader(context.Background(), schemaext.Desired, known)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%q\n", header)
	// Output:
	// false true
	// ""
}

func (*retention) Kind() schemaext.Kind     { return "example.org/retention" }
func (v *retention) Clone() schemaext.Value { return &retention{Days: v.Days} }
func (v *retention) Equal(other schemaext.Value) bool {
	w, ok := other.(*retention)
	return ok && v.Days == w.Days
}

// ExampleNewFacets demonstrates typed lookup and snapshot ownership. Changing
// the input or a returned value cannot change the stored declaration.
func ExampleNewFacets() {
	declaration := &retention{Days: 30}
	facets, err := schemaext.NewFacets(declaration)
	if err != nil {
		fmt.Println(err)
		return
	}
	declaration.Days = 90
	value, found, err := schemaext.FacetAs[*retention](facets, declaration.Kind())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(found, value.Days)
	// Output: true 30
}

// ExampleRegistry_Marshal demonstrates explicit local codecs, a versioned wire
// document, and reconstruction of the owner's concrete type.
func ExampleRegistry_Marshal() {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(payload.(*retention))
	}
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{
		Owner: "example.org/provider",
		Codec: schemaext.Codec{
			Prototype: &retention{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(`{"type":"object","properties":{"days":{"type":"integer","minimum":0,"maximum":4294967295,"default":0}},"additionalProperties":false}`),
			Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
				return payload.(*retention).Clone(), nil
			},
			Encode: encode, Canonical: encode,
			Decode: func(data json.RawMessage) (schemaext.Payload, error) {
				return schemaext.DecodeJSON[*retention](data)
			},
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	data, err := registry.Marshal(context.Background(), schemaext.Desired, []schemaext.Payload{&retention{Days: 30}})
	if err != nil {
		fmt.Println(err)
		return
	}
	values, err := registry.Unmarshal(context.Background(), data)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%T: %d days\n", values[0], values[0].(*retention).Days)
	// Output: *schemaext_test.retention: 30 days
}

// policyChange is an owner's change payload with its access assessment.
type policyChange struct {
	Policy string                 `json:"policy"`
	Access schemaext.AccessEffect `json:"access"`
}

func (*policyChange) Kind() schemaext.Kind { return "example.org/policy-change" }
func (v *policyChange) CloneChange() schemaext.ChangeValue {
	cloned := *v
	return &cloned
}
func (v *policyChange) AccessEffect() schemaext.AccessEffect { return v.Access }

// ExampleAccessEffectSource shows an owner's access assessment travelling
// through a versioned change codec. The owner computes the assessment while it
// still has the captured context and stores it in the payload; the codec embeds
// the published record schema, so a reader decodes the same assessment.
func ExampleAccessEffectSource() {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		return json.Marshal(payload.(*policyChange))
	}
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{
		Owner: "example.org/provider",
		Codec: schemaext.Codec{
			Prototype:      &policyChange{Access: schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "prototype"}},
			Representation: schemaext.Change, Version: 1,
			Definition: json.RawMessage(`{"type":"object","required":["policy","access"],"additionalProperties":false,` +
				`"properties":{"policy":{"type":"string"},"access":` + string(schemaext.AccessEffectSchema()) + `}}`),
			Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
				return payload.(*policyChange).CloneChange(), nil
			},
			Encode: encode, Canonical: encode,
			Decode: func(data json.RawMessage) (schemaext.Payload, error) {
				return schemaext.DecodeJSON[*policyChange](data)
			},
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	change := &policyChange{Policy: "tenant_rows", Access: schemaext.AccessEffect{
		Access: schemaext.AccessWidens, Reason: "a new permissive policy admits rows no other policy admits",
	}}
	data, err := registry.Marshal(context.Background(), schemaext.Change, []schemaext.Payload{change})
	if err != nil {
		fmt.Println(err)
		return
	}
	decoded, err := registry.Unmarshal(context.Background(), data)
	if err != nil {
		fmt.Println(err)
		return
	}
	effect := decoded[0].(schemaext.AccessEffectSource).AccessEffect()
	fmt.Println(effect.Access)
	fmt.Println(effect.Reason)

	_, err = registry.Marshal(context.Background(), schemaext.Change, []schemaext.Payload{&policyChange{Policy: "unassessed"}})
	fmt.Println(errors.Is(err, schemaext.ErrInvalidValue))
	// Output:
	// widens
	// a new permissive policy admits rows no other policy admits
	// true
}
