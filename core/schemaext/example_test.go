package schemaext_test

import (
	"context"
	"encoding/json"
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
