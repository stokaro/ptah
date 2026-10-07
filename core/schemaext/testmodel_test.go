package schemaext_test

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

const widgetKind schemaext.Kind = "example.org/widget"
const otherKind schemaext.Kind = "example.org/other"

type widget struct {
	ID    schemaext.Kind
	Names []string
	Order []int
	Count uint64
}

func (v *widget) Kind() schemaext.Kind { return v.ID }
func (v *widget) Clone() schemaext.Value {
	return &widget{ID: v.ID, Names: slices.Clone(v.Names), Order: slices.Clone(v.Order), Count: v.Count}
}
func (v *widget) Equal(other schemaext.Value) bool {
	w, ok := other.(*widget)
	if !ok || v.ID != w.ID || v.Count != w.Count || !slices.Equal(v.Order, w.Order) {
		return false
	}
	left, right := slices.Clone(v.Names), slices.Clone(w.Names)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

type otherWidget struct{ ID schemaext.Kind }

func (v *otherWidget) Kind() schemaext.Kind   { return v.ID }
func (v *otherWidget) Clone() schemaext.Value { return &otherWidget{ID: v.ID} }
func (v *otherWidget) Equal(other schemaext.Value) bool {
	w, ok := other.(*otherWidget)
	return ok && v.ID == w.ID
}

type widgetWire struct {
	Names []string `json:"names"`
	Order []int    `json:"order"`
	Count uint64   `json:"count"`
}

func widgetCodec(kind schemaext.Kind, representation schemaext.Representation) schemaext.Codec {
	return schemaext.Codec{
		Prototype: &widget{ID: kind}, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","properties":{"names":{"type":"array","ordering":"set"},"order":{"type":"array","ordering":"sequence"},"count":{"type":"uint64"}}}`),
		Clone: func(value schemaext.Payload) (schemaext.Payload, error) {
			v, ok := value.(*widget)
			if !ok {
				return nil, fmt.Errorf("unexpected widget type %T", value)
			}
			return v.Clone(), nil
		},
		Encode: encodeWidget,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			wire, err := schemaext.DecodeJSON[widgetWire](data)
			if err != nil {
				return nil, err
			}
			return &widget{ID: kind, Names: wire.Names, Order: wire.Order, Count: wire.Count}, nil
		},
		Canonical: func(value schemaext.Payload) (json.RawMessage, error) {
			v, ok := value.(*widget)
			if !ok {
				return nil, fmt.Errorf("unexpected widget type %T", value)
			}
			names := slices.Clone(v.Names)
			slices.Sort(names)
			return json.Marshal(widgetWire{Names: names, Order: v.Order, Count: v.Count})
		},
	}
}

func encodeWidget(value schemaext.Payload) (json.RawMessage, error) {
	v, ok := value.(*widget)
	if !ok {
		return nil, fmt.Errorf("unexpected widget type %T", value)
	}
	return json.Marshal(widgetWire{Names: v.Names, Order: v.Order, Count: v.Count})
}

func owned(codec schemaext.Codec) schemaext.OwnedCodec {
	return schemaext.OwnedCodec{Owner: "example.org/provider", Codec: codec}
}

func widgetRef(parent, name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(widgetKind),
		Parent: objectidentity.Part{Source: parent, Normalized: parent},
		Name:   objectidentity.Part{Source: name, Normalized: name}}
}
