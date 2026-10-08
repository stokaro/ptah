package schemaext_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestRegistry_RoundTripAndFingerprintOrdering(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	value := &widget{ID: widgetKind, Names: []string{"b", "a"}, Order: []int{2, 1}, Count: math.MaxUint64}
	data, err := registry.Marshal(context.Background(), schemaext.Desired, []schemaext.Payload{value})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, "18446744073709551615")
	c.Assert(string(data), qt.Contains, `"kind":"example.org/widget"`)
	decoded, err := registry.Unmarshal(context.Background(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{value})
	first, err := registry.Fingerprint(context.Background(), schemaext.Desired, []schemaext.Payload{value})
	c.Assert(err, qt.IsNil)
	value.Names = []string{"a", "b"}
	second, err := registry.Fingerprint(context.Background(), schemaext.Desired, []schemaext.Payload{value})
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.Equals, second)
	value.Order = []int{1, 2}
	third, err := registry.Fingerprint(context.Background(), schemaext.Desired, []schemaext.Payload{value})
	c.Assert(err, qt.IsNil)
	c.Assert(third, qt.Not(qt.Equals), first)
}

func TestRegistry_RefuseIncompatibleEnvelope(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	envelopes, err := registry.Encode(context.Background(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}})
	c.Assert(err, qt.IsNil)
	tests := []struct {
		name   string
		change func(*schemaext.Envelope)
		want   error
	}{
		{name: "format", change: func(e *schemaext.Envelope) { e.Format++ }, want: schemaext.ErrIncompatibleCodec},
		{name: "version", change: func(e *schemaext.Envelope) { e.Version++ }, want: schemaext.ErrIncompatibleCodec},
		{name: "owner", change: func(e *schemaext.Envelope) { e.Owner = "example.org/replacement" }, want: schemaext.ErrIncompatibleCodec},
		{name: "definition", change: func(e *schemaext.Envelope) { e.Definition = "changed" }, want: schemaext.ErrIncompatibleCodec},
		{name: "kind", change: func(e *schemaext.Envelope) { e.Kind = otherKind }, want: schemaext.ErrUnknownCodec},
		{name: "representation", change: func(e *schemaext.Envelope) { e.Representation = schemaext.Observed }, want: schemaext.ErrUnknownCodec},
		{name: "unknown field", change: func(e *schemaext.Envelope) { e.Payload = json.RawMessage(`{"unknown":1}`) }, want: schemaext.ErrInvalidValue},
		{name: "duplicate field", change: func(e *schemaext.Envelope) { e.Payload = json.RawMessage(`{"count":1,"count":2}`) }, want: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed := envelopes[0]
			test.change(&changed)
			values, err := registry.Decode(context.Background(), []schemaext.Envelope{envelopes[0], changed})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestRegistry_DefinitionChangesInvalidateArtifacts(t *testing.T) {
	c := qt.New(t)
	codec := widgetCodec(widgetKind, schemaext.Desired)
	before, err := schemaext.NewRegistry(owned(codec))
	c.Assert(err, qt.IsNil)
	codec.Definition = json.RawMessage(`{"type":"object","revision":"different"}`)
	after, err := schemaext.NewRegistry(owned(codec))
	c.Assert(err, qt.IsNil)
	payloads := []schemaext.Payload{&widget{ID: widgetKind}}
	envelopes, err := before.Encode(context.Background(), schemaext.Desired, payloads)
	c.Assert(err, qt.IsNil)
	_, err = after.Decode(context.Background(), envelopes)
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
	a, err := before.Fingerprint(context.Background(), schemaext.Desired, payloads)
	c.Assert(err, qt.IsNil)
	b, err := after.Fingerprint(context.Background(), schemaext.Desired, payloads)
	c.Assert(err, qt.IsNil)
	c.Assert(a, qt.Not(qt.Equals), b)
}

func TestRegistry_DiscardCanceledAndFailedResults(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codec := widgetCodec(widgetKind, schemaext.Desired)
	codec.Encode = func(value schemaext.Payload) (json.RawMessage, error) { cancel(); return encodeWidget(value) }
	registry, err := schemaext.NewRegistry(owned(codec))
	c.Assert(err, qt.IsNil)
	result, err := registry.Encode(ctx, schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	failure := errors.New("codec failed")
	codec.Encode = func(schemaext.Payload) (json.RawMessage, error) { return json.RawMessage(`{}`), failure }
	registry, err = schemaext.NewRegistry(owned(codec))
	c.Assert(err, qt.IsNil)
	result, err = registry.Encode(context.Background(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.IsNil)
}

func TestRegistry_RejectDuplicateOwnership(t *testing.T) {
	c := qt.New(t)
	first := owned(widgetCodec(widgetKind, schemaext.Desired))
	second := first
	second.Owner = "example.org/second"
	_, err := schemaext.NewRegistry(first, second)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
	_, err = schemaext.NewRegistry(first, first)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
}

func TestRegistry_RejectMalformedRegistrations(t *testing.T) {
	tests := []struct {
		name string
		edit func(*schemaext.OwnedCodec)
	}{
		{name: "owner", edit: func(c *schemaext.OwnedCodec) { c.Owner = "unqualified" }},
		{name: "nil prototype", edit: func(c *schemaext.OwnedCodec) { c.Codec.Prototype = nil }},
		{name: "typed nil prototype", edit: func(c *schemaext.OwnedCodec) { c.Codec.Prototype = (*widget)(nil) }},
		{name: "invalid kind", edit: func(c *schemaext.OwnedCodec) { c.Codec.Prototype = &widget{ID: "unqualified"} }},
		{name: "direction", edit: func(c *schemaext.OwnedCodec) { c.Codec.Representation = "unknown" }},
		{name: "version", edit: func(c *schemaext.OwnedCodec) { c.Codec.Version = 0 }},
		{name: "missing clone", edit: func(c *schemaext.OwnedCodec) { c.Codec.Clone = nil }},
		{name: "missing encode", edit: func(c *schemaext.OwnedCodec) { c.Codec.Encode = nil }},
		{name: "missing decode", edit: func(c *schemaext.OwnedCodec) { c.Codec.Decode = nil }},
		{name: "missing canonical", edit: func(c *schemaext.OwnedCodec) { c.Codec.Canonical = nil }},
		{name: "empty definition", edit: func(c *schemaext.OwnedCodec) { c.Codec.Definition = json.RawMessage(`{}`) }},
		{name: "nonobject definition", edit: func(c *schemaext.OwnedCodec) { c.Codec.Definition = json.RawMessage(`[]`) }},
		{name: "duplicate definition key", edit: func(c *schemaext.OwnedCodec) { c.Codec.Definition = json.RawMessage(`{"x":1,"x":2}`) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := owned(widgetCodec(widgetKind, schemaext.Desired))
			test.edit(&declaration)
			registry, err := schemaext.NewRegistry(declaration)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidCodec)
			c.Assert(registry.Definitions(), qt.HasLen, 0)
		})
	}
}

func TestRegistry_RejectOwnerIdentityChanges(t *testing.T) {
	tests := []struct {
		name string
		edit func(*schemaext.Codec)
	}{
		{name: "nil clone", edit: func(c *schemaext.Codec) {
			c.Clone = func(schemaext.Payload) (schemaext.Payload, error) { return nil, nil }
		}},
		{name: "typed nil clone", edit: func(c *schemaext.Codec) {
			c.Clone = func(schemaext.Payload) (schemaext.Payload, error) { return (*widget)(nil), nil }
		}},
		{name: "changed kind", edit: func(c *schemaext.Codec) {
			c.Clone = func(schemaext.Payload) (schemaext.Payload, error) { return &widget{ID: otherKind}, nil }
		}},
		{name: "changed concrete type", edit: func(c *schemaext.Codec) {
			c.Clone = func(schemaext.Payload) (schemaext.Payload, error) { return &otherWidget{ID: widgetKind}, nil }
		}},
		{name: "encoder changes identity", edit: func(c *schemaext.Codec) {
			c.Encode = func(payload schemaext.Payload) (json.RawMessage, error) {
				payload.(*widget).ID = otherKind
				return encodeWidget(payload)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := widgetCodec(widgetKind, schemaext.Desired)
			test.edit(&codec)
			registry, err := schemaext.NewRegistry(owned(codec))
			c.Assert(err, qt.IsNil)
			input := &widget{ID: widgetKind}
			result, err := registry.Encode(context.Background(), schemaext.Desired, []schemaext.Payload{input})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
			c.Assert(input.ID, qt.Equals, widgetKind)
		})
	}
}

func TestRegistry_RejectDecoderIdentityChanges(t *testing.T) {
	for _, result := range []schemaext.Payload{nil, (*widget)(nil), &widget{ID: otherKind}, &otherWidget{ID: widgetKind}} {
		c := qt.New(t)
		codec := widgetCodec(widgetKind, schemaext.Desired)
		codec.Decode = func(json.RawMessage) (schemaext.Payload, error) { return result, nil }
		registry, err := schemaext.NewRegistry(owned(codec))
		c.Assert(err, qt.IsNil)
		encoded, err := registry.Encode(context.Background(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}})
		c.Assert(err, qt.IsNil)
		decoded, err := registry.Decode(context.Background(), encoded)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(decoded, qt.IsNil)
	}
}

func TestRegistry_FreezesMetadata(t *testing.T) {
	c := qt.New(t)
	declaration := owned(widgetCodec(widgetKind, schemaext.Desired))
	registry, err := schemaext.NewRegistry(declaration)
	c.Assert(err, qt.IsNil)
	definitions := registry.Definitions()
	want := definitions[0]
	declaration.Codec.Prototype.(*widget).ID = otherKind
	declaration.Codec.Definition[0] = '!'
	declaration.Codec.Version++
	definitions[0].Definition = "changed"
	c.Assert(registry.Definitions(), qt.DeepEquals, []schemaext.CodecIdentity{want})
	encoded, err := registry.Marshal(context.Background(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}})
	c.Assert(err, qt.IsNil)
	decoded, err := registry.Unmarshal(context.Background(), encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{&widget{ID: widgetKind}})
}

func TestRegistry_EmptyDocumentsStillRequireAFormat(t *testing.T) {
	c := qt.New(t)
	registry := schemaext.Registry{}
	data, err := registry.Marshal(context.Background(), schemaext.Desired, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"format":1,"values":[]}`)
	values, err := registry.Unmarshal(context.Background(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 0)
	for _, input := range []string{`{}`, `{"format":2,"values":[]}`, `{"format":1}`, `{"format":1,"values":null}`} {
		_, err := registry.Unmarshal(context.Background(), []byte(input))
		c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
	}
}
