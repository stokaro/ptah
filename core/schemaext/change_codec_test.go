package schemaext_test

import (
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

const widgetChangeKind schemaext.Kind = "example.org/widget-change"

// widgetChange is a directional change between two widget counts.
type widgetChange struct{ Before, After uint64 }

func (*widgetChange) Kind() schemaext.Kind { return widgetChangeKind }
func (v *widgetChange) CloneChange() schemaext.ChangeValue {
	return &widgetChange{Before: v.Before, After: v.After}
}

func widgetChangeCodec(version uint32) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, ok := payload.(*widgetChange)
		if !ok {
			return nil, fmt.Errorf("unexpected widget change type %T", payload)
		}
		return json.Marshal(map[string]uint64{"before": v.Before, "after": v.After})
	}
	return schemaext.Codec{
		Prototype: &widgetChange{}, Representation: schemaext.Change, Version: version,
		Definition: json.RawMessage(`{"before":"uint64","after":"uint64"}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			return payload.(*widgetChange).CloneChange(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			wire, err := schemaext.DecodeJSON[map[string]uint64](data)
			return &widgetChange{Before: wire["before"], After: wire["after"]}, err
		},
	}
}

func changeRegistry(version uint32) schemaext.Registry {
	return must.Must(schemaext.NewRegistry(owned(widgetChangeCodec(version)), owned(widgetCodec(widgetKind, schemaext.Desired))))
}

func widgetChanges() []schemaext.ChangeRecord {
	return []schemaext.ChangeRecord{
		{Subject: widgetRef("orders", "first"), Value: &widgetChange{Before: 1, After: 2}},
		{Subject: widgetRef("orders", "second"), Value: &widgetChange{Before: 7, After: 3}},
	}
}

// A change is written with its subject and an envelope that names the owner,
// the namespaced kind, the representation and the codec version, and it reads
// back as the same records in the same order.
func TestRegistry_EncodeChanges_RoundTrip(t *testing.T) {
	c := qt.New(t)
	registry := changeRegistry(3)
	changes := widgetChanges()

	encoded, err := registry.EncodeChanges(t.Context(), changes)

	c.Assert(err, qt.IsNil)
	c.Assert(encoded, qt.HasLen, 2)
	c.Assert(encoded[0].Subject, qt.DeepEquals, changes[0].Subject)
	c.Assert(encoded[0].Value.Owner, qt.Equals, "example.org/provider")
	c.Assert(encoded[0].Value.Kind, qt.Equals, widgetChangeKind)
	c.Assert(encoded[0].Value.Representation, qt.Equals, schemaext.Change)
	c.Assert(encoded[0].Value.Version, qt.Equals, uint32(3))
	c.Assert(string(encoded[0].Value.Payload), qt.Equals, `{"after":2,"before":1}`)
	wire := must.Must(json.Marshal(encoded))
	var read []schemaext.EncodedChange
	c.Assert(json.Unmarshal(wire, &read), qt.IsNil)
	decoded, err := registry.DecodeChanges(t.Context(), read)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, changes)
	decoded[0].Value.(*widgetChange).After = 99
	c.Assert(changes[0].Value.(*widgetChange).After, qt.Equals, uint64(2))
}

func TestRegistry_EncodeChanges_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		changes []schemaext.ChangeRecord
		want    error
	}{
		{"a kind without a change codec", []schemaext.ChangeRecord{{Subject: widgetRef("orders", "first"), Value: &otherChange{}}}, schemaext.ErrUnknownCodec},
		{"no subject", []schemaext.ChangeRecord{{Value: &widgetChange{}}}, schemaext.ErrInvalidValue},
		{"no value", []schemaext.ChangeRecord{{Subject: widgetRef("orders", "first")}}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := changeRegistry(1).EncodeChanges(t.Context(), append(widgetChanges(), test.changes...))
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(encoded, qt.IsNil)
		})
	}
}

// The unknown-kind error names the kind, so a reader of a refused document
// knows which provider the runtime is missing.
func TestRegistry_EncodeChanges_NamesTheUnknownKind(t *testing.T) {
	c := qt.New(t)
	_, err := changeRegistry(1).EncodeChanges(t.Context(), []schemaext.ChangeRecord{{Subject: widgetRef("orders", "first"), Value: &otherChange{}}})
	var unknown *schemaext.UnknownCodecError
	c.Assert(err, qt.ErrorAs, &unknown)
	c.Assert(unknown.Kind, qt.Equals, otherChangeKind)
	c.Assert(unknown.Representation, qt.Equals, schemaext.Change)
	c.Assert(err, qt.ErrorMatches, `.*"example.org/other-change"/change`)
}

// A schema value recorded where a change belongs is refused for its recorded
// representation before any codec reads it.
func TestRegistry_DecodeChanges_RefusesAnotherRepresentation(t *testing.T) {
	c := qt.New(t)
	encoded := must.Must(changeRegistry(1).EncodeChanges(t.Context(), widgetChanges()))
	encoded[0].Value = must.Must(changeRegistry(1).Encode(t.Context(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}}))[0]

	decoded, err := changeRegistry(1).DecodeChanges(t.Context(), encoded)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*change "example.org/widget" is recorded as "desired"`)
	c.Assert(decoded, qt.IsNil)
}

func TestRegistry_DecodeChanges_FailurePath(t *testing.T) {
	encoded := must.Must(changeRegistry(1).EncodeChanges(t.Context(), widgetChanges()))
	widget := must.Must(changeRegistry(1).Encode(t.Context(), schemaext.Desired, []schemaext.Payload{&widget{ID: widgetKind}}))
	for _, test := range []struct {
		name     string
		registry schemaext.Registry
		edit     func([]schemaext.EncodedChange) []schemaext.EncodedChange
		want     error
	}{
		{"another codec version", changeRegistry(2), func(e []schemaext.EncodedChange) []schemaext.EncodedChange { return e }, schemaext.ErrIncompatibleCodec},
		{"no change codec", must.Must(schemaext.NewRegistry()), func(e []schemaext.EncodedChange) []schemaext.EncodedChange { return e }, schemaext.ErrUnknownCodec},
		{"a schema value recorded as a change", changeRegistry(1), func(e []schemaext.EncodedChange) []schemaext.EncodedChange {
			e[1].Value = widget[0]
			return e
		}, schemaext.ErrInvalidValue},
		{"no subject", changeRegistry(1), func(e []schemaext.EncodedChange) []schemaext.EncodedChange {
			e[1].Subject = objectidentity.ID{}
			return e
		}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := test.registry.DecodeChanges(t.Context(), test.edit(append([]schemaext.EncodedChange(nil), encoded...)))
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

const otherChangeKind schemaext.Kind = "example.org/other-change"

type otherChange struct{}

func (*otherChange) Kind() schemaext.Kind               { return otherChangeKind }
func (*otherChange) CloneChange() schemaext.ChangeValue { return &otherChange{} }
