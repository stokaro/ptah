package schemaext_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

const noteKind schemaext.Kind = "example.org/note"

// note is a model whose tags are a set and whose title is omitted while empty.
type note struct {
	Tags  []string `json:"tags"`
	Title string   `json:"title,omitempty"`
}

func (*note) Kind() schemaext.Kind { return noteKind }

func (v *note) Clone() schemaext.Value {
	if v == nil {
		return (*note)(nil)
	}
	cloned := *v
	cloned.Tags = slices.Clone(v.Tags)
	return &cloned
}

func (v *note) Equal(other schemaext.Value) bool {
	right, ok := other.(*note)
	return ok && v.Title == right.Title && slices.Equal(slices.Sorted(slices.Values(v.Tags)), slices.Sorted(slices.Values(right.Tags)))
}

// typedNoteRefusal is what the validator returns for a title it types itself.
var typedNoteRefusal = &schemaext.InvalidModelError{Kind: noteKind, Representation: schemaext.Desired, Message: "a typed refusal"}

func noteCodec() schemaext.Codec {
	return schemaext.ModelCodec[*note]{
		Prototype: &note{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(`{"type":"object"}`),
		Shape: func(data json.RawMessage) error {
			_, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "note", Allowed: []string{"tags", "title"},
				Required: []string{"tags"}, NonEmpty: []string{"title"}})
			return err
		},
		Validate: func(value *note) error {
			switch {
			case value == nil:
				return fmt.Errorf("%w: nil note", schemaext.ErrInvalidValue)
			case value.Title == "typed":
				return typedNoteRefusal
			case slices.Contains(value.Tags, ""):
				return fmt.Errorf("%w: a note tag cannot be empty", schemaext.ErrInvalidValue)
			}
			return nil
		},
		Canonical: func(value *note) *note {
			canonical := value.Clone().(*note)
			slices.Sort(canonical.Tags)
			return canonical
		},
	}.Codec()
}

// TestModelCodec_HappyPath pins the boundaries a valid value crosses: the
// encoding is the canonical form, it leaves its argument alone, decoding
// returns the value, and a clone is independent.
func TestModelCodec_HappyPath(t *testing.T) {
	c := qt.New(t)
	codec := noteCodec()
	value := &note{Tags: []string{"b", "a"}, Title: "t"}

	encoded, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	canonical, err := codec.Canonical(value)
	c.Assert(err, qt.IsNil)
	decoded, err := codec.Decode(json.RawMessage(`{"title":"t","tags":["a","b"]}`))
	c.Assert(err, qt.IsNil)
	cloned, err := codec.Clone(value)
	c.Assert(err, qt.IsNil)
	cloned.(*note).Tags[0] = "z"

	c.Assert(string(encoded), qt.Equals, `{"tags":["a","b"],"title":"t"}`)
	c.Assert(string(canonical), qt.Equals, string(encoded))
	c.Assert(value.Tags, qt.DeepEquals, []string{"b", "a"})
	c.Assert(decoded, qt.DeepEquals, schemaext.Payload(&note{Tags: []string{"a", "b"}, Title: "t"}))
	c.Assert(codec.Version, qt.Equals, uint32(1))
	c.Assert(codec.Prototype, qt.DeepEquals, schemaext.Payload(&note{}))
}

// TestModelCodec_FailurePath pins that every refusal is the typed model error
// of the codec's kind and representation, whichever boundary refused: a
// payload of another type, the shape, the struct decoder and the validator.
func TestModelCodec_FailurePath(t *testing.T) {
	codec := noteCodec()
	tests := []struct {
		name    string
		call    func() (any, error)
		wantErr string
	}{
		{name: "encoding another type", wantErr: `desired model "example.org/note": .*expected \*schemaext_test.note, got \*schemaext_test.widget`,
			call: func() (any, error) { return codec.Encode(&widget{ID: widgetKind}) }},
		{name: "cloning another type", wantErr: `desired model "example.org/note": .*expected \*schemaext_test.note, got <nil>`,
			call: func() (any, error) { return codec.Clone(nil) }},
		{name: "encoding an invalid value", wantErr: `desired model "example.org/note": .*a note tag cannot be empty`,
			call: func() (any, error) { return codec.Encode(&note{Tags: []string{""}}) }},
		{name: "a key the shape refuses", wantErr: `desired model "example.org/note": .*unknown note property "Tags"`,
			call: func() (any, error) { return codec.Decode(json.RawMessage(`{"Tags":[]}`)) }},
		{name: "an omitted value spelled out", wantErr: `desired model "example.org/note": .*note property "title" cannot be empty; omit it instead`,
			call: func() (any, error) { return codec.Decode(json.RawMessage(`{"tags":[],"title":""}`)) }},
		{name: "a value of the wrong type", wantErr: `desired model "example.org/note": .*decode concrete payload: .*`,
			call: func() (any, error) { return codec.Decode(json.RawMessage(`{"tags":"a"}`)) }},
		{name: "a decoded value the validator refuses", wantErr: `desired model "example.org/note": .*a note tag cannot be empty`,
			call: func() (any, error) { return codec.Decode(json.RawMessage(`{"tags":[""]}`)) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := test.call()
			var refusal *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Kind, qt.Equals, noteKind)
			c.Assert(refusal.Representation, qt.Equals, schemaext.Desired)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestModelCodec_FailurePath_KeepsATypedRefusal pins that a refusal the
// validator already typed is returned as it is, not wrapped in a second one.
func TestModelCodec_FailurePath_KeepsATypedRefusal(t *testing.T) {
	c := qt.New(t)

	encoded, err := noteCodec().Encode(&note{Tags: []string{"a"}, Title: "typed"})

	c.Assert(err, qt.Equals, error(typedNoteRefusal))
	c.Assert(encoded, qt.IsNil)
}

const noteChangeKind schemaext.Kind = "example.org/note-change"

// noteChange is a payload that is not a [schemaext.Value]: it has no Clone
// method, so its codec names one.
type noteChange struct {
	Title string `json:"title"`
}

func (*noteChange) Kind() schemaext.Kind { return noteChangeKind }

func noteChangeCodec(clone func(*noteChange) *noteChange) schemaext.Codec {
	return schemaext.ModelCodec[*noteChange]{
		Prototype: &noteChange{}, Representation: schemaext.Change, Version: 1, Definition: json.RawMessage(`{"type":"object"}`),
		Clone: clone,
	}.Codec()
}

// TestModelCodec_ClonesAPayloadThatIsNotAValue pins that a change or an
// operation codec clones through the function it names.
func TestModelCodec_ClonesAPayloadThatIsNotAValue(t *testing.T) {
	c := qt.New(t)
	value := &noteChange{Title: "t"}

	cloned, err := noteChangeCodec(func(v *noteChange) *noteChange { return &noteChange{Title: v.Title} }).Clone(value)

	c.Assert(err, qt.IsNil)
	c.Assert(cloned, qt.DeepEquals, schemaext.Payload(&noteChange{Title: "t"}))
	c.Assert(cloned, qt.Not(qt.Equals), schemaext.Payload(value))
}

// TestModelCodec_FailurePath_RefusesToCloneWithoutAFunction pins that a codec
// for a payload with no Clone method, and no function named, refuses rather
// than returning the value it was given.
func TestModelCodec_FailurePath_RefusesToCloneWithoutAFunction(t *testing.T) {
	c := qt.New(t)

	cloned, err := noteChangeCodec(nil).Clone(&noteChange{Title: "t"})

	c.Assert(err, qt.ErrorMatches, `the change codec of "example.org/note-change" has no clone function`)
	c.Assert(cloned, qt.IsNil)
}
