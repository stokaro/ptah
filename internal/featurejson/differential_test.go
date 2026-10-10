package featurejson_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/google/go-cmp/cmp"

	"ptah.run/core/schemaext"
	"ptah.run/internal/featurejson"
)

// The types below are documents without feature data in every form
// encoding/json treats specially. Each of them also declares a feature field
// it leaves zero, which encoding/json leaves out, so json.Marshal can encode
// the original while the projection still has to mirror every other field.

// Label is embedded without a tag. It is not a struct, so encoding/json names
// the field after the type instead of promoting anything.
type Label string

// Inner is embedded with a JSON name, which makes it an ordinary field.
type Inner struct {
	X int    `json:"x"`
	Y string `json:"y,omitempty"`
}

// textKey is a map key encoding/json writes through its text form.
type textKey struct{ A, B string }

func (k textKey) MarshalText() ([]byte, error) { return []byte(k.A + "/" + k.B), nil }

func (k *textKey) UnmarshalText(data []byte) error {
	k.A, k.B, _ = strings.Cut(string(data), "/")
	return nil
}

// valueZero decides through a value method that it is zero while it still
// holds a note.
type valueZero struct {
	N    int    `json:"n"`
	Note string `json:"note"`
}

func (v valueZero) IsZero() bool { return v.N == 0 }

// pointerZero decides the same through a pointer method.
type pointerZero struct {
	N    int    `json:"n"`
	Note string `json:"note"`
}

func (v *pointerZero) IsZero() bool { return v.N == 0 }

type zeroer interface{ IsZero() bool }

type shapes struct {
	Int      int             `json:"int"`
	Renamed  string          `json:"renamed_field"`
	Omitted  string          `json:"omitted,omitempty"`
	Quoted   int64           `json:"quoted,string"`
	Pointer  *int            `json:"pointer"`
	NilPtr   *int            `json:"nil_ptr,omitempty"`
	List     []string        `json:"list"`
	NilList  []string        `json:"nil_list"`
	Empty    []string        `json:"empty,omitempty"`
	Map      map[string]int  `json:"map"`
	Keyed    map[textKey]int `json:"keyed"`
	IntKeys  map[int]string  `json:"int_keys"`
	Array    [2]bool         `json:"array"`
	Any      any             `json:"any"`
	Raw      json.RawMessage `json:"raw,omitempty"`
	When     time.Time       `json:"when"`
	ByValue  valueZero       `json:"by_value,omitzero"`
	ByPtr    pointerZero     `json:"by_pointer,omitzero"`
	PtrZero  *pointerZero    `json:"ptr_zero,omitzero"`
	Check    zeroer          `json:"check,omitzero"`
	Untagged int
	Skipped  int `json:"-"`
	hidden   int
	Inner    `json:"inner"`
	Label
	Facets schemaext.Facets `json:"facets,omitzero"`
}

// counted holds feature data and is zero by a pointer method; flagged does
// the same by a value method; sealed is not zero only through a field
// encoding/json never writes.
type counted struct {
	N      int              `json:"n"`
	Note   string           `json:"note"`
	Facets schemaext.Facets `json:"facets,omitzero"`
}

func (c *counted) IsZero() bool { return c.N == 0 }

type flagged struct {
	On     bool             `json:"on"`
	Note   string           `json:"note"`
	Facets schemaext.Facets `json:"facets,omitzero"`
}

func (f flagged) IsZero() bool { return !f.On }

type sealed struct {
	secret int
	Facets schemaext.Facets `json:"facets,omitzero"`
}

type nested struct {
	Shapes    shapes                   `json:"shapes"`
	Pointer   *shapes                  `json:"pointer"`
	NilPtr    *shapes                  `json:"nil_pointer"`
	OmitPtr   *shapes                  `json:"omit_pointer,omitempty"`
	List      []shapes                 `json:"list"`
	NilList   []shapes                 `json:"nil_list"`
	EmptyOmit []shapes                 `json:"empty_omit,omitempty"`
	ZeroOmit  []shapes                 `json:"zero_omit,omitzero"`
	BothOmit  []shapes                 `json:"both_omit,omitempty,omitzero"`
	Array     [1]shapes                `json:"array"`
	Map       map[string]shapes        `json:"map"`
	Keyed     map[textKey]*shapes      `json:"keyed"`
	Counted   counted                  `json:"counted,omitzero"`
	Kept      counted                  `json:"kept,omitzero"`
	Flagged   flagged                  `json:"flagged,omitzero"`
	Raised    flagged                  `json:"raised,omitzero"`
	Sealed    sealed                   `json:"sealed,omitzero"`
	Changes   []schemaext.ChangeRecord `json:"changes,omitzero"`
}

func filledShapes() shapes {
	count := 3
	return shapes{
		Int: 1, Renamed: "renamed", Quoted: 42, Pointer: &count,
		List: []string{"a", "b"}, Empty: make([]string, 0), Map: map[string]int{"b": 2, "a": 1},
		Keyed: map[textKey]int{{A: "x", B: "y"}: 1}, IntKeys: map[int]string{2: "two", 10: "ten"},
		Array: [2]bool{true, false}, Any: map[string]any{"n": 1.5, "list": []any{"x", true}},
		Raw: json.RawMessage(`{"raw":[1,2]}`), When: time.Date(2026, 10, 10, 12, 30, 0, 5, time.UTC),
		ByValue: valueZero{Note: "zero by value"}, ByPtr: pointerZero{Note: "zero by pointer"},
		PtrZero: &pointerZero{N: 1, Note: "kept"}, Check: valueZero{Note: "zero inside an interface"},
		Untagged: 7, Skipped: 9, hidden: 10, Inner: Inner{X: 11}, Label: "label",
	}
}

func filledNested() nested {
	inner := filledShapes()
	return nested{
		Shapes: filledShapes(), Pointer: &inner, OmitPtr: nil,
		List: []shapes{filledShapes(), {}}, EmptyOmit: make([]shapes, 0), ZeroOmit: make([]shapes, 0), BothOmit: make([]shapes, 0),
		Array: [1]shapes{filledShapes()}, Map: map[string]shapes{"one": filledShapes(), "zero": {}},
		Keyed:   map[textKey]*shapes{{A: "k", B: "v"}: &inner, {A: "nil"}: nil},
		Counted: counted{Note: "zero by its pointer method"}, Kept: counted{N: 2, Note: "kept"},
		Flagged: flagged{Note: "zero by its value method"}, Raised: flagged{On: true},
		Sealed: sealed{secret: 1},
	}
}

// documentsWithoutFeatureData is the corpus, in the top-level forms a caller
// hands to Marshal.
func documentsWithoutFeatureData() []struct {
	name  string
	value any
} {
	full, zero := filledNested(), nested{}
	return []struct {
		name  string
		value any
	}{
		{"a filled document", full},
		{"a zero document", zero},
		{"a pointer to a document", &full},
		{"a list of documents", []nested{full, zero}},
		{"a map of documents", map[string]*nested{"full": &full, "nil": nil}},
		{"a document whose interface field holds a typed nil", shapes{Check: (*pointerZero)(nil)}},
		{"a document whose interface field holds a value", shapes{Check: &pointerZero{N: 4}}},
	}
}

// A document with no feature data is written exactly as encoding/json writes
// it, through the wire type the projection mirrors and not only when it skips
// the projection: every type here has a feature field, so each one is
// mirrored field by field.
func TestMarshal_MatchesEncodingJSONWithoutFeatureData(t *testing.T) {
	for _, test := range documentsWithoutFeatureData() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want, err := json.MarshalIndent(test.value, "", "  ")
			c.Assert(err, qt.IsNil)

			got, err := featurejson.MarshalIndent(t.Context(), codecs(), schemaext.Desired, test.value, "", "  ")

			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, string(want))
		})
	}
}

// decodedBothWays decodes data into a fresh T with encoding/json and with the
// projection.
func decodedBothWays[T any](c *qt.C, data string) (want, got T) {
	c.Helper()
	c.Assert(json.Unmarshal([]byte(data), &want), qt.IsNil)
	c.Assert(featurejson.Unmarshal(c.Context(), codecs(), schemaext.Desired, []byte(data), &got), qt.IsNil)
	return want, got
}

// unexported lets the comparison read the fields neither decoder writes.
var unexported = cmp.AllowUnexported(shapes{}, sealed{})

// A document with no feature data reads back as encoding/json reads it: its
// own output, and input that leans on encoding/json's leniency, with keys in
// another case, unknown keys and nulls.
func TestUnmarshal_MatchesEncodingJSONWithoutFeatureData(t *testing.T) {
	written := must.Must(json.Marshal(filledNested()))
	for _, test := range []struct {
		name string
		data string
	}{
		{"what encoding/json writes", string(written)},
		{"keys in another case", `{"SHAPES": {"INT": 5, "Renamed_Field": "r"}, "Pointer": {"inner": {"X": 3}}}`},
		{"unknown keys", `{"unknown": 1, "shapes": {"unknown": {"deep": true}, "int": 2}}`},
		{"nulls", `{"pointer": null, "list": null, "map": null, "shapes": {"list": null, "when": null, "Label": null}}`},
		{"a field encoding/json never reads", `{"shapes": {"hidden": 1, "-": 2, "Skipped": 3}}`},
		{"an embedded field under its own name", `{"shapes": {"inner": {"x": 6}, "Label": "l", "x": 7}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			want, got := decodedBothWays[nested](c, test.data)

			c.Assert(got, qt.CmpEquals(unexported), want)
		})
	}
}
