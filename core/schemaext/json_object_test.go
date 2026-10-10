package schemaext_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

var widgetShape = schemaext.ObjectShape{
	Name: "widget", Allowed: []string{"name", "size", "parent", "label", "tags"}, Required: []string{"name"}, Nullable: []string{"parent"},
	NonEmpty: []string{"label", "tags"},
}

func TestDecodeObject_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  map[string]json.RawMessage
	}{
		{name: "every key", input: `{"size": 2, "name": "a", "parent": null}`,
			want: map[string]json.RawMessage{"name": json.RawMessage(`"a"`), "size": json.RawMessage(`2`), "parent": json.RawMessage(`null`)}},
		{name: "only the required key", input: `{"name":"a"}`, want: map[string]json.RawMessage{"name": json.RawMessage(`"a"`)}},
		{name: "values under non-empty keys", input: `{"name":"a","label":"l","tags":["t"]}`,
			want: map[string]json.RawMessage{"name": json.RawMessage(`"a"`), "label": json.RawMessage(`"l"`), "tags": json.RawMessage(`["t"]`)}},
		{name: "empty values under other keys", input: `{"name":"","size":0}`,
			want: map[string]json.RawMessage{"name": json.RawMessage(`""`), "size": json.RawMessage(`0`)}},
		{name: "a number a float rounds to zero", input: `{"name":"a","label":1e-400}`,
			want: map[string]json.RawMessage{"name": json.RawMessage(`"a"`), "label": json.RawMessage(`1e-400`)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			fields, err := schemaext.DecodeObject(json.RawMessage(test.input), widgetShape)

			c.Assert(err, qt.IsNil)
			c.Assert(fields, qt.DeepEquals, test.want)
		})
	}
}

// TestDecodeObject_FailurePath pins what a struct decoder lets through and this
// refuses: a key in another letter case, a null the shape does not allow, and a
// missing key, besides values that are not one object.
func TestDecodeObject_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "null", input: `null`, want: `.*expected a widget object`},
		{name: "an array", input: `[{"name":"a"}]`, want: `.*expected a widget object`},
		{name: "a string", input: `"name"`, want: `.*expected a widget object`},
		{name: "invalid JSON", input: `{"name":`, want: `.*invalid JSON.*`},
		{name: "a key in another case", input: `{"Name":"a"}`, want: `.*unknown widget property "Name"`},
		{name: "an unknown key", input: `{"name":"a","colour":"red"}`, want: `.*unknown widget property "colour"`},
		{name: "a null the shape does not allow", input: `{"name":"a","size":null}`, want: `.*widget property "size" cannot be null`},
		{name: "a missing required key", input: `{"size":1}`, want: `.*missing widget property "name"`},
		{name: "a duplicate key", input: `{"name":"a","name":"b"}`, want: `.*duplicate object key "name".*`},
		{name: "an empty string spelled out", input: `{"name":"a","label":""}`, want: `.*widget property "label" cannot be empty; omit it instead`},
		{name: "false spelled out", input: `{"name":"a","label":false}`, want: `.*widget property "label" cannot be empty; omit it instead`},
		{name: "zero spelled out", input: `{"name":"a","label":-0.0e3}`, want: `.*widget property "label" cannot be empty; omit it instead`},
		{name: "an empty list spelled out", input: `{"name":"a","tags":[]}`, want: `.*widget property "tags" cannot be empty; omit it instead`},
		{name: "an empty object spelled out", input: `{"name":"a","tags":{ }}`, want: `.*widget property "tags" cannot be empty; omit it instead`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			fields, err := schemaext.DecodeObject(json.RawMessage(test.input), widgetShape)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(fields, qt.IsNil)
		})
	}
}

// TestDecodeObject_FailurePath_NamesTheSameKeyEveryTime pins that an object
// with several offending keys is refused with one message, naming the first in
// sorted order, however the map holding them happens to iterate.
func TestDecodeObject_FailurePath_NamesTheSameKeyEveryTime(t *testing.T) {
	c := qt.New(t)
	input := json.RawMessage(`{"zeta":1,"alpha":2,"mid":3,"omega":4,"name":"a"}`)
	for range 50 {
		fields, err := schemaext.DecodeObject(input, widgetShape)
		c.Assert(err, qt.ErrorMatches, `.*unknown widget property "alpha"`)
		c.Assert(fields, qt.IsNil)
	}
}
