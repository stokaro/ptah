package renderer_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
)

func TestAnnotationIsSafeProseAndClonedIndependently(t *testing.T) {
	for _, test := range []struct{ name, text, want string }{
		{"empty", "", "--\n"},
		{"multiline", "review\nDROP TABLE users;\r\n*/ SELECT 1;\r-- final", "-- review\n-- DROP TABLE users;\n-- */ SELECT 1;\n-- -- final\n"},
		{"trailing line", "review\n", "-- review\n--\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			text := test.text
			fragment, err := (renderer.Fragment{Annotation: &text}).Clone()
			c.Assert(err, qt.IsNil)
			text = "changed"
			result := renderer.Result{Complete: true, Fragments: []renderer.Fragment{fragment}}
			output, err := result.SQL()
			c.Assert(err, qt.IsNil)
			c.Assert(output, qt.Equals, test.want)
			encoded, err := json.Marshal(fragment)
			c.Assert(err, qt.IsNil)
			var decoded renderer.Fragment
			c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, fragment)
		})
	}
}

func TestAnnotationRefusesExecutableBodies(t *testing.T) {
	text := "note"
	for _, test := range []struct {
		name     string
		fragment renderer.Fragment
	}{
		{"sql", renderer.Fragment{Annotation: &text, SQL: "SELECT 1;"}},
		{"native", renderer.Fragment{Annotation: &text, Native: &nativeFragment{}}},
		{"execution inputs", renderer.Fragment{Annotation: &text, Inputs: []renderer.InputReference{{Source: "environment", Name: "VALUE"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.fragment.Validate(), qt.ErrorIs, renderer.ErrInvalidResult)
		})
	}
}

func TestAnnotationPositionsAreOrderedAndBounded(t *testing.T) {
	for _, test := range []struct {
		name  string
		notes []renderer.Annotation
		units int
		valid bool
	}{
		{"empty plan", []renderer.Annotation{{Before: 0}}, 0, true},
		{"trailing", []renderer.Annotation{{Before: 0}, {Before: 2}, {Before: 2}}, 2, true},
		{"negative", []renderer.Annotation{{Before: -1}}, 2, false},
		{"past end", []renderer.Annotation{{Before: 3}}, 2, false},
		{"backward", []renderer.Annotation{{Before: 1}, {Before: 0}}, 2, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := renderer.ValidateAnnotations(test.notes, test.units)
			c.Assert(err == nil, qt.Equals, test.valid)
		})
	}
}
