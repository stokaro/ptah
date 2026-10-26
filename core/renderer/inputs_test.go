package renderer_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
)

func TestExecutionInputsCloneWithSQLAndNativeBodies(t *testing.T) {
	for _, test := range []struct {
		name string
		body renderer.Fragment
	}{
		{"SQL", renderer.Fragment{SQL: "SELECT 1;"}},
		{"native", renderer.Fragment{Native: &nativeFragment{Data: []string{"original"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.body.Inputs = []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DECLARED_INPUT"}}
			cloned, err := test.body.Clone()
			c.Assert(err, qt.IsNil)
			test.body.Inputs[0].Name = "CHANGED"
			c.Assert(cloned.Inputs, qt.DeepEquals, []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DECLARED_INPUT"}})
		})
	}
}

func TestExecutionInputReferencesRefuseMalformedLists(t *testing.T) {
	for _, test := range []struct {
		name string
		refs []renderer.InputReference
	}{
		{"unknown source", []renderer.InputReference{{Source: "file", Name: "INPUT"}}},
		{"empty name", []renderer.InputReference{{Source: renderer.InputEnvironment}}},
		{"leading digit", []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "1INPUT"}}},
		{"assignment", []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "INPUT=value"}}},
		{"unicode", []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "INPÜT"}}},
		{"repeated", []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "INPUT"}, {Source: renderer.InputEnvironment, Name: "INPUT"}}},
		{"unsorted", []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "Z"}, {Source: renderer.InputEnvironment, Name: "A"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cloned, err := (renderer.Fragment{SQL: "SELECT 1;", Inputs: test.refs}).Clone()
			c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
			c.Assert(cloned, qt.DeepEquals, renderer.Fragment{})
		})
	}
}

func TestExecutionInputReferencesRequireExactUnitsAndRemainValueFree(t *testing.T) {
	c := qt.New(t)
	fragment := renderer.Fragment{SQL: "SELECT 1;", Inputs: []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DECLARED_INPUT"}}}
	result := renderer.Result{Complete: true, Fragments: []renderer.Fragment{fragment}}
	c.Assert(result.Validate(1), qt.ErrorIs, renderer.ErrInvalidResult)
	result.ExecutableUnits = true
	c.Assert(result.Validate(1), qt.IsNil)
	data, err := json.Marshal(fragment)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"sql":"SELECT 1;","inputs":[{"source":"environment","name":"DECLARED_INPUT"}]}`)
}
