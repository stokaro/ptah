package goschema

// White-box testing required: equivalentDefinitions is the internal canonical
// comparator used by every composite-schema conflict validator, and panic safety
// for future unexported fields cannot be observed through exported schema types
// until such a field exists.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

type definitionWithPrivateState struct {
	Name  string
	state int
}

func TestEquivalentDefinitions_HandlesUnexportedFields(t *testing.T) {
	c := qt.New(t)

	c.Assert(
		equivalentDefinitions(
			definitionWithPrivateState{Name: "users", state: 1},
			definitionWithPrivateState{Name: "users", state: 1},
		),
		qt.IsTrue,
	)
	c.Assert(
		equivalentDefinitions(
			definitionWithPrivateState{Name: "users", state: 1},
			definitionWithPrivateState{Name: "users", state: 2},
		),
		qt.IsFalse,
	)
}
