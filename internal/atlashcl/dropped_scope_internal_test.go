package atlashcl

// White-box testing required: the property under test is a property of the
// evaluation scope itself, not of any parse result. Reaching it from outside
// the package would mean enumerating expressions and hoping the list covers the
// next wildcard someone adds, which is exactly how the last two revisions
// shipped a hole.

import (
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/zclconf/go-cty/cty"
)

// TestDroppedBodyScopeHoldsOnlyKnownOpaqueValues pins the property the whole
// unknown-name policy rests on, at the one place it can be checked directly.
//
// Every previous exit-0-where-the-community-binary-exits-1 divergence in this
// package came from a value in this scope that was not fully known: a
// [cty.DynamicVal] standing in for a block whose members could not be
// enumerated, or one standing in for a variable whose type was not read. An
// unknown value makes member access, indexing and arithmetic evaluate to
// unknown instead of failing, so the expression passes and the file is
// accepted. A known capsule fails all three.
//
// Asserting on the scope rather than on a list of expressions is what makes
// this hold for expressions nobody thought to write down: there is no way to
// reach an unknown result from a scope that contains none.
func TestDroppedBodyScopeHoldsOnlyKnownOpaqueValues(t *testing.T) {
	c := qt.New(t)

	c.Assert(droppedBodyScope, qt.HasLen, 3)
	for name, value := range droppedBodyScope {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(value.IsKnown(), qt.IsTrue)
			c.Assert(value.IsNull(), qt.IsFalse)
			c.Assert(value.Type().Equals(cty.DynamicPseudoType), qt.IsFalse)
			c.Assert(value.Type().IsCapsuleType(), qt.IsTrue)
		})
	}
}

// TestDroppedBodyContextIsTheClosedScope pins that the evaluation context is
// the scope above and nothing else, so a root cannot be added to the context
// while the assertions above keep passing.
func TestDroppedBodyContextIsTheClosedScope(t *testing.T) {
	c := qt.New(t)

	c.Assert(slices.Sorted(maps.Keys(droppedBodyContext.Variables)), qt.DeepEquals,
		[]string{"bool", "int", "string"})
	for name, value := range droppedBodyContext.Variables {
		c.Assert(value.RawEquals(droppedBodyScope[name]), qt.IsTrue)
	}
	c.Assert(slices.Sorted(maps.Keys(droppedBodyContext.Functions)), qt.DeepEquals,
		slices.Sorted(maps.Keys(droppedBodyFunctions)))
}
