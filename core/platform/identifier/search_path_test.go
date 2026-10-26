package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
)

// TestSemanticsClone_CopiesTheSearchPath pins the path as part of the deep
// copy: a clone written to leaves the original's path as it was.
func TestSemanticsClone_CopiesTheSearchPath(t *testing.T) {
	c := qt.New(t)
	original := identifier.ForDialect("postgres")
	original.SearchPath = []string{"reporting", "public"}

	cloned := original.Clone()
	cloned.SearchPath[0] = "app"

	c.Assert(original.SearchPath, qt.DeepEquals, []string{"reporting", "public"})
}

// TestSemanticsEqual_ComparesTheSearchPath pins the path as a rule: two
// sessions that resolve unqualified names along different paths are not the
// same, and neither is a semantics with a path a zero one.
func TestSemanticsEqual_ComparesTheSearchPath(t *testing.T) {
	c := qt.New(t)
	left := identifier.ForDialect("postgres")
	left.SearchPath = []string{"reporting", "public"}
	right := left.Clone()
	right.SearchPath = []string{"public"}

	c.Assert(left.Equal(left.Clone()), qt.IsTrue)
	c.Assert(left.Equal(right), qt.IsFalse)
	c.Assert(identifier.Semantics{SearchPath: []string{"public"}}.IsZero(), qt.IsFalse)
}

// TestSemanticsNormalize_KeepsTheSearchPath pins that a complete semantics
// keeps the path it was read with through Normalize.
func TestSemanticsNormalize_KeepsTheSearchPath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	semantics.SearchPath = []string{"reporting", "public"}

	c.Assert(semantics.Normalize("postgres").SearchPath, qt.DeepEquals, []string{"reporting", "public"})
}
