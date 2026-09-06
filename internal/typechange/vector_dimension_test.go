package typechange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/typechange"
)

// TestVectorDimensionChange_IsReportedInBothDirections pins the rule against
// what pgvector actually does.
//
// Measured on pgvector 0.8.6 / PostgreSQL 17: `vector(384) -> vector(1024)`
// answers "expected 1024 dimensions, not 384" while any row holds a vector, and
// so do the two sibling types. Widening is not the safe direction here -- 384
// numbers do not become 1024 by padding -- which is why both directions are
// rows here (stokaro/ptah#2068).
func TestVectorDimensionChange_IsReportedInBothDirections(t *testing.T) {
	tests := []struct {
		name     string
		before   string
		after    string
		wantFrom int
		wantTo   int
	}{
		{name: "the epic's own transition", before: "vector(384)", after: "vector(1024)", wantFrom: 384, wantTo: 1024},
		{name: "the shrinking direction", before: "vector(1024)", after: "vector(384)", wantFrom: 1024, wantTo: 384},
		{name: "halfvec", before: "halfvec(3)", after: "halfvec(8)", wantFrom: 3, wantTo: 8},
		{name: "sparsevec", before: "sparsevec(5)", after: "sparsevec(9)", wantFrom: 5, wantTo: 9},
		{name: "a catalog spelling with padding", before: " VECTOR( 384 ) ", after: "vector(1024)", wantFrom: 384, wantTo: 1024},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			from, to, ok := typechange.VectorDimensionChange(test.before, test.after)

			c.Assert(ok, qt.IsTrue)
			c.Assert(from, qt.Equals, test.wantFrom)
			c.Assert(to, qt.Equals, test.wantTo)
		})
	}
}

// TestVectorDimensionChange_IsNotReportedForTheRestIsThe control.
//
// Every row is a transition the server performs, or one whose outcome the
// declared types do not determine. The unsized rows are the load-bearing ones:
// what pgvector checks is the dimension STORED, so a `vector` column holding a
// 3-dimension value takes `TYPE vector(3)` and refuses `TYPE vector(7)`, and
// nothing in the two type names separates those.
func TestVectorDimensionChange_IsNotReportedForTheRest(t *testing.T) {
	tests := []struct {
		name   string
		before string
		after  string
	}{
		{name: "the same dimension", before: "vector(384)", after: "vector(384)"},
		{name: "to the unsized form", before: "vector(3)", after: "vector"},
		{name: "from the unsized form", before: "vector", after: "vector(3)"},
		{name: "both unsized", before: "vector", after: "vector"},
		{name: "not a vector at all", before: "varchar(384)", after: "varchar(1024)"},
		{name: "a width, not a space", before: "bit(8)", after: "bit(16)"},
		{name: "one side is not a vector", before: "vector(384)", after: "text"},
		{name: "an empty side", before: "", after: "vector(384)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, ok := typechange.VectorDimensionChange(test.before, test.after)

			c.Assert(ok, qt.IsFalse)
		})
	}
}

// TestVectorDimensionChange_IsNotNarrowingOrWidening records why it needed a
// function of its own.
//
// The two existing questions answer false for it in both directions, because
// `vector` is in none of their categories. Without this, `vector(384) ->
// vector(1024)` reached the generic branch and was reported as a column type
// change like any other.
func TestVectorDimensionChange_IsNotNarrowingOrWidening(t *testing.T) {
	tests := []struct {
		name   string
		before string
		after  string
	}{
		{name: "growing", before: "vector(384)", after: "vector(1024)"},
		{name: "shrinking", before: "vector(1024)", after: "vector(384)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(typechange.IsNarrowing(test.before, test.after), qt.IsFalse)
			c.Assert(typechange.IsWidening(test.before, test.after), qt.IsFalse)
		})
	}
}
