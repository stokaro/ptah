package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// clearedRangeHCL is a range that writes one attribute empty on purpose and
// omits another entirely.
const clearedRangeHCL = `
schema "public" {}

range "measurement" {
  schema       = schema.public
  subtype      = bigint
  subtype_diff = ""
  canonical    = "int8canon"
}
`

// TestParse_AnEmptyRangeAttributeIsToldFromAnAbsentOne pins the distinction on
// the HCL surface.
//
// An omitted attribute and one written `subtype_diff = ""` reach the same empty
// string, and only the attribute's presence separates them. Omission says
// nothing about it, which is what keeps adoption over an existing database from
// planning away a SUBTYPE_DIFF nobody mentioned; an empty value says the range
// has none (stokaro/ptah#2223).
func TestParse_AnEmptyRangeAttributeIsToldFromAnAbsentOne(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(clearedRangeHCL), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Ranges, qt.HasLen, 1)
	rangeType := db.Ranges[0]

	c.Assert(rangeType.Clears("subtype_diff"), qt.IsTrue)
	c.Assert(rangeType.Canonical, qt.Equals, "int8canon")
	c.Assert(rangeType.Clears("canonical"), qt.IsFalse)
	c.Assert(rangeType.Clears("collation"), qt.IsFalse)
}

// TestRender_AnEmptyRangeAttributeSurvivesTheRoundTrip is the half that makes
// the distinction usable rather than only readable.
//
// Dropping the attribute on the way out would turn "this range has none" into
// "say nothing about it", which is a different instruction to the comparator --
// the shape of every round-trip loss this repository has fixed.
func TestRender_AnEmptyRangeAttributeSurvivesTheRoundTrip(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(clearedRangeHCL), "schema.hcl")
	c.Assert(err, qt.IsNil)

	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, `subtype_diff = ""`)

	again, err := atlashcl.Parse(rendered.Data, "rendered.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(again.Ranges, qt.HasLen, 1)
	c.Assert(again.Ranges[0].Clears("subtype_diff"), qt.IsTrue)
}

// TestRender_AnAbsentRangeAttributeStaysAbsent is the control.
//
// Only a declaration fills the cleared set -- a catalog read carries the values
// the server reported -- so a range that never mentioned the attribute must
// still render without it. Emitting an empty one here would put a removal
// instruction into every description Ptah writes.
func TestRender_AnAbsentRangeAttributeStaysAbsent(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "public" {}

range "measurement" {
  schema  = schema.public
  subtype = bigint
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "subtype_diff")
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "canonical")
}
