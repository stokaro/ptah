package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestRanges_AttributeChangesAreReported pins that a change to an EXISTING range
// type is detected.
//
// compare.Ranges built a set of range-type NAMES on each side and reported only
// additions and removals, so changing the subtype of a range that already exists
// produced an empty plan. Measured live on PostgreSQL 17.10: after applying
// subtype=timestamptz, a model asking for subtype=int8 answered
// "Schema is synced, no changes to be made." at TRUE_EXIT=0 while pg_range still
// read timestamptz (stokaro/ptah#931 item 2).
//
// One row per attribute the range grammar carries. Each row differs from the
// database in exactly one place, so a comparator that happened to compare only
// some of them fails on the rows it does not cover rather than passing on the
// strength of its neighbours.
func TestRanges_AttributeChangesAreReported(t *testing.T) {
	current := types.DBRange{
		Name:           "audit_range",
		Subtype:        "timestamp with time zone",
		SubtypeOpClass: "timestamptz_ops",
		Collation:      "",
		Canonical:      "",
		SubtypeDiff:    "",
	}

	tests := []struct {
		name      string
		target    goschema.Range
		changeKey string
		want      string
	}{
		{
			name:      "subtype",
			target:    goschema.Range{Name: "audit_range", Subtype: "int8"},
			changeKey: "subtype",
			want:      "timestamp with time zone -> int8",
		},
		{
			name:      "subtype_opclass",
			target:    goschema.Range{Name: "audit_range", Subtype: "timestamptz", SubtypeOpClass: "custom_ops"},
			changeKey: "subtype_opclass",
			want:      "timestamptz_ops -> custom_ops",
		},
		{
			name:      "collation",
			target:    goschema.Range{Name: "audit_range", Subtype: "timestamptz", Collation: "en_US"},
			changeKey: "collation",
			want:      " -> en_US",
		},
		{
			name:      "canonical",
			target:    goschema.Range{Name: "audit_range", Subtype: "timestamptz", Canonical: "audit_range_canonical"},
			changeKey: "canonical",
			want:      " -> audit_range_canonical",
		},
		{
			name:      "subtype_diff",
			target:    goschema.Range{Name: "audit_range", Subtype: "timestamptz", SubtypeDiff: "audit_range_diff"},
			changeKey: "subtype_diff",
			want:      " -> audit_range_diff",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Ranges: []goschema.Range{test.target}}
			database := &types.DBSchema{Ranges: []types.DBRange{current}}
			diff := &difftypes.SchemaDiff{}

			compare.Ranges(generated, database, diff, compare.CoverageOf(generated, database))

			c.Assert(diff.RangesAdded, qt.IsNil)
			c.Assert(diff.RangesRemoved, qt.IsNil)
			c.Assert(diff.RangesModified, qt.HasLen, 1)
			c.Assert(diff.RangesModified[0].RangeName, qt.Equals, "audit_range")
			c.Assert(diff.RangesModified[0].Changes[test.changeKey], qt.Equals, test.want)
			c.Assert(diff.RangesModified[0].CurrentSubtype, qt.Equals, "timestamp with time zone")
		})
	}
}

// TestRanges_UnchangedRangeReportsNothing is the no-false-positive control.
//
// It carries two traps a naive comparison falls into. The declared subtype is
// spelled `timestamptz` against the catalog's `timestamp with time zone`, which
// must canonicalize to equal; and the catalog always resolves an operator class
// even when the author named none, so an unconditional comparison of
// subtype_opclass would report a difference on every single run and never
// converge.
func TestRanges_UnchangedRangeReportsNothing(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Ranges: []goschema.Range{
		{Name: "audit_range", Subtype: "timestamptz"},
	}}
	database := &types.DBSchema{Ranges: []types.DBRange{{
		Name:           "audit_range",
		Subtype:        "timestamp with time zone",
		SubtypeOpClass: "timestamptz_ops",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.Ranges(generated, database, diff, compare.CoverageOf(generated, database))

	c.Assert(diff.RangesAdded, qt.IsNil)
	c.Assert(diff.RangesRemoved, qt.IsNil)
	c.Assert(diff.RangesModified, qt.IsNil)
}

// TestRanges_AddAndRemoveStillReported is the non-interference control for the
// behavior that already worked, so adding Modified cannot have replaced it.
func TestRanges_AddAndRemoveStillReported(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Ranges: []goschema.Range{{Name: "fresh", Subtype: "int4"}}}
	database := &types.DBSchema{Ranges: []types.DBRange{{Name: "legacy", Subtype: "int4"}}}
	diff := &difftypes.SchemaDiff{}

	compare.Ranges(generated, database, diff, compare.CoverageOf(generated, database))

	c.Assert(diff.RangesAdded, qt.DeepEquals, []string{"fresh"})
	c.Assert(diff.RangesRemoved, qt.DeepEquals, []string{"legacy"})
	c.Assert(diff.RangesModified, qt.IsNil)
}
