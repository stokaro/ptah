package pgtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

func TestRangeCompositionIgnoresSourceHolderNames(t *testing.T) {
	c := qt.New(t)
	left := &pgtype.DesiredRange{Subtype: "numeric", StructName: "First", Cleared: []string{pgtype.RangeSubtypeDiff}}
	right := left.Copy()
	right.StructName = "Second"
	composed, err := left.ComposeDeclaration(right)
	c.Assert(err, qt.IsNil)
	c.Assert(composed, qt.DeepEquals, left)
	composed.(*pgtype.DesiredRange).Cleared[0] = pgtype.RangeCanonical
	c.Assert(left.Cleared, qt.DeepEquals, []string{pgtype.RangeSubtypeDiff})
	c.Assert(right.Cleared, qt.DeepEquals, []string{pgtype.RangeSubtypeDiff})
	c.Assert(right.StructName, qt.Equals, "Second")
}

func TestRangeCompositionRefusesDifferentDefinitions(t *testing.T) {
	for _, test := range []struct {
		name  string
		right schemaext.Value
		want  error
	}{
		{"different subtype", &pgtype.DesiredRange{Subtype: "int8"}, schemaext.ErrDuplicate},
		{"different comment", &pgtype.DesiredRange{Subtype: "numeric", Comment: "other"}, schemaext.ErrDuplicate},
		{"different cleared attributes", &pgtype.DesiredRange{Subtype: "numeric", Cleared: []string{pgtype.RangeSubtypeDiff}}, schemaext.ErrDuplicate},
		{"missing subtype", &pgtype.DesiredRange{}, schemaext.ErrInvalidValue},
		{"another type family", &pgtype.DesiredDomain{BaseType: "numeric"}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			composed, err := (&pgtype.DesiredRange{Subtype: "numeric"}).ComposeDeclaration(test.right)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(composed, qt.IsNil)
		})
	}
}
