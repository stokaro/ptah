package generator

// White-box testing required: splitEnumValueAdditionDiff decides what travels
// into the leading no_transaction file, and the decision is not visible in the
// published migrations -- a removal that wrongly moved would produce a file
// carrying only a warning comment, which reads as an ordinary empty result.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestSplitEnumValueAdditionDiff_MovesOnlyTheAdditions is the control for the
// half of the split that decides what travels.
//
// A removal stays behind deliberately: PostgreSQL cannot execute one, so the
// planner writes a warning comment for it, and moving that comment into a
// no_transaction file of its own would separate it from the change it
// documents while producing a migration that does nothing.
func TestSplitEnumValueAdditionDiff_MovesOnlyTheAdditions(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded: []string{"users"},
		EnumsModified: []types.EnumDiff{{
			EnumName:      "status",
			ValuesAdded:   []string{"archived"},
			ValuesRemoved: []string{"draft"},
		}},
	}

	groups := splitEnumValueAdditionDiff(diff)

	c.Assert(groups.noTransaction.EnumsModified, qt.HasLen, 1)
	c.Assert(groups.noTransaction.EnumsModified[0].ValuesAdded, qt.DeepEquals, []string{"archived"})
	c.Assert(groups.noTransaction.EnumsModified[0].ValuesRemoved, qt.IsNil)
	c.Assert(groups.noTransaction.TablesAdded, qt.IsNil)

	c.Assert(groups.transactional.EnumsModified, qt.HasLen, 1)
	c.Assert(groups.transactional.EnumsModified[0].ValuesRemoved, qt.DeepEquals, []string{"draft"})
	c.Assert(groups.transactional.EnumsModified[0].ValuesAdded, qt.IsNil)
	c.Assert(groups.transactional.TablesAdded, qt.DeepEquals, []string{"users"})
}
