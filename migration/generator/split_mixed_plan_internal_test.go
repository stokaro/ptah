package generator

// White-box testing required: splitEnumValueAdditionDiff decides what travels
// into the leading no_transaction file, and the decision is not visible in the
// published migrations -- a removal that wrongly moved would produce a file
// carrying only a warning comment, which reads as an ordinary empty result.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
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

	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "users"}},
		EnumsModified: []difftypes.EnumDiff{{
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
	c.Assert(groups.transactional.TablesAdded.Names(), qt.DeepEquals, []string{"users"})
}

// TestSplitConstraintValidationDiff_MovesOnlyTheValidations is the control for
// the validation split: the validations travel to the file that follows, and
// every other change, a table addition here, stays in the transactional diff.
func TestSplitConstraintValidationDiff_MovesOnlyTheValidations(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "users"}},
		ConstraintsValidated: []difftypes.ConstraintValidation{
			{TableName: "ledger", Name: "fk_a"},
			{TableName: "ledger", Name: "fk_b"},
		},
	}

	groups := splitConstraintValidationDiff(diff)

	c.Assert(groups.transactional.TablesAdded, qt.HasLen, 1)
	c.Assert(groups.transactional.ConstraintsValidated, qt.HasLen, 0)
	c.Assert(groups.noTransaction.TablesAdded, qt.HasLen, 0)
	c.Assert(groups.noTransaction.ConstraintsValidated, qt.DeepEquals, diff.ConstraintsValidated)
	c.Assert(diff.ConstraintsValidated, qt.HasLen, 2, qt.Commentf("the input is left as it was"))
}
