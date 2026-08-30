package diffreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/diffreport"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestCategoriesOmitsASupplementList is stokaro/ptah#2476: a list that
// qualifies another list was reported as a change category of its own, so an
// operator was told two things had changed where one had.
func TestCategoriesOmitsASupplementList(t *testing.T) {
	t.Run("constraint-backed index removal", func(t *testing.T) {
		c := qt.New(t)

		diff := &difftypes.SchemaDiff{
			IndexesRemoved:                []difftypes.IndexRef{{Name: "uq_users_email", TableName: "users"}},
			ConstraintBackedIndexRemovals: []difftypes.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		}

		c.Assert(diffreport.Names(diffreport.Categories(diff)), qt.DeepEquals, []string{"indexes_removed"})
	})

	t.Run("foreign key removed with its table", func(t *testing.T) {
		c := qt.New(t)

		diff := &difftypes.SchemaDiff{
			ConstraintsRemoved:           []difftypes.ConstraintRemovalInfo{{Name: "fk_orders_user", TableName: "orders"}},
			ForeignKeysRemovedWithTables: []difftypes.ForeignKeyRemovalInfo{{Name: "fk_orders_user", TableName: "orders"}},
		}

		c.Assert(diffreport.Names(diffreport.Categories(diff)), qt.DeepEquals, []string{"constraints_removed"})
	})

	t.Run("a supplement alone reports nothing", func(t *testing.T) {
		c := qt.New(t)

		diff := &difftypes.SchemaDiff{
			ConstraintBackedIndexRemovals: []difftypes.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		}

		c.Assert(diffreport.Categories(diff), qt.HasLen, 0)
		c.Assert(diff.HasChanges(), qt.IsFalse)
	})
}

// TestCategoriesKeepsTheListASupplementQualifies is the control. Suppressing
// the qualifier must not suppress what it qualifies -- a report that dropped
// both would pass every assertion above while telling the operator nothing was
// removed at all.
func TestCategoriesKeepsTheListASupplementQualifies(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		IndexesRemoved: []difftypes.IndexRef{{Name: "uq_users_email", TableName: "users"}},
	}

	c.Assert(diffreport.Names(diffreport.Categories(diff)), qt.DeepEquals, []string{"indexes_removed"})
}

// TestIsChangeCategoryAnswersForEveryDeclaredSupplement binds the predicate to
// the declaration it reads: the report must decline exactly the lists the diff
// says are supplements, not a set it decides for itself.
func TestIsChangeCategoryAnswersForEveryDeclaredSupplement(t *testing.T) {
	c := qt.New(t)

	supplements := difftypes.SupplementLists()
	c.Assert(supplements, qt.Not(qt.HasLen), 0)

	for name, base := range supplements {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(reportedCategoryNames(name), qt.HasLen, 0)
			c.Assert(reportedCategoryNames(base), qt.DeepEquals, []string{base})
		})
	}
}

// reportedCategoryNames returns the categories a report prints for a diff whose
// one non-empty list is the one serializing under name.
func reportedCategoryNames(name string) []string {
	field, found := diffFieldByJSONName(name)
	if !found {
		return []string{"no SchemaDiff field serializes as " + name}
	}
	return diffreport.Names(diffreport.Categories(diffWithOnly(field)))
}
