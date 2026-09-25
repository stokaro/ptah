package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// NarrowsDeleteAction answers whether the list limits the action. A list naming
// every key column, in any order, says the same as no list.
func TestConstraintNarrowsDeleteAction(t *testing.T) {
	tests := []struct {
		name   string
		listed []string
		want   bool
	}{
		{name: "one of two columns", listed: []string{"b"}, want: true},
		{name: "no list", listed: nil, want: false},
		{name: "every column", listed: []string{"a", "b"}, want: false},
		{name: "every column in another order", listed: []string{"b", "a"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			constraint := schemamodel.Constraint{
				Type: "FOREIGN KEY", Columns: []string{"a", "b"}, OnDelete: "SET NULL", OnDeleteColumns: tt.listed,
			}

			c.Assert(constraint.NarrowsDeleteAction(), qt.Equals, tt.want)
		})
	}
}

// Two unnamed keys that differ only in their ON DELETE column list are two
// declarations, so deduplication keeps both.
func TestDeduplicate_KeepsKeysThatDifferOnlyInTheDeleteColumnList(t *testing.T) {
	c := qt.New(t)
	key := func(listed ...string) schemamodel.Constraint {
		return schemamodel.Constraint{
			StructName: "Child", Table: "children", Type: "FOREIGN KEY",
			Columns: []string{"a", "b"}, ForeignTable: "parents", ForeignColumns: []string{"a", "b"},
			OnDelete: "SET NULL", OnDeleteColumns: listed,
		}
	}
	database := &schemamodel.Database{Constraints: []schemamodel.Constraint{key("a"), key("b")}}

	schemamodel.Deduplicate(database)

	c.Assert(database.Constraints, qt.HasLen, 2)
}
