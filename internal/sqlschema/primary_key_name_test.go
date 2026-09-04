package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/sqlschema"
)

// TestConvertTable_CarriesThePrimaryKeyName is the hop where the name was lost.
//
// Everything else about the constraint was carried across -- its columns, its
// parts, its INCLUDE list -- and the one field that identifies it was not. A
// declaration naming its key therefore reached the renderer with nothing to
// render, so the inline collapse was a second symptom rather than the cause
// (stokaro/ptah#2180).
func TestConvertTable_CarriesThePrimaryKeyName(t *testing.T) {
	tests := []struct {
		name       string
		constraint *ast.ConstraintNode
		want       string
	}{
		{
			name:       "a named key",
			constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Name: "c_pk", Columns: []string{"b"}},
			want:       "c_pk",
		},
		{
			name:       "a named composite key",
			constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Name: "c_comp_pk", Columns: []string{"a", "b"}},
			want:       "c_comp_pk",
		},
		{
			// The control: a key with no name must not acquire one.
			name:       "an unnamed key",
			constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"b"}},
			want:       "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := &ast.CreateTableNode{
				Name: "t",
				Columns: []*ast.ColumnNode{
					{Name: "a", Type: "INTEGER"},
					{Name: "b", Type: "INTEGER"},
				},
				Constraints: []*ast.ConstraintNode{test.constraint},
			}

			converted := sqlschema.ToTable(table, "")

			c.Assert(converted.PrimaryKeyName, qt.Equals, test.want)
			// The columns still come across, so a conversion that carried the
			// name and dropped everything else would fail here.
			c.Assert(converted.PrimaryKey, qt.DeepEquals, test.constraint.Columns)
		})
	}
}
