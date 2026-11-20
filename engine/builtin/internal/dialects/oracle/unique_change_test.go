package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// uniqueChanged is a MODIFY of column a, whose uniqueness changes from
// previous, with changed naming the column's other changed properties.
func uniqueChanged(unique, previous bool, changed ast.ColumnProperties) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
		Column:            &ast.ColumnNode{Name: "a", Type: "VARCHAR2(40)", Nullable: true, Unique: unique},
		PreviousUnique:    previous,
		HasPreviousUnique: true,
		Changed:           changed,
		HasChanged:        true,
	}}}
}

// TestModifyColumn_WritesAChangedUnique_HappyPath renders each change to a
// column's own key. A column that gains one says UNIQUE in its MODIFY, and
// Oracle names the key as it names the UNIQUE a CREATE TABLE declares on the
// column. A column that loses one drops it by the column, since only the
// server knows the name it gave the key; a MODIFY goes first only when the
// column itself changes too. Measured on Oracle Free 23.26: each statement
// applies, and the next plan is empty.
func TestModifyColumn_WritesAChangedUnique_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		unique   bool
		previous bool
		changed  ast.ColumnProperties
		want     string
	}{
		{
			name: "gains its key", unique: true,
			want: "ALTER TABLE t MODIFY (a VARCHAR2(40) UNIQUE);\n",
		},
		{
			name: "gains its key with a new type", unique: true, changed: ast.ColumnProperties{Type: true},
			want: "ALTER TABLE t MODIFY (a VARCHAR2(40) UNIQUE);\n",
		},
		{
			name: "loses its key", previous: true,
			want: "ALTER TABLE t DROP UNIQUE (a);\n",
		},
		{
			name: "loses its key with a new type", previous: true, changed: ast.ColumnProperties{Type: true},
			want: "ALTER TABLE t MODIFY (a VARCHAR2(40));\nALTER TABLE t DROP UNIQUE (a);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered := render(c, capability.Oracle23(), uniqueChanged(test.unique, test.previous, test.changed))

			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

// TestModifyColumn_UnchangedUniqueIsNotRestated is the control: a MODIFY whose
// planner did not say the uniqueness changed writes no UNIQUE and drops no
// key, whatever the column node carries.
func TestModifyColumn_UnchangedUniqueIsNotRestated(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
		Column:     &ast.ColumnNode{Name: "a", Type: "VARCHAR2(40)", Nullable: true, Unique: true},
		Changed:    ast.ColumnProperties{Type: true},
		HasChanged: true,
	}}}

	rendered := render(c, capability.Oracle23(), node)

	c.Assert(rendered, qt.Equals, "ALTER TABLE t MODIFY (a VARCHAR2(40));\n")
}
