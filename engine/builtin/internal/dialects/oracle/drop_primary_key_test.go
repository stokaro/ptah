package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// TestDropConstraint_WritesAPrimaryKeyDrop renders the removal of a table's
// primary key. The key Oracle named for a column has a name no declaration
// knows, so a drop that names none is DROP PRIMARY KEY, which finds the key.
// A key that has a name is dropped by it. Measured on Oracle Free 23.26: both
// statements apply.
func TestDropConstraint_WritesAPrimaryKeyDrop(t *testing.T) {
	tests := []struct {
		name      string
		operation *ast.DropConstraintOperation
		want      string
	}{
		{
			name:      "a key with no name",
			operation: &ast.DropConstraintOperation{PrimaryKey: true},
			want:      "ALTER TABLE t DROP PRIMARY KEY;\n",
		},
		{
			name:      "a key with a name",
			operation: &ast.DropConstraintOperation{PrimaryKey: true, ConstraintName: "pk_t"},
			want:      "ALTER TABLE t DROP CONSTRAINT pk_t;\n",
		},
		{
			name:      "a constraint that is not the key",
			operation: &ast.DropConstraintOperation{ConstraintName: "uq_t"},
			want:      "ALTER TABLE t DROP CONSTRAINT uq_t;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{test.operation}}

			rendered := render(c, capability.Oracle23(), node)

			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}
