package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestPostgres_AlterTable_AlterConstraint names only the properties the
// operation changes: a statement that says nothing about deferrability must not
// reset it.
func TestPostgres_AlterTable_AlterConstraint(t *testing.T) {
	deferrable, notDeferrable := true, false
	tests := []struct {
		name string
		op   *ast.AlterConstraintOperation
		want string
	}{
		{
			name: "deferrable and deferred",
			op:   &ast.AlterConstraintOperation{ConstraintName: "fk_c", Deferrable: &deferrable, Initially: "deferred"},
			want: `ALTER TABLE orders ALTER CONSTRAINT fk_c DEFERRABLE INITIALLY DEFERRED;`,
		},
		{
			name: "not deferrable",
			op:   &ast.AlterConstraintOperation{ConstraintName: "fk_c", Deferrable: &notDeferrable},
			want: `ALTER TABLE orders ALTER CONSTRAINT fk_c NOT DEFERRABLE;`,
		},
		{
			name: "timing alone",
			op:   &ast.AlterConstraintOperation{ConstraintName: "fk_c", Initially: "immediate"},
			want: `ALTER TABLE orders ALTER CONSTRAINT fk_c INITIALLY IMMEDIATE;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			alter := &ast.AlterTableNode{Name: "orders", Operations: []ast.AlterOperation{test.op}}

			qt.New(t).Assert(legacyPostgresSQL(renderPG(t, alter)), qt.Contains, test.want)
		})
	}
}
