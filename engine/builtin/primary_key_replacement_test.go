package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
)

// TestRenderSQL_AKeyReplacementIsOneStatement writes a node that drops a constraint
// and adds a primary key as one statement, which is the only way CockroachDB takes a
// key's drop. Any other pair stays two statements.
func TestRenderSQL_AKeyReplacementIsOneStatement(t *testing.T) {
	drop := &ast.DropConstraintOperation{ConstraintName: "t_pkey"}
	tests := []struct {
		name string
		add  *ast.ConstraintNode
		want string
	}{
		{
			name: "a primary key",
			add:  ast.NewPrimaryKeyConstraint("code"),
			want: `ALTER TABLE "t" DROP CONSTRAINT "t_pkey", ADD PRIMARY KEY ("code");` + "\n\n",
		},
		{
			name: "a UNIQUE",
			add:  ast.NewUniqueConstraint("t_code", "code"),
			want: `ALTER TABLE "t" DROP CONSTRAINT "t_pkey";` + "\n" + `ALTER TABLE "t" ADD CONSTRAINT "t_code" UNIQUE ("code");` + "\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{drop, &ast.AddConstraintOperation{Constraint: test.add}}}

			got, err := builtin.RenderSQLWithCapabilities("cockroachdb", capability.ForDialect("cockroachdb"), node)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
