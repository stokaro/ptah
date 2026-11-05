package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
)

// TestDropFunctionStatement_NamesEveryRoutine pins the statement that drops
// routines together, each addressed as its own argument list says.
func TestDropFunctionStatement_NamesEveryRoutine(t *testing.T) {
	none, arguments := "", "n integer"
	tests := []struct {
		name string
		node *ast.DropFunctionNode
		want string
	}{
		{
			name: "one routine",
			node: &ast.DropFunctionNode{Name: "a_fn", Parameters: &none},
			want: "DROP FUNCTION \"a_fn\"();\n",
		},
		{
			name: "two routines",
			node: &ast.DropFunctionNode{Name: "a_fn", Parameters: &none, Others: []ast.DropFunctionTarget{{Name: "app.b_fn", Parameters: &arguments}}},
			want: "DROP FUNCTION \"a_fn\"(), \"app\".\"b_fn\"(n integer);\n",
		},
		{
			name: "a second routine dropped by name alone",
			node: &ast.DropFunctionNode{Name: "a_fn", Parameters: &none, Others: []ast.DropFunctionTarget{{Name: "b_fn"}}},
			want: "DROP FUNCTION \"a_fn\"(), \"b_fn\";\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL("postgres", test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
