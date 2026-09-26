package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
)

// A PostgreSQL DROP TRIGGER takes the function Ptah generated for the trigger
// with it, and leaves a function the trigger only runs.
func TestRenderSQL_DropTriggerFunction_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.DropTriggerNode
		want string
	}{
		{
			name: "a generated function goes with its trigger",
			node: ast.NewDropTrigger("owned", "users").SetIfExists().SetFunctionName("ptah_trigger_users_owned"),
			want: "DROP TRIGGER IF EXISTS \"owned\" ON \"users\";\nDROP FUNCTION IF EXISTS \"ptah_trigger_users_owned\"();\n",
		},
		{
			name: "a declared function stays",
			node: ast.NewDropTrigger("shared", "users").SetIfExists().SetExternalFunction(),
			want: "DROP TRIGGER IF EXISTS \"shared\" ON \"users\";\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL("postgres", test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
