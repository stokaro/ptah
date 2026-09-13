package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer/internal/dialects/mysql"
)

// TestMySQLRenderer_WrapperHandlersReachTheSharedBuffer pins that the handlers
// the MySQL wrapper dispatches to are observable through Output().
//
// The wrapper holds an inner mysqllike renderer and delegates Output() and
// Reset() to it. Declaring `var w bufwriter.Writer`, handing &w to the inner
// renderer and then storing `w` -- a COPY -- on itself makes the wrapper's own
// handlers write into a buffer nothing ever reads: `ptah schema render
// --root-dir ext --dialect mysql` prints "-- Statement 1/2" followed by a blank
// line where the extension comment belongs (stokaro/ptah#931 item 5).
//
// Each row drives the node through Accept, which is the dispatch every caller
// uses, and asserts the rendered text rather than merely that it is non-empty:
// a length check passes on garbage, and the orphaned-buffer defect is exactly
// the kind a length check keeps reporting green. CreateFunctionNode is here as
// the forwarded half of the property -- it reaches the shared renderer, and its
// line has to land in the same buffer.
func TestMySQLRenderer_WrapperHandlersReachTheSharedBuffer(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "ExtensionNode",
			node: &ast.ExtensionNode{Name: "pg_trgm"},
			want: "-- Extension pg_trgm not supported in MySQL",
		},
		{
			name: "DropExtensionNode",
			node: &ast.DropExtensionNode{Name: "pg_trgm"},
			want: "-- DROP EXTENSION pg_trgm not supported in MySQL",
		},
		{
			name: "CreateFunctionNode",
			node: &ast.CreateFunctionNode{
				Name: "touch", Returns: "int", Volatility: "IMMUTABLE", Body: "RETURN 1",
			},
			want: "CREATE FUNCTION `touch`() RETURNS int DETERMINISTIC RETURN 1;",
		},
		{
			name: "CreatePolicyNode",
			node: &ast.CreatePolicyNode{Name: "p1"},
			want: "-- CREATE POLICY p1 not supported in MySQL",
		},
		{
			name: "AlterTableEnableRLSNode",
			node: &ast.AlterTableEnableRLSNode{Table: "users"},
			want: "-- ALTER TABLE users ENABLE ROW LEVEL SECURITY not supported in MySQL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := mysql.New()
			renderer.Reset()

			err := test.node.Accept(renderer)

			c.Assert(err, qt.IsNil)
			c.Assert(renderer.Output(), qt.Not(qt.Equals), "")
			c.Assert(renderer.Output(), qt.Contains, test.want)
		})
	}
}
