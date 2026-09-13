package mariadb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer/internal/dialects/mariadb"
)

// TestMariaDBRenderer_WrapperHandlersReachTheSharedBuffer is the MariaDB half of
// the MySQL test of the same name; see that file for why an orphaned buffer made
// every handler render nothing (stokaro/ptah#931 item 5).
func TestMariaDBRenderer_WrapperHandlersReachTheSharedBuffer(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "ExtensionNode",
			node: &ast.ExtensionNode{Name: "pg_trgm"},
			want: "-- Extension pg_trgm not supported in MariaDB",
		},
		{
			name: "DropExtensionNode",
			node: &ast.DropExtensionNode{Name: "pg_trgm"},
			want: "-- DROP EXTENSION pg_trgm not supported in MariaDB",
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
			want: "-- CREATE POLICY p1 not supported in MariaDB",
		},
		{
			name: "AlterTableEnableRLSNode",
			node: &ast.AlterTableEnableRLSNode{Table: "users"},
			want: "-- ALTER TABLE users ENABLE ROW LEVEL SECURITY not supported in MariaDB",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := mariadb.New()
			renderer.Reset()

			err := test.node.Accept(renderer)

			c.Assert(err, qt.IsNil)
			c.Assert(renderer.Output(), qt.Not(qt.Equals), "")
			c.Assert(renderer.Output(), qt.Contains, test.want)
		})
	}
}
