package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

type placedPayload struct{}

func (*placedPayload) Kind() schemaext.Kind                 { return "example.org/placed" }
func (*placedPayload) CloneExtension() ast.ExtensionPayload { return &placedPayload{} }

func TestPlacementOf(t *testing.T) {
	owned := func() ast.AlterOperation { return &ast.ExtensionAlterOperation{Payload: &placedPayload{}} }
	common := func() ast.AlterOperation { return &ast.DropColumnOperation{ColumnName: "old"} }
	tests := []struct {
		name string
		node ast.Node
		want ast.ExtensionPlacement
	}{
		{name: "common statement", node: &ast.DropTableNode{Name: "old"}, want: ast.NoExtension},
		{name: "common alter table", node: &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{common()}}, want: ast.NoExtension},
		{name: "typed nil alter table", node: (*ast.AlterTableNode)(nil), want: ast.NoExtension},
		{name: "typed nil statement list", node: (*ast.StatementList)(nil), want: ast.NoExtension},
		{name: "extension statement", node: &ast.ExtensionStatement{Payload: &placedPayload{}}, want: ast.IsolatedExtension},
		{name: "typed nil extension statement", node: (*ast.ExtensionStatement)(nil), want: ast.IsolatedExtension},
		{name: "standalone alter operation", node: &ast.ExtensionAlterOperation{Payload: &placedPayload{}}, want: ast.IsolatedExtension},
		{name: "alter table with one owner operation", node: &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{owned()}}, want: ast.IsolatedExtension},
		{name: "list of one isolated node", node: &ast.StatementList{Statements: []ast.Node{&ast.ExtensionStatement{Payload: &placedPayload{}}}}, want: ast.IsolatedExtension},
		{name: "owner operation beside a common one", node: &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{common(), owned()}}, want: ast.MixedExtension},
		{name: "two owner operations", node: &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{owned(), owned()}}, want: ast.MixedExtension},
		{name: "list with a common statement", node: &ast.StatementList{Statements: []ast.Node{
			&ast.DropTableNode{Name: "old"}, &ast.ExtensionStatement{Payload: &placedPayload{}},
		}}, want: ast.MixedExtension},
		{name: "list of two owner statements", node: &ast.StatementList{Statements: []ast.Node{
			&ast.ExtensionStatement{Payload: &placedPayload{}}, &ast.ExtensionStatement{Payload: &placedPayload{}},
		}}, want: ast.MixedExtension},
		{name: "list holding a mixed node", node: &ast.StatementList{Statements: []ast.Node{
			&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{common(), owned()}},
		}}, want: ast.MixedExtension},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ast.PlacementOf(tc.node), qt.Equals, tc.want)
		})
	}
}
