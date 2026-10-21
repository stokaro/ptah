package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

type brokenClone struct {
	kind  schemaext.Kind
	clone ast.ExtensionPayload
}

func (p *brokenClone) Kind() schemaext.Kind                 { return p.kind }
func (p *brokenClone) CloneExtension() ast.ExtensionPayload { return p.clone }

func TestCloneExtensionPayload_RefusesInvalidClone(t *testing.T) {
	const kind schemaext.Kind = "example.org/feature"
	tests := []struct {
		name  string
		clone ast.ExtensionPayload
	}{
		{name: "nil"},
		{name: "typed nil", clone: (*brokenClone)(nil)},
		{name: "changed kind", clone: &brokenClone{kind: "example.org/other"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cloned, err := ast.CloneExtensionPayload(&brokenClone{kind: kind, clone: test.clone})
			c.Assert(err, qt.ErrorMatches, `extension "example.org/feature" returned an invalid clone`)
			c.Assert(cloned, qt.IsNil)
		})
	}
}

func TestOwnerTransactionsPreservesOrderAndUnknownEnvelopes(t *testing.T) {
	c := qt.New(t)
	node := &ast.StatementList{Statements: []ast.Node{
		&ast.ExtensionStatement{Transaction: plangraph.TransactionRequired},
		&ast.AlterTableNode{Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{},
			&ast.ExtensionAlterOperation{Transaction: plangraph.TransactionForbidden},
			(*ast.ExtensionAlterOperation)(nil),
		}},
		&ast.StatementList{Statements: []ast.Node{(*ast.ExtensionStatement)(nil)}},
	}}
	want := []plangraph.Transaction{plangraph.TransactionRequired, plangraph.TransactionForbidden, "", ""}
	got := ast.OwnerTransactions(node)
	c.Assert(got, qt.DeepEquals, want)
	c.Assert(ast.OwnerOperations(node), qt.HasLen, len(want))
	got[0] = plangraph.TransactionAllowed
	c.Assert(ast.OwnerTransactions(node), qt.DeepEquals, want)
	c.Assert(ast.OwnerTransactions(&ast.CreateTableNode{}), qt.HasLen, 0)
}
