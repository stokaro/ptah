package txrequire_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/txrequire"
)

// TestKind covers the classification stokaro/ptah#1714 needs: the two kinds a
// generator can order into files, and everything else.
//
// The distinction the rows below carry is direction. An enum value addition has
// to be committed BEFORE any statement that uses it and a concurrent index is
// built AFTER the table it indexes, so a caller that treated the two as one
// "non-transactional" bucket would order one of them wrongly.
func TestKind(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want txrequire.AutocommitKind
	}{
		{
			name: "a concurrent index build follows the table",
			node: &ast.IndexNode{Name: "idx_users_email", Concurrently: true},
			want: txrequire.KindConcurrentIndex,
		},
		{
			name: "a concurrent index drop is the same case",
			node: &ast.DropIndexNode{Name: "idx_users_email", Concurrently: true},
			want: txrequire.KindConcurrentIndex,
		},
		{
			name: "an enum value addition leads",
			node: &ast.AlterTypeNode{
				Name:       "status",
				Operations: []ast.TypeOperation{&ast.AddEnumValueOperation{Value: "archived"}},
			},
			want: txrequire.KindEnumValue,
		},
		{
			name: "an ALTER TYPE that adds no value is not the enum case",
			node: &ast.AlterTypeNode{Name: "status"},
			want: txrequire.KindUnsplittable,
		},
		{
			name: "an index built without CONCURRENTLY has no business here",
			node: &ast.IndexNode{Name: "idx_users_email"},
			want: txrequire.KindUnsplittable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(txrequire.Kind(test.node), qt.Equals, test.want)
		})
	}
}

// TestDescribe pins that a refusal can name the statement rather than its kind.
func TestDescribe(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "index", node: &ast.IndexNode{Name: "idx_users_email"}, want: "CREATE INDEX idx_users_email"},
		{name: "index drop", node: &ast.DropIndexNode{Name: "idx_users_email"}, want: "DROP INDEX idx_users_email"},
		{name: "type", node: &ast.AlterTypeNode{Name: "status"}, want: "ALTER TYPE status"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(txrequire.Describe(test.node), qt.Equals, test.want)
		})
	}
}

// TestUnsplittableMixError_NamesTheStatements pins what stokaro/ptah#1714 asked
// for when a plan cannot be split: the message names the statement that forced
// it, not the fact that one exists.
//
// It also names ONLY the unsplittable ones. Listing the concurrent index beside
// the offender would send the operator after a statement both generators handle
// perfectly well.
func TestUnsplittableMixError_NamesTheStatements(t *testing.T) {
	c := qt.New(t)

	err := txrequire.UnsplittableMixError([]ast.Node{
		&ast.IndexNode{Name: "idx_users_email", Concurrently: true},
		&ast.AlterTypeNode{Name: "status"},
	})

	c.Assert(err, qt.ErrorMatches, `(?s).*ALTER TYPE status.*`)
	c.Assert(err, qt.ErrorMatches, `(?s).*Apply that change in its own migration first.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "idx_users_email")
}
