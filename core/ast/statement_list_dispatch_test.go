package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestStatementListAccept_HandsTheVisitorTheListItself pins that a list is one
// node like any other: the visitor is handed the list, by pointer, and no
// statement in it reaches the visitor unless the visitor goes and gets it. That
// is what lets a renderer prepare every statement before it emits any.
func TestStatementListAccept_HandsTheVisitorTheListItself(t *testing.T) {
	c := qt.New(t)

	list := &ast.StatementList{Statements: []ast.Node{
		&ast.CreateTableNode{Name: "users"},
		&ast.IndexNode{Name: "idx_users_email"},
	}}
	visitor := &censusVisitor{}

	c.Assert(list.Accept(visitor), qt.IsNil)

	c.Assert(methodsCalled(visitor.calls), qt.DeepEquals, []string{"VisitStatementList"})
	c.Assert(visitor.calls[0].node, qt.Equals, any(list))
}

// TestStatementListAccept_DescendingVisitorSeesEachStatementInOrder pins the
// other half: a visitor that walks the list reaches every statement in
// declaration order, and the list carries no order of its own beyond the one it
// was built with.
func TestStatementListAccept_DescendingVisitorSeesEachStatementInOrder(t *testing.T) {
	c := qt.New(t)

	list := &ast.StatementList{Statements: []ast.Node{
		&ast.CreateTableNode{Name: "users"},
		&ast.IndexNode{Name: "idx_users_email"},
		&ast.CommentNode{Text: "seeded"},
	}}
	visitor := &MockVisitor{}

	c.Assert(list.Accept(visitor), qt.IsNil)

	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{
		"CreateTable:users",
		"Index:idx_users_email",
		"Comment:seeded",
	})
}

// TestStatementListAccept_FailurePath_AStatementRefusalReachesTheCaller pins
// that Accept returns what the walk returned, unchanged. A caller matching a
// refusal against the whole message reads the refusing statement's error and
// nothing the list added to it.
func TestStatementListAccept_FailurePath_AStatementRefusalReachesTheCaller(t *testing.T) {
	c := qt.New(t)

	list := &ast.StatementList{Statements: []ast.Node{
		&ast.CreateTableNode{Name: "users"},
		&ast.IndexNode{Name: "idx_users_email"},
	}}
	visitor := &MockVisitor{ReturnError: true}

	err := list.Accept(visitor)

	c.Assert(err, qt.ErrorMatches, "error visiting statement: mock error")
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"CreateTable:users"})
}
