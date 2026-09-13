package ast_test

import (
	"fmt"

	"ptah.run/core/ast"
)

// tableAndIndexNames collects object names from a statement list.
//
// One method, and the kinds it does not collect need no mention. A visitor that
// ignores what it does not recognize is the analysis case; a renderer is the
// other one, and answers an unrecognized node with an error, because silence
// there is indistinguishable from a deliberate skip.
type tableAndIndexNames struct {
	names []string
}

func (v *tableAndIndexNames) VisitNode(node ast.Node) error {
	switch n := node.(type) {
	case *ast.StatementList:
		for _, statement := range n.Statements {
			if err := statement.Accept(v); err != nil {
				return err
			}
		}
	case *ast.CreateTableNode:
		v.names = append(v.names, "table "+n.Name)
	case *ast.IndexNode:
		v.names = append(v.names, "index "+n.Name)
	}
	return nil
}

// ExampleVisitor drives a partial visitor over a statement list.
//
// Accept hands the visitor the node it was called on and nothing else, so the
// list is walked here rather than by the AST. That is what lets one visitor
// count only top-level statements and another descend into every nested list,
// without the AST choosing for either.
func ExampleVisitor() {
	schema := &ast.StatementList{Statements: []ast.Node{
		ast.NewEnum("user_status", "active", "suspended"),
		ast.NewCreateTable("users"),
		ast.NewIndex("idx_users_email", "users", "email"),
	}}

	visitor := &tableAndIndexNames{}
	if err := schema.Accept(visitor); err != nil {
		fmt.Println(err)
		return
	}
	for _, name := range visitor.names {
		fmt.Println(name)
	}

	// Output:
	// table users
	// index idx_users_email
}

// ExampleVisitorFunc collects the same names without declaring a type.
//
// The closure holds the state a struct would carry, and the walk is the same
// one: a visitor decides for itself whether to descend into a list.
func ExampleVisitorFunc() {
	schema := &ast.StatementList{Statements: []ast.Node{
		ast.NewCreateTable("users"),
		ast.NewIndex("idx_users_email", "users", "email"),
	}}

	var names []string
	var collect ast.VisitorFunc
	collect = func(node ast.Node) error {
		switch n := node.(type) {
		case *ast.StatementList:
			for _, statement := range n.Statements {
				if err := statement.Accept(collect); err != nil {
					return err
				}
			}
		case *ast.CreateTableNode:
			names = append(names, "table "+n.Name)
		case *ast.IndexNode:
			names = append(names, "index "+n.Name)
		}
		return nil
	}

	if err := schema.Accept(collect); err != nil {
		fmt.Println(err)
		return
	}
	for _, name := range names {
		fmt.Println(name)
	}

	// Output:
	// table users
	// index idx_users_email
}
