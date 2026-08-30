package ast_test

import (
	"fmt"

	"go.5x5.cz/ptah/core/ast"
)

// tableAndIndexNames collects object names from a statement list. Embedding
// [ast.NoopVisitor] satisfies every Visitor method with a silent no-op, so the
// struct overrides only the two node kinds it cares about and keeps compiling
// when Ptah adds node kinds.
type tableAndIndexNames struct {
	ast.NoopVisitor
	names []string
}

func (v *tableAndIndexNames) VisitCreateTable(node *ast.CreateTableNode) error {
	v.names = append(v.names, "table "+node.Name)
	return nil
}

func (v *tableAndIndexNames) VisitIndex(node *ast.IndexNode) error {
	v.names = append(v.names, "index "+node.Name)
	return nil
}

// ExampleNoopVisitor drives a partial visitor over a statement list. Accept
// dispatches each node to the method for its own kind; the EnumNode falls
// through to the embedded NoopVisitor and is skipped without an error — the
// forward-compatibility-for-silence trade the type's documentation describes.
func ExampleNoopVisitor() {
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
