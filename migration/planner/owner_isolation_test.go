package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/migration/planner"
)

type ownerOperation struct{}

func (*ownerOperation) Kind() schemaext.Kind                 { return "example.org/owner-operation" }
func (*ownerOperation) CloneExtension() ast.ExtensionPayload { return &ownerOperation{} }

// nodesPlanner is a third-party planner that plans the nodes it was given.
type nodesPlanner struct{ nodes []ast.Node }

func (p nodesPlanner) GenerateMigrationAST(context.Context, featureplan.Runtime, *difftypes.SchemaDiff) ([]ast.Node, error) {
	return p.nodes, nil
}

func nodesRuntime(c *qt.C, nodes ...ast.Node) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "example.org/owner", Targets: []engine.Target{{Name: "owner", Planner: func(planext.Options) planext.Planner { return nodesPlanner{nodes: nodes} }}}})
	c.Assert(err, qt.IsNil)
	return runtime
}

// Safety reports give each statement the verdict of the owner operation that
// rendered it, which holds only when the operation is a node of its own. A
// plan that puts one beside other work is refused at the planning boundary,
// whichever planner built it.
func TestGenerateSchemaDiffAST_FailurePath_RefusesAnOwnerOperationSharingANode(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{name: "alter table", node: &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
			&ast.DropColumnOperation{ColumnName: "old"}, &ast.ExtensionAlterOperation{Payload: &ownerOperation{}},
		}}},
		{name: "two owner operations", node: &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{
			&ast.ExtensionAlterOperation{Payload: &ownerOperation{}}, &ast.ExtensionAlterOperation{Payload: &ownerOperation{}},
		}}},
		{name: "statement list", node: &ast.StatementList{Statements: []ast.Node{
			&ast.DropTableNode{Name: "old"}, &ast.ExtensionStatement{Payload: &ownerOperation{}},
		}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := nodesRuntime(c, tc.node)
			nodes, err := planner.GenerateSchemaDiffASTWithOptions(t.Context(), runtime, &difftypes.SchemaDiff{}, "owner", planext.Options{})
			c.Assert(err, qt.ErrorIs, planner.ErrInvalidPlan)
			c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrInvalidSchemaDiff, qt.Commentf("a planner defect, not a defect of the diff"))
			c.Assert(err, qt.ErrorMatches, `(?s).*planned node 1 .* carries an owner operation beside other operations.*`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffAST_HappyPath_AcceptsIsolatedOwnerOperations(t *testing.T) {
	c := qt.New(t)
	isolated := []ast.Node{
		ast.NewComment("owner note"),
		&ast.ExtensionStatement{Payload: &ownerOperation{}},
		&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: &ownerOperation{}}}},
		&ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "old"}}},
	}
	runtime := nodesRuntime(c, isolated...)
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(t.Context(), runtime, &difftypes.SchemaDiff{}, "owner", planext.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, len(isolated))
}
