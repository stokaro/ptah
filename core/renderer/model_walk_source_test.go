package renderer_test

import (
	goast "go/ast"
	"go/parser"
	"go/token"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaRenderingStreamsTheModelWalk guards the architecture boundary from
// stokaro/ptah#2575: whole-schema rendering consumes the ordered model walk and
// must not rebuild an ast.StatementList through fromschema.FromDatabase.
func TestSchemaRenderingStreamsTheModelWalk(t *testing.T) {
	c := qt.New(t)
	file, err := parser.ParseFile(token.NewFileSet(), "renderer.go", nil, 0)
	c.Assert(err, qt.IsNil)

	var target *goast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*goast.FuncDecl)
		if ok && function.Name.Name == "GetOrderedCreateStatementsWithCapabilities" {
			target = function
			break
		}
	}
	c.Assert(target, qt.IsNotNil)

	walkCalls := 0
	wholeSchemaCollectorCalls := 0
	goast.Inspect(target.Body, func(node goast.Node) bool {
		call, ok := node.(*goast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*goast.SelectorExpr)
		if !ok {
			return true
		}
		packageName, ok := selector.X.(*goast.Ident)
		if !ok || packageName.Name != "fromschema" {
			return true
		}
		switch selector.Sel.Name {
		case "WalkDatabase":
			walkCalls++
		case "FromDatabase":
			wholeSchemaCollectorCalls++
		}
		return true
	})

	c.Assert(walkCalls, qt.Equals, 1)
	c.Assert(wholeSchemaCollectorCalls, qt.Equals, 0)
}
