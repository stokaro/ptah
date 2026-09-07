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
// must not rebuild an ast.StatementList through modelast.CollectDatabase.
//
// The walk moved into orderedCreateStatements when the omission-reporting entry
// point was added, so that both exported entry points render through one
// implementation rather than two that can drift (stokaro/ptah#2976). The guard
// followed it. Naming the exported function instead would now measure a body
// that only delegates, which is a guard that passes because it looks at nothing.
func TestSchemaRenderingStreamsTheModelWalk(t *testing.T) {
	c := qt.New(t)
	walkCalls, wholeSchemaCollectorCalls := modelCallsIn(c, "orderedCreateStatements")
	c.Assert(walkCalls, qt.Equals, 1)
	c.Assert(wholeSchemaCollectorCalls, qt.Equals, 0)
}

// TestExportedSchemaRendersDelegateTheModelWalk is the other half of that guard.
//
// One implementation streams the walk only while it is the only implementation.
// An exported entry point that grew a second walk would satisfy the test above
// unchanged, so each one is required to reach the model through nothing of its
// own.
func TestExportedSchemaRendersDelegateTheModelWalk(t *testing.T) {
	exported := []struct {
		name     string
		function string
	}{
		{name: "capability aware", function: "GetOrderedCreateStatementsWithCapabilities"},
		{name: "omission reporting", function: "GetOrderedCreateStatementsReportingOmissions"},
	}

	for _, test := range exported {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			walkCalls, wholeSchemaCollectorCalls := modelCallsIn(c, test.function)
			c.Assert(walkCalls, qt.Equals, 0)
			c.Assert(wholeSchemaCollectorCalls, qt.Equals, 0)
		})
	}
}

// modelCallsIn counts the modelast entry points the named function calls.
//
// Both files are parsed because the exported entry points no longer share one:
// a lookup limited to renderer.go would report zero calls for a function it
// never found, which reads exactly like a function that makes none.
func modelCallsIn(c *qt.C, name string) (walkCalls, wholeSchemaCollectorCalls int) {
	c.Helper()

	target := findFunction(c, name)

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
		if !ok || packageName.Name != "modelast" {
			return true
		}
		switch selector.Sel.Name {
		case "WalkDatabase":
			walkCalls++
		case "CollectDatabase":
			wholeSchemaCollectorCalls++
		}
		return true
	})

	return walkCalls, wholeSchemaCollectorCalls
}

// findFunction returns the named top-level function, failing when no file of
// the package declares it.
func findFunction(c *qt.C, name string) *goast.FuncDecl {
	c.Helper()

	for _, path := range []string{"renderer.go", "omissions.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		c.Assert(err, qt.IsNil)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*goast.FuncDecl)
			if ok && function.Name.Name == name {
				return function
			}
		}
	}

	c.Fatalf("no file declares %s, so this guard would measure nothing", name)
	return nil
}
