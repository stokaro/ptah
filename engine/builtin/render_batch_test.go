package builtin_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/engine/builtin"
)

func TestRenderBatchRefusalIdentifiesTheOriginalInput(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	node := ast.NewColumn("", "INTEGER")
	result, err := runtime.Render(t.Context(), renderer.Request{Target: "postgres", Nodes: []ast.Node{
		ast.NewRawSQL("SELECT 1;"), node,
	}})
	c.Assert(result, qt.DeepEquals, renderer.Result{})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	var refused *renderer.BatchRefusalError
	c.Assert(err, qt.ErrorAs, &refused)
	c.Assert(refused.Diagnostics, qt.HasLen, 1)
	c.Assert(*refused.Diagnostics[0].Input, qt.Equals, 1)
	var rendering *ptaherr.RenderError
	c.Assert(err, qt.ErrorAs, &rendering)
	c.Assert(rendering.Node, qt.Equals, node)
}

type failingRenderNode struct {
	ast.Node
	err error
}

func (n failingRenderNode) Accept(ast.Visitor) error { return n.err }

func TestRenderBatchDoesNotClassifyExecutionFailureAsARefusal(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	failure := errors.New("node execution failed")
	result, err := runtime.Render(t.Context(), renderer.Request{Target: "postgres", Nodes: []ast.Node{
		ast.NewRawSQL("SELECT 1;"), failingRenderNode{err: failure},
	}})
	c.Assert(result, qt.DeepEquals, renderer.Result{})
	c.Assert(err, qt.ErrorIs, failure)
	_, refused := errors.AsType[*renderer.BatchRefusalError](err)
	c.Assert(refused, qt.IsFalse)
}

func TestRenderBatchPreservesEmptyAndNestedNodeBoundaries(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	result, err := runtime.Render(t.Context(), renderer.Request{Target: "postgres", Nodes: []ast.Node{
		ast.NewRawSQL("SELECT 1;"),
		&ast.StatementList{},
		&ast.StatementList{Statements: []ast.Node{ast.NewRawSQL("SELECT 2;"), ast.NewRawSQL("SELECT 3;")}},
		ast.NewRawSQL("SELECT 4;"),
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Fragments, qt.DeepEquals, []string{"SELECT 1;\n", "", "SELECT 2;\nSELECT 3;\n", "SELECT 4;\n"})
	c.Assert(result.SQL(), qt.Equals, "SELECT 1;\nSELECT 2;\nSELECT 3;\nSELECT 4;\n")
}

func TestRenderBatchReportsOmissionsWithoutLeakingBetweenCalls(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	table := &ast.CreateTableNode{
		Name: "accounts",
		Columns: []*ast.ColumnNode{
			ast.NewColumn("id", "BIGINT").SetPrimary(),
			{Name: "email", Type: "VARCHAR(255)", Nullable: true},
		},
	}
	request := renderer.Request{Target: "ydb", Capabilities: capability.YDB262(), Nodes: []ast.Node{table}}
	result, err := runtime.Render(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Omissions, qt.HasLen, 1)
	c.Assert(result.Omissions[0].Dialect, qt.Equals, "ydb")
	c.Assert(result.Omissions[0].Name, qt.Equals, "accounts.email")
	c.Assert(result.Omissions[0].Property, qt.Equals, "type modifier")
	c.Assert(result.Omissions[0].Detail, qt.Equals, "VARCHAR(255): length 255")
	c.Assert(result.SQL(), qt.Contains, "`email` Utf8")

	sql, omissions, err := builtin.RenderSQLReportingOmissions(request.Target, request.Capabilities, table)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, result.SQL())
	c.Assert(omissions, qt.DeepEquals, result.Omissions)

	request.Nodes = []ast.Node{ast.NewRawSQL("SELECT 1;")}
	again, err := runtime.Render(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(again.Omissions, qt.HasLen, 0)
	c.Assert(again.SQL(), qt.Equals, "SELECT 1;\n")
}
