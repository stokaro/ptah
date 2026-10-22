//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
)

func TestProviderNativeUnitsRemainTypedAcrossProcess(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	create := capturedOperation(c, runtime, "records", `{"action":"create"}`)
	common := &ast.CreateTableNode{Name: "common", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER", Primary: true}}}
	request := renderer.Request{Target: "fixture", Capabilities: unwrappedOperationTarget(), Nodes: []ast.Node{common, &ast.ExtensionStatement{Payload: create, Transaction: plangraph.TransactionForbidden}}}
	result, err := runtime.Render(c.Context(), request)
	c.Assert(err, qt.IsNil)
	plan := planner.RenderedPlan{Request: request, Result: result, Dialect: "fixture"}
	units, err := plan.Units()
	c.Assert(err, qt.IsNil)
	c.Assert(units, qt.HasLen, 2)
	c.Assert(units[0].Fragment.SQL, qt.Equals, `CREATE TABLE "common" ("id" INTEGER NOT NULL PRIMARY KEY);`)
	c.Assert(units[1].Fragment.SQL, qt.Equals, "")
	c.Assert(units[1].Node, qt.Equals, 1)
	c.Assert(units[1].Fragment.Native.Kind(), qt.Equals, operationModel)
	c.Assert(ast.OwnerTransactions(request.Nodes[units[1].Node]), qt.DeepEquals, []plangraph.Transaction{plangraph.TransactionForbidden})
	verdicts, err := safety.OwnerVerdicts(request.Nodes, []int{units[0].Node, units[1].Node})
	c.Assert(err, qt.IsNil)
	c.Assert(verdicts[1].NodeType, qt.Equals, "*ast.ExtensionStatement")
	c.Assert(verdicts[1].Severity, qt.Equals, safety.Safe)
	c.Assert(verdicts[1].Access, qt.Equals, schemaext.AccessUnchanged)
	sql, err := plan.SQL()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(sql, qt.Equals, "")
	statements, err := plan.Statements()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(statements, qt.IsNil)
	original, err := runtime.Codecs().Encode(c.Context(), schemaext.Operation, []schemaext.Payload{create})
	c.Assert(err, qt.IsNil)
	rendered, err := runtime.Codecs().Encode(c.Context(), schemaext.Operation, []schemaext.Payload{units[1].Fragment.Native})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.DeepEquals, original)
	restored, err := runtime.Codecs().Decode(c.Context(), rendered)
	c.Assert(err, qt.IsNil)
	c.Assert(restored[0].(schemaext.EffectSource).Effect(), qt.DeepEquals, create.(schemaext.EffectSource).Effect())
	host.Close()
	cloned, err := units[1].Fragment.Clone()
	c.Assert(err, qt.IsNil)
	c.Assert(cloned.Native.(schemaext.EffectSource).Effect(), qt.DeepEquals, create.(schemaext.EffectSource).Effect())
}
