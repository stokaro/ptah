package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbscheme"
)

func TestCoordinationDeclarationsCreateCompleteStandaloneDefinitions(t *testing.T) {
	c := qt.New(t)
	value := &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}
	ref := ydbcoordination.Ref("app", "node.with.dot")
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Objects: []schemaext.Object{{Ref: ref, Value: value}}}
	result, err := coordinationRuntime(c).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Declarations, qt.HasLen, 1)
	c.Assert(result.Declarations[0].Subject, qt.Equals, ref)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 1)
	step := plan.Steps[0]
	c.Assert(step.Payload.Role, qt.Equals, ast.StatementExtension)
	c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: ref, Action: plangraph.Create}, {Subject: ydbscheme.Path("app", "node.with.dot"), Action: plangraph.Create}})
	operation := step.Payload.Payload.(*ydbast.CoordinationNode)
	c.Assert(operation.Change.Before, qt.IsNil)
	c.Assert(operation.Change.After, qt.DeepEquals, value)
	value.Spec.ReadConsistencyMode = "relaxed"
	c.Assert(operation.Change.After.Spec.ReadConsistencyMode, qt.Equals, "strict")
}

func TestCoordinationDeclarationsRefuseCommonPathCollision(t *testing.T) {
	c := qt.New(t)
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Objects: []schemaext.Object{
			{Ref: ydbcoordination.Ref("app", "first"), Value: &ydbcoordination.Desired{}},
			{Ref: ydbcoordination.Ref("app", "taken"), Value: &ydbcoordination.Desired{}},
		},
		CommonSteps: []featureplan.CommonStep{{ID: plangraph.StepID{Owner: "example.org/host", Name: "table"},
			Effects: []plangraph.Effect{{Subject: ydbscheme.Path("app", "taken"), Action: plangraph.Create}}}},
	}
	result, err := coordinationRuntime(c).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Declarations, qt.HasLen, 0)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(*result.Diagnostics[0].Object, qt.Equals, 1)
	c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(ydbcoordination.Kind))
}
