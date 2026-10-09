package ydbplan_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/engine"
)

func coordinationRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	codecs := append(ydbcoordination.Codecs(), ydbdiff.Codecs()...)
	codecs = append(codecs, ydbast.CoordinationCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Declarations: []engine.DeclarationPlanning{{Target: "ydb", Kinds: []schemaext.Kind{ydbcoordination.Kind}, OperationKinds: []schemaext.Kind{ydbast.CoordinationNodeKind}, Service: ydbplan.CoordinationService{}}},
		Planning:     []engine.Planning{{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.CoordinationNodeKind}, OperationKinds: []schemaext.Kind{ydbast.CoordinationNodeKind}, Service: ydbplan.CoordinationService{}}}})
	c.Assert(err, qt.IsNil)
	return runtime
}

func TestCoordinationPlanRetainsCompleteStandaloneOperations(t *testing.T) {
	c := qt.New(t)
	runtime := coordinationRuntime(c)
	before := &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict", SessionGracePeriodMillis: 15000}}
	changes := []schemaext.ChangeRecord{
		{Subject: ydbcoordination.Ref("app", "reset"), Value: &ydbdiff.CoordinationNode{Before: before, After: &ydbcoordination.Desired{}}},
		{Subject: ydbcoordination.Ref("", "created.with.dot"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}},
		{Subject: ydbcoordination.Ref("app", "dropped"), Value: &ydbdiff.CoordinationNode{Before: before}},
	}
	request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Changes: changes}
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 3)
	c.Assert(result.Parents, qt.HasLen, 0)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 3)
	seen := make(map[objectidentity.Key]*ydbast.CoordinationNode)
	for _, step := range plan.Steps {
		c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
		c.Assert(step.Payload.Role, qt.Equals, ast.StatementExtension)
		c.Assert(step.Payload.Parent, qt.Equals, objectidentity.ID{})
		operation := step.Payload.Payload.(*ydbast.CoordinationNode)
		seen[operation.Subject().Key()] = operation
		c.Assert(step.Effects, qt.HasLen, 2)
		c.Assert(step.Effects[1].Subject, qt.Equals, ydbscheme.Path(operation.Schema, operation.Name))
		c.Assert(step.Effects[0].Subject, qt.Equals, operation.Subject())
		c.Assert(step.Impact, qt.DeepEquals, operation.Effect())
	}
	for _, record := range changes {
		c.Assert(&seen[record.Subject.Key()].Change, qt.DeepEquals, record.Value)
	}
	// Input order changes receipts, not the physical operation order or IDs.
	slices.Reverse(request.Changes)
	again, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	reordered, err := plangraph.Schedule(t.Context(), again.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(reordered, qt.DeepEquals, plan)
	before.Spec.ReadConsistencyMode = "mutated"
	c.Assert(seen[ydbcoordination.Ref("app", "reset").Key()].Change.Before.Spec.ReadConsistencyMode, qt.Equals, "strict")
}

func TestCoordinationPlanRefusesWholeBatch(t *testing.T) {
	cases := []struct {
		name   string
		caps   capability.Capabilities
		ref    objectidentity.ID
		change *ydbdiff.CoordinationNode
		code   schemavalidation.Code
	}{
		{name: "missing capability", caps: capability.Capabilities{}, ref: ydbcoordination.Ref("", "second"), change: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, code: schemavalidation.UnsupportedFeature},
		{name: "reserved root lock", caps: capability.YDB262(), ref: ydbcoordination.Ref("", ydbcoordination.LockNode), change: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, code: schemavalidation.InvalidSchema},
		{name: "no effective change", caps: capability.YDB262(), ref: ydbcoordination.Ref("", "second"), change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Defaults()}}, code: schemavalidation.InvalidSchema},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: test.caps, Changes: []schemaext.ChangeRecord{
				{Subject: ydbcoordination.Ref("", "first"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}},
				{Subject: test.ref, Value: test.change},
			}}
			result, err := coordinationRuntime(c).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, test.code)
			c.Assert(result.Err(request), qt.IsNotNil)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

// The feature owner orders against resource identities supplied by the host,
// without importing table AST nodes or pretending the node belongs to a table.
func TestCoordinationPlanTradesSchemePaths(t *testing.T) {
	c := qt.New(t)
	runtime := coordinationRuntime(c)
	createTable := plangraph.StepID{Owner: "ptah.run/ydb", Name: "table/create"}
	dropTable := plangraph.StepID{Owner: "ptah.run/ydb", Name: "table/drop"}
	request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Changes: []schemaext.ChangeRecord{
			{Subject: ydbcoordination.Ref("app", "was-node"), Value: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}},
			{Subject: ydbcoordination.Ref("app", "was-table"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}},
		},
		CommonSteps: []featureplan.CommonStep{
			{ID: createTable, Effects: []plangraph.Effect{{Subject: ydbscheme.Path("app", "was-node"), Action: plangraph.Create}}},
			{ID: dropTable, Effects: []plangraph.Effect{{Subject: ydbscheme.Path("app", "was-table"), Action: plangraph.Drop}}},
		},
	}
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	dropNode, createNode := result.Changes[0].Steps[0], result.Changes[1].Steps[0]
	c.Assert(result.Contributions[0].Dependencies, qt.DeepEquals, []plangraph.Dependency{
		{Before: dropNode, After: createTable}, {Before: dropTable, After: createNode},
	})
	common := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb", Dependencies: []plangraph.Dependency{{Before: createTable, After: dropTable}}}
	for _, step := range request.CommonSteps {
		common.Steps = append(common.Steps, plangraph.Step[featureplan.Operation]{ID: step.ID, Effects: step.Effects})
	}
	plan, err := plangraph.Schedule(t.Context(), append([]plangraph.Contribution[featureplan.Operation]{common}, result.Contributions...)...)
	c.Assert(err, qt.IsNil)
	var ids []plangraph.StepID
	for _, step := range plan.Steps {
		ids = append(ids, step.ID)
	}
	c.Assert(ids, qt.DeepEquals, []plangraph.StepID{dropNode, createTable, dropTable, createNode})
	// Removing the dependencies leaves conflicting uses of each occupied slot.
	result.Contributions[0].Dependencies = nil
	_, err = plangraph.Schedule(t.Context(), append([]plangraph.Contribution[featureplan.Operation]{common}, result.Contributions...)...)
	c.Assert(err, qt.ErrorIs, plangraph.ErrConflict)
}

func TestCoordinationPlanRefusesCompetingPathOccupants(t *testing.T) {
	cases := []struct {
		name   string
		change *ydbdiff.CoordinationNode
		common plangraph.Action
	}{
		{"competing creations", &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, plangraph.Create},
		{"competing drops", &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}, plangraph.Drop},
		{"changing another occupant", &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}}, plangraph.Alter},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
				Changes: []schemaext.ChangeRecord{
					{Subject: ydbcoordination.Ref("", "first"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}},
					{Subject: ydbcoordination.Ref("app", "shared"), Value: test.change},
				},
				CommonSteps: []featureplan.CommonStep{{ID: plangraph.StepID{Owner: "ptah.run/ydb", Name: "other-occupant"}, Effects: []plangraph.Effect{{Subject: ydbscheme.Path("app", "shared"), Action: test.common}}}},
			}
			result, err := coordinationRuntime(c).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(*result.Diagnostics[0].Change, qt.Equals, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, schemavalidation.InvalidSchema)
			c.Assert(result.Err(request), qt.IsNotNil)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestCoordinationPathHandoffKeepsChangesWithTheirOccupant(t *testing.T) {
	cases := []struct {
		name        string
		change      *ydbdiff.CoordinationNode
		replacement plangraph.Action
		want        []string
	}{
		{"node replaces table", &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, plangraph.Drop, []string{"common/alter", "common/replace", "coordination/000000/create"}},
		{"table replaces node", &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}, plangraph.Create, []string{"coordination/000000/drop", "common/replace", "common/alter"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			slot := ydbscheme.Path("app", "shared")
			alterID := plangraph.StepID{Owner: "ptah.run/ydb", Name: "common/alter"}
			replaceID := plangraph.StepID{Owner: "ptah.run/ydb", Name: "common/replace"}
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
				Changes: []schemaext.ChangeRecord{{Subject: ydbcoordination.Ref("app", "shared"), Value: test.change}},
				CommonSteps: []featureplan.CommonStep{
					{ID: alterID, Effects: []plangraph.Effect{{Subject: slot, Action: plangraph.Alter}}},
					{ID: replaceID, Effects: []plangraph.Effect{{Subject: slot, Action: test.replacement}}},
				},
			}
			result, err := coordinationRuntime(c).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			common := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
			for _, step := range request.CommonSteps {
				common.Steps = append(common.Steps, plangraph.Step[featureplan.Operation]{ID: step.ID, Effects: step.Effects})
			}
			plan, err := plangraph.Schedule(t.Context(), append([]plangraph.Contribution[featureplan.Operation]{common}, result.Contributions...)...)
			c.Assert(err, qt.IsNil)
			var names []string
			for _, step := range plan.Steps {
				names = append(names, step.ID.Name)
			}
			c.Assert(names, qt.DeepEquals, test.want)
			// Reversing the required placement produces a cycle, never partial SQL.
			edge := result.Contributions[0].Dependencies[1]
			common.Dependencies = []plangraph.Dependency{{Before: edge.After, After: edge.Before}}
			refused, err := plangraph.Schedule(t.Context(), append([]plangraph.Contribution[featureplan.Operation]{common}, result.Contributions...)...)
			c.Assert(err, qt.ErrorIs, plangraph.ErrCycle)
			c.Assert(refused.Steps, qt.HasLen, 0)
		})
	}
}
