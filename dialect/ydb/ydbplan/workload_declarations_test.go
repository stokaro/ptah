package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
)

func workloadDeclarations(objects ...schemaext.Object) featureplan.DeclarationRequest {
	return featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.Capabilities{capability.ResourcePools: true, capability.StreamingQueries: true}, Objects: objects}
}

func TestWorkloadDeclarationsStartQueriesAfterDefaultSettingsAndRouting(t *testing.T) {
	c := qt.New(t)
	request := workloadDeclarations(
		ydbstreaming.DesiredObject("", "query", "Query", ydbstreaming.Spec{Text: "SELECT 1;", ResourcePool: "pool"}, false),
		ydbworkload.DesiredClassifierObject("route", "Route", ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 0}),
		ydbworkload.DesiredPoolObject("default", "Default", ydbworkload.PoolSpec{ResourceWeight: new(0.0)}),
		ydbworkload.DesiredPoolObject("pool", "Pool", ydbworkload.PoolSpec{}),
	)
	result, err := workloadRuntime(c).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Declarations, qt.HasLen, 4)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 4)
	c.Assert(plan.Steps[0].Payload.Payload, qt.DeepEquals, &ydbast.DefaultPoolSettings{Spec: ydbworkload.PoolSpec{ResourceWeight: new(0.0)}})
	c.Assert(plan.Steps[1].Payload.Payload.Kind(), qt.Equals, ydbast.ResourcePoolKind)
	c.Assert(plan.Steps[2].Payload.Payload.Kind(), qt.Equals, ydbast.ResourcePoolClassifierKind)
	c.Assert(plan.Steps[3].Payload.Payload.Kind(), qt.Equals, ydbast.StreamingQueryKind)
	c.Assert(result.Declarations[0].Subject, qt.DeepEquals, request.Objects[0].Ref)
	c.Assert(result.Declarations[0].Steps, qt.DeepEquals, []plangraph.StepID{plan.Steps[3].ID})
	c.Assert(result.Declarations[2].Steps, qt.DeepEquals, []plangraph.StepID{plan.Steps[0].ID})
	*plan.Steps[0].Payload.Payload.(*ydbast.DefaultPoolSettings).Spec.ResourceWeight = 99
	c.Assert(*request.Objects[2].Value.(*ydbworkload.DesiredPool).Spec.ResourceWeight, qt.Equals, 0.0)
}

func TestWorkloadDefaultDeclarationCanBeAnExplicitNoOp(t *testing.T) {
	c := qt.New(t)
	request := workloadDeclarations(ydbworkload.DesiredPoolObject("default", "Default", ydbworkload.PoolSpec{}))
	result, err := workloadRuntime(c).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Declarations, qt.HasLen, 1)
	c.Assert(result.Declarations[0].Steps, qt.HasLen, 0)
	c.Assert(result.Declarations[0].Strategy, qt.Contains, "preserving unspecified")
	request.Capabilities = nil
	result, err = workloadRuntime(c).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Object, qt.DeepEquals, new(0))
	c.Assert(result.Diagnostics[0].Problem.Feature, qt.Equals, string(capability.ResourcePools))
	c.Assert(result.Declarations, qt.HasLen, 0)
}

func TestWorkloadDeclarationRefusalKeepsOriginalObjectIndex(t *testing.T) {
	c := qt.New(t)
	request := workloadDeclarations(
		ydbstreaming.DesiredObject("", "query", "Query", ydbstreaming.Spec{Text: "SELECT 1;"}, false),
		ydbworkload.DesiredPoolObject("pool", "Pool", ydbworkload.PoolSpec{}),
		ydbworkload.DesiredPoolObject("default", "Default", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}),
	)
	result, err := workloadRuntime(c).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Object, qt.DeepEquals, new(2))
	c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(ydbworkload.PoolKind))
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "default pool cannot limit concurrent queries")
	c.Assert(result.Declarations, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 0)
}

func TestWorkloadDeclarationRefusesAnUndeclaredRoutingDestination(t *testing.T) {
	c := qt.New(t)
	request := workloadDeclarations(
		ydbstreaming.DesiredObject("", "query", "Query", ydbstreaming.Spec{Text: "SELECT 1;"}, false),
		ydbworkload.DesiredClassifierObject("route", "Route", ydbworkload.ClassifierSpec{ResourcePool: "missing", Rank: 0}),
	)
	result, err := workloadRuntime(c).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(ydbworkload.ClassifierKind))
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "which is not declared")
	c.Assert(result.Declarations, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 0)
}
