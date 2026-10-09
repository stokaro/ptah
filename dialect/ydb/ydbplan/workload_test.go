package ydbplan_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine"
)

func workloadRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	codecs := append(ydbworkload.Codecs(), ydbstreaming.Codecs()...)
	codecs = append(codecs, ydbdiff.ResourcePoolCodec(), ydbdiff.ResourcePoolClassifierCodec(), ydbdiff.StreamingCodec(),
		ydbast.ResourcePoolCodec(), ydbast.ResourcePoolClassifierCodec(), ydbast.StreamingCodec(), ydbast.DefaultPoolSettingsCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Planning: []engine.Planning{{Target: "ydb",
			Kinds:          []schemaext.Kind{ydbdiff.ResourcePoolKind, ydbdiff.ResourcePoolClassifierKind, ydbdiff.StreamingQueryKind},
			OperationKinds: []schemaext.Kind{ydbast.ResourcePoolKind, ydbast.ResourcePoolClassifierKind, ydbast.StreamingQueryKind},
			Service:        ydbplan.WorkloadStreamingService{},
		}},
		Declarations: []engine.DeclarationPlanning{{Target: "ydb",
			Kinds:          []schemaext.Kind{ydbworkload.PoolKind, ydbworkload.ClassifierKind, ydbstreaming.Kind},
			OperationKinds: []schemaext.Kind{ydbast.ResourcePoolKind, ydbast.ResourcePoolClassifierKind, ydbast.StreamingQueryKind, ydbast.DefaultPoolSettingsKind},
			Service:        ydbplan.WorkloadStreamingService{},
		}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func workloadRequest(changes ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.Capabilities{capability.ResourcePools: true, capability.StreamingQueries: true}, Changes: changes}
}

func workloadClassifierMove(name string, before, after int64) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbworkload.ClassifierRef(name), Value: &ydbdiff.ResourcePoolClassifier{
		Before: &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: before}},
		After:  &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: after}},
	}}
}

func TestWorkloadRankSwapReleasesBothRanksBeforeRecreation(t *testing.T) {
	c := qt.New(t)
	request := workloadRequest(workloadClassifierMove("beta", 20, 10), workloadClassifierMove("alpha", 10, 20))
	result, err := workloadRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 4)
	var names []string
	var operations []ydbast.PoolOperation
	for _, step := range plan.Steps {
		operation := step.Payload.Payload.(*ydbast.ResourcePoolClassifier)
		names = append(names, operation.Name)
		operations = append(operations, operation.Operation)
		c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
		c.Assert(step.Effects, qt.HasLen, 2)
		c.Assert(step.Impact.Impact, qt.Equals, schemaext.Behavioral)
	}
	c.Assert(names, qt.DeepEquals, []string{"alpha", "beta", "alpha", "beta"})
	c.Assert(operations, qt.DeepEquals, []ydbast.PoolOperation{ydbast.PoolDrop, ydbast.PoolDrop, ydbast.PoolCreate, ydbast.PoolCreate})
	c.Assert(result.Changes[0].Subject, qt.DeepEquals, request.Changes[0].Subject)
	c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{plan.Steps[1].ID, plan.Steps[3].ID})
	reversed, err := workloadRuntime(c).PlanFeatures(t.Context(), workloadRequest(request.Changes[1], request.Changes[0]))
	c.Assert(err, qt.IsNil)
	c.Assert(reversed.Contributions, qt.DeepEquals, result.Contributions)
}

func TestWorkloadFreeRankMoveAndLimitResetKeepBothOperands(t *testing.T) {
	c := qt.New(t)
	before := ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(5))}
	after := ydbworkload.PoolSpec{ResourceWeight: new(0.0)}
	request := workloadRequest(workloadClassifierMove("classifier", 10, 30), schemaext.ChangeRecord{
		Subject: ydbworkload.PoolRef("pool"), Value: &ydbdiff.ResourcePool{
			Before: &ydbworkload.ObservedPool{Spec: before}, After: &ydbworkload.DesiredPool{Spec: after},
		},
	})
	result, err := workloadRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 2)
	pool := plan.Steps[0].Payload.Payload.(*ydbast.ResourcePool)
	c.Assert(pool.Operation, qt.Equals, ydbast.PoolAlter)
	c.Assert(pool.Spec.ConcurrentQueryLimit, qt.IsNil)
	c.Assert(pool.Spec.ResourceWeight, qt.DeepEquals, new(0.0))
	c.Assert(pool.Previous.ConcurrentQueryLimit, qt.DeepEquals, new(int32(5)))
	classifier := plan.Steps[1].Payload.Payload.(*ydbast.ResourcePoolClassifier)
	c.Assert(classifier.Operation, qt.Equals, ydbast.PoolAlter)
	c.Assert(classifier.Previous.Rank, qt.Equals, int64(10))
	c.Assert(classifier.Spec.Rank, qt.Equals, int64(30))
	*pool.Spec.ResourceWeight, *pool.Previous.ConcurrentQueryLimit = 90, 99
	c.Assert(*after.ResourceWeight, qt.Equals, 0.0)
	c.Assert(*before.ConcurrentQueryLimit, qt.Equals, int32(5))
}

func TestWorkloadStreamingStopsAroundPoolAndPrincipalChanges(t *testing.T) {
	c := qt.New(t)
	request := workloadRequest(schemaext.ChangeRecord{Subject: ydbworkload.PoolRef("pool"), Value: &ydbdiff.ResourcePool{
		After: &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}},
	}}, schemaext.ChangeRecord{Subject: ydbstreaming.Ref("", "query"), Value: &ydbdiff.StreamingQuery{
		Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}},
		After:  &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 2;"}, AllowStateReset: true},
	}}, workloadClassifierMove("classifier", 10, 30))
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	request.CommonSteps = []featureplan.CommonStep{
		{ID: plangraph.StepID{Owner: "ptah.run/ydb", Name: "create-user"}, Effects: []plangraph.Effect{{Subject: builder.Role("new.user"), Action: plangraph.Create}}},
		{ID: plangraph.StepID{Owner: "ptah.run/ydb", Name: "drop-user"}, Effects: []plangraph.Effect{{Subject: builder.Role("old.user"), Action: plangraph.Drop}}},
	}
	result, err := workloadRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	common := plangraph.Contribution[featureplan.Operation]{Owner: "ptah.run/ydb"}
	for _, step := range request.CommonSteps {
		common.Steps = append(common.Steps, plangraph.Step[featureplan.Operation]{ID: step.ID, Effects: step.Effects})
	}
	plan, err := plangraph.Schedule(t.Context(), append(result.Contributions, common)...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 6)
	stop := plan.Steps[0].Payload.Payload.(*ydbast.StreamingQuery)
	resume := plan.Steps[5].Payload.Payload.(*ydbast.StreamingQuery)
	c.Assert(ydbstreaming.Running(stop.Spec), qt.IsFalse)
	c.Assert(ydbstreaming.Running(resume.Spec), qt.IsTrue)
	c.Assert(plan.Steps[1].ID, qt.DeepEquals, request.CommonSteps[0].ID)
	c.Assert(plan.Steps[2].Payload.Payload.Kind(), qt.Equals, ydbast.ResourcePoolKind)
	c.Assert(plan.Steps[3].Payload.Payload.Kind(), qt.Equals, ydbast.ResourcePoolClassifierKind)
	c.Assert(plan.Steps[4].ID, qt.DeepEquals, request.CommonSteps[1].ID)
	c.Assert(result.Changes[1].Steps, qt.DeepEquals, []plangraph.StepID{plan.Steps[0].ID, plan.Steps[5].ID})
	c.Assert(request.CommonSteps, qt.HasLen, 2)
}

func TestWorkloadClassifierMayKeepRoutingToARemovedPool(t *testing.T) {
	c := qt.New(t)
	request := workloadRequest(schemaext.ChangeRecord{Subject: ydbworkload.PoolRef("pool"), Value: &ydbdiff.ResourcePool{
		Before: &ydbworkload.ObservedPool{},
	}}, workloadClassifierMove("classifier", 10, 30))
	result, err := workloadRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 2)
	c.Assert(plan.Steps[0].Payload.Payload.(*ydbast.ResourcePool).Operation, qt.Equals, ydbast.PoolDrop)
	c.Assert(plan.Steps[1].Payload.Payload.(*ydbast.ResourcePoolClassifier).Spec.ResourcePool, qt.Equals, "pool")
}

func TestWorkloadEqualOperandsHaveReceiptsWithoutOperations(t *testing.T) {
	c := qt.New(t)
	result, err := workloadRuntime(c).PlanFeatures(t.Context(), workloadRequest(
		schemaext.ChangeRecord{Subject: ydbworkload.PoolRef("default"), Value: &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}, After: &ydbworkload.DesiredPool{}}},
		workloadClassifierMove("classifier", 10, 10),
	))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 2)
	for _, receipt := range result.Changes {
		c.Assert(receipt.Steps, qt.HasLen, 0)
		c.Assert(receipt.Strategy, qt.Not(qt.Equals), "")
	}
}

func TestWorkloadRefusalsDiscardOtherFamiliesAndKeepInputIndexes(t *testing.T) {
	streaming := schemaext.ChangeRecord{Subject: ydbstreaming.Ref("", "query"), Value: &ydbdiff.StreamingQuery{
		Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}},
		After:  &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 2;"}},
	}}
	pool := schemaext.ChangeRecord{Subject: ydbworkload.PoolRef("pool"), Value: &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}}}
	for _, tc := range []struct {
		name    string
		changes []schemaext.ChangeRecord
		index   int
		message string
	}{
		{"streaming refusal", []schemaext.ChangeRecord{pool, streaming}, 1, "allow_state_reset=true"},
		{"duplicate final rank", []schemaext.ChangeRecord{pool, workloadClassifierMove("a", 10, 30), workloadClassifierMove("b", 20, 30)}, 2, "cannot share rank 30"},
		{"duplicate captured rank", []schemaext.ChangeRecord{pool, workloadClassifierMove("a", 10, 30), workloadClassifierMove("b", 10, 40)}, 2, "share rank 10"},
		{"default creation", []schemaext.ChangeRecord{pool, {Subject: ydbworkload.PoolRef("default"), Value: &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}}}}, 1, "default pool cannot be created or dropped"},
		{"default removal", []schemaext.ChangeRecord{pool, {Subject: ydbworkload.PoolRef("default"), Value: &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}}}}, 1, "default pool cannot be created or dropped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := workloadRuntime(c).PlanFeatures(t.Context(), workloadRequest(tc.changes...))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Change, qt.DeepEquals, new(tc.index))
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, tc.message)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestWorkloadScopeAndCapabilitiesDoNotSelectFallbacks(t *testing.T) {
	c := qt.New(t)
	request := workloadRequest(workloadClassifierMove("classifier", 10, 30))
	request.Capabilities = nil
	result, err := workloadRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Feature, qt.Equals, string(capability.ResourcePools))
	c.Assert(result.Contributions, qt.HasLen, 0)
	request.Target = "postgres"
	result, err = (ydbplan.WorkloadStreamingService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err = (ydbplan.WorkloadStreamingService{}).PlanFeatures(ctx, workloadRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
