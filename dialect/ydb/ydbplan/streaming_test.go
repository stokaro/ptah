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
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine"
)

func streamingRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	codecs := append(ydbstreaming.Codecs(), ydbdiff.StreamingCodec(), ydbast.StreamingCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Planning:     []engine.Planning{{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.StreamingQueryKind}, OperationKinds: []schemaext.Kind{ydbast.StreamingQueryKind}, Service: ydbplan.StreamingService{}}},
		Declarations: []engine.DeclarationPlanning{{Target: "ydb", Kinds: []schemaext.Kind{ydbstreaming.Kind}, OperationKinds: []schemaext.Kind{ydbast.StreamingQueryKind}, Service: ydbplan.StreamingService{}}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func streamingPlanRequest(changes ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.Capabilities{capability.StreamingQueries: true}, Changes: changes}
}

func TestStreamingBodyChangeStopsBeforeCommonMutations(t *testing.T) {
	c := qt.New(t)
	runtime := streamingRuntime(c)
	before := ydbstreaming.Spec{Text: "SELECT 1;", Run: new(true)}
	after := ydbstreaming.Spec{Text: "SELECT 2;", Run: new(true)}
	ref := ydbstreaming.Ref("app", "query.v1")
	request := streamingPlanRequest(schemaext.ChangeRecord{Subject: ref, Value: &ydbdiff.StreamingQuery{
		Before: &ydbstreaming.Observed{Spec: before}, After: &ydbstreaming.Desired{Spec: after, AllowStateReset: true},
	}})
	commonID := plangraph.StepID{Owner: "ptah.run/ydb", Name: "table-change"}
	request.CommonSteps = []featureplan.CommonStep{{ID: commonID, Transaction: plangraph.TransactionForbidden}}
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.HasLen, 2)
	common := plangraph.Contribution[featureplan.Operation]{Owner: commonID.Owner, Steps: []plangraph.Step[featureplan.Operation]{{ID: commonID}}}
	plan, err := plangraph.Schedule(t.Context(), append(result.Contributions, common)...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 3)
	c.Assert(plan.Steps[1].ID, qt.DeepEquals, commonID)
	stop := plan.Steps[0].Payload.Payload.(*ydbast.StreamingQuery)
	resume := plan.Steps[2].Payload.Payload.(*ydbast.StreamingQuery)
	c.Assert(ydbstreaming.Running(stop.Spec), qt.IsFalse)
	c.Assert(stop.Spec.Text, qt.Equals, before.Text)
	c.Assert(stop.AllowStateReset, qt.IsFalse)
	c.Assert(resume.Previous, qt.DeepEquals, stop.Spec)
	c.Assert(resume.Spec, qt.DeepEquals, after)
	c.Assert(resume.AllowStateReset, qt.IsTrue)
	c.Assert(plan.Steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(plan.Steps[2].Impact.Impact, qt.Equals, schemaext.Destructive)
	c.Assert(*before.Run, qt.IsTrue)
}

func TestStreamingInvalidChangeDiscardsTheWholeBatch(t *testing.T) {
	c := qt.New(t)
	request := streamingPlanRequest(
		schemaext.ChangeRecord{Subject: ydbstreaming.Ref("", "added"), Value: &ydbdiff.StreamingQuery{After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}},
		schemaext.ChangeRecord{Subject: ydbstreaming.Ref("", "changed"), Value: &ydbdiff.StreamingQuery{
			Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}, After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 2;"}},
		}},
	)
	result, err := streamingRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Change, qt.DeepEquals, new(1))
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "allow_state_reset=true")
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
}

func TestStreamingStopOnlyDoesNotRestartOrReset(t *testing.T) {
	c := qt.New(t)
	request := streamingPlanRequest(schemaext.ChangeRecord{Subject: ydbstreaming.Ref("", "query"), Value: &ydbdiff.StreamingQuery{
		Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}},
		After:  &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}},
	}})
	result, err := streamingRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 1)
	c.Assert(result.Contributions[0].Steps, qt.HasLen, 1)
	operation := result.Contributions[0].Steps[0].Payload.Payload.(*ydbast.StreamingQuery)
	c.Assert(ydbstreaming.Running(operation.Spec), qt.IsFalse)
	c.Assert(operation.AllowStateReset, qt.IsFalse)
	c.Assert(operation.Effect().Impact, qt.Equals, schemaext.Behavioral)
}

func TestStreamingDeclarationUsesTheSameOwnerOperation(t *testing.T) {
	c := qt.New(t)
	object := ydbstreaming.DesiredObject("app", "query", "Holder", ydbstreaming.Spec{Text: "SELECT 1;"}, true)
	result, err := streamingRuntime(c).PlanDeclarations(t.Context(), featureplan.DeclarationRequest{
		Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.StreamingQueries: true}, Objects: []schemaext.Object{object},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Declarations, qt.HasLen, 1)
	operation := result.Contributions[0].Steps[0].Payload.Payload.(*ydbast.StreamingQuery)
	c.Assert(operation.Operation, qt.Equals, ydbast.StreamingCreate)
	c.Assert(operation.AllowStateReset, qt.IsFalse)
	c.Assert(operation.Creation, qt.DeepEquals, ydbast.StreamingCreation{OrReplace: true})
}

func TestStreamingPlanningRefusesWrongIdentityAndCancellation(t *testing.T) {
	c := qt.New(t)
	ref := ydbstreaming.Ref("", "query")
	ref.Parent = objectidentity.Part{Source: "table", Normalized: "table"}
	request := streamingPlanRequest(schemaext.ChangeRecord{Subject: ref, Value: &ydbdiff.StreamingQuery{After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}})
	result, err := (ydbplan.StreamingService{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err = (ydbplan.StreamingService{}).PlanFeatures(ctx, streamingPlanRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
