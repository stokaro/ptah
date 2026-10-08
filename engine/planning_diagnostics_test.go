package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

func planningRefusal(kind schemaext.Kind, change int) featureplan.Result {
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(kind), Feature: "retention", Message: "retained state cannot be recovered"},
		Change:  new(change),
	}}}
}

func splitPlanningProvider(first, second featureplan.Service) engine.Provider {
	provider := planningProvider(first)
	other := provider.Planning[0]
	provider.Planning[0].Kinds = []schemaext.Kind{conversionFirst}
	other.Kinds, other.Service = []schemaext.Kind{conversionSecond}, second
	provider.Planning = append(provider.Planning, other)
	return provider
}

func TestPlanningCollectsRefusalsAndRemapsChangeIndexes(t *testing.T) {
	c := qt.New(t)
	first, second := planningRefusal(conversionFirst, 1), planningRefusal(conversionSecond, 0)
	calls := [2]int{}
	provider := splitPlanningProvider(
		planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) { calls[0]++; return first, nil }),
		planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) { calls[1]++; return second, nil }),
	)
	request := planningRequest()
	result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.DeepEquals, [2]int{1, 1})
	c.Assert(result.Diagnostics, qt.HasLen, 2)
	c.Assert(*result.Diagnostics[0].Change, qt.Equals, 2)
	c.Assert(*result.Diagnostics[1].Change, qt.Equals, 1)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 0)
	*first.Diagnostics[0].Change = 99
	first.Diagnostics[0].Problem.Message = "mutated"
	c.Assert(*result.Diagnostics[0].Change, qt.Equals, 2)
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Equals, "retained state cannot be recovered")
}

func TestPlanningRefusalDiscardsSuccessfulContributionsInEitherOrder(t *testing.T) {
	for _, refusalFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "last owner refuses", true: "first owner refuses"}[refusalFirst], func(t *testing.T) {
			c := qt.New(t)
			services := map[bool]planningFunc{
				false: plannedFixture,
				true: func(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
					return planningRefusal(request.Changes[0].Value.Kind(), 0), nil
				},
			}
			result, err := mustRuntime(c, splitPlanningProvider(services[refusalFirst], services[!refusalFirst])).PlanFeatures(t.Context(), planningRequest())
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

func TestPlanningFailureAfterRefusalDiscardsAllDiagnostics(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider connection lost")
	provider := splitPlanningProvider(
		planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
			return planningRefusal(conversionFirst, 0), nil
		}),
		planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
			return planningRefusal(conversionSecond, 0), failure
		}),
	)
	result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), planningRequest())
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestPlanningRejectsMalformedRefusalOutcomes(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Result)
	}{
		{"incomplete", func(r *featureplan.Result) { r.Complete = false }},
		{"contribution", func(r *featureplan.Result) { r.Contributions = []plangraph.Contribution[featureplan.Operation]{{}} }},
		{"change receipt", func(r *featureplan.Result) { r.Changes = []featureplan.ChangePlan{{}} }},
		{"parent receipt", func(r *featureplan.Result) { r.Parents = []featureplan.ParentPlan{{}} }},
		{"negative change", func(r *featureplan.Result) { r.Diagnostics[0].Change = new(-1) }},
		{"missing change", func(r *featureplan.Result) { r.Diagnostics[0].Change = new(3) }},
		{"changed kind", func(r *featureplan.Result) { r.Diagnostics[0].Problem.Kind = "example.org/foreign" }},
		{"both indexes", func(r *featureplan.Result) { r.Diagnostics[0].Parent = new(0) }},
		{"parent without action", func(r *featureplan.Result) { r.Diagnostics[0].Change, r.Diagnostics[0].Parent = nil, new(0) }},
		{"negative parent", func(r *featureplan.Result) { r.Diagnostics[0].Change, r.Diagnostics[0].Parent = nil, new(-1) }},
		{"missing parent", func(r *featureplan.Result) { r.Diagnostics[0].Change, r.Diagnostics[0].Parent = nil, new(99) }},
		{"unknown code", func(r *featureplan.Result) { r.Diagnostics[0].Problem.Code = "future" }},
		{"empty message", func(r *featureplan.Result) { r.Diagnostics[0].Problem.Message = " " }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
				result := planningRefusal(conversionFirst, 0)
				test.edit(&result)
				return result, nil
			})
			result, err := mustRuntime(c, planningProvider(service)).PlanFeatures(t.Context(), planningRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

func TestPlanningCancellationDiscardsCompletedRefusal(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	provider := planningProvider(planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
		cancel()
		return planningRefusal(conversionFirst, 0), nil
	}))
	result, err := mustRuntime(c, provider).PlanFeatures(ctx, planningRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestParentPlanningDiagnosticsPreserveAssignmentWithoutChildChanges(t *testing.T) {
	c := qt.New(t)
	diagnostic := featureplan.Diagnostic{Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.UnsupportedFeature, Kind: string(conversionFirst), Message: "attached state cannot be reconstructed",
	}, Parent: new(0)}
	service := planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
		return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{diagnostic}}, nil
	})
	request := parentPlanningRequest()
	runtime := mustRuntime(c, parentPlanningProvider(service))
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Diagnostics[0].Parent, qt.DeepEquals, new(0))
	c.Assert(result.Parents, qt.HasLen, 0)
	diagnostic.Problem.Kind = "example.org/unassigned"
	result, err = runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
