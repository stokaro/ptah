package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

func commonPlanningRequest() featureplan.Request {
	request := planningRequest()
	parent := request.Tables[0].Subject
	column := objectidentity.NewBuilder(request.Identifiers).Column("items", "extra")
	request.CommonSteps = []featureplan.CommonStep{{
		ID: plangraph.StepID{Owner: "example.org/common", Name: "add-extra"}, Parent: parent,
		Effects: []plangraph.Effect{{Subject: column, Action: plangraph.Create}}, Transaction: plangraph.TransactionForbidden,
		AddedColumn: &ast.ColumnNode{Name: "extra", Type: "INTEGER", Default: &ast.DefaultValue{Value: "0"}},
	}}
	return request
}

func plannedCommonRewrite(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	result, err := plannedFixture(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	first := &result.Contributions[0].Steps[0]
	first.Effects = append(first.Effects, request.CommonSteps[0].Effects...)
	result.Contributions[0].Dependencies = nil
	result.Rewrites = []plangraph.Rewrite{{Sources: []plangraph.StepID{request.CommonSteps[0].ID}, Replacement: first.ID}}
	return result, nil
}

func TestPlanningCarriesCommonRewritesThroughTheCompleteGraph(t *testing.T) {
	c := qt.New(t)
	request := commonPlanningRequest()
	var reply featureplan.Result
	provider := planningProvider(planningFunc(func(ctx context.Context, received featureplan.Request) (featureplan.Result, error) {
		var err error
		reply, err = plannedCommonRewrite(ctx, received)
		return reply, err
	}))
	result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Rewrites, qt.DeepEquals, reply.Rewrites)
	reply.Rewrites[0].Sources[0].Name = "mutated"
	c.Assert(result.Rewrites[0].Sources[0], qt.Equals, request.CommonSteps[0].ID)
	common := request.CommonSteps[0]
	host := plangraph.Contribution[featureplan.Operation]{Owner: common.ID.Owner,
		Steps: []plangraph.Step[featureplan.Operation]{{ID: common.ID, Effects: common.Effects, Transaction: common.Transaction}},
	}
	plan, err := plangraph.ScheduleRewritten(t.Context(), host, result.Rewrites, result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, len(request.Changes))
	c.Assert(plan.Steps[0].ID, qt.Equals, result.Rewrites[0].Replacement)
	c.Assert(plan.Steps[0].Effects, qt.Contains, common.Effects[0])
}

func TestPlanningIsolatesCommonOperandsBetweenSelectedServices(t *testing.T) {
	c := qt.New(t)
	request := commonPlanningRequest()
	var received featureplan.CommonStep
	provider := splitPlanningProvider(
		planningFunc(func(ctx context.Context, input featureplan.Request) (featureplan.Result, error) {
			input.CommonSteps[0].AddedColumn.Default.Value = "changed"
			input.CommonSteps[0].Effects[0].Action = plangraph.Drop
			input.CommonSteps[0].ID.Name = "changed"
			return plannedFixture(ctx, input)
		}),
		planningFunc(func(ctx context.Context, input featureplan.Request) (featureplan.Result, error) {
			received = input.CommonSteps[0]
			return plannedFixture(ctx, input)
		}),
	)
	_, err := mustRuntime(c, provider).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(received, qt.DeepEquals, request.CommonSteps[0])
	c.Assert(request.CommonSteps[0].AddedColumn.Default.Value, qt.Equals, "0")
	c.Assert(request.CommonSteps[0].Effects[0].Action, qt.Equals, plangraph.Create)
}

func TestPlanningRejectsMalformedCommonStepsBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Request)
	}{
		{"duplicate step", func(r *featureplan.Request) { r.CommonSteps = append(r.CommonSteps, r.CommonSteps[0]) }},
		{"empty owner", func(r *featureplan.Request) { r.CommonSteps[0].ID.Owner = "" }},
		{"empty name", func(r *featureplan.Request) { r.CommonSteps[0].ID.Name = "" }},
		{"unknown transaction", func(r *featureplan.Request) { r.CommonSteps[0].Transaction = "future" }},
		{"invalid effect", func(r *featureplan.Request) { r.CommonSteps[0].Effects[0].Action = "future" }},
		{"uncaptured parent", func(r *featureplan.Request) { r.CommonSteps[0].Parent.Name.Source = "other" }},
		{"addition without parent", func(r *featureplan.Request) { r.CommonSteps[0].Parent = objectidentity.ID{} }},
		{"addition without name", func(r *featureplan.Request) { r.CommonSteps[0].AddedColumn.Name = "" }},
		{"addition without type", func(r *featureplan.Request) { r.CommonSteps[0].AddedColumn.Type = "" }},
		{"addition of another column", func(r *featureplan.Request) { r.CommonSteps[0].AddedColumn.Name = "other" }},
		{"addition with unknown effects", func(r *featureplan.Request) { r.CommonSteps[0].Effects = nil }},
		{"addition with drop effect", func(r *featureplan.Request) { r.CommonSteps[0].Effects[0].Action = plangraph.Drop }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := commonPlanningRequest()
			test.edit(&request)
			called := false
			provider := planningProvider(planningFunc(func(ctx context.Context, input featureplan.Request) (featureplan.Result, error) {
				called = true
				return plannedFixture(ctx, input)
			}))
			result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
			c.Assert(called, qt.IsFalse)
		})
	}
}

func TestPlanningValidatesCommonColumnFacetCodecs(t *testing.T) {
	c := qt.New(t)
	request := commonPlanningRequest()
	request.CommonSteps[0].AddedColumn.Facets = must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst}))
	called := false
	provider := planningProvider(planningFunc(func(ctx context.Context, input featureplan.Request) (featureplan.Result, error) {
		called = true
		return plannedFixture(ctx, input)
	}))
	result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
	c.Assert(called, qt.IsFalse)
}

func TestPlanningRejectsMalformedCommonRewriteClaims(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Result)
	}{
		{"no source", func(r *featureplan.Result) { r.Rewrites[0].Sources = nil }},
		{"unknown source", func(r *featureplan.Result) { r.Rewrites[0].Sources[0].Name = "missing" }},
		{"owner step as source", func(r *featureplan.Result) { r.Rewrites[0].Sources[0] = r.Contributions[0].Steps[1].ID }},
		{"duplicate source", func(r *featureplan.Result) {
			r.Rewrites[0].Sources = append(r.Rewrites[0].Sources, r.Rewrites[0].Sources[0])
		}},
		{"unknown replacement", func(r *featureplan.Result) { r.Rewrites[0].Replacement.Name = "missing" }},
		{"common replacement", func(r *featureplan.Result) { r.Rewrites[0].Replacement = r.Rewrites[0].Sources[0] }},
		{"duplicate replacement", func(r *featureplan.Result) { r.Rewrites = append(r.Rewrites, r.Rewrites[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := planningProvider(planningFunc(func(ctx context.Context, input featureplan.Request) (featureplan.Result, error) {
				reply, err := plannedCommonRewrite(ctx, input)
				test.edit(&reply)
				return reply, err
			}))
			result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), commonPlanningRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

func TestPlanningRefusalDiscardsCommonRewrites(t *testing.T) {
	c := qt.New(t)
	provider := splitPlanningProvider(planningFunc(plannedCommonRewrite), planningFunc(func(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
		return planningRefusal(request.Changes[0].Value.Kind(), 0), nil
	}))
	result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), commonPlanningRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Rewrites, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 0)
}
