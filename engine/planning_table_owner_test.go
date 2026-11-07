package engine_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

// tableOwnerProvider registers one planning service on target "custom" that
// owns its tables, beside the parent kinds given.
func tableOwnerProvider(service featureplan.Service, parentKinds ...schemaext.Kind) engine.Provider {
	p := parentPlanningProvider(service)
	p.Planning[0].ParentKinds, p.Planning[0].OwnsTables = parentKinds, true
	return p
}

// plannedOwnedTables answers as a service that owns tables: for each table,
// one receipt per parent kind and then one of kind featureplan.TableKind.
func plannedOwnedTables(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	for _, table := range request.Tables {
		for _, kind := range append(slices.Clone(request.ParentKinds), featureplan.TableKind) {
			result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: kind, Action: table.Action, Strategy: "drop the table"})
		}
	}
	return result, nil
}

// recordingPlanner answers with reply and keeps the request it received.
func recordingPlanner(received *featureplan.Request, reply featureplan.Service) featureplan.Service {
	return planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		*received = request
		return reply.PlanFeatures(ctx, request)
	})
}

func TestTableOwner_HappyPath(t *testing.T) {
	t.Run("an owner with parent kinds", func(t *testing.T) {
		c := qt.New(t)
		var received featureplan.Request
		p := tableOwnerProvider(recordingPlanner(&received, planningFunc(plannedOwnedTables)), conversionFirst, conversionSecond)

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())

		c.Assert(err, qt.IsNil)
		c.Assert(received.OwnsTables, qt.IsTrue)
		c.Assert(received.ParentKinds, qt.DeepEquals, []schemaext.Kind{conversionFirst, conversionSecond})
		c.Assert(result.Parents, qt.HasLen, 3)
		c.Assert(result.Parents[2].Kind, qt.Equals, featureplan.TableKind)
		c.Assert(result.Parents[2].Action, qt.Equals, featureplan.DropTable)
	})
	t.Run("an owner that plans nothing else", func(t *testing.T) {
		c := qt.New(t)
		var received featureplan.Request
		p := tableOwnerProvider(recordingPlanner(&received, planningFunc(plannedOwnedTables)))

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())

		c.Assert(err, qt.IsNil)
		c.Assert(received.OwnsTables, qt.IsTrue)
		c.Assert(received.ParentKinds, qt.HasLen, 0)
		c.Assert(result.Parents, qt.HasLen, 1)
		c.Assert(result.Parents[0].Kind, qt.Equals, featureplan.TableKind)
	})
	t.Run("an owner that refuses a table by name", func(t *testing.T) {
		c := qt.New(t)
		request := parentPlanningRequest()
		p := tableOwnerProvider(planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.UnsupportedFeature, Kind: string(featureplan.TableKind), Message: "the table cannot be dropped"}, Parent: new(0)}}}, nil
		}))

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(result.Diagnostics[0].Parent, qt.DeepEquals, new(0))
	})
	t.Run("a caller that claims ownership for a service with parent kinds", func(t *testing.T) {
		c := qt.New(t)
		var received featureplan.Request
		p := parentPlanningProvider(recordingPlanner(&received, planningFunc(plannedParents)))
		request := parentPlanningRequest()
		request.OwnsTables = true

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(received.OwnsTables, qt.IsFalse)
		c.Assert(result.Parents, qt.HasLen, 2)
	})
	t.Run("a caller that claims ownership for a service that plans changes", func(t *testing.T) {
		c := qt.New(t)
		var received featureplan.Request
		p := planningProvider(recordingPlanner(&received, planningFunc(plannedFixture)))
		request := planningRequest()
		request.OwnsTables = true

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(received.OwnsTables, qt.IsFalse)
		c.Assert(result.Parents, qt.HasLen, 0)
	})
	t.Run("a table that keeps its shape reaches no owner", func(t *testing.T) {
		c := qt.New(t)
		calls := 0
		p := tableOwnerProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
			calls++
			return plannedOwnedTables(ctx, request)
		}))
		request := parentPlanningRequest()
		request.Tables[0].Action = ""

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(calls, qt.Equals, 0)
		c.Assert(result.Parents, qt.HasLen, 0)
	})
}

// An owner accounts for each table with one table receipt after those of its
// parent kinds; a service that does not own tables answers none and cannot
// refuse a table by that kind.
func TestTableOwner_FailurePath(t *testing.T) {
	t.Run("no table receipt", func(t *testing.T) {
		c := qt.New(t)
		p := tableOwnerProvider(planningFunc(plannedParents), conversionFirst, conversionSecond)

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())

		c.Assert(err, qt.ErrorMatches, `.*planning omitted a parent receipt`)
		c.Assert(result, qt.DeepEquals, featureplan.Result{})
	})
	t.Run("the table receipt before a parent kind's", func(t *testing.T) {
		c := qt.New(t)
		p := tableOwnerProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
			result, err := plannedOwnedTables(ctx, request)
			result.Parents[0], result.Parents[2] = result.Parents[2], result.Parents[0]
			return result, err
		}), conversionFirst, conversionSecond)

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())

		c.Assert(err, qt.ErrorMatches, `.*invalid parent planning receipt`)
		c.Assert(result, qt.DeepEquals, featureplan.Result{})
	})
	t.Run("a table receipt from a service that does not own tables", func(t *testing.T) {
		c := qt.New(t)
		p := parentPlanningProvider(planningFunc(plannedOwnedTables))

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())

		c.Assert(err, qt.ErrorMatches, `.*planning returned an unrequested parent receipt`)
		c.Assert(result, qt.DeepEquals, featureplan.Result{})
	})
	t.Run("a table refusal from a service that does not own tables", func(t *testing.T) {
		c := qt.New(t)
		p := parentPlanningProvider(planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.UnsupportedFeature, Kind: string(featureplan.TableKind), Message: "the table cannot be dropped"}, Parent: new(0)}}}, nil
		}))

		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())

		c.Assert(err, qt.ErrorMatches, `.*planning diagnostic names an unassigned parent kind`)
		c.Assert(result, qt.DeepEquals, featureplan.Result{})
	})
}

// A target has at most one service that owns its tables, and a service that
// owns nothing and plans nothing is still incomplete.
func TestTableOwnerRegistration_FailurePath(t *testing.T) {
	t.Run("two owners on one target", func(t *testing.T) {
		c := qt.New(t)
		p := tableOwnerProvider(planningFunc(plannedOwnedTables))
		p.Planning = append(p.Planning, engine.Planning{Target: "custom", OwnsTables: true, Service: planningFunc(plannedOwnedTables)})

		runtime, err := engine.New(p)

		c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		c.Assert(err, qt.ErrorMatches, `.*a second planning service owns the tables of "custom"`)
		c.Assert(runtime, qt.IsNil)
	})
	t.Run("a service that neither owns tables nor plans a kind", func(t *testing.T) {
		c := qt.New(t)
		p := tableOwnerProvider(planningFunc(plannedOwnedTables))
		p.Planning[0].OwnsTables = false

		runtime, err := engine.New(p)

		c.Assert(err, qt.ErrorMatches, `.*incomplete planning registration for "custom"`)
		c.Assert(runtime, qt.IsNil)
	})
}
