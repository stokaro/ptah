package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func parentPlanningProvider(service featureplan.Service) engine.Provider {
	p := conversionProvider(nil)
	p.Conversions = nil
	p.Codecs = append(p.Codecs, planningCodec())
	p.Planning = []engine.Planning{{Target: "custom", ParentKinds: []schemaext.Kind{conversionFirst, conversionSecond}, OperationKinds: []schemaext.Kind{planningOperationKind}, Service: service}}
	return p
}

func parentPlanningRequest() featureplan.Request {
	r := planningRequest()
	r.Changes = nil
	r.Tables[0].Action = featureplan.DropTable
	r.Tables[0].Desired = schemacapture.TableDeclaration{}
	return r
}

func plannedParents(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	for _, table := range request.Tables {
		for _, kind := range request.ParentKinds {
			result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: kind, Action: table.Action, Strategy: "remove with parent"})
		}
	}
	return result, nil
}

func TestPlanningAssessesEmptyParentNamespacesWithoutChildChanges(t *testing.T) {
	c := qt.New(t)
	var received featureplan.Request
	calls := 0
	p := parentPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		calls++
		received = request
		return plannedParents(ctx, request)
	}))
	runtime := mustRuntime(c, p)
	p.Planning[0].ParentKinds[0] = "example.org/replaced"
	request := parentPlanningRequest()
	request.ParentKinds = []schemaext.Kind{conversionFirst}
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Parents, qt.HasLen, 2)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(received.ParentKinds, qt.DeepEquals, []schemaext.Kind{conversionFirst, conversionSecond})
	c.Assert(received.Tables[0].Current.FeatureCoverage.Lookup(conversionFirst, received.Tables[0].Subject).State, qt.Equals, schemaext.Uninspected)
	c.Assert(request.ParentKinds, qt.DeepEquals, []schemaext.Kind{conversionFirst})
}

func TestPlanningAccountsForStepsOwnedByAParentStrategy(t *testing.T) {
	c := qt.New(t)
	step := plangraph.StepID{Owner: "example.org/converter", Name: "preserve"}
	var reply featureplan.Result
	p := parentPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		result, err := plannedParents(ctx, request)
		result.Parents[0].Steps = []plangraph.StepID{step}
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{{Owner: step.Owner, Steps: []plangraph.Step[featureplan.Operation]{{ID: step,
			Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: request.Tables[0].Subject, Payload: &planningOperation{Values: []int{42}}},
		}}}}
		reply = result
		return result, err
	}))
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())
	c.Assert(err, qt.IsNil)
	reply.Parents[0].Steps[0].Name = "changed"
	c.Assert(result.Parents[0].Steps, qt.DeepEquals, []plangraph.StepID{step})
	c.Assert(result.Contributions[0].Steps[0].Payload.Payload.(*planningOperation).Values, qt.DeepEquals, []int{42})
}

// TestPlanningAssessesCreatedTables pins a table the plan creates: its owner
// receives the declaration and no observation, and answers with a receipt
// that names the step creating the table's child.
func TestPlanningAssessesCreatedTables(t *testing.T) {
	c := qt.New(t)
	step := plangraph.StepID{Owner: "example.org/converter", Name: "create-child"}
	request := parentPlanningRequest()
	request.Tables[0].Action = featureplan.CreateTable
	request.Tables[0].Desired, request.Tables[0].Current = planningRequest().Tables[0].Desired, schemacapture.TableObservation{}
	var received featureplan.Request
	p := parentPlanningProvider(planningFunc(func(ctx context.Context, input featureplan.Request) (featureplan.Result, error) {
		received = input
		result := must.Must(plannedParents(ctx, input))
		result.Parents[0].Steps = []plangraph.StepID{step}
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{{Owner: step.Owner, Steps: []plangraph.Step[featureplan.Operation]{{ID: step,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &planningOperation{Values: []int{7}}, Phase: featureplan.PhaseDependent},
		}}}}
		return result, nil
	}))

	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(received.Tables[0].Action, qt.Equals, featureplan.CreateTable)
	c.Assert(received.Tables[0].Desired.HasTable(), qt.IsTrue)
	c.Assert(received.Tables[0].Current.HasTable(), qt.IsFalse)
	c.Assert(result.Parents, qt.HasLen, 2)
	c.Assert(result.Parents[0].Action, qt.Equals, featureplan.CreateTable)
	c.Assert(result.Parents[0].Steps, qt.DeepEquals, []plangraph.StepID{step})
	c.Assert(result.Contributions[0].Steps[0].Payload.Phase, qt.Equals, featureplan.PhaseDependent)
}

func TestPlanningAssessesSurvivingParentsWithoutFeatureChanges(t *testing.T) {
	c := qt.New(t)
	request := commonPlanningRequest()
	request.Changes = nil
	request.Tables[0].Action = featureplan.AlterTable
	var received featureplan.Request
	provider := parentPlanningProvider(planningFunc(func(ctx context.Context, input featureplan.Request) (featureplan.Result, error) {
		received = input
		return plannedParents(ctx, input)
	}))
	result, err := mustRuntime(c, provider).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(received.CommonSteps, qt.DeepEquals, request.CommonSteps)
	c.Assert(received.Tables[0].Action, qt.Equals, featureplan.AlterTable)
	c.Assert(result.Parents, qt.HasLen, 2)
	c.Assert(result.Parents[0].Action, qt.Equals, featureplan.AlterTable)
}

func TestPlanningRefusesMalformedParentReceipts(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Result)
	}{
		{"missing completion", func(r *featureplan.Result) { r.Complete = false }},
		{"missing model", func(r *featureplan.Result) { r.Parents = r.Parents[:1] }},
		{"extra model", func(r *featureplan.Result) { r.Parents = append(r.Parents, r.Parents[0]) }},
		{"wrong kind", func(r *featureplan.Result) { r.Parents[0].Kind = conversionSecond }},
		{"wrong action", func(r *featureplan.Result) { r.Parents[0].Action = featureplan.RebuildTable }},
		{"changed spelling", func(r *featureplan.Result) { r.Parents[0].Subject.Name.Source = "ITEMS" }},
		{"missing strategy", func(r *featureplan.Result) { r.Parents[0].Strategy = "" }},
		{"missing step", func(r *featureplan.Result) {
			r.Parents[0].Steps = []plangraph.StepID{{Owner: "example.org/converter", Name: "absent"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := parentPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
				result, err := plannedParents(ctx, request)
				test.edit(&result)
				return result, err
			}))
			result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

func TestPlanningPreflightsUnassignedParentState(t *testing.T) {
	c := qt.New(t)
	calls := 0
	p := parentPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		calls++
		return plannedParents(ctx, request)
	}))
	p.Planning[0].ParentKinds = []schemaext.Kind{conversionFirst}
	request := parentPlanningRequest()
	facets, err := schemaext.NewFacets(&conversionValue{ID: conversionSecond, Number: 7})
	c.Assert(err, qt.IsNil)
	request.Tables[0].Current.Table.Facets = facets
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
	c.Assert(calls, qt.Equals, 0)
}

func TestPlanningDiscardsEarlierParentReceiptsAfterFailure(t *testing.T) {
	c := qt.New(t)
	p := parentPlanningProvider(planningFunc(plannedParents))
	p.Planning[0].ParentKinds = []schemaext.Kind{conversionFirst}
	failure := errors.New("parent provider unavailable")
	p.Planning = append(p.Planning, engine.Planning{Target: "custom", ParentKinds: []schemaext.Kind{conversionSecond}, Service: planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
		return featureplan.Result{}, failure
	})})
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestPlanningRefusesInvalidParentOwnership(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*engine.Provider)
	}{
		{"duplicate model", func(p *engine.Provider) {
			p.Planning[0].ParentKinds = append(p.Planning[0].ParentKinds, conversionFirst)
		}},
		{"competing service", func(p *engine.Provider) { p.Planning = append(p.Planning, p.Planning[0]) }},
		{"missing desired codec", func(p *engine.Provider) { p.Codecs = p.Codecs[1:] }},
		{"unowned model", func(p *engine.Provider) { p.Planning[0].ParentKinds[0] = "other.org/model" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := parentPlanningProvider(planningFunc(plannedParents))
			test.edit(&p)
			runtime, err := engine.New(p)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestPlanningPreflightsParentOperandsBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Request)
	}{
		{"missing observation", func(r *featureplan.Request) { r.Tables[0].Current = schemacapture.TableObservation{} }},
		{"missing rebuild declaration", func(r *featureplan.Request) { r.Tables[0].Action = featureplan.RebuildTable }},
		{"missing surviving declaration", func(r *featureplan.Request) { r.Tables[0].Action = featureplan.AlterTable }},
		{"drop with declaration", func(r *featureplan.Request) { r.Tables[0].Desired = planningRequest().Tables[0].Desired }},
		{"unknown action", func(r *featureplan.Request) { r.Tables[0].Action = "destroy" }},
		{"creation with an observation", func(r *featureplan.Request) {
			r.Tables[0].Action, r.Tables[0].Desired = featureplan.CreateTable, planningRequest().Tables[0].Desired
		}},
		{"creation without a declaration", func(r *featureplan.Request) {
			r.Tables[0].Action, r.Tables[0].Current = featureplan.CreateTable, schemacapture.TableObservation{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			p := parentPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
				calls++
				return plannedParents(ctx, request)
			}))
			request := parentPlanningRequest()
			test.edit(&request)
			result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestPlanningOnlyAssessesParentsForTheSelectedTarget(t *testing.T) {
	c := qt.New(t)
	p := parentPlanningProvider(planningFunc(plannedParents))
	p.Targets = append(p.Targets, engine.Target{Name: "other"})
	p.Planning = append(p.Planning, engine.Planning{Target: "other", ParentKinds: []schemaext.Kind{conversionFirst}, Service: planningFunc(func(context.Context, featureplan.Request) (featureplan.Result, error) {
		return featureplan.Result{}, errors.New("unselected service must not run")
	})})
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(result.Parents, qt.HasLen, 2)
}

func TestPlanningRefusesAnUnavailableParentService(t *testing.T) {
	c := qt.New(t)
	p := engine.Provider{ID: "example.org/empty", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}}}
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), parentPlanningRequest())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "(?s).*no parent planning service.*")
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestPlanningContextAloneDoesNotRequestAParentOperation(t *testing.T) {
	c := qt.New(t)
	p := engine.Provider{ID: "example.org/empty", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}}}
	request := parentPlanningRequest()
	request.Tables[0].Action = ""
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Parents, qt.HasLen, 0)
}

func TestPlanningDiscardsParentReceiptsAfterCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := parentPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		result, err := plannedParents(ctx, request)
		cancel()
		return result, err
	}))
	result, err := mustRuntime(c, p).PlanFeatures(ctx, parentPlanningRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}
