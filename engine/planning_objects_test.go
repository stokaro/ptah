package engine_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

func objectPlanningProvider(service featureplan.Service) engine.Provider {
	p := comparisonProvider(comparisonFunc(unchangedComparison))
	p.Codecs = append(p.Codecs, planningCodec())
	p.Planning = []engine.Planning{{Target: "custom", ContextKinds: []schemaext.Kind{conversionFirst}, OperationKinds: []schemaext.Kind{planningOperationKind}, Service: service}}
	return p
}

func plannedObjects(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	for _, subject := range request.Objects.Subjects(request.ContextKinds) {
		result.Objects = append(result.Objects, featureplan.ObjectPlan{Subject: subject, Kind: schemaext.Kind(subject.Kind), Strategy: "retained without changes"})
	}
	return result, nil
}

func objectPlanningRequest(c *qt.C) featureplan.Request {
	c.Helper()
	state := comparisonRequest(c).Desired
	return featureplan.Request{Target: "custom", Objects: featureplan.ObjectContext{Desired: state, Current: state}}
}

func TestPlanningAssessesUnchangedObjectsWithWholeOwnerContext(t *testing.T) {
	c := qt.New(t)
	var received featureplan.Request
	calls := 0
	p := objectPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		calls++
		received = request
		return plannedObjects(ctx, request)
	}))
	runtime := mustRuntime(c, p)
	request := objectPlanningRequest(c)
	request.ContextKinds = []schemaext.Kind{conversionSecond}
	request.Objects.ChangedSubjects = []objectidentity.ID{comparedRef(conversionSecond, "forged")}
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.ContextKinds, qt.DeepEquals, []schemaext.Kind{conversionFirst})
	c.Assert(received.Objects.Desired.Objects.Len(), qt.Equals, 2)
	c.Assert(received.Objects.Current.Objects.Len(), qt.Equals, 2)
	c.Assert(received.Objects.ChangedSubjects, qt.HasLen, 0)
	c.Assert(received.Changes, qt.HasLen, 0)
	c.Assert(result.Objects, qt.HasLen, 1)
	c.Assert(result.Objects[0].Subject, qt.Equals, comparedRef(conversionFirst, "first"))
}

func TestPlanningContextReceiptAccountsForOperationWithoutChange(t *testing.T) {
	c := qt.New(t)
	step := plangraph.StepID{Owner: "example.org/converter", Name: "restart"}
	var returned featureplan.Result
	p := objectPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		result, err := plannedObjects(ctx, request)
		result.Objects[0].Steps = []plangraph.StepID{step}
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{{Owner: step.Owner, Steps: []plangraph.Step[featureplan.Operation]{{ID: step, Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &planningOperation{Values: []int{42}}, Phase: featureplan.PhaseLifecycle}}}}}
		returned = result
		return result, err
	}))
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), objectPlanningRequest(c))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Objects[0].Steps, qt.DeepEquals, []plangraph.StepID{step})
	returned.Objects[0].Steps[0].Name = "mutated"
	returned.Contributions[0].Steps[0].Payload.Payload.(*planningOperation).Values[0] = 3
	c.Assert(result.Objects[0].Steps[0], qt.Equals, step)
	c.Assert(result.Contributions[0].Steps[0].Payload.Payload.(*planningOperation).Values, qt.DeepEquals, []int{42})
}

func TestPlanningVisitsUnknownEmptyObjectNamespace(t *testing.T) {
	c := qt.New(t)
	calls := 0
	p := objectPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		calls++
		c.Assert(request.Objects.Current.Coverage.Lookup(conversionFirst, comparedRef(conversionFirst, "unknown")).State, qt.Equals, schemaext.Uninspected)
		c.Assert(request.Objects.Subjects(request.ContextKinds), qt.HasLen, 0)
		return plannedObjects(ctx, request)
	}))
	runtime := mustRuntime(c, p)
	request := featureplan.Request{Target: "custom", Objects: featureplan.ObjectContext{Current: namespaceState(c, runtime.Codecs(), schemaext.Observed, schemaext.Uninspected)}}
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(result.Objects, qt.HasLen, 0)
}

func TestPlanningContextIncludesSubjectOnlyCoverage(t *testing.T) {
	c := qt.New(t)
	p := objectPlanningProvider(planningFunc(plannedObjects))
	runtime := mustRuntime(c, p)
	subject := comparedRef(conversionFirst, "unreadable")
	coverage := comparisonCoverage(c, runtime.Codecs(), schemaext.Uninspected, schemaext.SubjectCoverage{Kind: conversionFirst, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "definition unavailable"}})
	result, err := runtime.PlanFeatures(t.Context(), featureplan.Request{Target: "custom", Objects: featureplan.ObjectContext{Desired: schemaext.ObjectState{Coverage: coverage}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Objects, qt.HasLen, 1)
	c.Assert(result.Objects[0].Subject, qt.Equals, subject)
}

func TestPlanningRejectsMalformedObjectReceipts(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Result)
	}{
		{"missing", func(r *featureplan.Result) { r.Objects = nil }},
		{"duplicate", func(r *featureplan.Result) { r.Objects = append(r.Objects, r.Objects[0]) }},
		{"foreign kind", func(r *featureplan.Result) { r.Objects[0].Kind = conversionSecond }},
		{"foreign subject", func(r *featureplan.Result) { r.Objects[0].Subject = comparedRef(conversionFirst, "foreign") }},
		{"changed source spelling", func(r *featureplan.Result) { r.Objects[0].Subject.Name.Source = "FIRST" }},
		{"missing strategy", func(r *featureplan.Result) { r.Objects[0].Strategy = "" }},
		{"unknown step", func(r *featureplan.Result) {
			r.Objects[0].Steps = []plangraph.StepID{{Owner: "example.org/converter", Name: "missing"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := objectPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
				r, err := plannedObjects(ctx, request)
				test.edit(&r)
				return r, err
			}))
			result, err := mustRuntime(c, p).PlanFeatures(t.Context(), objectPlanningRequest(c))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}

func TestPlanningRejectsReorderedObjectReceipts(t *testing.T) {
	c := qt.New(t)
	p := objectPlanningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		result, err := plannedObjects(ctx, request)
		slices.Reverse(result.Objects)
		return result, err
	}))
	p.Planning[0].ContextKinds = []schemaext.Kind{conversionFirst, conversionSecond}
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), objectPlanningRequest(c))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestPlanningContextOwnershipIsUniqueAndTargetScoped(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*engine.Provider)
	}{
		{"duplicate kind", func(p *engine.Provider) {
			p.Planning[0].ContextKinds = append(p.Planning[0].ContextKinds, conversionFirst)
		}},
		{"competing service", func(p *engine.Provider) { p.Planning = append(p.Planning, p.Planning[0]) }},
		{"foreign kind", func(p *engine.Provider) { p.Planning[0].ContextKinds = []schemaext.Kind{"other.org/object"} }},
		{"missing observed codec", func(p *engine.Provider) {
			p.Codecs = slices.DeleteFunc(p.Codecs, func(c schemaext.Codec) bool {
				return c.Prototype.Kind() == conversionFirst && c.Representation == schemaext.Observed
			})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := objectPlanningProvider(planningFunc(plannedObjects))
			test.edit(&p)
			_, err := engine.New(p)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		})
	}
	t.Run("other target is not called", func(t *testing.T) {
		c := qt.New(t)
		calls := 0
		p := objectPlanningProvider(planningFunc(func(ctx context.Context, r featureplan.Request) (featureplan.Result, error) {
			calls++
			return plannedObjects(ctx, r)
		}))
		p.Targets = append(p.Targets, engine.Target{Name: "other"})
		result, err := mustRuntime(c, p).PlanFeatures(t.Context(), featureplan.Request{Target: "other"})
		c.Assert(err, qt.IsNil)
		c.Assert(calls, qt.Equals, 0)
		c.Assert(result.Complete, qt.IsTrue)
	})
}

func TestPlanningRemapsObjectDiagnosticsAcrossServices(t *testing.T) {
	c := qt.New(t)
	p := objectPlanningProvider(planningFunc(func(ctx context.Context, r featureplan.Request) (featureplan.Result, error) {
		return plannedObjects(ctx, r)
	}))
	p.Planning = append(p.Planning, engine.Planning{Target: "custom", ContextKinds: []schemaext.Kind{conversionSecond}, Service: planningFunc(func(_ context.Context, r featureplan.Request) (featureplan.Result, error) {
		c.Assert(r.Objects.AllSubjects(), qt.HasLen, 2)
		return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{ContextObject: new(1), Problem: schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: string(conversionSecond), Message: "dependency cannot be resolved"}}}}, nil
	})})
	request := objectPlanningRequest(c)
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Objects, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(*result.Diagnostics[0].ContextObject, qt.Equals, 1)
	var refused *featureplan.RefusalError
	c.Assert(errors.As(result.Err(request), &refused), qt.IsTrue)
}

func TestPlanningChangedSubjectsComeFromTheCompleteAcceptedBatch(t *testing.T) {
	c := qt.New(t)
	var received featureplan.Request
	p := objectPlanningProvider(planningFunc(func(ctx context.Context, r featureplan.Request) (featureplan.Result, error) {
		received = r
		return plannedObjects(ctx, r)
	}))
	p.Planning = append(p.Planning, engine.Planning{Target: "custom", Kinds: []schemaext.Kind{comparedKind}, OperationKinds: []schemaext.Kind{planningOperationKind}, Service: planningFunc(func(_ context.Context, r featureplan.Request) (featureplan.Result, error) {
		result := featureplan.Result{Complete: true}
		for _, change := range r.Changes {
			result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "accepted no-op"})
		}
		c.Assert(r.Objects.AllSubjects(), qt.HasLen, 0)
		return result, nil
	})})
	request := objectPlanningRequest(c)
	changed := comparedRef(conversionSecond, "second")
	changed.Name.Source = "SECOND"
	request.Changes = []schemaext.ChangeRecord{{Subject: changed, Value: &comparedChange{Number: 4}}}
	_, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(received.Changes, qt.HasLen, 0)
	c.Assert(received.Objects.ChangedSubjects, qt.DeepEquals, []objectidentity.ID{comparedRef(conversionSecond, "second")})
}

func TestPlanningDependencyReadinessRefusesBeforeDispatch(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(map[bool]string{false: "desired", true: "current"}[observed], func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			p := objectPlanningProvider(planningFunc(func(ctx context.Context, r featureplan.Request) (featureplan.Result, error) {
				calls++
				return plannedObjects(ctx, r)
			}))
			objects, err := schemaext.NewObjects(schemaext.Object{Ref: comparedRef(conversionFirst, "pending"), Value: &conversionValue{ID: conversionFirst, NeedsError: true}})
			c.Assert(err, qt.IsNil)
			request := featureplan.Request{Target: "custom"}
			if observed {
				request.Objects.Current.Objects = objects
			} else {
				request.Objects.Desired.Objects = objects
			}
			result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestPlanningObjectUnionPreservesDesiredSpellingWithoutParentReceipts(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, objectPlanningProvider(planningFunc(plannedObjects)))
	request := objectPlanningRequest(c)
	desired := comparedRef(conversionFirst, "FIRST")
	desired.Name.Normalized = "first"
	missing := comparedRef(conversionFirst, "missing")
	parent := comparisonRequest(c).Parents[0].Subject
	request.Objects.Desired.Coverage = comparisonCoverage(c, runtime.Codecs(), schemaext.Uninspected,
		schemaext.SubjectCoverage{Kind: conversionFirst, Subject: desired, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		schemaext.SubjectCoverage{Kind: conversionFirst, Subject: missing, Knowledge: schemaext.Knowledge{State: schemaext.Absent}},
		schemaext.SubjectCoverage{Kind: conversionFirst, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	desiredObjects, err := schemaext.NewObjects(schemaext.Object{Ref: desired, Value: &conversionValue{ID: conversionFirst}})
	c.Assert(err, qt.IsNil)
	request.Objects.Desired.Objects = desiredObjects
	c.Assert(request.Objects.Subjects([]schemaext.Kind{conversionFirst}), qt.DeepEquals, []objectidentity.ID{desired, missing})
	result, err := runtime.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Objects, qt.HasLen, 2)
	c.Assert(result.Objects[0].Subject, qt.Equals, desired)
	c.Assert(result.Objects[1].Subject, qt.Equals, missing)
}
