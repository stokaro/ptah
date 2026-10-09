package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type facetComparisonFunc func(context.Context, schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error)

func (f facetComparisonFunc) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return f(ctx, request)
}

func facetProvider(service schemaext.FacetComparisonService) engine.Provider {
	provider := comparisonProvider(nil)
	provider.Comparisons = nil
	provider.FacetComparisons = []engine.FacetComparison{{Target: "custom", OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, ChangeKinds: []schemaext.Kind{comparedKind}, Service: service}}
	return provider
}

func facetRequest() schemaext.FacetComparisonRequest {
	owner := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders")
	return schemaext.FacetComparisonRequest{
		Target: "alternate", Owners: []schemaext.ParentState{{Subject: owner, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: owner, Values: must.Must(schemaext.NewFacets(
			&conversionValue{ID: conversionFirst, Number: 1}, &conversionValue{ID: conversionSecond, Number: 2},
		))}}},
		Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: owner, Values: must.Must(schemaext.NewFacets(
			&conversionValue{ID: conversionFirst}, &conversionValue{ID: conversionSecond},
		))}}},
	}
}

func facetReply(request schemaext.FacetComparisonRequest) schemaext.FacetComparisonResult {
	return schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired, Changes: []schemaext.FacetChange{{
		Kind: conversionFirst, Change: schemaext.ChangeRecord{Subject: request.Owners[0].Subject, Value: &comparedChange{Number: 9}},
	}}}
}

func TestFacetComparisonBatchesModelsAndOwnsReplySnapshots(t *testing.T) {
	c := qt.New(t)
	calls := 0
	var received schemaext.FacetComparisonRequest
	var reply schemaext.FacetComparisonResult
	provider := facetProvider(facetComparisonFunc(func(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		received = request
		reply = facetReply(request)
		return reply, nil
	}))
	runtime := mustRuntime(c, provider)
	provider.FacetComparisons[0].Kinds[0] = "example.org/changed"
	provider.FacetComparisons[0].Service = nil
	request := facetRequest()
	result, err := runtime.CompareFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(received.Kinds, qt.DeepEquals, []schemaext.Kind{conversionFirst, conversionSecond})
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	c.Assert(result.Desired.Records[0].Values.Len(), qt.Equals, 2)
	reply.Changes[0].Change.Value.(*comparedChange).Number = 100
	received.Owners[0].Desired = false
	c.Assert(result.Changes[0].Change.Value.(*comparedChange).Number, qt.Equals, 9)
	c.Assert(request.Owners[0].Desired, qt.IsTrue)
}

func TestFacetComparisonRefusesMissingEvidenceAndParentOwnedChanges(t *testing.T) {
	for _, test := range []struct {
		name   string
		adjust func(*schemaext.FacetComparisonRequest)
	}{
		{name: "uninspected current value", adjust: func(r *schemaext.FacetComparisonRequest) { r.Current = schemaext.FacetState{} }},
		{name: "undeclared desired state", adjust: func(r *schemaext.FacetComparisonRequest) { r.Desired = schemaext.FacetState{} }},
		{name: "new common owner", adjust: func(r *schemaext.FacetComparisonRequest) {
			r.Current = schemaext.FacetState{}
			r.Owners[0].Current = false
		}},
		{name: "removed common owner", adjust: func(r *schemaext.FacetComparisonRequest) {
			r.Desired = schemaext.FacetState{}
			r.Owners[0].Desired = false
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := facetRequest()
			test.adjust(&request)
			runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				return facetReply(r), nil
			})))
			result, err := runtime.CompareFacets(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestFacetComparisonRefusesLostDeclarationsAndIncompleteReplies(t *testing.T) {
	for _, test := range []struct {
		name   string
		adjust func(*schemaext.FacetComparisonResult)
	}{
		{name: "incomplete", adjust: func(r *schemaext.FacetComparisonResult) { r.Complete = false }},
		{name: "lost declaration", adjust: func(r *schemaext.FacetComparisonResult) { r.Desired.Records = nil }},
		{name: "duplicate change", adjust: func(r *schemaext.FacetComparisonResult) { r.Changes = append(r.Changes, r.Changes[0]) }},
		{name: "conflicting diagnostic", adjust: func(r *schemaext.FacetComparisonResult) {
			r.Undecided = []schemaext.UndecidedChange{{Kind: conversionFirst, Subject: r.Changes[0].Change.Subject, Reason: "unknown"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				result := facetReply(request)
				test.adjust(&result)
				return result, nil
			})))
			result, err := runtime.CompareFacets(t.Context(), facetRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestFacetComparisonFailureAndCancellationDiscardOutput(t *testing.T) {
	failure := errors.New("facet owner disconnected")
	for _, test := range []struct {
		name   string
		cancel bool
		err    error
		want   error
	}{
		{name: "provider failure", err: failure, want: failure},
		{name: "cancellation", cancel: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelCall := map[bool]func(){true: cancel, false: func() {}}[test.cancel]
			runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				cancelCall()
				return facetReply(request), test.err
			})))
			result, err := runtime.CompareFacets(ctx, facetRequest())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestFacetComparisonRequiresSelectedOwner(t *testing.T) {
	c := qt.New(t)
	provider := facetProvider(nil)
	provider.FacetComparisons = nil
	runtime := mustRuntime(c, provider)
	result, err := runtime.CompareFacets(t.Context(), facetRequest())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Complete, qt.IsFalse)
}

func facetCoverage(registry schemaext.Registry, representation schemaext.Representation, state schemaext.KnowledgeState, claims ...schemaext.SubjectCoverage) schemaext.Coverage {
	var definitions []schemaext.KindCoverage
	for _, model := range registry.Definitions() {
		if (model.Kind == conversionFirst || model.Kind == conversionSecond) && model.Representation == representation {
			definitions = append(definitions, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: state, Reason: "source knowledge"}})
		}
	}
	return must.Must(schemaext.NewCoverage(representation, definitions, claims))
}

func TestFacetComparisonRespectsSubjectLimitsOverConcreteValues(t *testing.T) {
	for _, side := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
			t.Run(string(side)+"/"+string(state), func(t *testing.T) {
				c := qt.New(t)
				runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
					return facetReply(request), nil
				})))
				request := facetRequest()
				states := map[schemaext.Representation]*schemaext.FacetState{schemaext.Desired: &request.Desired, schemaext.Observed: &request.Current}
				states[side].Coverage = facetCoverage(runtime.Codecs(), side, schemaext.Complete, schemaext.SubjectCoverage{
					Kind: conversionFirst, Subject: request.Owners[0].Subject, Knowledge: schemaext.Knowledge{State: state, Reason: "partial settings"},
				})
				result, err := runtime.CompareFacets(t.Context(), request)
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(result.Complete, qt.IsFalse)
				c.Assert(result.Changes, qt.HasLen, 0)
			})
		}
	}
}

func TestFacetComparisonDistinguishesAbsentAndUnknownSettings(t *testing.T) {
	for _, side := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		t.Run(string(side), func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				return facetReply(request), nil
			})))
			request := facetRequest()
			states := map[schemaext.Representation]*schemaext.FacetState{schemaext.Desired: &request.Desired, schemaext.Observed: &request.Current}
			states[side].Records = nil
			unknown, err := runtime.CompareFacets(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(unknown.Complete, qt.IsFalse)
			states[side].Coverage = facetCoverage(runtime.Codecs(), side, schemaext.Uninspected, schemaext.SubjectCoverage{
				Kind: conversionFirst, Subject: request.Owners[0].Subject, Knowledge: schemaext.Knowledge{State: schemaext.Absent},
			})
			known := must.Must(runtime.CompareFacets(t.Context(), request))
			c.Assert(known.Complete, qt.IsTrue)
			c.Assert(known.Changes, qt.HasLen, 1)
		})
	}
}

func TestFacetComparisonRejectsCompetingChangesFromSeparateBatches(t *testing.T) {
	c := qt.New(t)
	provider := facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return facetReply(request), nil
	}))
	provider.FacetComparisons[0].Kinds = []schemaext.Kind{conversionFirst}
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target: "custom", OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Kinds: []schemaext.Kind{conversionSecond}, ChangeKinds: []schemaext.Kind{comparedKind}, Service: facetComparisonFunc(changedFacet),
	})
	runtime := mustRuntime(c, provider)
	result, err := runtime.CompareFacets(t.Context(), facetRequest())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Changes, qt.HasLen, 0)
}

func TestComparisonWithoutHandlerDistinguishesNoClaimFromEncounteredState(t *testing.T) {
	for _, test := range []struct {
		name  string
		state schemaext.KnowledgeState
		want  error
	}{
		{name: "known empty namespace", state: schemaext.Complete},
		{name: "uninspected namespace", state: schemaext.Uninspected},
		{name: "unrepresentable namespace", state: schemaext.Unrepresentable, want: ptaherr.ErrUnsupportedFeature},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := facetProvider(nil)
			provider.FacetComparisons = nil
			runtime := mustRuntime(c, provider)
			coverage := facetCoverage(runtime.Codecs(), schemaext.Desired, test.state)
			facets := facetRequest()
			facets.Desired = schemaext.FacetState{Coverage: coverage}
			facets.Current = schemaext.FacetState{}
			_, err := runtime.CompareFacets(t.Context(), facets)
			c.Assert(err, qt.ErrorIs, test.want)
			_, err = runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
				Target: "alternate", Desired: schemaext.ObjectState{Coverage: coverage}, Parents: facets.Owners,
			})
			c.Assert(err, qt.ErrorIs, test.want)
			_, err = runtime.CompareFeatures(t.Context(), schemaext.ComparisonRequest{
				Target: "alternate", Desired: schemaext.FeatureState{Coverage: coverage}, Owners: facets.Owners,
			})
			c.Assert(err, qt.ErrorIs, test.want)
		})
	}
}

func TestExplicitFacetLimitRequiresHandlerEvenWithoutValues(t *testing.T) {
	c := qt.New(t)
	provider := facetProvider(nil)
	provider.FacetComparisons = nil
	runtime := mustRuntime(c, provider)
	request := facetRequest()
	request.Desired = schemaext.FacetState{Coverage: facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Uninspected, schemaext.SubjectCoverage{
		Kind: conversionFirst, Subject: request.Owners[0].Subject, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "this table was not inspected"},
	})}
	request.Current = schemaext.FacetState{}
	_, err := runtime.CompareFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = runtime.CompareFeatures(t.Context(), schemaext.ComparisonRequest{Target: request.Target, Desired: schemaext.FeatureState{Coverage: request.Desired.Coverage}, Owners: request.Owners})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}

func TestUninspectedNamespaceKeepsItsKnowledgeWithoutTargetHandler(t *testing.T) {
	c := qt.New(t)
	provider := facetProvider(nil)
	provider.FacetComparisons = nil
	runtime := mustRuntime(c, provider)
	coverage := facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Uninspected)
	result := must.Must(runtime.CompareFeatures(t.Context(), schemaext.ComparisonRequest{Target: "alternate", Desired: schemaext.FeatureState{Coverage: coverage}}))
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Desired.Coverage.KindRecords(), qt.DeepEquals, coverage.KindRecords())
}
