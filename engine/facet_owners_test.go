package engine_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func mixedFacetProvider(service schemaext.FacetComparisonService) engine.Provider {
	provider := facetProvider(service)
	provider.FacetComparisons[0].Kinds = []schemaext.Kind{conversionFirst}
	provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{
		Target: "custom", OwnerKinds: []objectidentity.Kind{objectidentity.KindIndex},
		Kinds: []schemaext.Kind{conversionSecond}, ChangeKinds: []schemaext.Kind{comparedKind}, Service: service,
	})
	return provider
}

func mixedFacetRequest() schemaext.FacetComparisonRequest {
	request := facetRequest()
	index := objectidentity.NewBuilder(identifier.ForDialect("mysql")).IndexParts("", "orders", "by.status")
	request.Owners = append(request.Owners, schemaext.ParentState{Subject: index, Desired: true, Current: true})
	for _, state := range []*schemaext.FacetState{&request.Desired, &request.Current} {
		value := must.Must(schemaext.NewFacets(&conversionValue{ID: conversionSecond, Number: 3}))
		state.Records[0].Values = state.Records[0].Values.Without(conversionSecond)
		state.Records = append(state.Records, schemaext.FacetRecord{Subject: index, Values: value})
	}
	return request
}

func TestFacetComparisonRequiresExplicitDistinctOwnerKinds(t *testing.T) {
	for _, test := range []struct {
		name  string
		kinds []objectidentity.Kind
	}{
		{name: "missing"},
		{name: "empty", kinds: []objectidentity.Kind{""}},
		{name: "whitespace", kinds: []objectidentity.Kind{" table"}},
		{name: "duplicate", kinds: []objectidentity.Kind{objectidentity.KindTable, objectidentity.KindTable}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				return facetReply(r), nil
			}))
			provider.FacetComparisons[0].OwnerKinds = test.kinds
			_, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		})
	}
}

func TestFacetComparisonRoutesOwnersWithoutDependingOnValues(t *testing.T) {
	for _, test := range []struct {
		name      string
		namespace bool
	}{
		{name: "concrete values"},
		{name: "coverage without values", namespace: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			wantKinds := map[schemaext.Kind]objectidentity.Kind{conversionFirst: objectidentity.KindTable, conversionSecond: objectidentity.KindIndex}
			calls := make(map[schemaext.Kind]int)
			provider := mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				c.Assert(r.Kinds, qt.HasLen, 1)
				c.Assert(r.Owners, qt.HasLen, 1)
				kind := r.Kinds[0]
				calls[kind]++
				c.Assert(r.Owners[0].Subject.Kind, qt.Equals, wantKinds[kind])
				return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
			}))
			runtime := mustRuntime(c, provider)
			// Registration captures this slice; later mutations must not reroute it.
			provider.FacetComparisons[0].OwnerKinds[0] = objectidentity.KindIndex
			request := mixedFacetRequest()
			request.Desired = map[bool]schemaext.FacetState{
				false: request.Desired, true: {Coverage: facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)},
			}[test.namespace]
			request.Current = map[bool]schemaext.FacetState{
				false: request.Current, true: {Coverage: facetCoverage(runtime.Codecs(), schemaext.Observed, schemaext.Complete)},
			}[test.namespace]
			result := must.Must(runtime.CompareFacets(t.Context(), request))
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(calls, qt.DeepEquals, map[schemaext.Kind]int{conversionFirst: 1, conversionSecond: 1})
			c.Assert(result.Desired.Coverage, qt.DeepEquals, request.Desired.Coverage)
			c.Assert(result.Desired.Records, qt.HasLen, len(request.Desired.Records))
			c.Assert(request.Owners, qt.HasLen, 2)
		})
	}
}

func TestFacetComparisonRefusesMisplacedValuesAndCoverageBeforeDispatch(t *testing.T) {
	for _, side := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		for _, source := range []string{"value", "coverage"} {
			t.Run(string(side)+"/"+source, func(t *testing.T) {
				c := qt.New(t)
				calls := 0
				runtime := mustRuntime(c, mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
					calls++
					return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
				})))
				request := mixedFacetRequest()
				states := map[schemaext.Representation]*schemaext.FacetState{schemaext.Desired: &request.Desired, schemaext.Observed: &request.Current}
				state := states[side]
				misplaced := *state
				misplaced.Records = slices.Clone(state.Records)
				misplaced.Records[1].Values = must.Must(state.Records[1].Values.With(&conversionValue{ID: conversionFirst}))
				covered := *state
				covered.Coverage = facetCoverage(runtime.Codecs(), side, schemaext.Uninspected, schemaext.SubjectCoverage{
					Kind: conversionFirst, Subject: request.Owners[1].Subject,
					Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "unavailable"},
				})
				*state = map[string]schemaext.FacetState{"value": misplaced, "coverage": covered}[source]
				result, err := runtime.CompareFacets(t.Context(), request)
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
				combined, err := runtime.CompareFeatures(t.Context(), schemaext.ComparisonRequest{
					Target: request.Target, Owners: request.Owners,
					Desired: schemaext.FeatureState{Facets: request.Desired.Records, Coverage: request.Desired.Coverage},
					Current: schemaext.FeatureState{Facets: request.Current.Records, Coverage: request.Current.Coverage},
				})
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(combined, qt.DeepEquals, schemaext.ComparisonResult{})
				c.Assert(calls, qt.Equals, 0)
			})
		}
	}
}

func TestUnrelatedOwnerDoesNotReactivateExcludedFacetNamespace(t *testing.T) {
	c := qt.New(t)
	calls := 0
	runtime := mustRuntime(c, mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		calls++
		c.Assert(r.Kinds, qt.DeepEquals, []schemaext.Kind{conversionSecond})
		c.Assert(r.Owners[0].Subject.Kind, qt.Equals, objectidentity.KindIndex)
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	})))
	request := mixedFacetRequest()
	request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(conversionFirst, "foreign"))
	request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
	request.Current.Coverage = facetCoverage(runtime.Codecs(), schemaext.Observed, schemaext.Complete)
	result := must.Must(runtime.CompareFacets(t.Context(), request))
	c.Assert(calls, qt.Equals, 1)
	c.Assert(result.Desired.Coverage.KindRecords(), qt.HasLen, 1)
	c.Assert(result.Desired.Coverage.KindRecords()[0].Model.Kind, qt.Equals, conversionSecond)
	c.Assert(result.Desired.Records, qt.HasLen, 2)
}

func TestFacetComparisonRetainsForeignBindingsOutsideSelectedOwnerKinds(t *testing.T) {
	c := qt.New(t)
	calls := 0
	runtime := mustRuntime(c, mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		calls++
		c.Assert(r.Owners, qt.HasLen, 1)
		c.Assert(r.Desired.Records, qt.HasLen, 1)
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	})))
	request := mixedFacetRequest()
	values := must.Must(request.Desired.Records[1].Values.With(&conversionValue{ID: conversionFirst}))
	request.Desired.Records[1].Values = must.Must(values.WithTargetScope(conversionFirst, "foreign"))
	result := must.Must(runtime.CompareFacets(t.Context(), request))
	position := slices.IndexFunc(result.Desired.Records, func(record schemaext.FacetRecord) bool {
		return record.Subject.Kind == objectidentity.KindIndex
	})
	c.Assert(position, qt.Not(qt.Equals), -1)
	values = result.Desired.Records[position].Values
	c.Assert(values.TargetScope(conversionFirst), qt.DeepEquals, []string{"foreign"})
	c.Assert(values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionSecond})
	request.Desired = result.Desired
	again := must.Must(runtime.CompareFacets(t.Context(), request))
	c.Assert(again.Desired, qt.DeepEquals, result.Desired)
	c.Assert(calls, qt.Equals, 4)
}
