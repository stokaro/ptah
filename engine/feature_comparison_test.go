package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func combinedProvider(objects schemaext.ObjectComparisonService, facets schemaext.FacetComparisonService) engine.Provider {
	provider := comparisonProvider(objects)
	provider.Comparisons[0].Kinds = []schemaext.Kind{conversionFirst}
	provider.FacetComparisons = []engine.FacetComparison{{Target: "custom", OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Kinds: []schemaext.Kind{conversionSecond}, ChangeKinds: []schemaext.Kind{comparedKind}, Service: facets}}
	return provider
}

func combinedRequest(c *qt.C) schemaext.ComparisonRequest {
	c.Helper()
	objects, facets := comparisonRequest(c), facetRequest()
	return schemaext.ComparisonRequest{
		Target: "alternate", Owners: facets.Owners,
		Desired: schemaext.FeatureState{
			Objects: objects.Desired.Objects.Select(func(ref objectidentity.ID) bool { return schemaext.Kind(ref.Kind) == conversionFirst }),
			Facets:  []schemaext.FacetRecord{{Subject: facets.Owners[0].Subject, Values: facets.Desired.Records[0].Values.Without(conversionFirst)}},
		},
		Current: schemaext.FeatureState{Facets: []schemaext.FacetRecord{{Subject: facets.Owners[0].Subject, Values: facets.Current.Records[0].Values.Without(conversionFirst)}}},
	}
}

func changedObject(_ context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	return schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired, Changes: []schemaext.ChangeRecord{{Subject: comparedRef(conversionFirst, "first"), Value: &comparedChange{Number: 1}}}}, nil
}

func changedFacet(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	result := facetReply(request)
	result.Changes[0].Kind = conversionSecond
	return result, nil
}

func TestFeatureComparisonCombinesSeparateModelsAndCoverage(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, combinedProvider(comparisonFunc(changedObject), facetComparisonFunc(changedFacet)))
	request := combinedRequest(c)
	request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
	result := must.Must(runtime.CompareFeatures(t.Context(), request))
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.FacetChanges, qt.HasLen, 1)
	c.Assert(result.FacetChanges[0].Change.Subject, qt.DeepEquals, request.Owners[0].Subject)
	c.Assert(result.Desired.Coverage.KindRecords(), qt.DeepEquals, request.Desired.Coverage.KindRecords())
	c.Assert(result.Desired.Objects, qt.DeepEquals, request.Desired.Objects)
	c.Assert(result.Desired.Facets, qt.DeepEquals, request.Desired.Facets)
}

func TestFeatureComparisonValidatesBothSurfacesBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		adjust func(*schemaext.ComparisonRequest)
	}{
		{name: "duplicate facet owner", adjust: func(r *schemaext.ComparisonRequest) { r.Desired.Facets = append(r.Desired.Facets, r.Desired.Facets[0]) }},
		{name: "facet without common owner", adjust: func(r *schemaext.ComparisonRequest) { r.Owners = nil }},
		{name: "named object placed in facet", adjust: func(r *schemaext.ComparisonRequest) {
			r.Desired.Facets[0].Values = must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst}))
		}},
		{name: "facet placed in named objects", adjust: func(r *schemaext.ComparisonRequest) {
			r.Desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: comparedRef(conversionSecond, "misplaced"), Value: &conversionValue{ID: conversionSecond}}))
			r.Desired.Facets, r.Current.Facets = nil, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := mustRuntime(c, combinedProvider(comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
				calls++
				return changedObject(ctx, r)
			}), facetComparisonFunc(func(ctx context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				calls++
				return changedFacet(ctx, r)
			})))
			request := combinedRequest(c)
			test.adjust(&request)
			result, err := runtime.CompareFeatures(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(calls, qt.Equals, 0)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

func TestFeatureComparisonFacetFailureDiscardsCompletedObjectWork(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("facet provider disconnected")
	calls := 0
	runtime := mustRuntime(c, combinedProvider(comparisonFunc(func(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		calls++
		return changedObject(ctx, request)
	}), facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return facetReply(request), failure
	})))
	result, err := runtime.CompareFeatures(t.Context(), combinedRequest(c))
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.FacetChanges, qt.HasLen, 0)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
}

func TestFeatureComparisonRejectsAmbiguousModelRole(t *testing.T) {
	c := qt.New(t)
	provider := combinedProvider(comparisonFunc(changedObject), facetComparisonFunc(changedFacet))
	provider.Comparisons[0].Kinds = append(provider.Comparisons[0].Kinds, conversionSecond)
	runtime, err := engine.New(provider)
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}
