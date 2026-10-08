package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func TestFacetComparisonScopeKeepsOtherModelsOnSameOwner(t *testing.T) {
	c := qt.New(t)
	calls := 0
	runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		calls++
		subject := r.Owners[0].Subject
		c.Assert(r.Kinds, qt.DeepEquals, []schemaext.Kind{conversionSecond})
		c.Assert(r.Desired.Records[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionSecond})
		c.Assert(r.Current.Records[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionSecond})
		c.Assert(r.Includes(conversionSecond, subject), qt.IsTrue)
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired, Changes: []schemaext.FacetChange{{
			Kind: conversionSecond, Change: schemaext.ChangeRecord{Subject: subject, Value: &comparedChange{Number: 7}},
		}}}, nil
	})))
	request := facetRequest()
	request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(conversionFirst, "foreign"))
	request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
	request.Current.Coverage = facetCoverage(runtime.Codecs(), schemaext.Observed, schemaext.Complete)
	result := must.Must(runtime.CompareFacets(t.Context(), request))
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Kind, qt.Equals, conversionSecond)
	c.Assert(result.Desired.Records[0].Values.TargetScope(conversionFirst), qt.DeepEquals, []string{"foreign"})
	c.Assert(result.Desired.Records[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionSecond})
	request.Desired = result.Desired
	again := must.Must(runtime.CompareFacets(t.Context(), request))
	c.Assert(again.Desired, qt.DeepEquals, result.Desired)
	c.Assert(calls, qt.Equals, 2)
}

func TestExcludedFacetNeedsNoSelectedCodecOrComparer(t *testing.T) {
	c := qt.New(t)
	known := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	})))
	request := facetRequest()
	for _, kind := range request.Desired.Records[0].Values.Kinds() {
		request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(kind, "foreign"))
	}
	request.Desired.Coverage = facetCoverage(known.Codecs(), schemaext.Desired, schemaext.Complete)
	request.Current.Coverage = facetCoverage(known.Codecs(), schemaext.Observed, schemaext.Complete)
	runtime := mustRuntime(c, engine.Provider{ID: "example.org/empty", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}}})
	result := must.Must(runtime.CompareFacets(t.Context(), request))
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Desired.Records[0].Values.DeclaredKinds(), qt.HasLen, 2)
	c.Assert(result.Desired.Records[0].Values.Len(), qt.Equals, 0)
	combined := must.Must(runtime.CompareFeatures(t.Context(), schemaext.ComparisonRequest{
		Target: request.Target, Owners: request.Owners,
		Desired: schemaext.FeatureState{Facets: request.Desired.Records, Coverage: request.Desired.Coverage},
		Current: schemaext.FeatureState{Facets: request.Current.Records, Coverage: request.Current.Coverage},
	}))
	c.Assert(combined.Desired.Facets, qt.DeepEquals, result.Desired.Records)
	request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(conversionFirst, "alternate"))
	_, err := runtime.CompareFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(conversionFirst, "foreign"))
	other := request.Owners[0]
	other.Subject.Name.Source, other.Subject.Name.Normalized = "other", "other"
	request.Owners = append(request.Owners, other)
	_, err = runtime.CompareFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
}

func TestFacetComparisonRefusesActionsOnAnExcludedOwner(t *testing.T) {
	for _, test := range []struct {
		name   string
		adjust func(*schemaext.FacetComparisonResult)
	}{
		{name: "change", adjust: func(result *schemaext.FacetComparisonResult) {
			result.Changes = []schemaext.FacetChange{{Kind: conversionFirst, Change: schemaext.ChangeRecord{Subject: result.Desired.Records[0].Subject, Value: &comparedChange{Number: 9}}}}
		}},
		{name: "diagnostic", adjust: func(result *schemaext.FacetComparisonResult) {
			result.Undecided = []schemaext.UndecidedChange{{Kind: conversionFirst, Subject: result.Desired.Records[0].Subject, Reason: "must not compare an exclusion"}}
		}},
		{name: "adoption", adjust: func(result *schemaext.FacetComparisonResult) {
			result.Desired.Records[0].Values = result.Desired.Records[0].Values.Without(conversionFirst)
			result.Desired.Records[0].Values = must.Must(result.Desired.Records[0].Values.With(&conversionValue{ID: conversionFirst}))
		}},
		{name: "lost exclusion", adjust: func(result *schemaext.FacetComparisonResult) {
			result.Desired.Records[0].Values = result.Desired.Records[0].Values.Without(conversionFirst)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := facetRequest()
			excluded := request.Owners[0].Subject
			other := excluded
			other.Name.Source, other.Name.Normalized = "other", "other"
			request.Owners = append(request.Owners, schemaext.ParentState{Subject: other, Desired: true, Current: true})
			request.Desired.Records[0].Values = must.Must(request.Desired.Records[0].Values.WithTargetScope(conversionFirst, "foreign"))
			calls := 0
			runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				calls++
				c.Assert(r.Includes(conversionFirst, excluded), qt.IsFalse)
				c.Assert(r.Current.Records[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionSecond})
				c.Assert(r.Includes(conversionFirst, other), qt.IsTrue)
				c.Assert(r.Includes(conversionSecond, excluded), qt.IsTrue)
				result := schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}
				test.adjust(&result)
				return result, nil
			})))
			request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
			request.Current.Coverage = facetCoverage(runtime.Codecs(), schemaext.Observed, schemaext.Complete)
			result, err := runtime.CompareFacets(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(calls, qt.Equals, 1)
		})
	}
}
