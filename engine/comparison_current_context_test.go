package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// desiredOnlyKind is a model the comparison provider registers a desired
// codec for and no observed one.
const desiredOnlyKind schemaext.Kind = "example.org/desired-only"

// coverageKinds names the models coverage enrolls, in the order it lists them.
func coverageKinds(coverage schemaext.Coverage) []schemaext.Kind {
	kinds := make([]schemaext.Kind, 0, len(coverage.KindRecords()))
	for _, record := range coverage.KindRecords() {
		kinds = append(kinds, record.Model.Kind)
	}
	return kinds
}

// TestObjectComparison_DeliversTheCurrentObjectsOfItsDeclaredKinds pins that a
// service that reads a current context receives the current objects of its
// declared kinds and their coverage, which its own batch of the current state
// leaves out, and that a service that reads none receives the zero value.
func TestObjectComparison_DeliversTheCurrentObjectsOfItsDeclaredKinds(t *testing.T) {
	c := qt.New(t)
	var first, second schemaext.ObjectComparisonRequest
	provider := declaredObjectsProvider(recordingComparison(&first), recordingComparison(&second), conversionSecond)
	provider.Comparisons[0].CurrentContext = true
	runtime := mustRuntime(c, provider)
	request := comparisonRequest(c)
	request.Current = schemaext.ObjectState{Objects: request.Desired.Objects, Coverage: completeObservedCoverage(c, runtime.Codecs(), conversionFirst, conversionSecond)}

	_, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(first.Current.Objects.Refs(), qt.DeepEquals, []objectidentity.ID{comparedRef(conversionFirst, "first")})
	c.Assert(first.CurrentContext.Objects.Refs(), qt.DeepEquals, []objectidentity.ID{comparedRef(conversionSecond, "second")})
	c.Assert(coverageKinds(first.CurrentContext.Coverage), qt.DeepEquals, []schemaext.Kind{conversionSecond})
	c.Assert(first.CurrentContext.Facets, qt.HasLen, 0)
	c.Assert(second.CurrentContext.Objects.IsZero(), qt.IsTrue)
	c.Assert(second.CurrentContext.Coverage.IsZero(), qt.IsTrue)
}

// TestObjectComparison_DeliversTheCurrentFacetsOfItsDeclaredFacetKinds pins
// that a service that reads a current context receives every current facet of
// its declared facet kinds, each record holding those kinds alone, in owner
// order, with the coverage of those kinds, and not the current facets the
// caller named for the whole comparison.
func TestObjectComparison_DeliversTheCurrentFacetsOfItsDeclaredFacetKinds(t *testing.T) {
	c := qt.New(t)
	var first, second schemaext.ObjectComparisonRequest
	provider := declaredFacetsProvider(recordingComparison(&first), recordingComparison(&second), conversionSecond)
	provider.Comparisons[0].CurrentContext = true
	runtime := mustRuntime(c, provider)
	request := comparisonRequest(c)
	request.CurrentFacets = schemaext.FacetState{Records: []schemaext.FacetRecord{
		facetRecord(c, "users", &conversionValue{ID: conversionSecond, Number: 3}),
		facetRecord(c, "orders", &conversionValue{ID: conversionFirst, Number: 1}, &conversionValue{ID: conversionSecond, Number: 2}),
		facetRecord(c, "items", &conversionValue{ID: conversionFirst, Number: 4}),
	}, Coverage: completeObservedCoverage(c, runtime.Codecs(), conversionFirst, conversionSecond)}

	_, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(facetSummary(first.CurrentContext.Facets), qt.DeepEquals, []string{"orders: " + string(conversionSecond), "users: " + string(conversionSecond)})
	c.Assert(coverageKinds(first.CurrentContext.Coverage), qt.DeepEquals, []schemaext.Kind{conversionSecond})
	c.Assert(first.CurrentFacets.Records, qt.HasLen, 0)
	c.Assert(second.CurrentContext.Facets, qt.HasLen, 0)
}

// TestObjectComparison_RefusesAnInvalidCurrentContext pins what a service that
// reads a current context declares: at least one declared kind, each with an
// observed model its owner registers.
func TestObjectComparison_RefusesAnInvalidCurrentContext(t *testing.T) {
	t.Run("no declared kind", func(t *testing.T) {
		c := qt.New(t)
		provider := declaredObjectsProvider(comparisonFunc(unchangedComparison), comparisonFunc(unchangedComparison))
		provider.Comparisons[0].CurrentContext = true

		runtime, err := engine.New(provider)

		c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		c.Assert(err, qt.ErrorMatches, `.*reads a current context and names no declared kind`)
		c.Assert(runtime, qt.IsNil)
	})
	t.Run("a declared kind without an observed model", func(t *testing.T) {
		c := qt.New(t)
		provider := declaredFacetsProvider(comparisonFunc(unchangedComparison), comparisonFunc(unchangedComparison), desiredOnlyKind)
		provider.Codecs = append(provider.Codecs, conversionCodec(desiredOnlyKind, schemaext.Desired))
		provider.Comparisons[0].CurrentContext = true

		runtime, err := engine.New(provider)

		c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		c.Assert(err, qt.ErrorMatches, `.*invalid declared kind "example\.org/desired-only" .*`)
		c.Assert(runtime, qt.IsNil)
	})
}

// TestObjectComparison_ADeclaredKindNeedsNoObservedModelWithoutACurrentContext
// is the control of the observed-model refusal: a service that reads only the
// declared facets of a kind registers no observed model for it.
func TestObjectComparison_ADeclaredKindNeedsNoObservedModelWithoutACurrentContext(t *testing.T) {
	c := qt.New(t)
	provider := declaredFacetsProvider(comparisonFunc(unchangedComparison), comparisonFunc(unchangedComparison), desiredOnlyKind)
	provider.Codecs = append(provider.Codecs, conversionCodec(desiredOnlyKind, schemaext.Desired))

	runtime, err := engine.New(provider)

	c.Assert(err, qt.IsNil)
	c.Assert(runtime, qt.IsNotNil)
}
