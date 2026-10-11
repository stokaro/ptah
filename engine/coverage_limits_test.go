package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// firstLimit maps the policy kind onto the first model, naming an object by
// its own name.
var firstLimit = schemaext.LimitMapping{Header: coverage.Policy, Model: conversionFirst,
	Names: func(ref objectidentity.ID) []string { return []string{ref.Name.Source} }}

// limitedComparison returns a runtime whose provider maps mappings, and a
// pointer to the request its comparison service last received.
func limitedComparison(c *qt.C, mappings ...schemaext.LimitMapping) (*engine.Runtime, *schemaext.ObjectComparisonRequest) {
	c.Helper()
	received := new(schemaext.ObjectComparisonRequest)
	provider := comparisonProvider(comparisonFunc(func(_ context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		*received = request
		return schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}, nil
	}))
	provider.CoverageLimits = mappings
	return mustRuntime(c, provider), received
}

// heldOnOneSide is a comparison whose objects only one side holds, and both
// sides describe both models completely. held says which side holds them.
func heldOnOneSide(c *qt.C, runtime *engine.Runtime, held schemaext.Representation) schemaext.ComparisonRequest {
	c.Helper()
	objects := comparisonRequest(c)
	request := schemaext.ComparisonRequest{Target: "alternate", Owners: objects.Parents,
		Desired: schemaext.FeatureState{Coverage: facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)},
		Current: schemaext.FeatureState{Coverage: facetCoverage(runtime.Codecs(), schemaext.Observed, schemaext.Complete)},
	}
	sides := map[schemaext.Representation]*schemaext.FeatureState{schemaext.Desired: &request.Desired, schemaext.Observed: &request.Current}
	sides[held].Objects = objects.Desired.Objects
	return request
}

// TestFeatureComparison_FoldsMappedLimits_HappyPath pins that a side's
// not-described record reaches the owner as knowledge of the model it maps
// onto, asked about the objects the other side holds, and that a record
// nothing maps reaches no owner.
func TestFeatureComparison_FoldsMappedLimits_HappyPath(t *testing.T) {
	first := comparedRef(conversionFirst, "first")

	t.Run("the desired side's whole-kind record leaves the model uninspected", func(t *testing.T) {
		c := qt.New(t)
		runtime, received := limitedComparison(c, firstLimit)
		request := heldOnOneSide(c, runtime, schemaext.Observed)
		request.DesiredLimits = coverage.Set{}.WithKind(coverage.Policy)

		_, err := runtime.CompareFeatures(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(received.Desired.Coverage.Lookup(conversionFirst, first), qt.Equals,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the description does not describe policy"})
		c.Assert(received.Desired.Coverage.Lookup(conversionSecond, comparedRef(conversionSecond, "second")).State, qt.Equals, schemaext.Complete)
	})

	t.Run("the current side's record naming an object leaves that object unknown", func(t *testing.T) {
		c := qt.New(t)
		runtime, received := limitedComparison(c, firstLimit)
		request := heldOnOneSide(c, runtime, schemaext.Desired)
		request.CurrentLimits = coverage.Set{}.WithObject(coverage.Policy, "first")

		_, err := runtime.CompareFeatures(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(received.Current.Coverage.Lookup(conversionFirst, first), qt.Equals,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: `the description does not describe policy "first"`})
	})

	t.Run("control: without the record the model stays complete", func(t *testing.T) {
		c := qt.New(t)
		runtime, received := limitedComparison(c, firstLimit)

		_, err := runtime.CompareFeatures(t.Context(), heldOnOneSide(c, runtime, schemaext.Observed))

		c.Assert(err, qt.IsNil)
		c.Assert(received.Desired.Coverage.Lookup(conversionFirst, first).State, qt.Equals, schemaext.Complete)
	})

	t.Run("control: a record nothing maps changes nothing", func(t *testing.T) {
		c := qt.New(t)
		runtime, received := limitedComparison(c)
		request := heldOnOneSide(c, runtime, schemaext.Observed)
		request.DesiredLimits = coverage.Set{}.WithKind(coverage.Policy)

		_, err := runtime.CompareFeatures(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(received.Desired.Coverage.Lookup(conversionFirst, first).State, qt.Equals, schemaext.Complete)
	})
}

// TestRuntime_CoverageLimits_FailurePath refuses a mapping the runtime could
// never apply, before any service runs.
func TestRuntime_CoverageLimits_FailurePath(t *testing.T) {
	names := func(objectidentity.ID) []string { return nil }
	tests := []struct {
		name     string
		mappings []schemaext.LimitMapping
		want     string
	}{
		{
			name:     "a mapping without names",
			mappings: []schemaext.LimitMapping{{Header: coverage.Policy, Model: conversionFirst}},
			want:     `.*provider "example.org/converter": invalid feature value: a coverage limit mapping needs a not-described kind, a model and its names`,
		},
		{
			name:     "a kind no selected provider registers",
			mappings: []schemaext.LimitMapping{{Header: "changefeed", Model: conversionFirst, Names: names}},
			want:     `.*provider "example.org/converter" maps a coverage kind this runtime does not read: unknown coverage kind "changefeed".*`,
		},
		{
			name:     "a model the provider does not own",
			mappings: []schemaext.LimitMapping{{Header: coverage.Policy, Model: "example.org/unowned", Names: names}},
			want:     `.*maps coverage kind "policy" onto "example.org/unowned", which is not a named-object model it owns`,
		},
		{
			name:     "one pair twice",
			mappings: []schemaext.LimitMapping{firstLimit, firstLimit},
			want:     `.*coverage kind "policy" is mapped onto "example.org/first" by "example.org/converter" and "example.org/converter"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := comparisonProvider(comparisonFunc(unchangedComparison))
			provider.CoverageLimits = test.mappings

			runtime, err := engine.New(provider)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(runtime, qt.IsNil)
		})
	}

	t.Run("a facet model", func(t *testing.T) {
		c := qt.New(t)
		provider := combinedProvider(comparisonFunc(unchangedComparison), facetComparisonFunc(changedFacet))
		provider.CoverageLimits = []schemaext.LimitMapping{{Header: coverage.Policy, Model: conversionSecond, Names: names}}

		runtime, err := engine.New(provider)

		c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		c.Assert(err, qt.ErrorMatches, `.*onto "example.org/second", which is not a named-object model it owns`)
		c.Assert(runtime, qt.IsNil)
	})

	t.Run("control: the same mapping onto a named-object model registers", func(t *testing.T) {
		c := qt.New(t)
		provider := comparisonProvider(comparisonFunc(unchangedComparison))
		provider.CoverageLimits = []schemaext.LimitMapping{firstLimit}

		c.Assert(must.Must(engine.New(provider)), qt.IsNotNil)
	})
}
