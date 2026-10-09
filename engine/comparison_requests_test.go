package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// requestRecorder is an object comparison that keeps the request it received
// and changes nothing.
func requestRecorder(received *schemaext.ObjectComparisonRequest, calls *int) schemaext.ObjectComparisonService {
	return comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		*received = r
		*calls++
		return unchangedComparison(ctx, r)
	})
}

// TestObjectComparison_RoutesChangeRequestsToTheirOwner sends each change
// request to the owner of its subject's kind, in subject and action order, and
// puts that kind in the owner's batch even when neither state holds an object
// of it.
func TestObjectComparison_RoutesChangeRequestsToTheirOwner(t *testing.T) {
	c := qt.New(t)
	var received schemaext.ObjectComparisonRequest
	calls := 0
	provider := comparisonProvider(requestRecorder(&received, &calls))
	provider.Comparisons[0].Actions = []string{"rotate", "touch"}
	runtime := mustRuntime(c, provider)
	request := schemaext.ObjectComparisonRequest{Target: "alternate", Requests: []schemaext.ChangeRequest{
		{Subject: comparedRef(conversionSecond, "b"), Action: "rotate"},
		{Subject: comparedRef(conversionFirst, "a"), Action: "touch"},
		{Subject: comparedRef(conversionFirst, "a"), Action: "rotate"},
	}}

	result, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Kinds, qt.DeepEquals, []schemaext.Kind{conversionFirst, conversionSecond})
	c.Assert(received.Requests, qt.DeepEquals, []schemaext.ChangeRequest{
		{Subject: comparedRef(conversionFirst, "a"), Action: "rotate"},
		{Subject: comparedRef(conversionFirst, "a"), Action: "touch"},
		{Subject: comparedRef(conversionSecond, "b"), Action: "rotate"},
	})
	c.Assert(request.Requests[0].Subject, qt.DeepEquals, comparedRef(conversionSecond, "b"))
}

// TestObjectComparison_RefusesAChangeRequestNoOwnerAccepts refuses, before any
// owner runs, a request whose action the owner did not declare, one for a kind
// no owner compares, a duplicate, and a request without an action or a named
// subject.
func TestObjectComparison_RefusesAChangeRequestNoOwnerAccepts(t *testing.T) {
	unowned := comparedRef("example.org/unowned", "a")
	unnamed := comparedRef(conversionFirst, "a")
	unnamed.Name = objectidentity.Part{}
	tests := []struct {
		name     string
		requests []schemaext.ChangeRequest
		wantErr  string
		wantIs   error
	}{
		{name: "an undeclared action", requests: []schemaext.ChangeRequest{{Subject: comparedRef(conversionFirst, "a"), Action: "reset"}},
			wantErr: `.*the owner of "example.org/first" accepts no "reset" request.*`, wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "a kind no owner compares", requests: []schemaext.ChangeRequest{{Subject: unowned, Action: "rotate"}},
			wantErr: `.*no object comparison for "custom"/"example.org/unowned"`, wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "a duplicate", requests: []schemaext.ChangeRequest{{Subject: comparedRef(conversionFirst, "a"), Action: "rotate"}, {Subject: comparedRef(conversionFirst, "a"), Action: "rotate"}},
			wantErr: `.*invalid or duplicate change request "rotate".*`, wantIs: schemaext.ErrInvalidValue},
		{name: "no action", requests: []schemaext.ChangeRequest{{Subject: comparedRef(conversionFirst, "a")}},
			wantErr: `.*invalid or duplicate change request "".*`, wantIs: schemaext.ErrInvalidValue},
		{name: "an unnamed subject", requests: []schemaext.ChangeRequest{{Subject: unnamed, Action: "rotate"}},
			wantErr: `.*invalid or duplicate change request "rotate".*`, wantIs: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var received schemaext.ObjectComparisonRequest
			calls := 0
			provider := comparisonProvider(requestRecorder(&received, &calls))
			provider.Comparisons[0].Actions = []string{"rotate"}
			runtime := mustRuntime(c, provider)

			result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "alternate", Requests: test.requests})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

// TestObjectComparison_RefusesAnInvalidActionRegistration refuses an empty or
// repeated action when the owner registers.
func TestObjectComparison_RefusesAnInvalidActionRegistration(t *testing.T) {
	for _, actions := range [][]string{{""}, {" rotate"}, {"rotate", "rotate"}} {
		t.Run(actions[0], func(t *testing.T) {
			c := qt.New(t)
			provider := comparisonProvider(comparisonFunc(unchangedComparison))
			provider.Comparisons[0].Actions = actions
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

// TestFeatureComparison_CarriesChangeRequestsToObjectOwners passes a feature
// comparison's requests to the named-object owner and refuses one that names
// a facet model, which takes no request.
func TestFeatureComparison_CarriesChangeRequestsToObjectOwners(t *testing.T) {
	c := qt.New(t)
	var received schemaext.ObjectComparisonRequest
	calls := 0
	provider := combinedProvider(requestRecorder(&received, &calls), facetComparisonFunc(changedFacet))
	provider.Comparisons[0].Actions = []string{"rotate"}
	runtime := mustRuntime(c, provider)
	request := combinedRequest(c)
	request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
	request.Requests = []schemaext.ChangeRequest{{Subject: comparedRef(conversionFirst, "first"), Action: "rotate"}}

	must.Must(runtime.CompareFeatures(t.Context(), request))

	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Requests, qt.DeepEquals, request.Requests)

	request.Requests = []schemaext.ChangeRequest{{Subject: comparedRef(conversionSecond, "second"), Action: "rotate"}}
	result, err := runtime.CompareFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorMatches, `.*a change request names facet model "example.org/second"`)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaext.ComparisonResult{})
	c.Assert(calls, qt.Equals, 1)
}
