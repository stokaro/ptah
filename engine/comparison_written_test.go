package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// errUnreadable is the refusal spellingResolver gives a spelling it cannot
// read.
var errUnreadable = errors.New("not a path")

// spellingResolver is an object comparison that changes nothing and resolves
// each written request to the desired object of its batch whose name the
// request spells, and refuses the spelling "unreadable". It records the
// requests of each call.
func spellingResolver(received *[][]schemaext.ChangeRequest) schemaext.ObjectComparisonService {
	return comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		*received = append(*received, r.Requests)
		result, err := unchangedComparison(ctx, r)
		for i, change := range r.Requests {
			if change.Written == "unreadable" {
				return schemaext.ObjectComparisonResult{}, errUnreadable
			}
			for _, ref := range r.Desired.Objects.Refs() {
				if change.Written == ref.Name.Source {
					result.Resolved = append(result.Resolved, schemaext.ResolvedRequest{Request: i, Subject: ref})
				}
			}
		}
		return result, err
	})
}

// splitComparisonProvider registers one object comparison for each of the
// two kinds, both accepting a rotate request.
func splitComparisonProvider(first, second schemaext.ObjectComparisonService) engine.Provider {
	provider := comparisonProvider(first)
	provider.Comparisons = []engine.ObjectComparison{
		{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, ChangeKinds: []schemaext.Kind{comparedKind}, Actions: []string{"rotate"}, Service: first},
		{Target: "custom", Kinds: []schemaext.Kind{conversionSecond}, ChangeKinds: []schemaext.Kind{comparedKind}, Actions: []string{"rotate"}, Service: second},
	}
	return provider
}

// TestObjectComparison_OffersAWrittenRequestToEveryOwnerOfItsAction sends a
// request that names its object as the operator wrote it to each owner that
// accepts its action, since only an owner can read the spelling, and returns
// the object the one that recognized it resolved it to, at the request's place
// in the caller's list. An owner receives its requests in subject order, where
// a written one names none and comes first.
func TestObjectComparison_OffersAWrittenRequestToEveryOwnerOfItsAction(t *testing.T) {
	c := qt.New(t)
	var first, second [][]schemaext.ChangeRequest
	runtime := mustRuntime(c, splitComparisonProvider(spellingResolver(&first), spellingResolver(&second)))
	request := comparisonRequest(c)
	request.Requests = []schemaext.ChangeRequest{{Subject: comparedRef(conversionFirst, "first"), Action: "rotate"}, {Written: "second", Action: "rotate"}}

	result, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.DeepEquals, [][]schemaext.ChangeRequest{{{Written: "second", Action: "rotate"}, {Subject: comparedRef(conversionFirst, "first"), Action: "rotate"}}})
	c.Assert(second, qt.DeepEquals, [][]schemaext.ChangeRequest{{{Written: "second", Action: "rotate"}}})
	c.Assert(result.Resolved, qt.DeepEquals, []schemaext.ResolvedRequest{{Request: 1, Subject: comparedRef(conversionSecond, "second")}})
}

// TestObjectComparison_RunsAnOwnerAWrittenRequestNeedsWithoutItsObjects asks
// an owner whose kinds neither state holds, because a written request names no
// kind and that owner may be the one that reads it.
func TestObjectComparison_RunsAnOwnerAWrittenRequestNeedsWithoutItsObjects(t *testing.T) {
	c := qt.New(t)
	var first, second [][]schemaext.ChangeRequest
	runtime := mustRuntime(c, splitComparisonProvider(spellingResolver(&first), spellingResolver(&second)))
	request := comparisonRequest(c)
	request.Desired.Objects = request.Desired.Objects.Select(func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(conversionFirst) })
	request.Requests = []schemaext.ChangeRequest{{Written: "first", Action: "rotate"}}

	result, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.DeepEquals, [][]schemaext.ChangeRequest{{{Written: "first", Action: "rotate"}}})
	c.Assert(result.Resolved, qt.DeepEquals, []schemaext.ResolvedRequest{{Request: 0, Subject: comparedRef(conversionFirst, "first")}})
}

// TestObjectComparison_RefusesAWrittenRequestNotResolvedOnce refuses, naming
// the spelling, a written request no owner recognized and one two owners
// resolved, so a typo cannot read as a change done.
func TestObjectComparison_RefusesAWrittenRequestNotResolvedOnce(t *testing.T) {
	both := comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		result, err := unchangedComparison(ctx, r)
		result.Resolved = []schemaext.ResolvedRequest{{Request: 0, Subject: r.Desired.Objects.Refs()[0]}}
		return result, err
	})
	var ignored [][]schemaext.ChangeRequest
	tests := []struct {
		name          string
		first, second schemaext.ObjectComparisonService
		written       string
		wantErr       string
		wantIs        error
	}{
		{name: "no owner recognizes it", first: spellingResolver(&ignored), second: spellingResolver(&ignored), written: "frist",
			wantErr: `.*no owner on "custom" reads the "rotate" request "frist"`, wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "two owners resolve it", first: both, second: both, written: "first",
			wantErr: `.*2 owners on "custom" read the "rotate" request "first"`, wantIs: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, splitComparisonProvider(test.first, test.second))
			request := comparisonRequest(c)
			request.Requests = []schemaext.ChangeRequest{{Written: test.written, Action: "rotate"}}

			result, err := runtime.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
}

// TestObjectComparison_RefusesAWrittenRequestBeforeAnyOwnerRuns refuses a
// written request whose action no owner on the target accepts, one that names
// a subject too, and a spelling with surrounding space or a line break, before
// any owner runs.
func TestObjectComparison_RefusesAWrittenRequestBeforeAnyOwnerRuns(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ChangeRequest
		wantErr string
		wantIs  error
	}{
		{name: "an action no owner accepts", request: schemaext.ChangeRequest{Written: "first", Action: "reset"},
			wantErr: `.*no owner on "custom" reads the "reset" request "first"`, wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "a subject beside the spelling", request: schemaext.ChangeRequest{Written: "first", Subject: comparedRef(conversionFirst, "first"), Action: "rotate"},
			wantErr: `.*change request "rotate" names its subject both as .* and as written "first"`, wantIs: schemaext.ErrInvalidValue},
		{name: "surrounding space", request: schemaext.ChangeRequest{Written: " first", Action: "rotate"},
			wantErr: `.*invalid or duplicate change request "rotate" for " first"`, wantIs: schemaext.ErrInvalidValue},
		{name: "a line break", request: schemaext.ChangeRequest{Written: "fir\nst", Action: "rotate"},
			wantErr: `.*invalid or duplicate change request "rotate" for "fir\\nst"`, wantIs: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var first, second [][]schemaext.ChangeRequest
			runtime := mustRuntime(c, splitComparisonProvider(spellingResolver(&first), spellingResolver(&second)))
			request := comparisonRequest(c)
			request.Requests = []schemaext.ChangeRequest{test.request}

			result, err := runtime.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
			c.Assert(first, qt.HasLen, 0)
			c.Assert(second, qt.HasLen, 0)
		})
	}
}

// TestObjectComparison_RefusesAnUntrustedResolution refuses a reply that
// resolves a request of another batch, a request that names its subject, one
// request twice, or a written request to an object the batch does not hold
// as desired. The owner's batch holds the written request first.
func TestObjectComparison_RefusesAnUntrustedResolution(t *testing.T) {
	tests := []struct {
		name     string
		resolved []schemaext.ResolvedRequest
		wantErr  string
	}{
		{name: "an index outside the batch", resolved: []schemaext.ResolvedRequest{{Request: 2, Subject: comparedRef(conversionFirst, "first")}},
			wantErr: `.*comparison resolved no written request of its batch, or one twice`},
		{name: "a request with a subject", resolved: []schemaext.ResolvedRequest{{Request: 1, Subject: comparedRef(conversionFirst, "first")}},
			wantErr: `.*comparison resolved no written request of its batch, or one twice`},
		{name: "one request twice", resolved: []schemaext.ResolvedRequest{{Request: 0, Subject: comparedRef(conversionFirst, "first")}, {Request: 0, Subject: comparedRef(conversionFirst, "first")}},
			wantErr: `.*comparison resolved no written request of its batch, or one twice`},
		{name: "an object the batch does not hold", resolved: []schemaext.ResolvedRequest{{Request: 0, Subject: comparedRef(conversionFirst, "other")}},
			wantErr: `.*comparison resolved "first" to .*, which it compares no desired object as`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var ignored [][]schemaext.ChangeRequest
			untrusted := comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
				result, err := unchangedComparison(ctx, r)
				result.Resolved = test.resolved
				return result, err
			})
			runtime := mustRuntime(c, splitComparisonProvider(untrusted, spellingResolver(&ignored)))
			request := comparisonRequest(c)
			request.Requests = []schemaext.ChangeRequest{{Subject: comparedRef(conversionFirst, "first"), Action: "rotate"}, {Written: "first", Action: "rotate"}}

			result, err := runtime.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
}

// TestFeatureComparison_CarriesAWrittenRequestToTheOwnersOfItsAction runs the
// object owner that accepts a written request's action even when neither
// state holds an object or a coverage record of its kinds, and refuses the
// spelling it does not recognize.
func TestFeatureComparison_CarriesAWrittenRequestToTheOwnersOfItsAction(t *testing.T) {
	c := qt.New(t)
	var received [][]schemaext.ChangeRequest
	provider := combinedProvider(spellingResolver(&received), facetComparisonFunc(changedFacet))
	provider.Comparisons[0].Actions = []string{"rotate"}
	runtime := mustRuntime(c, provider)
	request := combinedRequest(c)
	request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
	request.Desired.Objects = schemaext.Objects{}
	request.Requests = []schemaext.ChangeRequest{{Written: "first", Action: "rotate"}}

	result, err := runtime.CompareFeatures(t.Context(), request)

	c.Assert(err, qt.ErrorMatches, `.*no owner on "custom" reads the "rotate" request "first"`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, schemaext.ComparisonResult{})
	c.Assert(received, qt.DeepEquals, [][]schemaext.ChangeRequest{{{Written: "first", Action: "rotate"}}})
}

// TestObjectComparison_ReturnsAnOwnersRefusalOfASpelling returns the refusal
// of an owner that reads a spelling as one of its own and finds it malformed,
// and no part of an answer.
func TestObjectComparison_ReturnsAnOwnersRefusalOfASpelling(t *testing.T) {
	c := qt.New(t)
	var first, second [][]schemaext.ChangeRequest
	runtime := mustRuntime(c, splitComparisonProvider(spellingResolver(&first), spellingResolver(&second)))
	request := comparisonRequest(c)
	request.Requests = []schemaext.ChangeRequest{{Written: "unreadable", Action: "rotate"}}

	result, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.ErrorIs, errUnreadable)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}
