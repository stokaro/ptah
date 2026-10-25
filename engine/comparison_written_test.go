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

// errUnreadable is the refusal writtenReader gives a spelling it cannot read.
var errUnreadable = errors.New("not a path")

// writtenReader is an object comparison that reads the spellings in names as
// the objects they map to, refuses "unreadable", and records the request it
// compares.
type writtenReader struct {
	names    map[string]objectidentity.ID
	received *schemaext.ObjectComparisonRequest
	calls    *int
}

func (w writtenReader) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	*w.received = request
	*w.calls++
	return unchangedComparison(ctx, request)
}

func (w writtenReader) ResolveWritten(_ context.Context, _, written string) (objectidentity.ID, bool, error) {
	if written == "unreadable" {
		return objectidentity.ID{}, false, errUnreadable
	}
	ref, found := w.names[written]
	return ref, found, nil
}

// writtenRuntime is a runtime whose one comparison accepts rotate requests and
// reads a and a-again as the first-kind object a, and b as the second-kind
// object b.
func writtenRuntime(c *qt.C, received *schemaext.ObjectComparisonRequest, calls *int) *engine.Runtime {
	c.Helper()
	provider := comparisonProvider(writtenReader{names: map[string]objectidentity.ID{
		"a": comparedRef(conversionFirst, "a"), "a-again": comparedRef(conversionFirst, "a"), "b": comparedRef(conversionSecond, "b"),
	}, received: received, calls: calls})
	provider.Comparisons[0].Actions = []string{schemaext.RotateAction}
	return mustRuntime(c, provider)
}

// A written request reaches its owner as the subject the owner read it as,
// beside a request that named its subject. Two spellings of one object, and a
// spelling of an object another request already names, ask once.
func TestObjectComparison_ResolvesWrittenRequests(t *testing.T) {
	c := qt.New(t)
	var received schemaext.ObjectComparisonRequest
	calls := 0
	runtime := writtenRuntime(c, &received, &calls)
	requests := append(schemaext.WrittenRequests(schemaext.RotateAction, []string{"b", "a", "a-again", "a"}),
		schemaext.ChangeRequest{Subject: comparedRef(conversionSecond, "b"), Action: schemaext.RotateAction})

	result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "alternate", Requests: requests})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Requests, qt.DeepEquals, []schemaext.ChangeRequest{
		{Subject: comparedRef(conversionFirst, "a"), Action: schemaext.RotateAction},
		{Subject: comparedRef(conversionSecond, "b"), Action: schemaext.RotateAction},
	})
}

// A feature comparison reads its written requests before it decides which
// owners take part, so the kind a spelling names is compared.
func TestFeatureComparison_ResolvesWrittenRequests(t *testing.T) {
	c := qt.New(t)
	var received schemaext.ObjectComparisonRequest
	calls := 0
	runtime := writtenRuntime(c, &received, &calls)
	request := schemaext.ComparisonRequest{Target: "alternate", Requests: schemaext.WrittenRequests(schemaext.RotateAction, []string{"b"})}

	_, err := runtime.CompareFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Kinds, qt.DeepEquals, []schemaext.Kind{conversionSecond})
	c.Assert(received.Requests, qt.DeepEquals, []schemaext.ChangeRequest{{Subject: comparedRef(conversionSecond, "b"), Action: schemaext.RotateAction}})
}

// A spelling no owner reads, one its owner refuses, one that also names a
// subject, and one for an action the owner does not accept are refused before
// any owner compares.
func TestObjectComparison_RefusesAWrittenRequestNoOwnerReads(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ChangeRequest
		wantErr string
		wantIs  error
	}{
		{name: "a spelling no owner reads", request: schemaext.ChangeRequest{Action: schemaext.RotateAction, Written: "nosuch"},
			wantErr: `.*no owner on "custom" reads the "rotate" request "nosuch"`, wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "a spelling its owner refuses", request: schemaext.ChangeRequest{Action: schemaext.RotateAction, Written: "unreadable"},
			wantErr: `not a path`, wantIs: errUnreadable},
		{name: "a subject and a spelling", request: schemaext.ChangeRequest{Subject: comparedRef(conversionFirst, "a"), Action: schemaext.RotateAction, Written: "a"},
			wantErr: `.*names its subject both as .* and as written "a"`, wantIs: schemaext.ErrInvalidValue},
		{name: "an action no owner accepts", request: schemaext.ChangeRequest{Action: "reset", Written: "a"},
			wantErr: `.*no owner on "custom" reads the "reset" request "a"`, wantIs: ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var received schemaext.ObjectComparisonRequest
			calls := 0
			runtime := writtenRuntime(c, &received, &calls)

			result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "alternate", Requests: []schemaext.ChangeRequest{test.request}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

// Two owners that both read one spelling make it ambiguous, and an owner that
// reads a spelling as an object of a kind it does not compare is broken; both
// are refused rather than resolved by order.
func TestObjectComparison_RefusesAnAmbiguousOrForeignResolution(t *testing.T) {
	tests := []struct {
		name          string
		first, second map[string]objectidentity.ID
		wantErr       string
		wantIs        error
	}{
		{name: "two owners read it",
			first: map[string]objectidentity.ID{"a": comparedRef(conversionFirst, "a")}, second: map[string]objectidentity.ID{"a": comparedRef(conversionSecond, "a")},
			wantErr: `.*2 owners on "custom" read the "rotate" request "a"`, wantIs: schemaext.ErrInvalidValue},
		{name: "an owner reads it as another owner's kind",
			first: map[string]objectidentity.ID{"a": comparedRef(conversionSecond, "a")}, second: make(map[string]objectidentity.ID),
			wantErr: `.*resolved to .*, a kind its owner does not compare`, wantIs: engine.ErrInvalidRegistration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var received schemaext.ObjectComparisonRequest
			calls := 0
			provider := comparisonProvider(writtenReader{names: test.first, received: &received, calls: &calls})
			first := provider.Comparisons[0]
			first.Kinds, first.Actions = []schemaext.Kind{conversionFirst}, []string{schemaext.RotateAction}
			second := first
			second.Kinds = []schemaext.Kind{conversionSecond}
			second.Service = writtenReader{names: test.second, received: &received, calls: &calls}
			provider.Comparisons = []engine.ObjectComparison{first, second}
			runtime := mustRuntime(c, provider)

			_, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "alternate",
				Requests: schemaext.WrittenRequests(schemaext.RotateAction, []string{"a"})})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}
