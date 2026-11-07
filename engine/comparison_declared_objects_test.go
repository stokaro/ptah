package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// declaredObjectsProvider splits the comparison provider's kinds between two
// services: the first owns conversionFirst and reads the declared objects of
// conversionSecond, which the second owns.
func declaredObjectsProvider(first, second schemaext.ObjectComparisonService, declared ...schemaext.Kind) engine.Provider {
	p := comparisonProvider(first)
	p.Comparisons = []engine.ObjectComparison{
		{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, ChangeKinds: []schemaext.Kind{comparedKind}, DeclaredKinds: declared, Service: first},
		{Target: "custom", Kinds: []schemaext.Kind{conversionSecond}, ChangeKinds: []schemaext.Kind{comparedKind}, Service: second},
	}
	return p
}

// recordingComparison answers unchanged and keeps the request it received.
func recordingComparison(received *schemaext.ObjectComparisonRequest) comparisonFunc {
	return func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		*received = r
		return unchangedComparison(ctx, r)
	}
}

// TestObjectComparison_DeliversDeclaredObjects pins that a service whose
// declaration names another service's model receives the identity of every
// desired object of it, which its own batch of the desired state leaves out,
// and that a service naming none receives none. What a caller sets is
// replaced, since the runtime computes the list from the desired state.
func TestObjectComparison_DeliversDeclaredObjects(t *testing.T) {
	c := qt.New(t)
	var first, second schemaext.ObjectComparisonRequest
	runtime := mustRuntime(c, declaredObjectsProvider(recordingComparison(&first), recordingComparison(&second), conversionSecond))
	request := comparisonRequest(c)
	request.DeclaredObjects = []objectidentity.ID{comparedRef(conversionFirst, "first")}

	_, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(first.Desired.Objects.Refs(), qt.DeepEquals, []objectidentity.ID{comparedRef(conversionFirst, "first")})
	c.Assert(first.DeclaredObjects, qt.DeepEquals, []objectidentity.ID{comparedRef(conversionSecond, "second")})
	c.Assert(second.DeclaredObjects, qt.HasLen, 0)
}

// TestObjectComparison_RefusesAnInvalidDeclaredKind pins the declared kinds a
// service may name: models its owner registers, each once, none its own.
func TestObjectComparison_RefusesAnInvalidDeclaredKind(t *testing.T) {
	for _, test := range []struct {
		name     string
		declared []schemaext.Kind
	}{
		{"the service's own kind", []schemaext.Kind{conversionFirst}},
		{"a kind named twice", []schemaext.Kind{conversionSecond, conversionSecond}},
		{"a kind its owner does not register", []schemaext.Kind{"example.org/unregistered"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(declaredObjectsProvider(comparisonFunc(unchangedComparison), comparisonFunc(unchangedComparison), test.declared...))

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, `.*invalid declared kind .*`)
			c.Assert(runtime, qt.IsNil)
		})
	}
}
