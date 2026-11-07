//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// comparisonAnchor is the object model of the fixture's anchors comparison,
// whose declared objects its objects comparison reads.
const comparisonAnchor = schemaext.Kind("example.org/fixture/retention-anchor")

// anchorsRequest is an object comparison of the retention object keep, which
// both sides hold with the same value, beside the anchors given, which both
// sides hold too. Every value is captured under a target profile that lists
// features, as the host captures one for an engine that negotiated them.
func anchorsRequest(c *qt.C, runtime *engine.Runtime, features []string, anchors ...string) schemaext.ObjectComparisonRequest {
	c.Helper()
	keep := objectidentity.ID{Kind: objectidentity.Kind(comparisonNative), Name: objectidentity.Part{Source: "keep", Normalized: "keep"}}
	desired := []schemaext.Object{{Ref: keep, Value: profiledModel(c, runtime, features, schemaext.Desired, comparisonNative, keep, `{"seconds":60}`)}}
	current := []schemaext.Object{{Ref: keep, Value: profiledModel(c, runtime, features, schemaext.Observed, comparisonNative, keep, `{"seconds":60}`)}}
	for _, name := range anchors {
		anchor := objectidentity.ID{Kind: objectidentity.Kind(comparisonAnchor), Name: objectidentity.Part{Source: name, Normalized: name}}
		desired = append(desired, schemaext.Object{Ref: anchor, Value: profiledModel(c, runtime, features, schemaext.Desired, comparisonAnchor, anchor, `{"seconds":60}`)})
		current = append(current, schemaext.Object{Ref: anchor, Value: profiledModel(c, runtime, features, schemaext.Observed, comparisonAnchor, anchor, `{"seconds":60}`)})
	}
	desiredObjects, err := schemaext.NewObjects(desired...)
	c.Assert(err, qt.IsNil)
	currentObjects, err := schemaext.NewObjects(current...)
	c.Assert(err, qt.IsNil)
	complete := schemaext.Knowledge{State: schemaext.Complete}
	desiredCoverage, err := comparisonCoverage(c, runtime, comparisonNative, schemaext.Desired, complete).
		Combine(comparisonCoverage(c, runtime, comparisonAnchor, schemaext.Desired, complete))
	c.Assert(err, qt.IsNil)
	currentCoverage, err := comparisonCoverage(c, runtime, comparisonNative, schemaext.Observed, complete).
		Combine(comparisonCoverage(c, runtime, comparisonAnchor, schemaext.Observed, complete))
	c.Assert(err, qt.IsNil)
	return schemaext.ObjectComparisonRequest{Target: "fixture", Identifiers: comparisonRules(),
		Desired: schemaext.ObjectState{Objects: desiredObjects, Coverage: desiredCoverage},
		Current: schemaext.ObjectState{Objects: currentObjects, Coverage: currentCoverage}}
}

// declaredObjectsProvider starts the comparison fixture in the mode whose
// objects comparison reads the declared anchors.
func declaredObjectsProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderEnvironment(c, "comparison-models", []string{"FIXTURE_DECLARED_OBJECTS=on"})
	return runtime
}

// TestProviderReadsTheDeclaredObjectsOfAnotherComparison_FailurePath gives a
// provider that negotiated models.declared-objects a retention object whose
// anchor the desired state does not declare. The anchors belong to another
// comparison of the engine, so the objects comparison sees none in its own
// state; the host names the declared ones, and the fixture refuses the object
// and says why.
func TestProviderReadsTheDeclaredObjectsOfAnotherComparison_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := declaredObjectsProvider(c)
	request := anchorsRequest(c, runtime, []string{"models.declared-objects"}, "orders")

	result, err := runtime.CompareObjects(c.Context(), request)

	c.Assert(err, qt.ErrorMatches, `.*Retention object keep keeps its rows in anchor keep, which the schema does not declare`)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}

// TestProviderReadsTheDeclaredObjectsOfAnotherComparison_HappyPath is the
// control: with the anchor declared the object is compared as before, and a
// target whose profile does not list the feature receives no declared object
// and compares as a host of contract 25.32 does.
func TestProviderReadsTheDeclaredObjectsOfAnotherComparison_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []string
		anchors  []string
	}{
		{"the anchor is declared", []string{"models.declared-objects"}, []string{"keep", "orders"}},
		{"a target that does not list the feature", make([]string, 0), []string{"orders"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := declaredObjectsProvider(c)
			request := anchorsRequest(c, runtime, test.features, test.anchors...)

			result, err := runtime.CompareObjects(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}
