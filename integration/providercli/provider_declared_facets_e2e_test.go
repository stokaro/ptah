//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// facetsRequest is an object comparison of the retention object keep, which
// both sides hold with the same value, beside a desired retention facet on
// each table given. Every value is captured under a target profile that lists
// features, as the host captures one for an engine that negotiated them.
func facetsRequest(c *qt.C, runtime *engine.Runtime, features []string, tables ...string) schemaext.ObjectComparisonRequest {
	c.Helper()
	keep := objectidentity.ID{Kind: objectidentity.Kind(comparisonNative), Name: objectidentity.Part{Source: "keep", Normalized: "keep"}}
	desired, err := schemaext.NewObjects(schemaext.Object{Ref: keep, Value: profiledModel(c, runtime, features, schemaext.Desired, comparisonNative, keep, `{"seconds":60}`)})
	c.Assert(err, qt.IsNil)
	current, err := schemaext.NewObjects(schemaext.Object{Ref: keep, Value: profiledModel(c, runtime, features, schemaext.Observed, comparisonNative, keep, `{"seconds":60}`)})
	c.Assert(err, qt.IsNil)
	complete := schemaext.Knowledge{State: schemaext.Complete}
	request := schemaext.ObjectComparisonRequest{Target: "fixture", Identifiers: comparisonRules(),
		Desired: schemaext.ObjectState{Objects: desired, Coverage: comparisonCoverage(c, runtime, comparisonNative, schemaext.Desired, complete)},
		Current: schemaext.ObjectState{Objects: current, Coverage: comparisonCoverage(c, runtime, comparisonNative, schemaext.Observed, complete)}}
	for _, name := range tables {
		table := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: name, Normalized: name}}
		facets, err := schemaext.NewFacets(profiledModel(c, runtime, features, schemaext.Desired, comparisonFacet, table, `{"seconds":60}`))
		c.Assert(err, qt.IsNil)
		request.DeclaredFacets = append(request.DeclaredFacets, schemaext.FacetRecord{Subject: table, Values: facets})
	}
	return request
}

// declaredFacetsProvider starts the comparison fixture in the mode whose
// objects comparison reads the declared retention facets of tables.
func declaredFacetsProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderEnvironment(c, "comparison-models", []string{"FIXTURE_DECLARED_FACETS=on"})
	return runtime
}

// TestProviderReadsTheDeclaredFacetsOfAnotherComparison_FailurePath gives a
// provider that negotiated models.declared-facets a retention object whose
// table declares no retention facet. The facets belong to another comparison
// of the engine, so the objects comparison sees none in its own state; the
// host hands it the declared ones, and the fixture refuses the object and
// says why.
func TestProviderReadsTheDeclaredFacetsOfAnotherComparison_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := declaredFacetsProvider(c)
	request := facetsRequest(c, runtime, []string{"models.declared-facets"}, "orders")

	result, err := runtime.CompareObjects(c.Context(), request)

	c.Assert(err, qt.ErrorMatches, `.*Retention object keep follows the retention of table keep, which the schema does not declare`)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}

// TestProviderReadsTheDeclaredFacetsOfAnotherComparison_HappyPath is the
// control: with the table's facet declared the object is compared as before,
// and a target whose profile does not list the feature receives no declared
// facet and compares as a host of contract 25.36 does.
func TestProviderReadsTheDeclaredFacetsOfAnotherComparison_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []string
		tables   []string
	}{
		{"the table's facet is declared", []string{"models.declared-facets"}, []string{"keep", "orders"}},
		{"a target that does not list the feature", make([]string, 0), []string{"orders"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := declaredFacetsProvider(c)
			request := facetsRequest(c, runtime, test.features, test.tables...)

			result, err := runtime.CompareObjects(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}
