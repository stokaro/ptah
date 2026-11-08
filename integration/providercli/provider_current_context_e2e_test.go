//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// currentContextProvider starts the comparison fixture in the mode whose
// objects comparison reads the declared and the current retention facets of
// tables.
func currentContextProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderEnvironment(c, "comparison-models", []string{"FIXTURE_DECLARED_FACETS=on", "FIXTURE_CURRENT_CONTEXT=on"})
	return runtime
}

// currentFacetsRequest is [facetsRequest] with a desired retention facet on
// table orders alone, and a current retention facet on each table held, under
// a complete coverage of the facet model.
func currentFacetsRequest(c *qt.C, runtime *engine.Runtime, features []string, held ...string) schemaext.ObjectComparisonRequest {
	c.Helper()
	request := facetsRequest(c, runtime, features, "orders")
	request.CurrentFacets.Coverage = comparisonCoverage(c, runtime, comparisonFacet, schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete})
	for _, name := range held {
		table := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: name, Normalized: name}}
		facets, err := schemaext.NewFacets(profiledModel(c, runtime, features, schemaext.Observed, comparisonFacet, table, `{"seconds":60}`))
		c.Assert(err, qt.IsNil)
		request.CurrentFacets.Records = append(request.CurrentFacets.Records, schemaext.FacetRecord{Subject: table, Values: facets})
	}
	return request
}

// TestProviderReadsTheCurrentStateOfAnotherComparison_HappyPath gives a
// provider that negotiated models.current-context a retention object whose
// table declares no retention facet and holds one. The facet belongs to
// another comparison of the engine, and the object comparison sees neither
// the declared nor the held one in its own state; the host hands it the held
// facet in the current context, and the fixture compares the object as one
// that follows a facet the plan keeps.
func TestProviderReadsTheCurrentStateOfAnotherComparison_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := currentContextProvider(c)
	request := currentFacetsRequest(c, runtime, []string{"models.declared-facets", "models.current-context"}, "keep")

	result, err := runtime.CompareObjects(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
}

// TestProviderReadsTheCurrentStateOfAnotherComparison_FailurePath is the
// control: a table that holds no retention facet leaves the object refused,
// and so does a target whose profile does not list the feature, which
// receives no current context and compares as a host of contract 25.39 does.
func TestProviderReadsTheCurrentStateOfAnotherComparison_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		features []string
		held     []string
	}{
		{"the table holds no retention facet", []string{"models.declared-facets", "models.current-context"}, []string{"orders"}},
		{"a target that does not list the feature", []string{"models.declared-facets"}, []string{"keep"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := currentContextProvider(c)
			request := currentFacetsRequest(c, runtime, test.features, test.held...)

			result, err := runtime.CompareObjects(c.Context(), request)

			c.Assert(err, qt.ErrorMatches, `.*Retention object keep follows the retention of table keep, which the schema does not declare`)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
}
