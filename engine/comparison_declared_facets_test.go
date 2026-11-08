package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// declaredFacetsProvider is the comparison provider whose first service owns
// conversionFirst and reads the declared facets of the kinds given.
func declaredFacetsProvider(first, second schemaext.ObjectComparisonService, declared ...schemaext.Kind) engine.Provider {
	p := declaredObjectsProvider(first, second)
	p.Comparisons[0].DeclaredFacetKinds = declared
	return p
}

// facetRecord is a desired facet record of table name holding values.
func facetRecord(c *qt.C, name string, values ...schemaext.Value) schemaext.FacetRecord {
	c.Helper()
	facets, err := schemaext.NewFacets(values...)
	c.Assert(err, qt.IsNil)
	return schemaext.FacetRecord{Subject: objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", name), Values: facets}
}

// facetSummary names each record of facets as table: kinds.
func facetSummary(facets []schemaext.FacetRecord) []string {
	summary := make([]string, 0, len(facets))
	for _, record := range facets {
		line := record.Subject.Name.Source + ":"
		for _, kind := range record.Values.Kinds() {
			line += " " + string(kind)
		}
		summary = append(summary, line)
	}
	return summary
}

// TestObjectComparison_DeliversDeclaredFacets pins that a service whose
// declaration names a facet model receives every desired facet of it, each
// record holding that kind alone, in owner order, and leaving out a record
// that holds none; a service naming none receives none.
func TestObjectComparison_DeliversDeclaredFacets(t *testing.T) {
	c := qt.New(t)
	var first, second schemaext.ObjectComparisonRequest
	runtime := mustRuntime(c, declaredFacetsProvider(recordingComparison(&first), recordingComparison(&second), conversionSecond))
	request := comparisonRequest(c)
	request.DeclaredFacets = []schemaext.FacetRecord{
		facetRecord(c, "users", &conversionValue{ID: conversionSecond, Number: 3}),
		facetRecord(c, "orders", &conversionValue{ID: conversionFirst, Number: 1}, &conversionValue{ID: conversionSecond, Number: 2}),
		facetRecord(c, "items", &conversionValue{ID: conversionFirst, Number: 4}),
	}

	_, err := runtime.CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(facetSummary(first.DeclaredFacets), qt.DeepEquals, []string{"orders: " + string(conversionSecond), "users: " + string(conversionSecond)})
	c.Assert(second.DeclaredFacets, qt.HasLen, 0)
}

// TestObjectComparison_RefusesAnInvalidDeclaredFacetKind pins the declared
// facet kinds a service may name: models its owner registers, each once, none
// its own and none it names among its declared kinds.
func TestObjectComparison_RefusesAnInvalidDeclaredFacetKind(t *testing.T) {
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

			runtime, err := engine.New(declaredFacetsProvider(comparisonFunc(unchangedComparison), comparisonFunc(unchangedComparison), test.declared...))

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, `.*invalid declared kind .*`)
			c.Assert(runtime, qt.IsNil)
		})
	}
	t.Run("a kind named as a declared kind too", func(t *testing.T) {
		c := qt.New(t)
		provider := declaredFacetsProvider(comparisonFunc(unchangedComparison), comparisonFunc(unchangedComparison), conversionSecond)
		provider.Comparisons[0].DeclaredKinds = []schemaext.Kind{conversionSecond}

		runtime, err := engine.New(provider)

		c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		c.Assert(runtime, qt.IsNil)
	})
}
