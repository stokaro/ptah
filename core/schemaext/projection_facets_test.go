package schemaext_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func facetProjectionRegistry(c *qt.C) schemaext.Registry {
	c.Helper()
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Observed)), owned(widgetCodec(otherKind, schemaext.Observed)))
	c.Assert(err, qt.IsNil)
	return registry
}

func facetProjectionCoverage(registry schemaext.Registry, claims ...schemaext.SubjectCoverage) schemaext.Coverage {
	var kinds []schemaext.KindCoverage
	for _, definition := range registry.Definitions() {
		kinds = append(kinds, schemaext.KindCoverage{Model: definition, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "partial table inspection"}})
	}
	return must.Must(schemaext.NewCoverage(schemaext.Observed, kinds, claims))
}

func TestFacetProjectionPreservesBindingsSiblingsAndKnowledge(t *testing.T) {
	c := qt.New(t)
	registry := facetProjectionRegistry(c)
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	changed, created, retained := builder.TableParts("", "changed"), builder.TableParts("", "created"), builder.TableParts("", "retained")
	coverage := facetProjectionCoverage(registry, schemaext.SubjectCoverage{Kind: widgetKind, Subject: created, Knowledge: schemaext.Knowledge{State: schemaext.Absent}})
	attached := must.Must(schemaext.NewFacets(&widget{ID: widgetKind, Count: 1}, &widget{ID: otherKind, Count: 2}))
	attached = must.Must(attached.WithTargetScope(widgetKind, "postgres"))
	input := schemaext.FacetState{Coverage: coverage, Records: []schemaext.FacetRecord{{Subject: changed, Values: attached}, {Subject: retained, Values: attached}}}
	value := &widget{ID: widgetKind, Count: 3, Names: []string{"captured"}}
	result, err := registry.ProjectFacets(t.Context(), input, []schemaext.FacetProjection{
		{Subject: changed, Kind: widgetKind, Value: value},
		{Subject: changed, Kind: otherKind},
		{Subject: created, Kind: widgetKind, Value: &widget{ID: widgetKind, Count: 4}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Records, qt.HasLen, 3)
	c.Assert(result.Records[0].Subject, qt.Equals, changed)
	c.Assert(result.Records[0].Values.Len(), qt.Equals, 1)
	c.Assert(result.Records[0].Values.TargetScope(widgetKind), qt.DeepEquals, []string{"postgres"})
	actual, found, err := schemaext.FacetAs[*widget](result.Records[0].Values, widgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(actual.Count, qt.Equals, uint64(3))
	value.Names[0] = "mutated"
	c.Assert(actual.Names, qt.DeepEquals, []string{"captured"})
	c.Assert(result.Records[2].Values, qt.DeepEquals, attached)
	c.Assert(result.Coverage.Lookup(widgetKind, created).State, qt.Equals, schemaext.Complete)
	c.Assert(result.Coverage.Lookup(otherKind, changed).State, qt.Equals, schemaext.Absent)
	c.Assert(result.Coverage.Lookup(widgetKind, retained), qt.Equals, coverage.Lookup(widgetKind, retained))
	c.Assert(input.Records[0].Values, qt.DeepEquals, attached)
	c.Assert(input.Coverage.Lookup(widgetKind, created).State, qt.Equals, schemaext.Absent)
}

func TestFacetProjectionRejectsUnknownContradictoryOrExcludedSources(t *testing.T) {
	c := qt.New(t)
	registry := facetProjectionRegistry(c)
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders")
	present := must.Must(schemaext.NewFacets(&widget{ID: widgetKind}))
	scoped := must.Must(present.WithTargetScope(widgetKind, "mysql"))
	excluded := must.Must(scoped.ForTarget(must.Must(schemaext.NewTargetSelection("postgres"))))
	for _, test := range []struct {
		name     string
		facets   schemaext.Facets
		coverage schemaext.Coverage
	}{
		{name: "unenrolled", facets: present},
		{name: "unknown missing", coverage: facetProjectionCoverage(registry)},
		{name: "unreadable present", facets: present, coverage: facetProjectionCoverage(registry, schemaext.SubjectCoverage{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown option"}})},
		{name: "contradictory absence", facets: present, coverage: facetProjectionCoverage(registry, schemaext.SubjectCoverage{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Absent}})},
		{name: "excluded", facets: excluded, coverage: facetProjectionCoverage(registry, schemaext.SubjectCoverage{Kind: widgetKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Absent}})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := registry.ProjectFacets(t.Context(), schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: ref, Values: test.facets}}, Coverage: test.coverage}, []schemaext.FacetProjection{{Subject: ref, Kind: widgetKind}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, schemaext.FacetState{})
		})
	}
}

func TestFacetProjectionRefusesInvalidBatchesAtomically(t *testing.T) {
	c := qt.New(t)
	registry := facetProjectionRegistry(c)
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders")
	input := schemaext.FacetState{Coverage: facetProjectionCoverage(registry), Records: []schemaext.FacetRecord{{Subject: ref, Values: must.Must(schemaext.NewFacets(&widget{ID: widgetKind}, &widget{ID: otherKind}))}}}
	for _, test := range []struct {
		name       string
		projection schemaext.FacetProjection
	}{
		{name: "duplicate", projection: schemaext.FacetProjection{Subject: ref, Kind: widgetKind}},
		{name: "missing subject", projection: schemaext.FacetProjection{Kind: otherKind}},
		{name: "invalid kind", projection: schemaext.FacetProjection{Subject: ref, Kind: "invalid"}},
		{name: "wrong kind", projection: schemaext.FacetProjection{Subject: ref, Kind: otherKind, Value: &widget{ID: widgetKind}}},
		{name: "typed nil", projection: schemaext.FacetProjection{Subject: ref, Kind: otherKind, Value: (*widget)(nil)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := registry.ProjectFacets(t.Context(), input, []schemaext.FacetProjection{{Subject: ref, Kind: widgetKind, Value: &widget{ID: widgetKind, Count: 4}}, test.projection})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, schemaext.FacetState{})
		})
	}
}

func TestFacetProjectionHonorsCancellationAndEmptyBatches(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (schemaext.Registry{}).ProjectFacets(ctx, schemaext.FacetState{}, nil)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.FacetState{})
	result, err = (schemaext.Registry{}).ProjectFacets(t.Context(), schemaext.FacetState{}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Records, qt.HasLen, 0)
	c.Assert(result.Coverage.IsZero(), qt.IsTrue)
}

func TestFacetReplacementPreservesUnrelatedModels(t *testing.T) {
	c := qt.New(t)
	registry := facetProjectionRegistry(c)
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders")
	attached := must.Must(schemaext.NewFacets(&widget{ID: widgetKind, Count: 1}, &widget{ID: otherKind, Count: 2}))
	attached = must.Must(attached.WithTargetScope(otherKind, "mysql"))
	input := schemaext.FacetState{Coverage: facetProjectionCoverage(registry), Records: []schemaext.FacetRecord{{Subject: ref, Values: attached}}}
	result, err := registry.ProjectFacets(t.Context(), input, []schemaext.FacetProjection{{Subject: ref, Kind: widgetKind, Value: &widget{ID: widgetKind, Count: 3}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Records[0].Values.Len(), qt.Equals, 2)
	retained, found, err := schemaext.FacetAs[*widget](result.Records[0].Values, otherKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(retained.Count, qt.Equals, uint64(2))
	c.Assert(result.Records[0].Values.TargetScope(otherKind), qt.DeepEquals, []string{"mysql"})
	c.Assert(result.Coverage.Lookup(otherKind, ref), qt.Equals, input.Coverage.Lookup(otherKind, ref))
}
