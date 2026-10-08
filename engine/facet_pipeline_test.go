package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

func facetSchemas() (*schemamodel.Database, *catalog.Database) {
	request := facetRequest()
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "orders", StructName: "Orders", Facets: request.Desired.Records[0].Values}}},
		&catalog.Database{Tables: []catalog.Table{{Name: "orders", Facets: request.Current.Records[0].Values}}}
}

func TestTableFacetComparisonAttachesChangesAndCapturesBothStates(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return facetReply(request), nil
	})))
	desired, current := facetSchemas()
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.HasChanges(), qt.IsTrue)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	table := diff.TablesModified[0]
	c.Assert(table.FeatureChanges, qt.HasLen, 1)
	c.Assert(table.FeatureChanges[0].Subject.Parent.Empty(), qt.IsTrue)
	c.Assert(table.FeatureChanges[0].Subject.Name.Source, qt.Equals, "orders")
	c.Assert(table.Desired.Table.Facets, qt.DeepEquals, desired.Tables[0].Facets)
	c.Assert(table.Current.Table.Facets, qt.DeepEquals, current.Tables[0].Facets)
	// Capturing a facet change must not turn its owner into a named feature.
	c.Assert(table.Desired.OwnedObjects.Len(), qt.Equals, 0)
	c.Assert(table.Current.OwnedObjects.Len(), qt.Equals, 0)
}

func TestTableFacetAdoptionPrecedesCommonChangeCapture(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: schemaext.FacetState{Records: request.Current.Records}}, nil
	})))
	desired, current := facetSchemas()
	desired.Tables[0].Facets = schemaext.Facets{}
	desired.Tables[0].Comment = "common change needs preserved settings"
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].Desired.Table.Facets, qt.DeepEquals, current.Tables[0].Facets)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 0)
	c.Assert(desired.Tables[0].Facets.Len(), qt.Equals, 0)
}

func TestFacetHostRefusesUncapturedNonTableAttachments(t *testing.T) {
	for _, side := range []string{"desired", "observed"} {
		t.Run(side, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				calls++
				return facetReply(request), nil
			})))
			desired, current := facetSchemas()
			slots := map[string]*schemaext.Facets{"desired": &desired.Facets, "observed": &current.Facets}
			*slots[side] = desired.Tables[0].Facets
			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

type unfinishedFeatureRuntime struct{ *engine.Runtime }

func (r unfinishedFeatureRuntime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	result, err := r.Runtime.CompareFeatures(ctx, request)
	result.Complete = false
	return result, err
}

func TestFacetHostRequiresCompletedRuntimeReply(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return facetReply(request), nil
	})))
	desired, current := facetSchemas()
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", unfinishedFeatureRuntime{runtime})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(diff, qt.IsNil)
}
