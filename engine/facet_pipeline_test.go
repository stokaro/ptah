package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/atlascompat"
	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

func TestResolvedTableFacetsReachComparisonAndCapturedPlans(t *testing.T) {
	c := qt.New(t)
	var compared schemaext.FacetComparisonRequest
	provider := facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		compared = request
		return facetReply(request), nil
	}))
	provider.Targets[0].Preparation = preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		request.Tables[0].ResolvedFacets = must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst, Number: 42}))
		return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
	})
	runtime := mustRuntime(c, provider)
	desired, current := facetSchemas()
	desired.Tables[0].Facets = must.Must(desired.Tables[0].Facets.WithTargetScope(conversionFirst, "custom"))
	source := desired.Tables[0].Facets
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime)
	c.Assert(err, qt.IsNil)
	resolved, found, err := schemaext.FacetAs[*conversionValue](compared.Desired.Records[0].Values, conversionFirst)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(resolved.Number, qt.Equals, 42)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	prepared := diff.TablesModified[0].Desired.Table.Facets
	c.Assert(prepared, qt.DeepEquals, compared.Desired.Records[0].Values)
	c.Assert(prepared.TargetScope(conversionFirst), qt.DeepEquals, []string{"custom"})
	c.Assert(prepared.Kinds(), qt.DeepEquals, source.Kinds())
	c.Assert(diff.TablePreparation.Source[0].Desired.Table.Facets, qt.DeepEquals, source)
	c.Assert(diff.TablePreparation.Prepared[0].Desired.Table.Facets, qt.DeepEquals, source)
	c.Assert(diff.TablePreparation.Source[0].ResolvedFacets.IsZero(), qt.IsTrue)
	c.Assert(desired.Tables[0].Facets, qt.DeepEquals, source)
	c.Assert(compared.Current.Records[0].Values, qt.DeepEquals, current.Tables[0].Facets)
}

func TestInvalidTableIdentityIsRefusedBeforePreparation(t *testing.T) {
	c := qt.New(t)
	called := false
	provider := preparationProvider(preparationFunc(func(context.Context, schemapreparation.Request) (schemapreparation.Result, error) {
		called = true
		return schemapreparation.Result{}, nil
	}))
	runtime := mustRuntime(c, provider)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Event"}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, "alternate", runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	var refusal *schemadiff.RefusalError
	c.Assert(err, qt.ErrorAs, &refusal)
	c.Assert(diff, qt.IsNil)
	c.Assert(called, qt.IsFalse)
}

func facetSchemas() (*schemamodel.Database, *catalog.Database) {
	request := facetRequest()
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "orders", StructName: "Orders", Facets: request.Desired.Records[0].Values}}},
		&catalog.Database{Tables: []catalog.Table{{Name: "orders", Facets: request.Current.Records[0].Values}}}
}

func TestTableFacetsReachSelectedASTRenderer(t *testing.T) {
	c := qt.New(t)
	desired, _ := facetSchemas()
	list, err := atlascompat.SchemaToAST(*desired, "custom")
	c.Assert(err, qt.IsNil)
	c.Assert(list.Statements, qt.HasLen, 1)
	calls := 0
	runtime := mustRuntime(c, engine.Provider{ID: "example.org/table-rendering", Targets: []engine.Target{{
		Name: "custom", Aliases: []string{"alternate"},
		Rendering: renderingFunc(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
			calls++
			c.Assert(request.Target, qt.Equals, "custom")
			c.Assert(request.Nodes, qt.HasLen, 1)
			table, ok := request.Nodes[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Facets, qt.DeepEquals, desired.Tables[0].Facets)
			c.Assert(table.Facets.Len(), qt.Equals, 2)
			return renderer.Result{Complete: true, Fragments: []string{"owned table settings;"}}, nil
		}),
	}}})
	result, err := runtime.Render(t.Context(), renderer.Request{Target: "alternate", Nodes: list.Statements})
	c.Assert(err, qt.IsNil)
	c.Assert(result.SQL(), qt.Equals, "owned table settings;")
	c.Assert(calls, qt.Equals, 1)
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

func TestFacetScopeSurvivesHostProjectionAndTableCaptures(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		c.Assert(request.Kinds, qt.DeepEquals, []schemaext.Kind{conversionSecond})
		return schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}, nil
	})))
	desired, current := facetSchemas()
	desired.Tables[0].Facets = must.Must(desired.Tables[0].Facets.WithTargetScope(conversionFirst, "foreign"))
	selected := must.Must(runtime.ResolveTarget("alternate"))
	projected := must.Must(schemamodel.ScopeToTarget(desired, selected))
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), projected, current, "alternate", runtime))
	c.Assert(diff.HasChanges(), qt.IsFalse)
	projected.Tables[0].Comment = "capture the common change"
	diff = must.Must(schemadiff.CompareWithDialect(t.Context(), projected, current, "alternate", runtime))
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	table := diff.TablesModified[0]
	c.Assert(table.FeatureChanges, qt.HasLen, 0)
	c.Assert(table.Desired.Table.Facets.TargetScope(conversionFirst), qt.DeepEquals, []string{"foreign"})
	c.Assert(table.Desired.Table.Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{conversionSecond})
	// Rebuilding a parent still needs its actual settings, including settings
	// excluded from source comparison. Its observation must retain that state.
	c.Assert(table.Current.Table.Facets, qt.DeepEquals, current.Tables[0].Facets)
	c.Assert(desired.Tables[0].Facets.Len(), qt.Equals, 2)
}
