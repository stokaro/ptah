package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

func TestWholeSchemaServicesScopeDeclarationsBeforeCodecChecks(t *testing.T) {
	c := qt.New(t)
	facets, err := schemaext.NewFacets(&validationValue{})
	c.Assert(err, qt.IsNil)
	schema := &schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{
		{Name: "foreign_type", Dialects: []string{"postgres"}, Facets: facets},
		{Name: "local_type", Dialects: []string{" ALTERNATE "}},
	}}
	calls := 0
	runtime := mustRuntime(c, engine.Provider{ID: "example.org/scoped", Targets: []engine.Target{{
		Name: "custom", Aliases: []string{"alternate"},
		Validation: validationFunc(func(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
			calls++
			c.Assert(request.Target, qt.Equals, "custom")
			c.Assert(request.Schema.CompositeTypes, qt.HasLen, 1)
			c.Assert(request.Schema.CompositeTypes[0].Name, qt.Equals, "local_type")
			return schemavalidation.Result{Complete: true}, nil
		}),
		SchemaRendering: schemaRenderFunc(func(_ context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
			calls++
			c.Assert(request.Target, qt.Equals, "custom")
			c.Assert(request.Schema.CompositeTypes, qt.HasLen, 1)
			c.Assert(request.Schema.CompositeTypes[0].Name, qt.Equals, "local_type")
			return renderer.SchemaResult{Complete: true}, nil
		}),
	}}})
	_, err = runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "alternate", Schema: schema})
	c.Assert(err, qt.IsNil)
	_, err = runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "alternate", Schema: schema})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 2)
	c.Assert(schema.CompositeTypes, qt.HasLen, 2)
	// Removing the exclusion must restore the refusal before either dispatch.
	schema.CompositeTypes[0].Dialects = nil
	_, err = runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "alternate", Schema: schema})
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	_, err = runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "alternate", Schema: schema})
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(calls, qt.Equals, 2)
}

func TestWholeSchemaServicesProjectFacetBindingsBeforeCodecs(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}}}
	facets := must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst}))
	facets = must.Must(facets.WithTargetScope(facets.Kinds()[0], "foreign"))
	source.Tables[0].Facets = facets
	known := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	})))
	source.FeatureCoverage = facetCoverage(known.Codecs(), schemaext.Desired, schemaext.Complete).SelectKinds([]schemaext.Kind{conversionFirst})
	calls := 0
	check := func(schema *schemamodel.Database) {
		calls++
		c.Assert(schema.Tables[0].Facets.Len(), qt.Equals, 0)
		c.Assert(schema.Tables[0].Facets.DeclaredKinds(), qt.DeepEquals, facets.Kinds())
		c.Assert(schema.FeatureCoverage.KindRecords(), qt.DeepEquals, source.FeatureCoverage.KindRecords())
	}
	runtime := mustRuntime(c, engine.Provider{ID: "example.org/scoped", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"},
		Validation: validationFunc(func(_ context.Context, r schemavalidation.Request) (schemavalidation.Result, error) {
			check(r.Schema)
			return schemavalidation.Result{Complete: true}, nil
		}),
		SchemaRendering: schemaRenderFunc(func(_ context.Context, r renderer.SchemaRequest) (renderer.SchemaResult, error) {
			check(r.Schema)
			return renderer.SchemaResult{Complete: true}, nil
		}),
	}}})
	_, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "alternate", Schema: source})
	c.Assert(err, qt.IsNil)
	_, err = runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "alternate", Schema: source})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 2)
	c.Assert(source.Tables[0].Facets.Len(), qt.Equals, 1)
	source.Tables[0].Facets = must.Must(source.Tables[0].Facets.WithTargetScope(conversionFirst, "alternate"))
	_, err = runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "alternate", Schema: source})
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(calls, qt.Equals, 2)
	source.Tables[0].Facets = facets
	source.Tables = append(source.Tables, schemamodel.Table{Name: "included", StructName: "Included", Facets: must.Must(facets.WithTargetScope(conversionFirst))})
	_, err = runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "alternate", Schema: source})
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(calls, qt.Equals, 2)
}
