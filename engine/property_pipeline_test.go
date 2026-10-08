package engine_test

import (
	"context"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
	"ptah.run/migration/schemadiff"
)

func decodePipelineProperties(_ context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	values := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		number, err := strconv.Atoi(fragment.Properties["number"])
		if err != nil {
			return nil, err
		}
		values = append(values, &conversionValue{ID: fragment.Kind, Number: number})
	}
	return values, nil
}

func TestSourcePropertiesReachSelectedSchemaServicesAndPreparation(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{Tables: []schemamodel.Table{
		{Name: "orders", StructName: "Orders", Overrides: map[string]map[string]string{"alternate": {"number": "42", "unclaimed": "retained"}}},
		{Name: "excluded", StructName: "Excluded", Overrides: map[string]map[string]string{"other": {"number": "invalid"}}},
	}}
	var rendered, validated *schemamodel.Database
	var prepared schemapreparation.Request
	provider := facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}, nil
	}))
	properties := propertyProvider(propertyService{decode: decodePipelineProperties})
	provider.Properties = properties.Properties
	provider.Targets[0].Preparation = preparationFunc(func(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		prepared = request.Clone()
		return (schemapreparation.Identity{}).PrepareTables(ctx, request)
	})
	provider.Targets[0].SchemaRendering = schemaRenderFunc(func(_ context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
		rendered = request.Schema
		return renderer.SchemaResult{Complete: true}, nil
	})
	provider.Targets[0].Validation = validationFunc(func(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
		validated = request.Schema
		return schemavalidation.Result{Complete: true}, nil
	})
	runtime := mustRuntime(c, provider)
	_, err := runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "alternate", Schema: source})
	c.Assert(err, qt.IsNil)
	_, err = runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "alternate", Schema: source})
	c.Assert(err, qt.IsNil)
	_, err = schemadiff.CompareWithDialect(t.Context(), source, &catalog.Database{}, "alternate", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(prepared.Tables, qt.HasLen, 2)
	for _, table := range []schemamodel.Table{rendered.Tables[0], validated.Tables[0], prepared.Tables[0].Desired.Table} {
		value, found, err := schemaext.FacetAs[*conversionValue](table.Facets, conversionFirst)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(value.Number, qt.Equals, 42)
		c.Assert(table.Facets.TargetScope(conversionFirst), qt.DeepEquals, []string{"custom"})
		c.Assert(table.Overrides, qt.DeepEquals, map[string]map[string]string{"alternate": {"unclaimed": "retained"}})
	}
	c.Assert(rendered.Tables, qt.HasLen, 2)
	c.Assert(validated.Tables, qt.HasLen, 2)
	c.Assert(source.Tables[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(source.Tables[0].Overrides["alternate"]["number"], qt.Equals, "42")
}
