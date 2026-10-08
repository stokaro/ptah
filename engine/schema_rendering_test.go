package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

type schemaRenderFunc func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error)

func (f schemaRenderFunc) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	return f(ctx, request)
}
func schemaRenderProvider(service renderer.SchemaService) engine.Provider {
	return engine.Provider{ID: "example.org/schema", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alias"}, SchemaRendering: service}}}
}

func TestRuntimeSchemaRenderingUsesSelectedService(t *testing.T) {
	c := qt.New(t)
	calls := 0
	service := schemaRenderFunc(func(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "custom")
		c.Assert(request.Schema.Tables, qt.HasLen, 2)
		return renderer.SchemaResult{Complete: true, Statements: []string{"selected;"}}, nil
	})
	provider := schemaRenderProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Targets[0].SchemaRendering = nil
	result, err := runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: " ALIAS ", Schema: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "first"}, {Name: "second"}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Statements, qt.DeepEquals, []string{"selected;"})
	c.Assert(calls, qt.Equals, 1)
}

func TestRuntimeSchemaRenderingRequiresRegistrationAndModels(t *testing.T) {
	c := qt.New(t)
	_, err := engine.New(schemaRenderProvider(schemaRenderFunc(nil)))
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	runtime := mustRuntime(c, schemaRenderProvider(nil))
	result, err := runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "custom", Schema: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
	result, err = runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "unknown", Schema: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
	calls := 0
	runtime = mustRuntime(c, schemaRenderProvider(schemaRenderFunc(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		return renderer.SchemaResult{Complete: true}, nil
	})))
	facets, err := schemaext.NewFacets(&validationValue{})
	c.Assert(err, qt.IsNil)
	result, err = runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "custom", Schema: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", Facets: facets}}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
	c.Assert(calls, qt.Equals, 0)
}

func TestRuntimeSchemaRenderingChecksCompletionAndCancellation(t *testing.T) {
	for _, test := range []struct {
		name     string
		complete bool
		want     error
	}{
		{name: "incomplete", want: renderer.ErrInvalidResult}, {name: "canceled", complete: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelCall := map[bool]func(){true: cancel, false: func() {}}[test.complete]
			runtime := mustRuntime(c, schemaRenderProvider(schemaRenderFunc(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
				cancelCall()
				return renderer.SchemaResult{Complete: test.complete, Statements: []string{"unusable;"}}, nil
			})))
			result, err := runtime.RenderSchema(ctx, renderer.SchemaRequest{Target: "custom", Schema: &schemamodel.Database{}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
		})
	}
}
