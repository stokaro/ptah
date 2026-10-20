package builtin_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
)

type composedSchemaService struct{ validationError error }

func (composedSchemaService) RenderSchema(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
	return renderer.SchemaResult{Complete: true, Statements: []string{"provider schema;"}}, nil
}
func (s composedSchemaService) ValidateSchema(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
	return schemavalidation.Result{}, s.validationError
}

func TestAdditionalProviderKeepsItsSchemaServices(t *testing.T) {
	c := qt.New(t)
	refusal := errors.New("provider validation failed")
	service := composedSchemaService{validationError: refusal}
	runtime, err := builtin.New(engine.Provider{ID: "example.org/additional", Targets: []engine.Target{{Name: "additional", Aliases: []string{"extra"}, SchemaRendering: service, Validation: service}}})
	c.Assert(err, qt.IsNil)
	schema := &schemamodel.Database{}
	rendered, err := runtime.RenderSchema(c.Context(), renderer.SchemaRequest{Target: "extra", Schema: schema})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Statements, qt.DeepEquals, []string{"provider schema;"})
	_, err = runtime.ValidateSchema(c.Context(), schemavalidation.Request{Target: "additional", Schema: schema})
	c.Assert(err, qt.ErrorIs, refusal)
	_, err = runtime.ResolveTarget("sqlite")
	c.Assert(err, qt.IsNil)
	plain, err := builtin.New()
	c.Assert(err, qt.IsNil)
	_, err = plain.ResolveTarget("additional")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
}

func TestAdditionalProviderDoesNotInheritBundledServices(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New(engine.Provider{ID: "example.org/additional", Targets: []engine.Target{{Name: "additional"}}})
	c.Assert(err, qt.IsNil)
	_, err = runtime.RenderSchema(c.Context(), renderer.SchemaRequest{Target: "additional", Schema: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = runtime.ValidateSchema(c.Context(), schemavalidation.Request{Target: "additional", Schema: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = builtin.New(engine.Provider{ID: "example.org/conflict", Targets: []engine.Target{{Name: "sqlite"}}})
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
}
