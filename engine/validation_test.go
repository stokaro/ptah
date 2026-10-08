package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

type validationFunc func(context.Context, schemavalidation.Request) (schemavalidation.Result, error)

func (f validationFunc) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	return f(ctx, request)
}

func validationProvider(service schemavalidation.Service) engine.Provider {
	return engine.Provider{ID: "example.org/validator", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Validation: service}}}
}

func TestRuntimeValidationUsesFrozenSelectionAndWholeSchema(t *testing.T) {
	c := qt.New(t)
	calls := 0
	service := validationFunc(func(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "custom")
		c.Assert(request.Schema.Tables, qt.HasLen, 2)
		return schemavalidation.Result{Complete: true}, nil
	})
	provider := validationProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Targets[0].Validation = nil
	result, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{
		Target: " ALTERNATE ", Schema: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "first"}, {Name: "second"}}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(calls, qt.Equals, 1)
}

func TestRuntimeValidationRequiresExplicitSupport(t *testing.T) {
	c := qt.New(t)
	_, err := engine.New(validationProvider(validationFunc(nil)))
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	runtime := mustRuntime(c, validationProvider(nil))
	result, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "custom", Schema: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
	result, err = runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "unknown", Schema: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
}

func TestRuntimeValidationDiscardsIncompleteAndFailedReplies(t *testing.T) {
	failure := errors.New("provider failed")
	for _, test := range []struct {
		name     string
		complete bool
		failure  error
		want     error
	}{
		{name: "incomplete", want: schemavalidation.ErrInvalidResult},
		{name: "failed", complete: true, failure: failure, want: failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := validationFunc(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
				return schemavalidation.Result{Complete: test.complete}, test.failure
			})
			runtime := mustRuntime(c, validationProvider(service))
			result, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "custom", Schema: &schemamodel.Database{}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
		})
	}
}

type validationValue struct{}

func (*validationValue) Kind() schemaext.Kind   { return "example.org/validation-value" }
func (*validationValue) Clone() schemaext.Value { return &validationValue{} }
func (*validationValue) Equal(other schemaext.Value) bool {
	_, ok := other.(*validationValue)
	return ok
}

func TestRuntimeValidationChecksModelsBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	calls := 0
	service := validationFunc(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		return schemavalidation.Result{Complete: true}, nil
	})
	runtime := mustRuntime(c, validationProvider(service))
	facets, err := schemaext.NewFacets(&validationValue{})
	c.Assert(err, qt.IsNil)
	result, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "custom", Schema: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", Facets: facets}}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
	c.Assert(calls, qt.Equals, 0)
}

func TestRuntimeValidationDiscardsCanceledCompletedReply(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := validationFunc(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
		cancel()
		return schemavalidation.Result{Complete: true}, nil
	})
	runtime := mustRuntime(c, validationProvider(service))
	result, err := runtime.ValidateSchema(ctx, schemavalidation.Request{Target: "custom", Schema: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
}
