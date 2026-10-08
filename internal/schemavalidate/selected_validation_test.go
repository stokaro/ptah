package schemavalidate_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/internal/schemavalidate"
)

type selectedValidator func(context.Context, schemavalidation.Request) (schemavalidation.Result, error)

func (f selectedValidator) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	return f(ctx, request)
}

func TestCollectUsesSelectedWholeSchemaValidation(t *testing.T) {
	c := qt.New(t)
	calls := 0
	caps := capability.Capabilities{capability.CreateIndexConcurrently: true}
	service := selectedValidator(func(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.NoSkipped, qt.IsTrue)
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		c.Assert(request.Identifiers.Equal(identifier.ForDialect("postgres")), qt.IsTrue)
		c.Assert(request.Schema.Tables, qt.HasLen, 2)
		return schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.OmittedDeclaration, Kind: "widget", Object: "sample", Message: "level is omitted"}}}, nil
	})
	problems, err := schemavalidate.CollectWithOptions(t.Context(), service, &schemamodel.Database{Tables: []schemamodel.Table{{Name: "first"}, {Name: "second"}}}, "postgres", schemavalidate.Options{Capabilities: caps, NoSkipped: true})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(problems, qt.DeepEquals, []schemavalidate.Problem{{Dialect: "postgres", Kind: "widget", Object: "sample", Message: "level is omitted"}})
}

func TestCollectDiscardsCommonProblemsWhenValidationFails(t *testing.T) {
	failure := errors.New("validation transport failed")
	for _, test := range []struct {
		name     string
		complete bool
		failure  error
		cancel   bool
		want     error
	}{
		{name: "failure", complete: true, failure: failure, want: failure},
		{name: "incomplete", want: schemavalidation.ErrInvalidResult},
		{name: "canceled", complete: true, cancel: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelCall := map[bool]func(){true: cancel, false: func() {}}[test.cancel]
			service := selectedValidator(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
				cancelCall()
				return schemavalidation.Result{Complete: test.complete, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "schema", Message: "partial finding"}}}, test.failure
			})
			problems, err := schemavalidate.Collect(ctx, service, &schemamodel.Database{Indexes: []schemamodel.Index{{Name: "orphan", TableName: "absent"}}}, "postgres")
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(problems, qt.IsNil)
		})
	}
}

func (f selectedValidator) ResolveTarget(name string) (schemaext.TargetSelection, error) {
	return schemaext.NewTargetSelection(name)
}

func TestCollectResolvesRegisteredAliasBeforeDefaultFacts(t *testing.T) {
	c := qt.New(t)
	calls := 0
	service := selectedValidator(func(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Capabilities, qt.DeepEquals, capability.ForDialect("postgres"))
		c.Assert(request.Identifiers.Equal(identifier.ForDialect("postgres")), qt.IsTrue)
		return schemavalidation.Result{Complete: true}, nil
	})
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/validation", Targets: []engine.Target{{Name: "postgres", Aliases: []string{"alternate"}, Validation: service}}}))
	problems, err := schemavalidate.Collect(t.Context(), runtime, &schemamodel.Database{}, "alternate")
	c.Assert(err, qt.IsNil)
	c.Assert(problems, qt.HasLen, 0)
	c.Assert(calls, qt.Equals, 1)
	_, err = schemavalidate.Collect(t.Context(), runtime, nil, "unregistered")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(calls, qt.Equals, 1)
}
