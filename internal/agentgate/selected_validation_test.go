package agentgate_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine/builtin"
	"ptah.run/internal/agentgate"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/builtintest"
)

type selectedValidator func(context.Context, schemavalidation.Request) (schemavalidation.Result, error)

func (f selectedValidator) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	return f(ctx, request)
}

func TestSchemaGatePresentsSelectedValidationDiagnostics(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassSchema, map[string]string{"models.go": goodSchema})
	calls := 0
	service := selectedValidator(func(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Schema.Tables, qt.HasLen, 1)
		return schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "widget", Object: "sample", Message: "provider refusal"}}}, nil
	})
	selected, err := agentgate.New(agentgate.Options{Owners: builtintest.Runtime(), Dialect: "postgres", Validation: service, Rendering: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	report, err := selected.Run(t.Context(), scope)
	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsFalse)
	c.Assert(calls, qt.Equals, 1)
	result := resultFor(c, report, agentgate.GateSchemaValidate)
	c.Assert(result.OK, qt.IsFalse)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Message, qt.Equals, "sample: provider refusal")
}

func TestSchemaGateReturnsValidationFailureWithoutReport(t *testing.T) {
	failure := errors.New("validator connection lost")
	for _, test := range []struct {
		name     string
		complete bool
		failure  error
		want     error
	}{
		{name: "failed", complete: true, failure: failure, want: failure},
		{name: "incomplete", want: schemavalidation.ErrInvalidResult},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := scopeFor(c, agentpolicy.ClassSchema, map[string]string{"models.go": goodSchema})
			service := selectedValidator(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
				return schemavalidation.Result{Complete: test.complete}, test.failure
			})
			selected, err := agentgate.New(agentgate.Options{Owners: builtintest.Runtime(), Dialect: "postgres", Validation: service, Rendering: must.Must(builtin.New())})
			c.Assert(err, qt.IsNil)
			report, err := selected.Run(t.Context(), scope)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(report, qt.DeepEquals, agentgate.Report{})
		})
	}
}

func TestSchemaGateCancellationIsNotASchemaProblem(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassSchema, map[string]string{"models.go": goodSchema})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := selectedValidator(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
		cancel()
		return schemavalidation.Result{Complete: true}, nil
	})
	selected, err := agentgate.New(agentgate.Options{Owners: builtintest.Runtime(), Dialect: "postgres", Validation: service, Rendering: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	report, err := selected.Run(ctx, scope)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report, qt.DeepEquals, agentgate.Report{})
}

func (f selectedValidator) ResolveTarget(name string) (schemaext.TargetSelection, error) {
	return schemaext.NewTargetSelection(name)
}
