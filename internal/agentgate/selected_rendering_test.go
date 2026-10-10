package agentgate_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/renderer"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine/builtin"
	"ptah.run/internal/agentgate"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/builtintest"
)

type selectedSchemaRenderer func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error)

func (f selectedSchemaRenderer) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	return f(ctx, request)
}

func TestSchemaGatePresentsCompletedRenderingRefusal(t *testing.T) {
	c := qt.New(t)
	scope := scopeFor(c, agentpolicy.ClassSchema, map[string]string{"models.go": goodSchema})
	calls := 0
	service := selectedSchemaRenderer(func(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Schema.Tables, qt.HasLen, 1)
		return renderer.SchemaResult{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "schema", Message: "provider schema refusal"}}}, nil
	})
	selected, err := agentgate.New(agentgate.Options{Annotations: builtintest.Annotations(), Dialect: "postgres", Validation: must.Must(builtin.New()), Rendering: service})
	c.Assert(err, qt.IsNil)
	report, err := selected.Run(t.Context(), scope)
	c.Assert(err, qt.IsNil)
	c.Assert(report.OK, qt.IsFalse)
	c.Assert(calls, qt.Equals, 1)
	result := resultFor(c, report, agentgate.GateSchemaRender)
	c.Assert(result.OK, qt.IsFalse)
	c.Assert(result.Diagnostics[0].Message, qt.Equals, "provider schema refusal")
}

func TestSchemaGateDoesNotReportRenderingFailureAsSchemaRefusal(t *testing.T) {
	failure := errors.New("schema renderer disconnected")
	for _, test := range []struct {
		name          string
		complete      bool
		failure, want error
	}{
		{name: "failure", complete: true, failure: failure, want: failure},
		{name: "incomplete", want: renderer.ErrInvalidResult},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := scopeFor(c, agentpolicy.ClassSchema, map[string]string{"models.go": goodSchema})
			service := selectedSchemaRenderer(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
				return renderer.SchemaResult{Complete: test.complete, Statements: []string{"partial;"}}, test.failure
			})
			selected, err := agentgate.New(agentgate.Options{Annotations: builtintest.Annotations(), Dialect: "postgres", Validation: must.Must(builtin.New()), Rendering: service})
			c.Assert(err, qt.IsNil)
			report, err := selected.Run(t.Context(), scope)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(report, qt.DeepEquals, agentgate.Report{})
		})
	}
}
