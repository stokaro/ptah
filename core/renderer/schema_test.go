package renderer_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
)

type schemaRenderFunc func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error)

func (f schemaRenderFunc) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	return f(ctx, request)
}

func TestRenderSchemaPreservesWholeBatchAndSnapshotsContainers(t *testing.T) {
	c := qt.New(t)
	statements := []string{"CREATE FIRST;", "CREATE SECOND;"}
	omissions := []renderer.Omission{{Dialect: "custom", Kind: "table", Name: "second", Reason: "unsupported", Property: "comment"}}
	request := renderer.SchemaRequest{Target: "custom", Capabilities: capability.Capabilities{capability.TransactionalDDL: true}, Schema: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "first"}, {Name: "second"}}}}
	calls := 0
	service := schemaRenderFunc(func(ctx context.Context, received renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(received, qt.DeepEquals, request)
		received.Schema.Tables = nil
		received.Capabilities[capability.TransactionalDDL] = false
		return renderer.SchemaResult{Complete: true, Statements: statements, Omissions: omissions}, nil
	})
	result, err := renderer.RenderSchema(t.Context(), service, request)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	statements[0] = "changed"
	omissions[0].Name = "changed"
	c.Assert(result.Statements, qt.DeepEquals, []string{"CREATE FIRST;", "CREATE SECOND;"})
	c.Assert(result.Omissions[0].Name, qt.Equals, "second")
	c.Assert(request.Schema.Tables, qt.HasLen, 2)
	c.Assert(request.Capabilities[capability.TransactionalDDL], qt.IsTrue)
}

func TestRenderSchemaRejectsIncompleteAndMalformedResults(t *testing.T) {
	for _, reply := range []renderer.SchemaResult{
		{}, {Statements: []string{"prefix;"}}, {Complete: true, Statements: []string{" "}},
		{Complete: true, Omissions: []renderer.Omission{{Kind: "table"}}},
		{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: "unknown", Kind: "schema", Message: "invalid"}}},
		{Complete: true, Statements: []string{"prefix;"}, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "schema", Message: "refused"}}},
	} {
		c := qt.New(t)
		service := schemaRenderFunc(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) { return reply, nil })
		result, err := renderer.RenderSchema(t.Context(), service, renderer.SchemaRequest{Target: "custom", Schema: &schemamodel.Database{}})
		c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
		c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
	}
}

func TestRenderSchemaDiscardsFailedAndCanceledOutput(t *testing.T) {
	failure := errors.New("provider disconnected")
	for _, test := range []struct {
		name          string
		failure, want error
	}{
		{name: "failure", failure: failure, want: failure}, {name: "canceled", want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			service := schemaRenderFunc(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
				cancel()
				return renderer.SchemaResult{Complete: true, Statements: []string{"unusable;"}}, test.failure
			})
			result, err := renderer.RenderSchema(ctx, service, renderer.SchemaRequest{Target: "custom", Schema: &schemamodel.Database{}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
		})
	}
}

func TestRenderSchemaReturnsTypedRefusalWithoutSQL(t *testing.T) {
	c := qt.New(t)
	diagnostics := []schemavalidation.Diagnostic{{Code: schemavalidation.UnsupportedFeature, Kind: "table", Object: "items", Feature: "custom mode", Message: "mode is unavailable"}}
	service := schemaRenderFunc(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
		return renderer.SchemaResult{Complete: true, Diagnostics: diagnostics}, nil
	})
	result, err := renderer.RenderSchema(t.Context(), service, renderer.SchemaRequest{Target: "custom", Schema: &schemamodel.Database{}})
	c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	var refusal *renderer.SchemaRefusalError
	c.Assert(err, qt.ErrorAs, &refusal)
	diagnostics[0].Message = "changed"
	c.Assert(refusal.Error(), qt.Equals, "mode is unavailable")
	c.Assert(refusal.Diagnostics[0].Object, qt.Equals, "items")
}

func TestRenderSchemaRequiresExplicitInputsBeforeDispatch(t *testing.T) {
	calls := 0
	service := schemaRenderFunc(func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		return renderer.SchemaResult{Complete: true}, nil
	})
	for _, test := range []struct {
		name    string
		ctx     context.Context
		service renderer.SchemaService
		schema  *schemamodel.Database
		want    error
	}{
		{name: "context", service: service, schema: &schemamodel.Database{}, want: schemaext.ErrInvalidValue},
		{name: "service", ctx: t.Context(), schema: &schemamodel.Database{}, want: schemaext.ErrInvalidValue},
		{name: "typed nil", ctx: t.Context(), service: schemaRenderFunc(nil), schema: &schemamodel.Database{}, want: schemaext.ErrInvalidValue},
		{name: "schema", ctx: t.Context(), service: service, want: ptaherr.ErrInvalidSchemaDiff},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := renderer.RenderSchema(test.ctx, test.service, renderer.SchemaRequest{Target: "custom", Schema: test.schema})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, renderer.SchemaResult{})
		})
	}
	qt.New(t).Assert(calls, qt.Equals, 0)
}
