package atlasreport_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlasreport"
)

type selectedSchemaRenderer struct {
	*engine.Runtime
	render func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error)
}

func (s selectedSchemaRenderer) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	return s.render(ctx, request)
}

func TestSchemaInspectSQLUsesSelectedWholeSchemaRenderer(t *testing.T) {
	c := qt.New(t)
	calls := 0
	caps := capability.Capabilities{capability.TransactionalDDL: true}
	selected := selectedSchemaRenderer{Runtime: inspectRuntime(c), render: func(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		c.Assert(request.Schema.Tables, qt.HasLen, 2)
		c.Assert(request.Schema.Schemas, qt.HasLen, 0)
		return renderer.SchemaResult{Complete: true, Statements: []string{"SELECTED FIRST;\n", "SELECTED SECOND;\n"}}, nil
	}}
	schema := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}, Tables: []schemamodel.Table{{Name: "first"}, {Name: "second"}}}
	report, err := atlasreport.NewSchemaInspectReport(t.Context(), schema, &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, atlasreport.SchemaInspectReportOptions{}, selected)
	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "SELECTED FIRST;\nSELECTED SECOND;\n")
	c.Assert(calls, qt.Equals, 1)
	c.Assert(schema.Schemas, qt.HasLen, 1)
}

func TestSchemaInspectSQLDiscardsFailedServiceOutput(t *testing.T) {
	failure := errors.New("schema renderer failed")
	for _, test := range []struct {
		name          string
		complete      bool
		failure, want error
	}{
		{name: "failed", complete: true, failure: failure, want: failure},
		{name: "incomplete", want: renderer.ErrInvalidResult},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected := selectedSchemaRenderer{Runtime: inspectRuntime(c), render: func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
				return renderer.SchemaResult{Complete: test.complete, Statements: []string{"unusable;"}}, test.failure
			}}
			report, err := atlasreport.NewSchemaInspectReport(t.Context(), &schemamodel.Database{}, &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres"}, nil, atlasreport.SchemaInspectReportOptions{}, selected)
			c.Assert(err, qt.IsNil)
			sql, err := report.MarshalSQL()
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
