package builtin

import (
	"context"

	"ptah.run/core/renderer"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/renderdiag"
)

type schemaRenderingService struct{}

func (schemaRenderingService) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	if err := ctx.Err(); err != nil {
		return renderer.SchemaResult{}, err
	}
	sink := &renderdiag.Sink{}
	statements, err := orderedCreateStatements(ctx, request.Schema, request.Target, request.Capabilities, sink)
	if ctx.Err() != nil {
		return renderer.SchemaResult{}, ctx.Err()
	}
	if err != nil {
		return renderer.SchemaResult{Complete: true, Diagnostics: []schemavalidation.Diagnostic{schemaDiagnostic(err)}}, nil
	}
	return renderer.SchemaResult{Complete: true, Statements: statements, Omissions: publicOmissions(request.Target, sink)}, nil
}
