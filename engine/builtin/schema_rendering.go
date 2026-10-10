package builtin

import (
	"context"
	"errors"

	"ptah.run/core/featureplan"
	"ptah.run/core/renderer"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/modelast"
	"ptah.run/internal/renderdiag"
)

type schemaRenderingService struct {
	declarations featureplan.DeclarationRuntime
}

func (s schemaRenderingService) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	if err := ctx.Err(); err != nil {
		return renderer.SchemaResult{}, err
	}
	sink := &renderdiag.Sink{}
	statements, err := orderedCreateStatements(ctx, s.declarations, request, sink)
	if ctx.Err() != nil {
		return renderer.SchemaResult{}, ctx.Err()
	}
	if err != nil {
		if _, failed := errors.AsType[*modelast.DeclarationServiceError](err); failed {
			return renderer.SchemaResult{}, err
		}
		return renderer.SchemaResult{Complete: true, Diagnostics: []schemavalidation.Diagnostic{schemaDiagnostic(err)}}, nil
	}
	return renderer.SchemaResult{Complete: true, Statements: statements, Omissions: publicOmissions(request.Target, sink)}, nil
}
