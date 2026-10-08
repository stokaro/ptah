package builtin

import (
	"context"
	"errors"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemavalidation"
)

type validationService struct{}

func (validationService) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	if err := ctx.Err(); err != nil {
		return schemavalidation.Result{}, err
	}
	result := schemavalidation.Result{Complete: true}
	if err := ValidateSchemaWithCapabilities(request.Schema, request.Target, request.Capabilities); err != nil {
		result.Diagnostics = []schemavalidation.Diagnostic{schemaDiagnostic(err)}
		return result, nil
	}
	if !request.NoSkipped {
		return result, nil
	}
	rendered, err := renderer.RenderSchema(ctx, schemaRenderingService{}, renderer.SchemaRequest{
		Target: request.Target, Schema: request.Schema, Capabilities: request.Capabilities, Identifiers: request.Identifiers,
	})
	if err != nil {
		result.Diagnostics = []schemavalidation.Diagnostic{schemaDiagnostic(err)}
		return result, nil
	}
	for _, omission := range rendered.Omissions {
		message := "would be skipped"
		if omission.Property != "" {
			message = omission.Message()
		}
		if omission.Remedy != "" {
			message += "; " + omission.Remedy
		}
		result.Diagnostics = append(result.Diagnostics, schemavalidation.Diagnostic{
			Code: schemavalidation.OmittedDeclaration, Kind: omission.Kind, Object: omission.Name, Message: message,
		})
	}
	return result, nil
}

func schemaDiagnostic(err error) schemavalidation.Diagnostic {
	diagnostic := schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: "schema", Message: err.Error()}
	if errors.Is(err, ptaherr.ErrUnsupportedFeature) {
		diagnostic.Code = schemavalidation.UnsupportedFeature
	}
	if capability, ok := errors.AsType[*ptaherr.CapabilityError](err); ok {
		diagnostic.Feature = capability.Feature
	}
	return diagnostic
}
