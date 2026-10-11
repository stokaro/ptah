package engine

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// ValidateSchema invokes the selected target's offline schema validator. A
// missing target or validation service is an error; registration never grants
// validation implicitly. Errors and cancellation return no diagnostics.
func (r *Runtime) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemavalidation.Result{}, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return schemavalidation.Result{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if selected.validation == nil {
		return schemavalidation.Result{}, fmt.Errorf("%w: target %q has no schema validation service", ptaherr.ErrUnsupportedFeature, selected.name)
	}
	request.Target = selected.name
	prepared, err := r.prepareSchemaProperties(ctx, request.Schema, selected.selection)
	if err != nil {
		return schemavalidation.Result{}, err
	}
	if prepared.refusal != nil {
		return schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{*prepared.refusal}}, nil
	}
	request.Schema = prepared.schema
	if err := r.validateDeclaredModels(ctx, request.Schema); err != nil {
		return schemavalidation.Result{}, err
	}
	return schemavalidation.Validate(ctx, selected.validation, request)
}
