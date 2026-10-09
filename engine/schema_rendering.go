package engine

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
)

// RenderSchema invokes the selected target's whole-schema rendering service.
// Model codecs are checked before dispatch. A target with only AST rendering
// cannot render schemas implicitly. Errors and cancellation expose no SQL.
func (r *Runtime) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return renderer.SchemaResult{}, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return renderer.SchemaResult{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if selected.schemaRendering == nil {
		return renderer.SchemaResult{}, fmt.Errorf("%w: target %q has no schema rendering service", ptaherr.ErrUnsupportedFeature, selected.name)
	}
	scoped, err := r.prepareSchemaProperties(ctx, request.Schema, selected.selection)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	request.Schema = scoped
	if err := r.validateDeclaredModels(ctx, request.Schema); err != nil {
		return renderer.SchemaResult{}, err
	}
	request.Target = selected.name
	return renderer.RenderSchema(ctx, selected.schemaRendering, request)
}

// validateDeclaredModels is shared by whole-schema services so adding a model
// slot cannot make validation and rendering disagree about codec registration.
func (r *Runtime) validateDeclaredModels(ctx context.Context, schema *schemamodel.Database) error {
	if schema == nil {
		return nil
	}
	if _, err := r.codecs.SnapshotObjectState(ctx, schemaext.Desired, schemaext.ObjectState{Objects: schema.FeatureObjects, Coverage: declaredModelCoverage(schema)}); err != nil {
		return err
	}
	for _, facets := range schema.FacetSlots() {
		values, err := facets.Values()
		if err != nil {
			return err
		}
		if _, err := r.codecs.SnapshotValues(ctx, schemaext.Desired, values); err != nil {
			return err
		}
	}
	return nil
}

// prepareSchemaProperties scopes declarations before decoding selected source
// properties. Both whole-schema services use the same immutable lowering path.
func (r *Runtime) prepareSchemaProperties(ctx context.Context, schema *schemamodel.Database, target schemaext.TargetSelection) (*schemamodel.Database, error) {
	scoped, err := schemamodel.ScopeToTarget(schema, target)
	if err != nil || scoped == nil {
		return scoped, err
	}
	return schemaproperties.Decode(ctx, scoped, target.Name(), r)
}
