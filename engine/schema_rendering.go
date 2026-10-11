package engine

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/core/schemavalidation"
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
	prepared, err := r.prepareSchemaProperties(ctx, request.Schema, selected.selection)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	if prepared.refusal != nil {
		return renderer.SchemaResult{}, &renderer.SchemaRefusalError{Target: selected.name,
			Diagnostics: []schemavalidation.Diagnostic{*prepared.refusal}}
	}
	request.Schema = prepared.schema
	if err := r.validateDeclaredModels(ctx, request.Schema); err != nil {
		return renderer.SchemaResult{}, err
	}
	request.Target = selected.name
	result, err := renderer.RenderSchema(ctx, selected.schemaRendering, request)
	if err != nil || len(prepared.skipped) == 0 || len(result.Diagnostics) != 0 {
		return result, err
	}
	return withSkipped(result, prepared.skipped), nil
}

// withSkipped reports the objects the render left out because the target
// does not serve their model: one comment statement each, ahead of the
// render's own, and one omission each.
func withSkipped(result renderer.SchemaResult, skipped []schemaext.SkippedObject) renderer.SchemaResult {
	statements := make([]string, 0, len(skipped)+len(result.Statements))
	omissions := make([]renderer.Omission, 0, len(skipped)+len(result.Omissions))
	for _, object := range skipped {
		statements = append(statements, SkippedComment(object))
		omissions = append(omissions, renderer.Omission{Dialect: object.Target, Reason: "unsupported", Kind: object.Label,
			Name: object.Subject.Path()})
	}
	result.Statements = append(statements, result.Statements...)
	result.Omissions = append(omissions, result.Omissions...)
	return result
}

// SkippedComment is the SQL comment a render or a plan writes for an object
// the target skipped: the target, then [schemaext.SkippedObject.Note].
func SkippedComment(object schemaext.SkippedObject) string {
	return "-- " + strings.ToUpper(object.Target) + ": " + object.Note()
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
// properties, and leaves out the objects the target does not serve whose
// owners answer [schemaext.OffTargetSkip], which it returns. An object whose
// owner refuses it there comes back as the completed refusal. Both
// whole-schema services use the same immutable lowering path.
func (r *Runtime) prepareSchemaProperties(ctx context.Context, schema *schemamodel.Database, target schemaext.TargetSelection,
) (preparedSchema, error) {
	scoped, err := schemamodel.ScopeToTarget(schema, target)
	if err != nil || scoped == nil {
		return preparedSchema{schema: scoped}, err
	}
	scope, err := r.scopeOffTarget(target.Name(), scoped.FeatureObjects)
	if err != nil || scope.refusal != nil {
		return preparedSchema{refusal: scope.refusal}, err
	}
	if len(scope.skipped) != 0 {
		trimmed := *scoped
		trimmed.FeatureObjects = scope.objects
		scoped = &trimmed
	}
	decoded, err := schemaproperties.Decode(ctx, scoped, target.Name(), r)
	if err != nil {
		return preparedSchema{}, err
	}
	return preparedSchema{schema: decoded, skipped: scope.skipped}, nil
}

// preparedSchema is a schema scoped to a target: what the target's services
// read, the objects it skipped, or the completed refusal of one it refuses.
type preparedSchema struct {
	schema  *schemamodel.Database
	skipped []schemaext.SkippedObject
	refusal *schemavalidation.Diagnostic
}
