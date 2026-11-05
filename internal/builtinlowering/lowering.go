// Package builtinlowering composes common schema lowering with selected owner
// services. It supplies bundled common-object metadata without registering or
// selecting a fallback runtime.
package builtinlowering

import (
	"context"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/modelast"
	"ptah.run/internal/pgeffects"
	pgplanner "ptah.run/internal/planner/dialects/postgres"
)

// ForTarget binds the explicit runtime to local common-object metadata. Missing
// services remain unavailable. The caller owns the context and capability facts.
func ForTarget(ctx context.Context, runtime featureplan.DeclarationRuntime, target string, caps capability.Capabilities) modelast.Lowering {
	result := modelast.Lowering{Context: ctx, Runtime: runtime, Capabilities: caps}
	if platform.IsPostgresFamily(target) {
		result.CommonMetadata = postgresMetadata(target)
		result.ResolvesBodyAtCreate = pgplanner.ResolvesBodyAtCreate
	}
	return result
}

// postgresMetadata describes the relations and columns each common statement
// of a PostgreSQL-family render creates, and the declared types it uses, so a
// feature owner can order itself against them: after the tables it reads,
// before the views that read it, and a type before the columns and routines
// that use it.
func postgresMetadata(target string) func(context.Context, []ast.Node, []schemaext.DefinedType) ([]featureplan.CommonStep, error) {
	return func(ctx context.Context, nodes []ast.Node, types []schemaext.DefinedType) ([]featureplan.CommonStep, error) {
		effects := pgeffects.Sequence(objectidentity.NewBuilder(identifier.ForDialect(target)), nodes)
		result := make([]featureplan.CommonStep, len(nodes))
		for i, node := range nodes {
			result[i].Effects = effects[i]
			if effects[i] != nil {
				result[i].Effects = slices.Concat(effects[i], pgeffects.TypeReads(node, types))
			}
		}
		return result, ctx.Err()
	}
}
