// Package builtinlowering composes common schema lowering with selected owner
// services. It supplies bundled common-object metadata without registering or
// selecting a fallback runtime.
package builtinlowering

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/modelast"
	"ptah.run/internal/pgeffects"
)

// ForTarget binds the explicit runtime to local common-object metadata. Missing
// services remain unavailable. The caller owns the context and capability facts.
func ForTarget(ctx context.Context, runtime featureplan.DeclarationRuntime, target string, caps capability.Capabilities) modelast.Lowering {
	result := modelast.Lowering{Context: ctx, Runtime: runtime, Capabilities: caps}
	switch {
	case platform.NormalizeDialect(target) == platform.YDB:
		result.CommonMetadata = ydbMetadata
	case platform.IsPostgresFamily(target):
		result.CommonMetadata = postgresMetadata(target)
	}
	return result
}

// postgresMetadata describes the relations and columns each common statement
// of a PostgreSQL-family render creates, so a feature owner can order itself
// against them: after the tables it reads, before the views that read it.
func postgresMetadata(target string) func(context.Context, []ast.Node) ([]featureplan.CommonStep, error) {
	return func(ctx context.Context, nodes []ast.Node) ([]featureplan.CommonStep, error) {
		effects := pgeffects.Sequence(objectidentity.NewBuilder(identifier.ForDialect(target)), nodes)
		result := make([]featureplan.CommonStep, len(nodes))
		for i := range nodes {
			result[i].Effects = effects[i]
		}
		return result, ctx.Err()
	}
}

func ydbMetadata(ctx context.Context, nodes []ast.Node) ([]featureplan.CommonStep, error) {
	result := make([]featureplan.CommonStep, len(nodes))
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.YDB))
	for i, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// A schema rendered from nothing creates each declared secret before
		// any other statement, so a read through an absolute path, which an
		// unknown root leaves out, orders nothing it would not already.
		effects, err := ydbscheme.CommonEffects(builder, "", node)
		if err != nil {
			return nil, err
		}
		result[i].Effects = effects
		if len(effects) > 0 {
			result[i].Transaction = plangraph.TransactionForbidden
		}
	}
	return result, nil
}
