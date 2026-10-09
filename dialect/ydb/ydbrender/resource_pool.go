package ydbrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/internal/ydbpool"
)

// ResourcePoolHandler validates and renders standalone workload pool operations.
func ResourcePoolHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.ResourcePool{}, ast.StatementExtension, validatePool, renderPool)
}

// ResourcePoolClassifierHandler handles standalone query routing operations.
func ResourcePoolClassifierHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.ResourcePoolClassifier{}, ast.StatementExtension, validateClassifier, renderClassifier)
}

func validatePool(ctx renderer.ExtensionContext, v *ydbast.ResourcePool) error {
	if err := workloadTarget(ctx); err != nil {
		return err
	}
	return workloadValidation(v.Validate())
}

func validateClassifier(ctx renderer.ExtensionContext, v *ydbast.ResourcePoolClassifier) error {
	if err := workloadTarget(ctx); err != nil {
		return err
	}
	return workloadValidation(v.Validate())
}

func workloadTarget(ctx renderer.ExtensionContext) error {
	if ctx.Target != "ydb" {
		return fmt.Errorf("%w: resource pools require YDB", ptaherr.ErrUnsupportedDialect)
	}
	if !ctx.Capabilities.Has(capability.ResourcePools) {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(capability.ResourcePools), Err: ptaherr.ErrUnsupportedFeature,
			Message: "resource pools require target capability resource_pools; " + ydbpool.FlagHint}
	}
	return nil
}

func workloadValidation(err error) error {
	if err == nil {
		return nil
	}
	return &ptaherr.RenderError{Dialect: "ydb", Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
}

func renderPool(_ renderer.ExtensionContext, v *ydbast.ResourcePool) ([]string, error) {
	switch v.Operation {
	case ydbast.PoolCreate:
		return []string{ydbpool.CreatePoolStatement(v.Name, *v.Spec)}, nil
	case ydbast.PoolAlter:
		return workloadStatement(ydbpool.AlterPoolStatement(v.Name, *v.Spec, *v.Previous)), nil
	case ydbast.PoolDrop:
		return []string{ydbpool.DropPoolStatement(v.Name)}, nil
	default:
		return nil, fmt.Errorf("%w: unknown pool operation %q", ptaherr.ErrInvalidSchemaDiff, v.Operation)
	}
}

func renderClassifier(_ renderer.ExtensionContext, v *ydbast.ResourcePoolClassifier) ([]string, error) {
	switch v.Operation {
	case ydbast.PoolCreate:
		return []string{ydbpool.CreateClassifierStatement(v.Name, *v.Spec)}, nil
	case ydbast.PoolAlter:
		return workloadStatement(ydbpool.AlterClassifierStatement(v.Name, *v.Spec, *v.Previous)), nil
	case ydbast.PoolDrop:
		return []string{ydbpool.DropClassifierStatement(v.Name)}, nil
	default:
		return nil, fmt.Errorf("%w: unknown classifier operation %q", ptaherr.ErrInvalidSchemaDiff, v.Operation)
	}
}

func workloadStatement(statement string) []string {
	if statement == "" {
		return nil
	}
	return []string{statement}
}
