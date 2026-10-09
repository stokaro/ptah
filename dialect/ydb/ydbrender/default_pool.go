package ydbrender

import (
	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbworkload"
)

// DefaultPoolSettingsHandler sets declared limits without resetting any setting
// omitted by the source. Registration grants no workload capability.
func DefaultPoolSettingsHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.DefaultPoolSettings{}, ast.StatementExtension,
		func(ctx renderer.ExtensionContext, value *ydbast.DefaultPoolSettings) error {
			if err := workloadTarget(ctx); err != nil {
				return err
			}
			return workloadValidation(value.Validate())
		}, renderDefaultPoolSettings)
}

func renderDefaultPoolSettings(_ renderer.ExtensionContext, value *ydbast.DefaultPoolSettings) ([]string, error) {
	return workloadStatement(ydbworkload.SetPoolStatement(ydbworkload.DefaultPool, value.Spec)), nil
}
