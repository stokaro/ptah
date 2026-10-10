package ydbrender

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbexternal"
)

// ExternalDataSourceHandler renders one statement on an external data source:
// CREATE EXTERNAL DATA SOURCE, CREATE OR REPLACE on a target that takes it,
// DROP EXTERNAL DATA SOURCE, or a recreation as a DROP and a CREATE. A
// credential is never in the text: an option
// names the secret that holds it. A declaration the target cannot hold is
// refused before anything is written.
func ExternalDataSourceHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.ExternalDataSource{}, ast.StatementExtension, validateExternalDataSource, renderExternalDataSource)
}

// ExternalTableHandler renders one statement on an external table as
// [ExternalDataSourceHandler] does on a data source. DROP EXTERNAL TABLE
// leaves the files the table reads where they are.
func ExternalTableHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.ExternalTable{}, ast.StatementExtension, validateExternalTable, renderExternalTable)
}

func validateExternalDataSource(ctx renderer.ExtensionContext, value *ydbast.ExternalDataSource) error {
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	display := ydbexternal.Display(value.Schema, value.Name)
	return externalRefusal(ctx, value.Operation, "external data source "+display, "DROP EXTERNAL DATA SOURCE "+display,
		func(caps capability.Capabilities) *ydbexternal.Refusal {
			return ydbexternal.CheckDataSource(display, value.Spec, caps)
		})
}

func validateExternalTable(ctx renderer.ExtensionContext, value *ydbast.ExternalTable) error {
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	display := ydbexternal.Display(value.Schema, value.Name)
	return externalRefusal(ctx, value.Operation, "external table "+display, "DROP EXTERNAL TABLE "+display,
		func(caps capability.Capabilities) *ydbexternal.Refusal {
			return ydbexternal.CheckTable(display, value.Spec, caps)
		})
}

// externalRefusal refuses a target other than YDB, and a statement the
// target's capabilities cannot take: any statement without
// external_data_sources, a declaration the line refuses, and CREATE OR
// REPLACE without external_object_replace.
func externalRefusal(ctx renderer.ExtensionContext, operation ydbast.ExternalOperation, subject, drop string,
	check func(capability.Capabilities) *ydbexternal.Refusal,
) error {
	caps := ctx.Capabilities
	if ctx.Target != "ydb" {
		caps = capability.Capabilities{}
	}
	switch operation {
	case ydbast.ExternalDrop:
		return ydbexternal.CheckDrop(drop, caps).Err(ctx.Target)
	case ydbast.ExternalReplace:
		if err := check(caps).Err(ctx.Target); err != nil {
			return err
		}
		return ydbexternal.CheckReplace(subject, caps).Err(ctx.Target)
	default:
		return check(caps).Err(ctx.Target)
	}
}

func renderExternalDataSource(_ renderer.ExtensionContext, value *ydbast.ExternalDataSource) ([]string, error) {
	switch value.Operation {
	case ydbast.ExternalDrop:
		return []string{ydbexternal.DropDataSourceStatement(value.Schema, value.Name)}, nil
	case ydbast.ExternalRecreate:
		return []string{ydbexternal.DropDataSourceStatement(value.Schema, value.Name),
			ydbexternal.CreateDataSourceStatement(value.Schema, value.Name, value.Spec, ydbexternal.Create)}, nil
	case ydbast.ExternalReplace:
		return []string{ydbexternal.CreateDataSourceStatement(value.Schema, value.Name, value.Spec, ydbexternal.Replace)}, nil
	default:
		return []string{ydbexternal.CreateDataSourceStatement(value.Schema, value.Name, value.Spec, ydbexternal.Create)}, nil
	}
}

func renderExternalTable(_ renderer.ExtensionContext, value *ydbast.ExternalTable) ([]string, error) {
	switch value.Operation {
	case ydbast.ExternalDrop:
		return []string{ydbexternal.DropTableStatement(value.Schema, value.Name)}, nil
	case ydbast.ExternalReplace:
		return []string{ydbexternal.CreateTableStatement(value.Schema, value.Name, value.Spec, ydbexternal.Replace)}, nil
	default:
		return []string{ydbexternal.CreateTableStatement(value.Schema, value.Name, value.Spec, ydbexternal.Create)}, nil
	}
}
