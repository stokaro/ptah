package ydbrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// StreamingHandler supplies the selected owner's validation and rendering for
// standalone streaming operations. Registration grants no server capability.
func StreamingHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.StreamingQuery{}, ast.StatementExtension, validateStreaming, renderStreaming)
}

func validateStreaming(ctx renderer.ExtensionContext, value *ydbast.StreamingQuery) error {
	if ctx.Target != "ydb" {
		return fmt.Errorf("%w: streaming queries require YDB", ptaherr.ErrUnsupportedDialect)
	}
	if err := ydbstreaming.Refuse(ctx.Target, ctx.Capabilities, "streaming query "+value.QualifiedName()); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

func renderStreaming(_ renderer.ExtensionContext, value *ydbast.StreamingQuery) ([]string, error) {
	switch value.Operation {
	case ydbast.StreamingCreate:
		return []string{ydbstreaming.Create(value.QualifiedName(), value.Spec, ydbstreaming.CreateOptions{OrReplace: value.Creation.OrReplace, IfNotExists: value.Creation.IfNotExists})}, nil
	case ydbast.StreamingAlter:
		statement, err := ydbstreaming.Alter(value.QualifiedName(), value.Spec, value.Previous, ydbstreaming.AlterOptions{AllowStateReset: value.AllowStateReset})
		if err != nil {
			return nil, err
		}
		return []string{statement}, nil
	case ydbast.StreamingDrop:
		return []string{ydbstreaming.Drop(value.QualifiedName())}, nil
	default:
		return nil, fmt.Errorf("%w: unknown streaming operation %q", ptaherr.ErrInvalidSchemaDiff, value.Operation)
	}
}
