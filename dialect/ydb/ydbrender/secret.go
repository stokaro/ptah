package ydbrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretHandler supplies the owner's validation and rendering for statements
// on YDB secrets. Each statement names the variable a value comes from and
// never the value. Registration grants no server capability.
func SecretHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.Secret{}, ast.StatementExtension, validateSecret, renderSecret)
}

func validateSecret(ctx renderer.ExtensionContext, value *ydbast.Secret) error {
	if ctx.Target != "ydb" {
		return fmt.Errorf("%w: secrets require YDB", ptaherr.ErrUnsupportedDialect)
	}
	if err := ydbsecret.Refuse(ctx.Target, ctx.Capabilities, secretSubject(value)); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

func secretSubject(value *ydbast.Secret) string {
	switch value.Operation {
	case ydbast.SecretRotate:
		return "ALTER SECRET " + value.Path()
	case ydbast.SecretDrop:
		return "DROP SECRET " + value.Path()
	default:
		return "secret " + value.Path()
	}
}

func renderSecret(_ renderer.ExtensionContext, value *ydbast.Secret) ([]string, error) {
	switch value.Operation {
	case ydbast.SecretCreate:
		return []string{ydbsecret.CreateStatement(value.Schema, value.Name, value.ValueEnv)}, nil
	case ydbast.SecretRotate:
		return []string{ydbsecret.AlterStatement(value.Schema, value.Name, value.ValueEnv)}, nil
	case ydbast.SecretDrop:
		return []string{ydbsecret.DropStatement(value.Schema, value.Name)}, nil
	default:
		return nil, fmt.Errorf("%w: unknown secret operation %q", ptaherr.ErrInvalidSchemaDiff, value.Operation)
	}
}
