package synonym

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/sqlident"
)

// Handlers returns the render descriptor of an [Operation], for SQL Server
// and Oracle.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{renderer.TypedHandler(&Operation{}, ast.StatementExtension, validate, render)}
}

func validate(ctx renderer.ExtensionContext, value *Operation) error {
	if !supported(ctx.Target) {
		return renderer.UnsupportedExtension(ctx.Target, OperationKind, ast.StatementExtension)
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

// render writes the statements of one operation.
//
// The target is a name, not a body, so it is quoted part by part like the
// alias: written verbatim, it would break on the first reserved word or space.
// An empty middle part stays empty, which is how SQL Server spells a linked
// server name without a database. Neither engine has ALTER SYNONYM, so a
// retarget drops the alias and creates it again.
func render(ctx renderer.ExtensionContext, value *Operation) ([]string, error) {
	synonym := value.Synonym
	quote := quoter(ctx.Target)
	alias := quoteParts(quote, []string{synonym.Schema, synonym.Name})
	parts := TargetParts(synonym.Target)
	first := 0
	for first < 3 && parts[first] == "" {
		first++
	}
	target := quoteParts(quote, parts[first:])
	drop := "DROP SYNONYM" + dropGuard(ctx) + " " + alias + ";"
	create := "CREATE SYNONYM " + alias + " FOR " + target + ";"
	var statements []string
	switch value.Action {
	case Drop:
		return []string{drop}, nil
	case Retarget:
		statements = append(statements, drop)
	}
	if synonym.Comment != "" {
		statements = append(statements, "-- "+synonym.Comment)
	}
	return append(statements, create), nil
}

// dropGuard is the IF EXISTS a drop carries where the target has one: every
// SQL Server release Ptah supports, and an Oracle release whose capabilities
// say so.
func dropGuard(ctx renderer.ExtensionContext) string {
	if platform.NormalizeDialect(ctx.Target) == platform.SQLServer || ctx.Capabilities.Has(capability.ObjectExistenceGuards) {
		return " IF EXISTS"
	}
	return ""
}

func quoter(target string) func(string) string {
	if platform.NormalizeDialect(target) == platform.Oracle {
		return func(name string) string { return sqlident.Ident(platform.Oracle, name) }
	}
	return func(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }
}

// quoteParts quotes each present part and joins them with dots. A leading
// empty part is left out, and an empty part after a present one stays empty.
func quoteParts(quote func(string) string, parts []string) string {
	written := make([]string, 0, len(parts))
	for _, value := range parts {
		switch {
		case value == "" && len(written) == 0:
		case value == "":
			written = append(written, "")
		default:
			written = append(written, quote(value))
		}
	}
	return strings.Join(written, ".")
}
