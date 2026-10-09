// Package chrender validates and renders ClickHouse-owned extension payloads.
// Callers select these handlers explicitly; no global registry is installed.
package chrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

// Handlers returns independent descriptors for the supported operation roles.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		renderer.TypedHandler(&chast.AlterTTL{}, ast.AlterExtension, validateTTL, renderTTL),
		renderer.TypedHandler(&chast.AddSkippingIndex{}, ast.AlterExtension, validateIndex, renderIndex),
		renderer.TypedHandler(&chast.DropSkippingIndex{}, ast.AlterExtension, validateDropIndex, renderDropIndex),
		renderer.TypedHandler(&chast.ModifyRefresh{}, ast.AlterExtension, validateRefresh, renderRefresh),
	}
}

// Registry creates a local handler registry. Unknown kinds and roles fail
// closed; an ALTER payload requires its parent before SQL can be rendered.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validateTTL(ctx renderer.ExtensionContext, op *chast.AlterTTL) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse TTL operation on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	return chsql.ValidateTTLChange(&op.Change)
}

func renderTTL(ctx renderer.ExtensionContext, op *chast.AlterTTL) ([]string, error) {
	clause := "REMOVE TTL"
	if op.Change.After.TTL.Value != "" {
		clause = "MODIFY TTL " + op.Change.After.TTL.Value
	}
	return []string{fmt.Sprintf("ALTER TABLE %s %s;", ctx.Parent.Name, clause)}, nil
}
