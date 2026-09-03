package renderer

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/ptaherr"
)

// An index and a UNIQUE, PRIMARY KEY or CHECK constraint each have one payload
// that no dialect can render as nothing: a key part list, or a check
// expression. Rendering an empty one produced `ON "t" ()`, `UNIQUE ()` and
// `CHECK ()` -- syntactically invalid on every engine, emitted after a
// successful render, so a direct-model caller received broken migration SQL and
// only found out when a server refused it (stokaro/ptah#2790).
//
// The refusals live on the AST nodes rather than on the schema model because
// both entry points converge here: RenderSQL is handed AST directly, and
// GetOrderedCreateStatements walks the model into AST through
// modelast.WalkDatabase and renders each node through the same visitor. One
// guard therefore covers both, and there is no second copy to drift.
//
// FOREIGN KEY and EXCLUDE are deliberately absent. Both already refuse an empty
// payload -- validateASTForeignKey counts its column lists, and the PostgreSQL
// renderer reports an exclude constraint missing its method or elements -- and a
// second guard over the same shape would leave neither of them measurable,
// since either could be deleted while the other kept the tests green.

// validateIndexKeyParts refuses an index that names nothing to index.
//
// An index is satisfied by a plain column list or by parts, and a part counts
// when it carries a column name or an expression: a functional index such as
// `(lower(a))` has no column and is valid, while a part with neither is not a
// key part at all.
func validateIndexKeyParts(dialect string, node *ast.IndexNode) error {
	if node == nil || hasNamedColumn(node.Columns) || indexHasEffectivePart(node.Parts) {
		return nil
	}
	return emptyKeyPartsError(dialect, "index", node.Name, "at least one column or expression")
}

func indexHasEffectivePart(parts []ast.IndexPart) bool {
	for _, part := range parts {
		if strings.TrimSpace(part.Name) != "" || strings.TrimSpace(part.Expr) != "" {
			return true
		}
	}
	return false
}

// hasNamedColumn reports whether a column list names anything.
//
// Length alone is not the question, and the difference is reachable rather than
// theoretical: a structured part carrying only a direction or a prefix length
// converts to a column entry with an empty name, so a list of those has a
// length and still indexes nothing. Rendering it emitted `("" DESC, "")`.
func hasNamedColumn(columns []string) bool {
	for _, column := range columns {
		if strings.TrimSpace(column) != "" {
			return true
		}
	}
	return false
}

// validateConstraintKeyParts refuses a UNIQUE or PRIMARY KEY constraint that
// names no column, and a CHECK constraint that carries no expression.
func validateConstraintKeyParts(dialect string, node *ast.ConstraintNode) error {
	if node == nil {
		return nil
	}
	switch node.Type {
	case ast.UniqueConstraint, ast.PrimaryKeyConstraint:
		if hasNamedColumn(node.Columns) || constraintHasEffectiveColumnPart(node.ColumnParts) {
			return nil
		}
		return emptyKeyPartsError(dialect, constraintKind(node.Type), node.Name, "at least one column")
	case ast.CheckConstraint:
		if strings.TrimSpace(node.Expression) != "" {
			return nil
		}
		return emptyKeyPartsError(dialect, constraintKind(node.Type), node.Name, "an expression")
	default:
		return nil
	}
}

func constraintHasEffectiveColumnPart(parts []ast.ConstraintColumn) bool {
	for _, part := range parts {
		if strings.TrimSpace(part.Name) != "" || strings.TrimSpace(part.Expr) != "" {
			return true
		}
	}
	return false
}

func constraintKind(constraintType ast.ConstraintType) string {
	return constraintType.String() + " constraint"
}

// emptyKeyPartsError reports the empty payload as an invalid schema rather than
// an unsupported feature: no dialect accepts the shape, so there is no target
// that would have rendered it.
func emptyKeyPartsError(dialect, kind, name, requirement string) error {
	described := kind
	if strings.TrimSpace(name) != "" {
		described = fmt.Sprintf("%s %q", kind, name)
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrInvalidSchemaDiff,
		Message: described + " declares no key parts; it needs " + requirement,
	}
}
