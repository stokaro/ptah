package renderer

// White-box testing required: the nesting bound is an unexported guard on the
// renderer, and a cycle would exhaust the stack before any exported call could
// report it.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

func TestRenderRefusesACyclicWithClause(t *testing.T) {
	c := qt.New(t)

	// CommonTableExpression.Query is a pointer, so a caller can point a CTE at
	// a statement that already contains it. Without the bound this recurses
	// until the stack runs out, which is a crash rather than a diagnostic.
	stmt := &ast.SelectStatement{From: "t", Columns: []ast.ResultColumn{{Name: "id"}}}
	stmt.With = []ast.CommonTableExpression{{Name: "loop", Query: stmt}}

	_, _, err := RenderSelect(stmt, "postgres")

	c.Assert(err, qt.ErrorMatches, `renderer: WITH clause nests deeper than \d+, which a cycle would do`)
}

func TestRenderRefusesAnUnnamedOrEmptyCTE(t *testing.T) {
	c := qt.New(t)
	base := func(cte ast.CommonTableExpression) *ast.SelectStatement {
		return &ast.SelectStatement{
			From:    "t",
			Columns: []ast.ResultColumn{{Name: "id"}},
			With:    []ast.CommonTableExpression{cte},
		}
	}

	_, _, err := RenderSelect(base(ast.CommonTableExpression{
		Query: &ast.SelectStatement{From: "u", Columns: []ast.ResultColumn{{Name: "id"}}},
	}), "postgres")
	c.Assert(err, qt.ErrorMatches, `renderer: common table expression requires a name`)

	_, _, err = RenderSelect(base(ast.CommonTableExpression{Name: "x"}), "postgres")
	c.Assert(err, qt.ErrorMatches, `renderer: common table expression "x" requires a query`)
}
