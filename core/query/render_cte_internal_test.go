package query

// White-box testing required: the nesting bound is an unexported guard on the
// renderer, and a cycle would exhaust the stack before any exported call could
// report it.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRenderRefusesACyclicWithClause(t *testing.T) {
	c := qt.New(t)

	// CommonTableExpression.Query is a pointer, so a caller can point a CTE at
	// a statement that already contains it. Without the bound this recurses
	// until the stack runs out, which is a crash rather than a diagnostic.
	stmt := &SelectStatement{From: "t", Columns: []ResultColumn{{Name: "id"}}}
	stmt.With = []CommonTableExpression{{Name: "loop", Query: stmt}}

	_, _, err := RenderSelect(stmt, "postgres")

	c.Assert(err, qt.ErrorMatches, `renderer: statement nests deeper than \d+, which a cycle would do`)
}

func TestRenderRefusesAnUnnamedOrEmptyCTE(t *testing.T) {
	c := qt.New(t)
	base := func(cte CommonTableExpression) *SelectStatement {
		return &SelectStatement{
			From:    "t",
			Columns: []ResultColumn{{Name: "id"}},
			With:    []CommonTableExpression{cte},
		}
	}

	_, _, err := RenderSelect(base(CommonTableExpression{
		Query: &SelectStatement{From: "u", Columns: []ResultColumn{{Name: "id"}}},
	}), "postgres")
	c.Assert(err, qt.ErrorMatches, `renderer: common table expression requires a name`)

	_, _, err = RenderSelect(base(CommonTableExpression{Name: "x"}), "postgres")
	c.Assert(err, qt.ErrorMatches, `renderer: common table expression "x" requires a query`)
}

func TestRenderRefusesInWithBothValuesAndSubquery(t *testing.T) {
	c := qt.New(t)

	// Both set describes two different membership tests. Picking one silently
	// would answer a question the caller never asked.
	stmt := &SelectStatement{
		From:    "users",
		Columns: []ResultColumn{{Name: "id"}},
		Where: &InExpr{
			Operand:  &ColumnRef{Name: "id"},
			Values:   []Expression{&BoundValue{Value: 1}},
			Subquery: &SelectStatement{From: "posts", Columns: []ResultColumn{{Name: "author_id"}}},
		},
	}

	_, _, err := RenderSelect(stmt, "postgres")

	c.Assert(err, qt.ErrorMatches, `renderer: IN takes either values or a subquery, not both`)
}

func TestRenderRefusesACyclicSubquery(t *testing.T) {
	c := qt.New(t)

	// A subquery holds a pointer too, so the same cycle is reachable through
	// EXISTS as through WITH, and the one bound covers both.
	stmt := &SelectStatement{From: "t", Columns: []ResultColumn{{Name: "id"}}}
	stmt.Where = &ExistsExpr{Query: stmt}

	_, _, err := RenderSelect(stmt, "postgres")

	c.Assert(err, qt.ErrorMatches, `renderer: statement nests deeper than \d+, which a cycle would do`)
}
