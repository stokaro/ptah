package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// renderWhere renders "SELECT * FROM t WHERE <expr>" for PostgreSQL and returns
// the WHERE-clause behavior, so constructor tests can assert the observable SQL
// contract rather than the internal node shape.
func renderWhere(tb testing.TB, expr ast.Expression) (string, []any) {
	c := qt.New(tb)
	c.Helper()
	sql, args, err := renderer.RenderSelect(&ast.SelectStatement{From: "t", Where: expr}, platform.Postgres)
	c.Assert(err, qt.IsNil)
	return sql, args
}

func TestExpressionConstructors(t *testing.T) {
	tests := []struct {
		name     string
		expr     ast.Expression
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "eq",
			expr:     query.Eq("a", 1),
			wantSQL:  `SELECT * FROM "t" WHERE "a" = $1`,
			wantArgs: []any{1},
		},
		{
			name:     "ne",
			expr:     query.Ne("a", 1),
			wantSQL:  `SELECT * FROM "t" WHERE "a" <> $1`,
			wantArgs: []any{1},
		},
		{
			name:     "lt",
			expr:     query.Lt("a", 1),
			wantSQL:  `SELECT * FROM "t" WHERE "a" < $1`,
			wantArgs: []any{1},
		},
		{
			name:     "le",
			expr:     query.Le("a", 1),
			wantSQL:  `SELECT * FROM "t" WHERE "a" <= $1`,
			wantArgs: []any{1},
		},
		{
			name:     "gt",
			expr:     query.Gt("a", 1),
			wantSQL:  `SELECT * FROM "t" WHERE "a" > $1`,
			wantArgs: []any{1},
		},
		{
			name:     "ge",
			expr:     query.Ge("a", 1),
			wantSQL:  `SELECT * FROM "t" WHERE "a" >= $1`,
			wantArgs: []any{1},
		},
		{
			name:     "in string slice",
			expr:     query.In("status", []string{"in_use", "sold"}),
			wantSQL:  `SELECT * FROM "t" WHERE "status" IN ($1, $2)`,
			wantArgs: []any{"in_use", "sold"},
		},
		{
			name:     "in int64 slice",
			expr:     query.In("id", []int64{1, 2, 3}),
			wantSQL:  `SELECT * FROM "t" WHERE "id" IN ($1, $2, $3)`,
			wantArgs: []any{int64(1), int64(2), int64(3)},
		},
		{
			name:     "is null",
			expr:     query.IsNull("deleted_at"),
			wantSQL:  `SELECT * FROM "t" WHERE "deleted_at" IS NULL`,
			wantArgs: nil,
		},
		{
			name:     "is not null",
			expr:     query.IsNotNull("deleted_at"),
			wantSQL:  `SELECT * FROM "t" WHERE "deleted_at" IS NOT NULL`,
			wantArgs: nil,
		},
		{
			name:     "and",
			expr:     query.And(query.Eq("a", 1), query.Eq("b", 2)),
			wantSQL:  `SELECT * FROM "t" WHERE ("a" = $1 AND "b" = $2)`,
			wantArgs: []any{1, 2},
		},
		{
			name:     "or",
			expr:     query.Or(query.Eq("a", 1), query.Eq("b", 2)),
			wantSQL:  `SELECT * FROM "t" WHERE ("a" = $1 OR "b" = $2)`,
			wantArgs: []any{1, 2},
		},
		{
			name:     "not",
			expr:     query.Not(query.Eq("a", 1)),
			wantSQL:  `SELECT * FROM "t" WHERE NOT ("a" = $1)`,
			wantArgs: []any{1},
		},
		{
			name:     "nested and or not",
			expr:     query.And(query.Eq("a", 1), query.Or(query.Eq("b", 2), query.Not(query.Eq("c", 3)))),
			wantSQL:  `SELECT * FROM "t" WHERE ("a" = $1 AND ("b" = $2 OR NOT ("c" = $3)))`,
			wantArgs: []any{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args := renderWhere(c.TB, tt.expr)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, tt.wantArgs)
		})
	}
}

func TestSelectBuilder_Projection(t *testing.T) {
	tests := []struct {
		name string
		stmt *ast.SelectStatement
		want []ast.ResultColumn
	}{
		{
			name: "named columns",
			stmt: query.Select("id", "name").From("t").Build(),
			want: []ast.ResultColumn{{Name: "id"}, {Name: "name"}},
		},
		{
			name: "explicit star",
			stmt: query.Select("*").From("t").Build(),
			want: []ast.ResultColumn{{Star: true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tt.stmt.Columns, qt.DeepEquals, tt.want)
		})
	}
}

func TestSelectBuilder_NoColumnsIsSelectStar(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select().From("t").Build()
	c.Assert(stmt.Columns, qt.HasLen, 0)

	sql, _, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT * FROM "t"`)
}

func TestSelectBuilder_LimitOffset(t *testing.T) {
	c := qt.New(t)

	unbounded := query.Select("*").From("t").Build()
	c.Assert(unbounded.Limit, qt.IsNil)
	c.Assert(unbounded.Offset, qt.IsNil)

	bounded := query.Select("*").From("t").Limit(24).Offset(48).Build()
	c.Assert(bounded.Limit, qt.IsNotNil)
	c.Assert(*bounded.Limit, qt.Equals, int64(24))
	c.Assert(bounded.Offset, qt.IsNotNil)
	c.Assert(*bounded.Offset, qt.Equals, int64(48))
}

func TestSelectBuilder_OrderByAccumulates(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select("*").
		From("t").
		OrderBy(query.Asc("name")).
		OrderBy(query.Desc("id")).
		Build()

	c.Assert(stmt.OrderBy, qt.DeepEquals, []ast.OrderByClause{
		{Column: "name", Direction: ast.SortAscending},
		{Column: "id", Direction: ast.SortDescending},
	})
}

func TestSelectBuilder_WhereReplaces(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select("*").
		From("t").
		Where(query.Eq("a", 1)).
		Where(query.Eq("b", 2)).
		Build()

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT * FROM "t" WHERE "b" = $1`)
	c.Assert(args, qt.DeepEquals, []any{2})
}

func TestSelectBuilder_FluentEndToEnd(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select("id", "name").
		From("commodities").
		Where(query.And(
			query.Eq("draft", false),
			query.In("status", []string{"in_use", "sold"}),
			query.Or(
				query.IsNotNull("deleted_at"),
				query.Not(query.Gt("count", int64(10))),
			),
		)).
		OrderBy(query.Asc("name"), query.Asc("id")).
		Limit(24).
		Offset(0).
		Build()

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id", "name" FROM "commodities" WHERE ("draft" = $1 AND "status" IN ($2, $3) AND ("deleted_at" IS NOT NULL OR NOT ("count" > $4))) ORDER BY "name" ASC, "id" ASC LIMIT $5 OFFSET $6`)
	c.Assert(args, qt.DeepEquals, []any{false, "in_use", "sold", int64(10), int64(24), int64(0)})
}

func TestSelectBuilder_SharedWhereFragment(t *testing.T) {
	c := qt.New(t)

	// A WHERE fragment can be built once and attached to multiple statements,
	// which is the pattern a paged list shares with its COUNT(*).
	filter := query.And(query.Eq("draft", false), query.Eq("tenant_id", "acme"))

	page := query.Select("id").From("commodities").Where(filter).Limit(20).Build()
	total := query.Select("id").From("commodities").Where(filter).Build()

	pageSQL, pageArgs, err := renderer.RenderSelect(page, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(pageSQL, qt.Equals, `SELECT "id" FROM "commodities" WHERE ("draft" = $1 AND "tenant_id" = $2) LIMIT $3`)
	c.Assert(pageArgs, qt.DeepEquals, []any{false, "acme", int64(20)})

	totalSQL, totalArgs, err := renderer.RenderSelect(total, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(totalSQL, qt.Equals, `SELECT "id" FROM "commodities" WHERE ("draft" = $1 AND "tenant_id" = $2)`)
	c.Assert(totalArgs, qt.DeepEquals, []any{false, "acme"})
}

func TestSelectBuilder_DegenerateExpressionsErrorCleanly(t *testing.T) {
	// Degenerate constructor inputs must surface a clean RenderSelect error, not
	// a panic, when the statement is rendered.
	tests := []struct {
		name        string
		expr        ast.Expression
		wantErrLike string
	}{
		{
			name:        "empty in list",
			expr:        query.In("status", []string{}),
			wantErrLike: "renderer: IN requires at least one value",
		},
		{
			name:        "and without operands",
			expr:        query.And(),
			wantErrLike: "renderer: logical expression requires at least one operand",
		},
		{
			name:        "or without operands",
			expr:        query.Or(),
			wantErrLike: "renderer: logical expression requires at least one operand",
		},
		{
			name:        "not without operand",
			expr:        query.Not(nil),
			wantErrLike: "renderer: NOT requires an operand",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := query.Select("*").From("t").Where(tt.expr).Build()
			sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}
