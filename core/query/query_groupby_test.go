package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// renderProjection renders "SELECT <expr> FROM t" for PostgreSQL and returns the
// observable SQL and args, so aggregate constructors can be asserted through the
// public rendering contract rather than the internal node shape.
func renderProjection(c *qt.C, expr ast.Expression) (string, []any) {
	c.Helper()
	stmt := query.Select().Exprs(expr).From("t").Build()
	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	return sql, args
}

func TestAggregateConstructors(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		expr    ast.Expression
		wantSQL string
	}{
		{name: "count star", expr: query.CountStar(), wantSQL: `SELECT COUNT(*) FROM "t"`},
		{name: "count", expr: query.Count("id"), wantSQL: `SELECT COUNT("id") FROM "t"`},
		{name: "count distinct", expr: query.CountDistinct("status"), wantSQL: `SELECT COUNT(DISTINCT "status") FROM "t"`},
		{name: "sum", expr: query.Sum("total"), wantSQL: `SELECT SUM("total") FROM "t"`},
		{name: "avg", expr: query.Avg("total"), wantSQL: `SELECT AVG("total") FROM "t"`},
		{name: "min", expr: query.Min("total"), wantSQL: `SELECT MIN("total") FROM "t"`},
		{name: "max", expr: query.Max("total"), wantSQL: `SELECT MAX("total") FROM "t"`},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args := renderProjection(c, tt.expr)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestCountStarArgumentIsCountStar(t *testing.T) {
	c := qt.New(t)

	// Count("*") is a convenience for COUNT(*): it renders identically to
	// CountStar and never the invalid COUNT("*") over a column named "*".
	starSQL, starArgs := renderProjection(c, query.CountStar())
	countSQL, countArgs := renderProjection(c, query.Count("*"))

	c.Assert(countSQL, qt.Equals, `SELECT COUNT(*) FROM "t"`)
	c.Assert(countSQL, qt.Equals, starSQL)
	c.Assert(countArgs, qt.HasLen, 0)
	c.Assert(starArgs, qt.HasLen, 0)
}

func TestAggregateStarArgumentRejected(t *testing.T) {
	c := qt.New(t)

	// Only Count("*") maps to the star form. Every other "*" aggregate argument —
	// a non-COUNT aggregate, COUNT(DISTINCT *), or a qualified star — has no valid
	// SQL form, so it is rejected at render time rather than emitting a quoted "*".
	tests := []struct {
		name string
		expr ast.Expression
	}{
		{name: "sum star", expr: query.Sum("*")},
		{name: "avg star", expr: query.Avg("*")},
		{name: "min star", expr: query.Min("*")},
		{name: "max star", expr: query.Max("*")},
		{name: "count distinct star", expr: query.CountDistinct("*")},
		{name: "qualified count star", expr: query.Col("u", "*").Count()},
		{name: "qualified sum star", expr: query.Col("o", "*").Sum()},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			stmt := query.Select().Exprs(tt.expr).From("t").Build()
			sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.ErrorMatches, `.*is not a valid column reference.*`)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func TestColumnAggregateMethods(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		expr    ast.Expression
		wantSQL string
	}{
		{name: "count", expr: query.Col("u", "id").Count(), wantSQL: `SELECT COUNT("u"."id") FROM "t"`},
		{name: "count distinct", expr: query.Col("u", "id").CountDistinct(), wantSQL: `SELECT COUNT(DISTINCT "u"."id") FROM "t"`},
		{name: "sum", expr: query.Col("o", "total").Sum(), wantSQL: `SELECT SUM("o"."total") FROM "t"`},
		{name: "avg", expr: query.Col("o", "total").Avg(), wantSQL: `SELECT AVG("o"."total") FROM "t"`},
		{name: "min", expr: query.Col("o", "total").Min(), wantSQL: `SELECT MIN("o"."total") FROM "t"`},
		{name: "max", expr: query.Col("o", "total").Max(), wantSQL: `SELECT MAX("o"."total") FROM "t"`},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args := renderProjection(c, tt.expr)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestExprComparison(t *testing.T) {
	c := qt.New(t)

	// Expr wraps an aggregate so it can be compared against a bound value, the
	// shape of a HAVING predicate. The value is always bound, never inlined.
	tests := []struct {
		name    string
		expr    ast.Expression
		wantSQL string
	}{
		{name: "eq", expr: query.Expr(query.CountStar()).Eq(int64(5)), wantSQL: `SELECT * FROM "t" WHERE COUNT(*) = $1`},
		{name: "ne", expr: query.Expr(query.CountStar()).Ne(int64(5)), wantSQL: `SELECT * FROM "t" WHERE COUNT(*) <> $1`},
		{name: "lt", expr: query.Expr(query.Sum("total")).Lt(int64(5)), wantSQL: `SELECT * FROM "t" WHERE SUM("total") < $1`},
		{name: "le", expr: query.Expr(query.Sum("total")).Le(int64(5)), wantSQL: `SELECT * FROM "t" WHERE SUM("total") <= $1`},
		{name: "gt", expr: query.Expr(query.CountStar()).Gt(int64(5)), wantSQL: `SELECT * FROM "t" WHERE COUNT(*) > $1`},
		{name: "ge", expr: query.Expr(query.Avg("total")).Ge(int64(5)), wantSQL: `SELECT * FROM "t" WHERE AVG("total") >= $1`},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args := renderWhere(c, tt.expr)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(5)})
		})
	}
}

func TestSelectBuilder_Distinct(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select("status").Distinct().From("orders").Build()
	c.Assert(stmt.Distinct, qt.IsTrue)

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT DISTINCT "status" FROM "orders"`)
	c.Assert(args, qt.HasLen, 0)
}

func TestSelectBuilder_GroupBy(t *testing.T) {
	c := qt.New(t)

	// GroupBy accepts qualified columns and, via an empty qualifier, bare columns;
	// both accumulate in order across calls.
	stmt := query.Select("*").
		From("orders").
		GroupBy(query.Col("", "status")).
		GroupBy(query.Col("o", "kind")).
		Build()

	c.Assert(stmt.GroupBy, qt.DeepEquals, []ast.ColumnRef{
		{Name: "status"},
		{Qualifier: "o", Name: "kind"},
	})
}

func TestSelectBuilder_ExprsAndExprAs(t *testing.T) {
	c := qt.New(t)

	// Exprs projects expressions without an alias; ExprAs attaches one. Both append
	// after any columns already selected.
	stmt := query.Select().
		Columns(query.Col("", "status")).
		Exprs(query.Sum("total")).
		ExprAs(query.CountStar(), "n").
		From("orders").
		Build()

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "status", SUM("total"), COUNT(*) AS "n" FROM "orders"`)
	c.Assert(args, qt.HasLen, 0)
}

func TestSelectBuilder_GroupByHavingEndToEnd(t *testing.T) {
	c := qt.New(t)

	// A grouped aggregate query with a WHERE filter, a HAVING over COUNT(*), and a
	// LIMIT, proving the fluent API composes and placeholders order across clauses.
	stmt := query.Select("status").
		ExprAs(query.CountStar(), "n").
		From("orders").
		Where(query.Eq("tenant_id", "acme")).
		GroupBy(query.Col("", "status")).
		Having(query.Expr(query.CountStar()).Gt(int64(5))).
		OrderBy(query.Asc("status")).
		Limit(10).
		Build()

	wantArgs := []any{"acme", int64(5), int64(10)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres",
			dialect: platform.Postgres,
			wantSQL: `SELECT "status", COUNT(*) AS "n" FROM "orders" WHERE "tenant_id" = $1 GROUP BY "status" HAVING COUNT(*) > $2 ORDER BY "status" ASC LIMIT $3`,
		},
		{
			name:    "mysql",
			dialect: platform.MySQL,
			wantSQL: "SELECT `status`, COUNT(*) AS `n` FROM `orders` WHERE `tenant_id` = ? GROUP BY `status` HAVING COUNT(*) > ? ORDER BY `status` ASC LIMIT ?",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestSelectBuilder_HavingComposesWithBooleans(t *testing.T) {
	c := qt.New(t)

	// A HAVING can combine several aggregate predicates with And, since each
	// comparison is an ordinary expression.
	stmt := query.Select().
		Columns(query.Col("u", "id")).
		ExprAs(query.Col("o", "id").Count(), "orders").
		FromAs("users", "u").
		InnerJoin("orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
		GroupBy(query.Col("u", "id")).
		Having(query.And(
			query.Expr(query.Col("o", "id").Count()).Gt(int64(1)),
			query.Expr(query.Col("o", "total").Sum()).Ge(int64(100)),
		)).
		Build()

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "u"."id", COUNT("o"."id") AS "orders" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" GROUP BY "u"."id" HAVING (COUNT("o"."id") > $1 AND SUM("o"."total") >= $2)`)
	c.Assert(args, qt.DeepEquals, []any{int64(1), int64(100)})
}
