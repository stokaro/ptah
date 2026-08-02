package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

func TestColumnExpressionConstructors(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		expr     ast.Expression
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "eq binds the value and qualifies the column",
			expr:     query.Col("u", "id").Eq(1),
			wantSQL:  `SELECT * FROM "t" WHERE "u"."id" = $1`,
			wantArgs: []any{1},
		},
		{
			name:     "ne",
			expr:     query.Col("u", "id").Ne(1),
			wantSQL:  `SELECT * FROM "t" WHERE "u"."id" <> $1`,
			wantArgs: []any{1},
		},
		{
			name:     "lt",
			expr:     query.Col("o", "total").Lt(int64(10)),
			wantSQL:  `SELECT * FROM "t" WHERE "o"."total" < $1`,
			wantArgs: []any{int64(10)},
		},
		{
			name:     "le",
			expr:     query.Col("o", "total").Le(int64(10)),
			wantSQL:  `SELECT * FROM "t" WHERE "o"."total" <= $1`,
			wantArgs: []any{int64(10)},
		},
		{
			name:     "gt",
			expr:     query.Col("o", "total").Gt(int64(10)),
			wantSQL:  `SELECT * FROM "t" WHERE "o"."total" > $1`,
			wantArgs: []any{int64(10)},
		},
		{
			name:     "ge",
			expr:     query.Col("o", "total").Ge(int64(10)),
			wantSQL:  `SELECT * FROM "t" WHERE "o"."total" >= $1`,
			wantArgs: []any{int64(10)},
		},
		{
			name:     "eqcol compares two columns without binding a value",
			expr:     query.Col("o", "user_id").EqCol(query.Col("u", "id")),
			wantSQL:  `SELECT * FROM "t" WHERE "o"."user_id" = "u"."id"`,
			wantArgs: nil,
		},
		{
			name:     "is null",
			expr:     query.Col("u", "deleted_at").IsNull(),
			wantSQL:  `SELECT * FROM "t" WHERE "u"."deleted_at" IS NULL`,
			wantArgs: nil,
		},
		{
			name:     "is not null",
			expr:     query.Col("u", "deleted_at").IsNotNull(),
			wantSQL:  `SELECT * FROM "t" WHERE "u"."deleted_at" IS NOT NULL`,
			wantArgs: nil,
		},
		{
			name:     "empty qualifier renders a bare column",
			expr:     query.Col("", "id").Eq(1),
			wantSQL:  `SELECT * FROM "t" WHERE "id" = $1`,
			wantArgs: []any{1},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args := renderWhere(c, tt.expr)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, tt.wantArgs)
		})
	}
}

func TestColumnOrderByTerms(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select("*").
		From("t").
		OrderBy(query.Col("u", "name").Asc(), query.Col("o", "total").Desc()).
		Build()

	c.Assert(stmt.OrderBy, qt.DeepEquals, []ast.OrderByClause{
		{Qualifier: "u", Column: "name", Direction: ast.SortAscending},
		{Qualifier: "o", Column: "total", Direction: ast.SortDescending},
	})
}

func TestSelectBuilder_FromAs(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select("*").FromAs("users", "u").Build()
	c.Assert(stmt.From, qt.Equals, "users")
	c.Assert(stmt.FromAlias, qt.Equals, "u")

	sql, _, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT * FROM "users" "u"`)
}

func TestSelectBuilder_FromClearsPreviousAlias(t *testing.T) {
	c := qt.New(t)

	// From after FromAs drops the alias, so a later From wins cleanly.
	stmt := query.Select("*").FromAs("users", "u").From("people").Build()
	c.Assert(stmt.From, qt.Equals, "people")
	c.Assert(stmt.FromAlias, qt.Equals, "")
}

func TestSelectBuilder_Columns(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		stmt *ast.SelectStatement
		want []ast.ResultColumn
	}{
		{
			name: "qualified columns replace the implicit star",
			stmt: query.Select().Columns(query.Col("u", "id"), query.Col("o", "total")).From("t").Build(),
			want: []ast.ResultColumn{{Qualifier: "u", Name: "id"}, {Qualifier: "o", Name: "total"}},
		},
		{
			name: "qualified columns append after bare columns",
			stmt: query.Select("id").Columns(query.Col("o", "total")).From("t").Build(),
			want: []ast.ResultColumn{{Name: "id"}, {Qualifier: "o", Name: "total"}},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(tt.stmt.Columns, qt.DeepEquals, tt.want)
		})
	}
}

func TestSelectBuilder_QualifiedStarProjection(t *testing.T) {
	c := qt.New(t)

	// Col(alias, "*") projects a qualified star, rendering "u".* rather than the
	// invalid "u"."*".
	stmt := query.Select().
		Columns(query.Col("u", "*"), query.Col("o", "total")).
		FromAs("users", "u").
		InnerJoin("orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
		Build()

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "u".*, "o"."total" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id"`)
	c.Assert(args, qt.HasLen, 0)
}

func TestSelectBuilder_JoinMethodsSetType(t *testing.T) {
	c := qt.New(t)

	on := query.Col("b", "a_id").EqCol(query.Col("a", "id"))

	tests := []struct {
		name     string
		build    func() *ast.SelectStatement
		wantType ast.JoinType
	}{
		{
			name:     "inner",
			build:    func() *ast.SelectStatement { return query.Select("*").From("a").InnerJoin("b", "x", on).Build() },
			wantType: ast.JoinInner,
		},
		{
			name:     "left",
			build:    func() *ast.SelectStatement { return query.Select("*").From("a").LeftJoin("b", "x", on).Build() },
			wantType: ast.JoinLeft,
		},
		{
			name:     "right",
			build:    func() *ast.SelectStatement { return query.Select("*").From("a").RightJoin("b", "x", on).Build() },
			wantType: ast.JoinRight,
		},
		{
			name:     "full",
			build:    func() *ast.SelectStatement { return query.Select("*").From("a").FullJoin("b", "x", on).Build() },
			wantType: ast.JoinFull,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			stmt := tt.build()
			c.Assert(stmt.Joins, qt.HasLen, 1)
			c.Assert(stmt.Joins[0].Type, qt.Equals, tt.wantType)
			c.Assert(stmt.Joins[0].Table, qt.Equals, "b")
			c.Assert(stmt.Joins[0].Alias, qt.Equals, "x")
			c.Assert(stmt.Joins[0].On, qt.Equals, on)
		})
	}
}

func TestSelectBuilder_JoinsAccumulateInOrder(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select("*").
		FromAs("users", "u").
		InnerJoin("orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
		LeftJoin("payments", "p", query.Col("p", "order_id").EqCol(query.Col("o", "id"))).
		Build()

	c.Assert(stmt.Joins, qt.HasLen, 2)
	c.Assert(stmt.Joins[0].Type, qt.Equals, ast.JoinInner)
	c.Assert(stmt.Joins[0].Table, qt.Equals, "orders")
	c.Assert(stmt.Joins[1].Type, qt.Equals, ast.JoinLeft)
	c.Assert(stmt.Joins[1].Table, qt.Equals, "payments")
}

func TestSelectBuilder_JoinFluentEndToEnd(t *testing.T) {
	c := qt.New(t)

	stmt := query.Select().
		Columns(query.Col("u", "id"), query.Col("u", "name"), query.Col("o", "total")).
		FromAs("users", "u").
		InnerJoin("orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
		Where(query.And(
			query.Col("o", "status").Eq("paid"),
			query.Col("u", "active").Eq(true),
		)).
		OrderBy(query.Col("u", "name").Asc(), query.Col("o", "total").Desc()).
		Limit(5).
		Build()

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "u"."id", "u"."name", "o"."total" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" WHERE ("o"."status" = $1 AND "u"."active" = $2) ORDER BY "u"."name" ASC, "o"."total" DESC LIMIT $3`)
	c.Assert(args, qt.DeepEquals, []any{"paid", true, int64(5)})
}
