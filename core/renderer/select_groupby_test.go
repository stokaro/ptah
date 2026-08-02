package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

func TestRenderSelect_Distinct(t *testing.T) {
	c := qt.New(t)

	stmt := &ast.SelectStatement{
		Distinct: true,
		Columns:  []ast.ResultColumn{{Name: "status"}},
		From:     "orders",
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{name: "postgres", dialect: platform.Postgres, wantSQL: `SELECT DISTINCT "status" FROM "orders"`},
		{name: "mysql", dialect: platform.MySQL, wantSQL: "SELECT DISTINCT `status` FROM `orders`"},
		{name: "sqlite", dialect: platform.SQLite, wantSQL: `SELECT DISTINCT "status" FROM "orders"`},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_GroupBy(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		stmt    *ast.SelectStatement
		wantSQL string
	}{
		{
			name: "bare columns",
			stmt: &ast.SelectStatement{
				Columns: []ast.ResultColumn{{Name: "status"}},
				From:    "orders",
				GroupBy: []ast.ColumnRef{{Name: "status"}, {Name: "kind"}},
			},
			wantSQL: `SELECT "status" FROM "orders" GROUP BY "status", "kind"`,
		},
		{
			name: "qualified columns in a join",
			stmt: &ast.SelectStatement{
				Columns:   []ast.ResultColumn{{Qualifier: "u", Name: "id"}},
				From:      "users",
				FromAlias: "u",
				Joins: []ast.JoinClause{
					{Type: ast.JoinInner, Table: "orders", Alias: "o", On: eqCols("o", "user_id", "u", "id")},
				},
				GroupBy: []ast.ColumnRef{{Qualifier: "u", Name: "id"}, {Qualifier: "o", Name: "status"}},
			},
			wantSQL: `SELECT "u"."id" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" GROUP BY "u"."id", "o"."status"`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(tt.stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

// groupedCountQuery groups by a column, projects COUNT(*), filters rows before
// grouping (WHERE) and groups after (HAVING), so the placeholder ordering across
// WHERE, HAVING, and LIMIT can be asserted in one statement.
func groupedCountQuery() *ast.SelectStatement {
	limit := int64(10)
	return &ast.SelectStatement{
		Columns: []ast.ResultColumn{
			{Name: "status"},
			{Expr: &ast.FuncCall{Name: "COUNT", Star: true}, Alias: "n"},
		},
		From:  "orders",
		Where: &ast.Comparison{Left: &ast.ColumnRef{Name: "tenant_id"}, Operator: ast.OpEqual, Right: &ast.BoundValue{Value: "acme"}},
		GroupBy: []ast.ColumnRef{
			{Name: "status"},
		},
		Having: &ast.Comparison{
			Left:     &ast.FuncCall{Name: "COUNT", Star: true},
			Operator: ast.OpGreaterThan,
			Right:    &ast.BoundValue{Value: int64(5)},
		},
		OrderBy: []ast.OrderByClause{{Column: "status", Direction: ast.SortAscending}},
		Limit:   &limit,
	}
}

func TestRenderSelect_GroupByHavingPlaceholderOrdering(t *testing.T) {
	c := qt.New(t)

	// A bound WHERE value takes the first placeholder, the HAVING value the second,
	// and the LIMIT bound the third, proving HAVING binds after WHERE and before
	// LIMIT and that GROUP BY (a COUNT(*) with no arguments) binds nothing.
	wantArgs := []any{"acme", int64(5), int64(10)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres numbers where then having then limit",
			dialect: platform.Postgres,
			wantSQL: `SELECT "status", COUNT(*) AS "n" FROM "orders" WHERE "tenant_id" = $1 GROUP BY "status" HAVING COUNT(*) > $2 ORDER BY "status" ASC LIMIT $3`,
		},
		{
			name:    "mysql keeps the order with question placeholders",
			dialect: platform.MySQL,
			wantSQL: "SELECT `status`, COUNT(*) AS `n` FROM `orders` WHERE `tenant_id` = ? GROUP BY `status` HAVING COUNT(*) > ? ORDER BY `status` ASC LIMIT ?",
		},
		{
			name:    "sqlite keeps the order with question placeholders",
			dialect: platform.SQLite,
			wantSQL: `SELECT "status", COUNT(*) AS "n" FROM "orders" WHERE "tenant_id" = ? GROUP BY "status" HAVING COUNT(*) > ? ORDER BY "status" ASC LIMIT ?`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(groupedCountQuery(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderSelect_AggregateProjection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		expr    ast.Expression
		alias   string
		wantSQL string
	}{
		{
			name:    "count star",
			expr:    &ast.FuncCall{Name: "COUNT", Star: true},
			wantSQL: `SELECT COUNT(*) FROM "t"`,
		},
		{
			name:    "count column",
			expr:    &ast.FuncCall{Name: "COUNT", Args: []ast.Expression{&ast.ColumnRef{Name: "id"}}},
			wantSQL: `SELECT COUNT("id") FROM "t"`,
		},
		{
			name:    "count distinct column",
			expr:    &ast.FuncCall{Name: "COUNT", Args: []ast.Expression{&ast.ColumnRef{Name: "status"}}, Distinct: true},
			wantSQL: `SELECT COUNT(DISTINCT "status") FROM "t"`,
		},
		{
			name:    "sum",
			expr:    &ast.FuncCall{Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT SUM("total") FROM "t"`,
		},
		{
			name:    "avg",
			expr:    &ast.FuncCall{Name: "AVG", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT AVG("total") FROM "t"`,
		},
		{
			name:    "min",
			expr:    &ast.FuncCall{Name: "MIN", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT MIN("total") FROM "t"`,
		},
		{
			name:    "max",
			expr:    &ast.FuncCall{Name: "MAX", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT MAX("total") FROM "t"`,
		},
		{
			name:    "aliased aggregate",
			expr:    &ast.FuncCall{Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}},
			alias:   "grand_total",
			wantSQL: `SELECT SUM("total") AS "grand_total" FROM "t"`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			col := ast.ResultColumn{Expr: tt.expr, Alias: tt.alias}
			stmt := &ast.SelectStatement{Columns: []ast.ResultColumn{col}, From: "t"}
			sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_AggregateOverQualifiedColumnInJoin(t *testing.T) {
	c := qt.New(t)

	// COUNT("u"."id") and SUM("o"."total") each quote both qualifier parts, mix
	// with a grouped column, and render in a join query across dialects.
	stmt := &ast.SelectStatement{
		Columns: []ast.ResultColumn{
			{Qualifier: "u", Name: "name"},
			{Expr: &ast.FuncCall{Name: "COUNT", Args: []ast.Expression{&ast.ColumnRef{Qualifier: "o", Name: "id"}}}, Alias: "orders"},
			{Expr: &ast.FuncCall{Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Qualifier: "o", Name: "total"}}}, Alias: "spent"},
		},
		From:      "users",
		FromAlias: "u",
		Joins: []ast.JoinClause{
			{Type: ast.JoinInner, Table: "orders", Alias: "o", On: eqCols("o", "user_id", "u", "id")},
		},
		GroupBy: []ast.ColumnRef{{Qualifier: "u", Name: "name"}},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres",
			dialect: platform.Postgres,
			wantSQL: `SELECT "u"."name", COUNT("o"."id") AS "orders", SUM("o"."total") AS "spent" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" GROUP BY "u"."name"`,
		},
		{
			name:    "mysql",
			dialect: platform.MySQL,
			wantSQL: "SELECT `u`.`name`, COUNT(`o`.`id`) AS `orders`, SUM(`o`.`total`) AS `spent` FROM `users` `u` INNER JOIN `orders` `o` ON `o`.`user_id` = `u`.`id` GROUP BY `u`.`name`",
		},
		{
			name:    "sqlite",
			dialect: platform.SQLite,
			wantSQL: `SELECT "u"."name", COUNT("o"."id") AS "orders", SUM("o"."total") AS "spent" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" GROUP BY "u"."name"`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_AggregateInHavingBindsValue(t *testing.T) {
	c := qt.New(t)

	// SUM("total") in the projection carries no value, but comparing it against a
	// bound value in HAVING binds exactly one placeholder.
	stmt := &ast.SelectStatement{
		Columns: []ast.ResultColumn{{Name: "status"}, {Expr: &ast.FuncCall{Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}}}},
		From:    "orders",
		GroupBy: []ast.ColumnRef{{Name: "status"}},
		Having: &ast.Comparison{
			Left:     &ast.FuncCall{Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}},
			Operator: ast.OpGreaterThanOrEqual,
			Right:    &ast.BoundValue{Value: int64(1000)},
		},
	}

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "status", SUM("total") FROM "orders" GROUP BY "status" HAVING SUM("total") >= $1`)
	c.Assert(args, qt.DeepEquals, []any{int64(1000)})
}

func TestRenderSelect_FunctionNameNeverQuotedButValidated(t *testing.T) {
	c := qt.New(t)

	// The function name is a keyword emitted verbatim, so a well-formed name is not
	// quoted even though its column argument is.
	c.Run("name is not quoted", func(c *qt.C) {
		stmt := &ast.SelectStatement{
			Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "COUNT", Args: []ast.Expression{&ast.ColumnRef{Name: "id"}}}}},
			From:    "t",
		}
		sql, _, err := renderer.RenderSelect(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `SELECT COUNT("id") FROM "t"`)
	})

	// An unsafe function name cannot be smuggled through as SQL: because the name
	// is not quoted, the renderer rejects anything that is not a simple identifier.
	c.Run("injection name is rejected", func(c *qt.C) {
		stmt := &ast.SelectStatement{
			Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "COUNT(*) FROM secrets; --", Star: true}}},
			From:    "t",
		}
		sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, `renderer: function name "COUNT\(\*\) FROM secrets; --" is not a valid identifier`)
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})
}

func TestRenderSelect_GroupByHavingErrors(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		stmt        *ast.SelectStatement
		wantErrLike string
	}{
		{
			name:        "empty group by column",
			stmt:        &ast.SelectStatement{From: "t", GroupBy: []ast.ColumnRef{{Name: "  "}}},
			wantErrLike: "renderer: GROUP BY term has an empty column",
		},
		{
			name:        "function with empty name",
			stmt:        &ast.SelectStatement{Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "  ", Star: true}}}, From: "t"},
			wantErrLike: "renderer: function call has an empty name",
		},
		{
			name:        "function without arguments or star",
			stmt:        &ast.SelectStatement{Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "SUM"}}}, From: "t"},
			wantErrLike: "renderer: function SUM requires at least one argument",
		},
		{
			name:        "star with arguments",
			stmt:        &ast.SelectStatement{Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "COUNT", Star: true, Args: []ast.Expression{&ast.ColumnRef{Name: "id"}}}}}, From: "t"},
			wantErrLike: "renderer: function COUNT with a star takes no arguments",
		},
		{
			name:        "star with distinct",
			stmt:        &ast.SelectStatement{Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "COUNT", Star: true, Distinct: true}}}, From: "t"},
			wantErrLike: "renderer: function COUNT cannot combine DISTINCT with a star",
		},
		{
			name:        "typed-nil function call does not panic",
			stmt:        &ast.SelectStatement{From: "t", Having: (*ast.FuncCall)(nil)},
			wantErrLike: "renderer: nil function call",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(tt.stmt, platform.Postgres)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func TestRenderSelect_Phase3ZeroValuesAreBackwardCompatible(t *testing.T) {
	c := qt.New(t)

	// A statement that sets none of the Phase 3 fields (Distinct, GroupBy, Having,
	// ResultColumn.Expr/Alias) renders byte-identically to the Phase 1/2 output.
	limit := int64(24)
	offset := int64(0)
	stmt := &ast.SelectStatement{
		Columns: []ast.ResultColumn{{Name: "id"}, {Name: "name"}},
		From:    "commodities",
		Where: &ast.Comparison{
			Left:     &ast.ColumnRef{Name: "draft"},
			Operator: ast.OpEqual,
			Right:    &ast.BoundValue{Value: false},
		},
		OrderBy: []ast.OrderByClause{{Column: "name", Direction: ast.SortAscending}},
		Limit:   &limit,
		Offset:  &offset,
	}

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id", "name" FROM "commodities" WHERE "draft" = $1 ORDER BY "name" ASC LIMIT $2 OFFSET $3`)
	c.Assert(args, qt.DeepEquals, []any{false, int64(24), int64(0)})
}

// onWhereHavingLimitOffset carries a bound value in the JOIN ON, the WHERE, the
// HAVING, and both the LIMIT and OFFSET, so a single statement pins the full
// placeholder order across every binding clause.
func onWhereHavingLimitOffset() *ast.SelectStatement {
	limit := int64(10)
	offset := int64(20)
	return &ast.SelectStatement{
		Columns: []ast.ResultColumn{
			{Qualifier: "o", Name: "status"},
			{Expr: &ast.FuncCall{Name: "COUNT", Star: true}, Alias: "n"},
		},
		From:      "orders",
		FromAlias: "o",
		Joins: []ast.JoinClause{
			{
				Type:  ast.JoinInner,
				Table: "users",
				Alias: "u",
				On: &ast.LogicalExpr{
					Operator: ast.LogicalAnd,
					Operands: []ast.Expression{
						eqCols("u", "id", "o", "user_id"),
						&ast.Comparison{Left: &ast.ColumnRef{Qualifier: "u", Name: "status"}, Operator: ast.OpEqual, Right: &ast.BoundValue{Value: "active"}},
					},
				},
			},
		},
		Where:   &ast.Comparison{Left: &ast.ColumnRef{Qualifier: "o", Name: "total"}, Operator: ast.OpGreaterThan, Right: &ast.BoundValue{Value: int64(100)}},
		GroupBy: []ast.ColumnRef{{Qualifier: "o", Name: "status"}},
		Having: &ast.Comparison{
			Left:     &ast.FuncCall{Name: "COUNT", Star: true},
			Operator: ast.OpGreaterThan,
			Right:    &ast.BoundValue{Value: int64(5)},
		},
		Limit:  &limit,
		Offset: &offset,
	}
}

func TestRenderSelect_OnWhereHavingLimitOffsetPlaceholderOrdering(t *testing.T) {
	c := qt.New(t)

	// The bound values are numbered strictly by render order: JOIN ON first, then
	// WHERE, then HAVING, then LIMIT, then OFFSET — regardless of placeholder style.
	wantArgs := []any{"active", int64(100), int64(5), int64(10), int64(20)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres dollar placeholders",
			dialect: platform.Postgres,
			wantSQL: `SELECT "o"."status", COUNT(*) AS "n" FROM "orders" "o" INNER JOIN "users" "u" ON ("u"."id" = "o"."user_id" AND "u"."status" = $1) WHERE "o"."total" > $2 GROUP BY "o"."status" HAVING COUNT(*) > $3 LIMIT $4 OFFSET $5`,
		},
		{
			name:    "mysql question placeholders",
			dialect: platform.MySQL,
			wantSQL: "SELECT `o`.`status`, COUNT(*) AS `n` FROM `orders` `o` INNER JOIN `users` `u` ON (`u`.`id` = `o`.`user_id` AND `u`.`status` = ?) WHERE `o`.`total` > ? GROUP BY `o`.`status` HAVING COUNT(*) > ? LIMIT ? OFFSET ?",
		},
		{
			name:    "sqlite question placeholders",
			dialect: platform.SQLite,
			wantSQL: `SELECT "o"."status", COUNT(*) AS "n" FROM "orders" "o" INNER JOIN "users" "u" ON ("u"."id" = "o"."user_id" AND "u"."status" = ?) WHERE "o"."total" > ? GROUP BY "o"."status" HAVING COUNT(*) > ? LIMIT ? OFFSET ?`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(onWhereHavingLimitOffset(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderSelect_HavingWithoutGroupBy(t *testing.T) {
	c := qt.New(t)

	// A HAVING is valid without GROUP BY: the aggregate spans the whole table. No
	// GROUP BY clause is emitted, and the HAVING value takes the first placeholder.
	stmt := &ast.SelectStatement{
		Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "COUNT", Star: true}, Alias: "n"}},
		From:    "orders",
		Having: &ast.Comparison{
			Left:     &ast.FuncCall{Name: "COUNT", Star: true},
			Operator: ast.OpGreaterThan,
			Right:    &ast.BoundValue{Value: int64(5)},
		},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{name: "postgres", dialect: platform.Postgres, wantSQL: `SELECT COUNT(*) AS "n" FROM "orders" HAVING COUNT(*) > $1`},
		{name: "mysql", dialect: platform.MySQL, wantSQL: "SELECT COUNT(*) AS `n` FROM `orders` HAVING COUNT(*) > ?"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			sql, args, err := renderer.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(5)})
		})
	}
}

func TestRenderSelect_HavingWithOffsetOnly(t *testing.T) {
	c := qt.New(t)

	// A HAVING value binds before the OFFSET value even when there is no LIMIT: the
	// HAVING takes the first placeholder and the OFFSET the second. On MySQL,
	// MariaDB, and SQLite the offset-only "no limit" sentinel is a literal between
	// HAVING and OFFSET, so it consumes no placeholder and does not shift the order.
	offset := int64(5)
	stmt := &ast.SelectStatement{
		Columns: []ast.ResultColumn{{Expr: &ast.FuncCall{Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}}, Alias: "s"}},
		From:    "orders",
		GroupBy: []ast.ColumnRef{{Name: "status"}},
		Having: &ast.Comparison{
			Left:     &ast.FuncCall{Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "total"}}},
			Operator: ast.OpGreaterThan,
			Right:    &ast.BoundValue{Value: int64(1000)},
		},
		Offset: &offset,
	}

	wantArgs := []any{int64(1000), int64(5)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres emits a bare offset",
			dialect: platform.Postgres,
			wantSQL: `SELECT SUM("total") AS "s" FROM "orders" GROUP BY "status" HAVING SUM("total") > $1 OFFSET $2`,
		},
		{
			name:    "mysql synthesizes a max-bigint limit before offset",
			dialect: platform.MySQL,
			wantSQL: "SELECT SUM(`total`) AS `s` FROM `orders` GROUP BY `status` HAVING SUM(`total`) > ? LIMIT 18446744073709551615 OFFSET ?",
		},
		{
			name:    "sqlite synthesizes a negative-one limit before offset",
			dialect: platform.SQLite,
			wantSQL: `SELECT SUM("total") AS "s" FROM "orders" GROUP BY "status" HAVING SUM("total") > ? LIMIT -1 OFFSET ?`,
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
