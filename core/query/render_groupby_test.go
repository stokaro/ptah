package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
)

func TestRenderSelect_Distinct(t *testing.T) {
	stmt := &query.SelectStatement{
		Distinct: true,
		Columns:  []query.ResultColumn{{Name: "status"}},
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_GroupBy(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *query.SelectStatement
		wantSQL string
	}{
		{
			name: "bare columns",
			stmt: &query.SelectStatement{
				Columns: []query.ResultColumn{{Name: "status"}},
				From:    "orders",
				GroupBy: []query.ColumnRef{{Name: "status"}, {Name: "kind"}},
			},
			wantSQL: `SELECT "status" FROM "orders" GROUP BY "status", "kind"`,
		},
		{
			name: "qualified columns in a join",
			stmt: &query.SelectStatement{
				Columns:   []query.ResultColumn{{Qualifier: "u", Name: "id"}},
				From:      "users",
				FromAlias: "u",
				Joins: []query.JoinClause{
					{Type: query.JoinInner, Table: "orders", Alias: "o", On: eqCols("o", "user_id", "u", "id")},
				},
				GroupBy: []query.ColumnRef{{Qualifier: "u", Name: "id"}, {Qualifier: "o", Name: "status"}},
			},
			wantSQL: `SELECT "u"."id" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" GROUP BY "u"."id", "o"."status"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(tt.stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

// groupedCountQuery groups by a column, projects COUNT(*), filters rows before
// grouping (WHERE) and groups after (HAVING), so the placeholder ordering across
// WHERE, HAVING, and LIMIT can be asserted in one statement.
func groupedCountQuery() *query.SelectStatement {
	limit := int64(10)
	return &query.SelectStatement{
		Columns: []query.ResultColumn{
			{Name: "status"},
			{Expr: &query.FuncCall{Name: "COUNT", Star: true}, Alias: "n"},
		},
		From:  "orders",
		Where: &query.Comparison{Left: &query.ColumnRef{Name: "tenant_id"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: "acme"}},
		GroupBy: []query.ColumnRef{
			{Name: "status"},
		},
		Having: &query.Comparison{
			Left:     &query.FuncCall{Name: "COUNT", Star: true},
			Operator: query.OpGreaterThan,
			Right:    &query.BoundValue{Value: int64(5)},
		},
		OrderBy: []query.OrderByClause{{Column: "status", Direction: query.SortAscending}},
		Limit:   &limit,
	}
}

func TestRenderSelect_GroupByHavingPlaceholderOrdering(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(groupedCountQuery(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderSelect_AggregateProjection(t *testing.T) {
	tests := []struct {
		name    string
		expr    query.Expression
		alias   string
		wantSQL string
	}{
		{
			name:    "count star",
			expr:    &query.FuncCall{Name: "COUNT", Star: true},
			wantSQL: `SELECT COUNT(*) FROM "t"`,
		},
		{
			name:    "count column",
			expr:    &query.FuncCall{Name: "COUNT", Args: []query.Expression{&query.ColumnRef{Name: "id"}}},
			wantSQL: `SELECT COUNT("id") FROM "t"`,
		},
		{
			name:    "count distinct column",
			expr:    &query.FuncCall{Name: "COUNT", Args: []query.Expression{&query.ColumnRef{Name: "status"}}, Distinct: true},
			wantSQL: `SELECT COUNT(DISTINCT "status") FROM "t"`,
		},
		{
			name:    "sum",
			expr:    &query.FuncCall{Name: "SUM", Args: []query.Expression{&query.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT SUM("total") FROM "t"`,
		},
		{
			name:    "avg",
			expr:    &query.FuncCall{Name: "AVG", Args: []query.Expression{&query.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT AVG("total") FROM "t"`,
		},
		{
			name:    "min",
			expr:    &query.FuncCall{Name: "MIN", Args: []query.Expression{&query.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT MIN("total") FROM "t"`,
		},
		{
			name:    "max",
			expr:    &query.FuncCall{Name: "MAX", Args: []query.Expression{&query.ColumnRef{Name: "total"}}},
			wantSQL: `SELECT MAX("total") FROM "t"`,
		},
		{
			name:    "aliased aggregate",
			expr:    &query.FuncCall{Name: "SUM", Args: []query.Expression{&query.ColumnRef{Name: "total"}}},
			alias:   "grand_total",
			wantSQL: `SELECT SUM("total") AS "grand_total" FROM "t"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			col := query.ResultColumn{Expr: tt.expr, Alias: tt.alias}
			stmt := &query.SelectStatement{Columns: []query.ResultColumn{col}, From: "t"}
			sql, args, err := query.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_AggregateOverQualifiedColumnInJoin(t *testing.T) {
	// COUNT("u"."id") and SUM("o"."total") each quote both qualifier parts, mix
	// with a grouped column, and render in a join query across dialects.
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{
			{Qualifier: "u", Name: "name"},
			{Expr: &query.FuncCall{Name: "COUNT", Args: []query.Expression{&query.ColumnRef{Qualifier: "o", Name: "id"}}}, Alias: "orders"},
			{Expr: &query.FuncCall{Name: "SUM", Args: []query.Expression{&query.ColumnRef{Qualifier: "o", Name: "total"}}}, Alias: "spent"},
		},
		From:      "users",
		FromAlias: "u",
		Joins: []query.JoinClause{
			{Type: query.JoinInner, Table: "orders", Alias: "o", On: eqCols("o", "user_id", "u", "id")},
		},
		GroupBy: []query.ColumnRef{{Qualifier: "u", Name: "name"}},
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
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
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Name: "status"}, {Expr: &query.FuncCall{Name: "SUM", Args: []query.Expression{&query.ColumnRef{Name: "total"}}}}},
		From:    "orders",
		GroupBy: []query.ColumnRef{{Name: "status"}},
		Having: &query.Comparison{
			Left:     &query.FuncCall{Name: "SUM", Args: []query.Expression{&query.ColumnRef{Name: "total"}}},
			Operator: query.OpGreaterThanOrEqual,
			Right:    &query.BoundValue{Value: int64(1000)},
		},
	}

	sql, args, err := query.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "status", SUM("total") FROM "orders" GROUP BY "status" HAVING SUM("total") >= $1`)
	c.Assert(args, qt.DeepEquals, []any{int64(1000)})
}

func TestRenderSelect_FunctionNameNeverQuotedButValidated(t *testing.T) {
	// The function name is a keyword emitted verbatim, so a well-formed name is not
	// quoted even though its column argument is.
	t.Run("name is not quoted", func(t *testing.T) {
		c := qt.New(t)
		stmt := &query.SelectStatement{
			Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "COUNT", Args: []query.Expression{&query.ColumnRef{Name: "id"}}}}},
			From:    "t",
		}
		sql, _, err := query.RenderSelect(stmt, platform.Postgres)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Equals, `SELECT COUNT("id") FROM "t"`)
	})

	// An unsafe function name cannot be smuggled through as SQL: because the name
	// is not quoted, the renderer rejects anything that is not a simple identifier.
	t.Run("injection name is rejected", func(t *testing.T) {
		c := qt.New(t)
		stmt := &query.SelectStatement{
			Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "COUNT(*) FROM secrets; --", Star: true}}},
			From:    "t",
		}
		sql, args, err := query.RenderSelect(stmt, platform.Postgres)
		c.Assert(err, qt.ErrorMatches, `renderer: function name "COUNT\(\*\) FROM secrets; --" is not a valid identifier`)
		c.Assert(sql, qt.Equals, "")
		c.Assert(args, qt.IsNil)
	})
}

func TestRenderSelect_GroupByHavingErrors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *query.SelectStatement
		wantErrLike string
	}{
		{
			name:        "empty group by column",
			stmt:        &query.SelectStatement{From: "t", GroupBy: []query.ColumnRef{{Name: "  "}}},
			wantErrLike: "renderer: GROUP BY term has an empty column",
		},
		{
			name:        "function with empty name",
			stmt:        &query.SelectStatement{Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "  ", Star: true}}}, From: "t"},
			wantErrLike: "renderer: function call has an empty name",
		},
		{
			// The message names the window alternative now: a zero-argument
			// call is meaningless without one and is the whole shape of a
			// ranking function with one (stokaro/ptah#941).
			name:        "function without arguments, star or window",
			stmt:        &query.SelectStatement{Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "SUM"}}}, From: "t"},
			wantErrLike: "renderer: function SUM requires at least one argument, or a window that supplies its input",
		},
		{
			name:        "star with arguments",
			stmt:        &query.SelectStatement{Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "COUNT", Star: true, Args: []query.Expression{&query.ColumnRef{Name: "id"}}}}}, From: "t"},
			wantErrLike: "renderer: function COUNT with a star takes no arguments",
		},
		{
			name:        "star with distinct",
			stmt:        &query.SelectStatement{Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "COUNT", Star: true, Distinct: true}}}, From: "t"},
			wantErrLike: "renderer: function COUNT cannot combine DISTINCT with a star",
		},
		{
			name:        "typed-nil function call does not panic",
			stmt:        &query.SelectStatement{From: "t", Having: (*query.FuncCall)(nil)},
			wantErrLike: "renderer: nil function call",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(tt.stmt, platform.Postgres)
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
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Name: "id"}, {Name: "name"}},
		From:    "commodities",
		Where: &query.Comparison{
			Left:     &query.ColumnRef{Name: "draft"},
			Operator: query.OpEqual,
			Right:    &query.BoundValue{Value: false},
		},
		OrderBy: []query.OrderByClause{{Column: "name", Direction: query.SortAscending}},
		Limit:   &limit,
		Offset:  &offset,
	}

	sql, args, err := query.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id", "name" FROM "commodities" WHERE "draft" = $1 ORDER BY "name" ASC LIMIT $2 OFFSET $3`)
	c.Assert(args, qt.DeepEquals, []any{false, int64(24), int64(0)})
}

// onWhereHavingLimitOffset carries a bound value in the JOIN ON, the WHERE, the
// HAVING, and both the LIMIT and OFFSET, so a single statement pins the full
// placeholder order across every binding clause.
func onWhereHavingLimitOffset() *query.SelectStatement {
	limit := int64(10)
	offset := int64(20)
	return &query.SelectStatement{
		Columns: []query.ResultColumn{
			{Qualifier: "o", Name: "status"},
			{Expr: &query.FuncCall{Name: "COUNT", Star: true}, Alias: "n"},
		},
		From:      "orders",
		FromAlias: "o",
		Joins: []query.JoinClause{
			{
				Type:  query.JoinInner,
				Table: "users",
				Alias: "u",
				On: &query.LogicalExpr{
					Operator: query.LogicalAnd,
					Operands: []query.Expression{
						eqCols("u", "id", "o", "user_id"),
						&query.Comparison{Left: &query.ColumnRef{Qualifier: "u", Name: "status"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: "active"}},
					},
				},
			},
		},
		Where:   &query.Comparison{Left: &query.ColumnRef{Qualifier: "o", Name: "total"}, Operator: query.OpGreaterThan, Right: &query.BoundValue{Value: int64(100)}},
		GroupBy: []query.ColumnRef{{Qualifier: "o", Name: "status"}},
		Having: &query.Comparison{
			Left:     &query.FuncCall{Name: "COUNT", Star: true},
			Operator: query.OpGreaterThan,
			Right:    &query.BoundValue{Value: int64(5)},
		},
		Limit:  &limit,
		Offset: &offset,
	}
}

func TestRenderSelect_OnWhereHavingLimitOffsetPlaceholderOrdering(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(onWhereHavingLimitOffset(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderSelect_HavingWithoutGroupBy(t *testing.T) {
	// A HAVING is valid without GROUP BY: the aggregate spans the whole table. No
	// GROUP BY clause is emitted, and the HAVING value takes the first placeholder.
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "COUNT", Star: true}, Alias: "n"}},
		From:    "orders",
		Having: &query.Comparison{
			Left:     &query.FuncCall{Name: "COUNT", Star: true},
			Operator: query.OpGreaterThan,
			Right:    &query.BoundValue{Value: int64(5)},
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(5)})
		})
	}
}

func TestRenderSelect_HavingWithOffsetOnly(t *testing.T) {
	// A HAVING value binds before the OFFSET value even when there is no LIMIT: the
	// HAVING takes the first placeholder and the OFFSET the second. On MySQL,
	// MariaDB, and SQLite the offset-only "no limit" sentinel is a literal between
	// HAVING and OFFSET, so it consumes no placeholder and does not shift the order.
	offset := int64(5)
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Expr: &query.FuncCall{Name: "SUM", Args: []query.Expression{&query.ColumnRef{Name: "total"}}}, Alias: "s"}},
		From:    "orders",
		GroupBy: []query.ColumnRef{{Name: "status"}},
		Having: &query.Comparison{
			Left:     &query.FuncCall{Name: "SUM", Args: []query.Expression{&query.ColumnRef{Name: "total"}}},
			Operator: query.OpGreaterThan,
			Right:    &query.BoundValue{Value: int64(1000)},
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}
