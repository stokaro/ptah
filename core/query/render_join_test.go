package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
)

// eqCols builds a "left = right" comparison of two qualified columns, the shape
// of an equi-join ON condition.
func eqCols(lq, ln, rq, rn string) *query.Comparison {
	return &query.Comparison{
		Left:     &query.ColumnRef{Qualifier: lq, Name: ln},
		Operator: query.OpEqual,
		Right:    &query.ColumnRef{Qualifier: rq, Name: rn},
	}
}

// twoTableJoin exercises an aliased FROM, one INNER JOIN, and qualified columns
// in the projection, ON, WHERE, and ORDER BY clauses at once.
func twoTableJoin() *query.SelectStatement {
	limit := int64(5)
	return &query.SelectStatement{
		Columns: []query.ResultColumn{
			{Qualifier: "u", Name: "id"},
			{Qualifier: "u", Name: "name"},
			{Qualifier: "o", Name: "total"},
		},
		From:      "users",
		FromAlias: "u",
		Joins: []query.JoinClause{
			{Type: query.JoinInner, Table: "orders", Alias: "o", On: eqCols("o", "user_id", "u", "id")},
		},
		Where: &query.LogicalExpr{
			Operator: query.LogicalAnd,
			Operands: []query.Expression{
				&query.Comparison{Left: &query.ColumnRef{Qualifier: "o", Name: "status"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: "paid"}},
				&query.Comparison{Left: &query.ColumnRef{Qualifier: "u", Name: "active"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: true}},
			},
		},
		OrderBy: []query.OrderByClause{
			{Qualifier: "u", Column: "name", Direction: query.SortAscending},
			{Qualifier: "o", Column: "total", Direction: query.SortDescending},
		},
		Limit: &limit,
	}
}

func TestRenderSelect_TwoTableJoin(t *testing.T) {
	wantArgs := []any{"paid", true, int64(5)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres qualifies with double quotes and dollar placeholders",
			dialect: platform.Postgres,
			wantSQL: `SELECT "u"."id", "u"."name", "o"."total" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" WHERE ("o"."status" = $1 AND "u"."active" = $2) ORDER BY "u"."name" ASC, "o"."total" DESC LIMIT $3`,
		},
		{
			name:    "mysql qualifies with backticks and question placeholders",
			dialect: platform.MySQL,
			wantSQL: "SELECT `u`.`id`, `u`.`name`, `o`.`total` FROM `users` `u` INNER JOIN `orders` `o` ON `o`.`user_id` = `u`.`id` WHERE (`o`.`status` = ? AND `u`.`active` = ?) ORDER BY `u`.`name` ASC, `o`.`total` DESC LIMIT ?",
		},
		{
			name:    "mariadb matches mysql",
			dialect: platform.MariaDB,
			wantSQL: "SELECT `u`.`id`, `u`.`name`, `o`.`total` FROM `users` `u` INNER JOIN `orders` `o` ON `o`.`user_id` = `u`.`id` WHERE (`o`.`status` = ? AND `u`.`active` = ?) ORDER BY `u`.`name` ASC, `o`.`total` DESC LIMIT ?",
		},
		{
			name:    "sqlite qualifies with double quotes and question placeholders",
			dialect: platform.SQLite,
			wantSQL: `SELECT "u"."id", "u"."name", "o"."total" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" WHERE ("o"."status" = ? AND "u"."active" = ?) ORDER BY "u"."name" ASC, "o"."total" DESC LIMIT ?`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(twoTableJoin(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

// threeTableJoin chains an INNER JOIN and a LEFT JOIN so the joins render in
// declared order.
func threeTableJoin() *query.SelectStatement {
	return &query.SelectStatement{
		Columns: []query.ResultColumn{
			{Qualifier: "u", Name: "name"},
			{Qualifier: "o", Name: "id"},
			{Qualifier: "p", Name: "amount"},
		},
		From:      "users",
		FromAlias: "u",
		Joins: []query.JoinClause{
			{Type: query.JoinInner, Table: "orders", Alias: "o", On: eqCols("o", "user_id", "u", "id")},
			{Type: query.JoinLeft, Table: "payments", Alias: "p", On: eqCols("p", "order_id", "o", "id")},
		},
		Where:   &query.Comparison{Left: &query.ColumnRef{Qualifier: "o", Name: "status"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: "paid"}},
		OrderBy: []query.OrderByClause{{Qualifier: "o", Column: "id", Direction: query.SortAscending}},
	}
}

func TestRenderSelect_ThreeTableJoin(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres",
			dialect: platform.Postgres,
			wantSQL: `SELECT "u"."name", "o"."id", "p"."amount" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" LEFT JOIN "payments" "p" ON "p"."order_id" = "o"."id" WHERE "o"."status" = $1 ORDER BY "o"."id" ASC`,
		},
		{
			name:    "mysql",
			dialect: platform.MySQL,
			wantSQL: "SELECT `u`.`name`, `o`.`id`, `p`.`amount` FROM `users` `u` INNER JOIN `orders` `o` ON `o`.`user_id` = `u`.`id` LEFT JOIN `payments` `p` ON `p`.`order_id` = `o`.`id` WHERE `o`.`status` = ? ORDER BY `o`.`id` ASC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(threeTableJoin(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{"paid"})
		})
	}
}

func TestRenderSelect_JoinTypeKeywords(t *testing.T) {
	tests := []struct {
		name     string
		joinType query.JoinType
		wantSQL  string
	}{
		{name: "inner", joinType: query.JoinInner, wantSQL: `SELECT * FROM "a" INNER JOIN "b" ON "b"."a_id" = "a"."id"`},
		{name: "left", joinType: query.JoinLeft, wantSQL: `SELECT * FROM "a" LEFT JOIN "b" ON "b"."a_id" = "a"."id"`},
		{name: "right", joinType: query.JoinRight, wantSQL: `SELECT * FROM "a" RIGHT JOIN "b" ON "b"."a_id" = "a"."id"`},
		{name: "full", joinType: query.JoinFull, wantSQL: `SELECT * FROM "a" FULL OUTER JOIN "b" ON "b"."a_id" = "a"."id"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &query.SelectStatement{
				From: "a",
				Joins: []query.JoinClause{
					{Type: tt.joinType, Table: "b", On: eqCols("b", "a_id", "a", "id")},
				},
			}
			sql, args, err := query.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_JoinWithoutAliasUsesTableNameQualifier(t *testing.T) {
	c := qt.New(t)

	// An unaliased join renders the bare table name and callers qualify columns
	// by the table name itself.
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Qualifier: "orders", Name: "total"}},
		From:    "users",
		Joins: []query.JoinClause{
			{Type: query.JoinInner, Table: "orders", On: eqCols("orders", "user_id", "users", "id")},
		},
	}

	sql, args, err := query.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "orders"."total" FROM "users" INNER JOIN "orders" ON "orders"."user_id" = "users"."id"`)
	c.Assert(args, qt.HasLen, 0)
}

func TestRenderSelect_JoinAliasAndQualifierQuoteEscaping(t *testing.T) {
	// A quote-bearing FROM alias, join alias, and column qualifier are all
	// escaped by doubling the dialect quote character, so none can break out.
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Qualifier: `a"x`, Name: `c"d`}},
		From:    "t",
		// FromAlias intentionally omitted here; the join alias carries the payload.
		Joins: []query.JoinClause{
			{
				Type:  query.JoinInner,
				Table: "u",
				Alias: `b"y`,
				On:    eqCols(`b"y`, "id", `a"x`, "uid"),
			},
		},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres doubles embedded double quotes",
			dialect: platform.Postgres,
			wantSQL: `SELECT "a""x"."c""d" FROM "t" INNER JOIN "u" "b""y" ON "b""y"."id" = "a""x"."uid"`,
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

func TestRenderSelect_FromAliasQuoteEscaping(t *testing.T) {
	c := qt.New(t)

	// A backtick-bearing FROM alias is escaped for MySQL by doubling the backtick.
	stmt := &query.SelectStatement{
		From:      "t",
		FromAlias: "a`x",
	}

	sql, args, err := query.RenderSelect(stmt, platform.MySQL)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "SELECT * FROM `t` `a``x`")
	c.Assert(args, qt.HasLen, 0)
}

func TestRenderSelect_JoinPlaceholderOrderingAcrossOnWhereLimit(t *testing.T) {
	// A bound value inside the JOIN ON is numbered before the WHERE value, which
	// is numbered before the LIMIT bound, proving placeholders follow render
	// order across ON, WHERE, and LIMIT.
	limit := int64(10)
	stmt := &query.SelectStatement{
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
		Where: &query.Comparison{Left: &query.ColumnRef{Qualifier: "o", Name: "total"}, Operator: query.OpGreaterThan, Right: &query.BoundValue{Value: int64(100)}},
		Limit: &limit,
	}

	wantArgs := []any{"active", int64(100), int64(10)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres numbers on then where then limit",
			dialect: platform.Postgres,
			wantSQL: `SELECT * FROM "orders" "o" INNER JOIN "users" "u" ON ("u"."id" = "o"."user_id" AND "u"."status" = $1) WHERE "o"."total" > $2 LIMIT $3`,
		},
		{
			name:    "mysql keeps the same order with question placeholders",
			dialect: platform.MySQL,
			wantSQL: "SELECT * FROM `orders` `o` INNER JOIN `users` `u` ON (`u`.`id` = `o`.`user_id` AND `u`.`status` = ?) WHERE `o`.`total` > ? LIMIT ?",
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

func TestRenderSelect_SQLiteAcceptsInnerAndLeftJoin(t *testing.T) {
	// INNER and LEFT joins are supported by every SQLite version.
	tests := []struct {
		name     string
		joinType query.JoinType
		wantSQL  string
	}{
		{name: "inner", joinType: query.JoinInner, wantSQL: `SELECT * FROM "a" INNER JOIN "b" ON "b"."a_id" = "a"."id"`},
		{name: "left", joinType: query.JoinLeft, wantSQL: `SELECT * FROM "a" LEFT JOIN "b" ON "b"."a_id" = "a"."id"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &query.SelectStatement{
				From:  "a",
				Joins: []query.JoinClause{{Type: tt.joinType, Table: "b", On: eqCols("b", "a_id", "a", "id")}},
			}
			sql, args, err := query.RenderSelect(stmt, platform.SQLite)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_SQLiteRejectsRightAndFullJoin(t *testing.T) {
	// SQLite gained RIGHT and FULL joins only in 3.39; the renderer rejects them
	// rather than emit SQL that fails at execution time on an older engine.
	tests := []struct {
		name        string
		joinType    query.JoinType
		wantErrLike string
	}{
		{name: "right", joinType: query.JoinRight, wantErrLike: "renderer: SQLite does not support RIGHT JOIN"},
		{name: "full", joinType: query.JoinFull, wantErrLike: "renderer: SQLite does not support FULL OUTER JOIN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &query.SelectStatement{
				From:  "a",
				Joins: []query.JoinClause{{Type: tt.joinType, Table: "b", On: eqCols("b", "a_id", "a", "id")}},
			}
			sql, args, err := query.RenderSelect(stmt, platform.SQLite)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func TestRenderSelect_MySQLLikeRejectsFullJoin(t *testing.T) {
	// MySQL and MariaDB have no FULL [OUTER] JOIN in any version; the renderer
	// rejects it rather than emit SQL the database would fail on. The error names
	// the normalized dialect.
	tests := []struct {
		name        string
		dialect     string
		wantErrLike string
	}{
		{name: "mysql", dialect: platform.MySQL, wantErrLike: "renderer: mysql does not support FULL OUTER JOIN"},
		{name: "mariadb", dialect: platform.MariaDB, wantErrLike: "renderer: mariadb does not support FULL OUTER JOIN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &query.SelectStatement{
				From:  "a",
				Joins: []query.JoinClause{{Type: query.JoinFull, Table: "b", On: eqCols("b", "a_id", "a", "id")}},
			}
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}

func TestRenderSelect_MySQLLikeAcceptsRightJoin(t *testing.T) {
	// RIGHT JOIN is valid on MySQL and MariaDB; only FULL is blocked there.
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &query.SelectStatement{
				From:  "a",
				Joins: []query.JoinClause{{Type: query.JoinRight, Table: "b", On: eqCols("b", "a_id", "a", "id")}},
			}
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, "SELECT * FROM `a` RIGHT JOIN `b` ON `b`.`a_id` = `a`.`id`")
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_QualifiedStar(t *testing.T) {
	// A qualified star renders "u".* (not the invalid "u"."*"), and mixes with
	// ordinary qualified columns.
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{
			{Qualifier: "u", Name: "*"},
			{Qualifier: "o", Name: "total"},
		},
		From:      "users",
		FromAlias: "u",
		Joins: []query.JoinClause{
			{Type: query.JoinInner, Table: "orders", Alias: "o", On: eqCols("o", "user_id", "u", "id")},
		},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres",
			dialect: platform.Postgres,
			wantSQL: `SELECT "u".*, "o"."total" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id"`,
		},
		{
			name:    "mysql",
			dialect: platform.MySQL,
			wantSQL: "SELECT `u`.*, `o`.`total` FROM `users` `u` INNER JOIN `orders` `o` ON `o`.`user_id` = `u`.`id`",
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

func TestRenderSelect_IdentifiersAreTrimmedBeforeQuoting(t *testing.T) {
	c := qt.New(t)

	// Surrounding whitespace on a table, alias, or qualifier is trimmed before
	// quoting, matching sqlident.Qualified, so it never lands inside the quotes.
	limit := int64(1)
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Qualifier: " u ", Name: "id"}},
		From:    " users ",
		// FromAlias carries surrounding whitespace too.
		FromAlias: " u ",
		Joins: []query.JoinClause{
			{Type: query.JoinInner, Table: " orders ", Alias: " o ", On: eqCols(" o ", "user_id", " u ", "id")},
		},
		OrderBy: []query.OrderByClause{{Qualifier: " u ", Column: "id", Direction: query.SortAscending}},
		Limit:   &limit,
	}

	sql, args, err := query.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "u"."id" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" ORDER BY "u"."id" ASC LIMIT $1`)
	c.Assert(args, qt.DeepEquals, []any{int64(1)})
}

func TestRenderSelect_TwoBoundJoinOnsPlaceholderOrdering(t *testing.T) {
	// Two joins, each carrying a bound value in its ON, prove the exact ordering:
	// join1 ON -> $1, join2 ON -> $2, WHERE -> $3, LIMIT -> $4.
	limit := int64(10)
	stmt := &query.SelectStatement{
		From:      "a",
		FromAlias: "a0",
		Joins: []query.JoinClause{
			{
				Type:  query.JoinInner,
				Table: "b",
				Alias: "b0",
				On: &query.LogicalExpr{
					Operator: query.LogicalAnd,
					Operands: []query.Expression{
						eqCols("b0", "a_id", "a0", "id"),
						&query.Comparison{Left: &query.ColumnRef{Qualifier: "b0", Name: "status"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: "active"}},
					},
				},
			},
			{
				Type:  query.JoinLeft,
				Table: "c",
				Alias: "c0",
				On: &query.LogicalExpr{
					Operator: query.LogicalAnd,
					Operands: []query.Expression{
						eqCols("c0", "b_id", "b0", "id"),
						&query.Comparison{Left: &query.ColumnRef{Qualifier: "c0", Name: "kind"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: "primary"}},
					},
				},
			},
		},
		Where: &query.Comparison{Left: &query.ColumnRef{Qualifier: "a0", Name: "active"}, Operator: query.OpEqual, Right: &query.BoundValue{Value: true}},
		Limit: &limit,
	}

	wantArgs := []any{"active", "primary", true, int64(10)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres numbers each on then where then limit",
			dialect: platform.Postgres,
			wantSQL: `SELECT * FROM "a" "a0" INNER JOIN "b" "b0" ON ("b0"."a_id" = "a0"."id" AND "b0"."status" = $1) LEFT JOIN "c" "c0" ON ("c0"."b_id" = "b0"."id" AND "c0"."kind" = $2) WHERE "a0"."active" = $3 LIMIT $4`,
		},
		{
			name:    "mysql keeps the order with question placeholders",
			dialect: platform.MySQL,
			wantSQL: "SELECT * FROM `a` `a0` INNER JOIN `b` `b0` ON (`b0`.`a_id` = `a0`.`id` AND `b0`.`status` = ?) LEFT JOIN `c` `c0` ON (`c0`.`b_id` = `b0`.`id` AND `c0`.`kind` = ?) WHERE `a0`.`active` = ? LIMIT ?",
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

func TestRenderSelect_JoinErrors(t *testing.T) {
	tests := []struct {
		name        string
		join        query.JoinClause
		wantErrLike string
	}{
		{
			name:        "missing table",
			join:        query.JoinClause{Type: query.JoinInner, On: eqCols("b", "a_id", "a", "id")},
			wantErrLike: "renderer: join requires a table",
		},
		{
			name:        "nil on condition",
			join:        query.JoinClause{Type: query.JoinInner, Table: "b"},
			wantErrLike: "renderer: join requires an ON condition",
		},
		{
			name:        "unknown join type",
			join:        query.JoinClause{Type: query.JoinType(42), Table: "b", On: eqCols("b", "a_id", "a", "id")},
			wantErrLike: "renderer: unknown join type 42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &query.SelectStatement{From: "a", Joins: []query.JoinClause{tt.join}}
			sql, args, err := query.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}
