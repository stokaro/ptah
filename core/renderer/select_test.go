package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// representativeSelect exercises columns, WHERE with AND/OR/NOT/IN/comparison/
// IS NOT NULL, ORDER BY with a tie-breaker, and LIMIT/OFFSET in a single tree.
func representativeSelect() *ast.SelectStatement {
	limit := int64(24)
	offset := int64(0)
	return &ast.SelectStatement{
		Columns: []ast.ResultColumn{{Name: "id"}, {Name: "name"}},
		From:    "commodities",
		Where: &ast.LogicalExpr{
			Operator: ast.LogicalAnd,
			Operands: []ast.Expression{
				&ast.Comparison{
					Left:     &ast.ColumnRef{Name: "draft"},
					Operator: ast.OpEqual,
					Right:    &ast.BoundValue{Value: false},
				},
				&ast.InExpr{
					Operand: &ast.ColumnRef{Name: "status"},
					Values: []ast.Expression{
						&ast.BoundValue{Value: "in_use"},
						&ast.BoundValue{Value: "sold"},
					},
				},
				&ast.LogicalExpr{
					Operator: ast.LogicalOr,
					Operands: []ast.Expression{
						&ast.NullTest{Operand: &ast.ColumnRef{Name: "deleted_at"}, Negated: true},
						&ast.NotExpr{Operand: &ast.Comparison{
							Left:     &ast.ColumnRef{Name: "count"},
							Operator: ast.OpGreaterThan,
							Right:    &ast.BoundValue{Value: int64(10)},
						}},
					},
				},
			},
		},
		OrderBy: []ast.OrderByClause{
			{Column: "name", Direction: ast.SortAscending},
			{Column: "id", Direction: ast.SortDescending},
		},
		Limit:  &limit,
		Offset: &offset,
	}
}

func TestRenderSelect_Representative(t *testing.T) {
	wantArgs := []any{false, "in_use", "sold", int64(10), int64(24), int64(0)}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{
			name:    "postgres uses dollar placeholders and double quotes",
			dialect: platform.Postgres,
			wantSQL: `SELECT "id", "name" FROM "commodities" WHERE ("draft" = $1 AND "status" IN ($2, $3) AND ("deleted_at" IS NOT NULL OR NOT ("count" > $4))) ORDER BY "name" ASC, "id" DESC LIMIT $5 OFFSET $6`,
		},
		{
			name:    "mysql uses question placeholders and backticks",
			dialect: platform.MySQL,
			wantSQL: "SELECT `id`, `name` FROM `commodities` WHERE (`draft` = ? AND `status` IN (?, ?) AND (`deleted_at` IS NOT NULL OR NOT (`count` > ?))) ORDER BY `name` ASC, `id` DESC LIMIT ? OFFSET ?",
		},
		{
			name:    "mariadb matches mysql",
			dialect: platform.MariaDB,
			wantSQL: "SELECT `id`, `name` FROM `commodities` WHERE (`draft` = ? AND `status` IN (?, ?) AND (`deleted_at` IS NOT NULL OR NOT (`count` > ?))) ORDER BY `name` ASC, `id` DESC LIMIT ? OFFSET ?",
		},
		{
			name:    "sqlite uses question placeholders and double quotes",
			dialect: platform.SQLite,
			wantSQL: `SELECT "id", "name" FROM "commodities" WHERE ("draft" = ? AND "status" IN (?, ?) AND ("deleted_at" IS NOT NULL OR NOT ("count" > ?))) ORDER BY "name" ASC, "id" DESC LIMIT ? OFFSET ?`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderSelect(representativeSelect(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderSelect_DialectAliasesNormalize(t *testing.T) {
	stmt := &ast.SelectStatement{
		Columns: []ast.ResultColumn{{Star: true}},
		From:    "users",
		Where: &ast.Comparison{
			Left:     &ast.ColumnRef{Name: "id"},
			Operator: ast.OpEqual,
			Right:    &ast.BoundValue{Value: int64(7)},
		},
	}

	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{name: "postgresql alias", dialect: "postgresql", wantSQL: `SELECT * FROM "users" WHERE "id" = $1`},
		{name: "pgx alias", dialect: "pgx", wantSQL: `SELECT * FROM "users" WHERE "id" = $1`},
		{name: "cockroachdb dollar placeholders", dialect: platform.CockroachDB, wantSQL: `SELECT * FROM "users" WHERE "id" = $1`},
		{name: "sqlite3 alias", dialect: "sqlite3", wantSQL: `SELECT * FROM "users" WHERE "id" = ?`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(7)})
		})
	}
}

func TestRenderSelect_ProjectionVariants(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *ast.SelectStatement
		wantSQL string
	}{
		{
			name:    "no columns renders star",
			stmt:    &ast.SelectStatement{From: "t"},
			wantSQL: `SELECT * FROM "t"`,
		},
		{
			name:    "explicit star",
			stmt:    &ast.SelectStatement{Columns: []ast.ResultColumn{{Star: true}}, From: "t"},
			wantSQL: `SELECT * FROM "t"`,
		},
		{
			name:    "named columns are quoted",
			stmt:    &ast.SelectStatement{Columns: []ast.ResultColumn{{Name: "a"}, {Name: "b"}}, From: "t"},
			wantSQL: `SELECT "a", "b" FROM "t"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderSelect(tt.stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_LimitOffsetPlaceholderOrdering(t *testing.T) {
	c := qt.New(t)

	// With no WHERE clause, the LIMIT/OFFSET bounds take the first placeholders,
	// proving the bounds are numbered by emission order, not treated specially.
	limit := int64(5)
	offset := int64(10)
	stmt := &ast.SelectStatement{
		From:   "t",
		Limit:  &limit,
		Offset: &offset,
	}

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT * FROM "t" LIMIT $1 OFFSET $2`)
	c.Assert(args, qt.DeepEquals, []any{int64(5), int64(10)})
}

func TestRenderSelect_OffsetWithoutLimit(t *testing.T) {
	// A bare OFFSET is valid on PostgreSQL, but MySQL/MariaDB/SQLite only accept
	// OFFSET as a suffix of LIMIT, so a "no limit" sentinel is synthesized. The
	// OFFSET stays bound; the sentinel is a literal and takes no placeholder.
	tests := []struct {
		name    string
		dialect string
		wantSQL string
	}{
		{name: "postgres emits bare offset", dialect: platform.Postgres, wantSQL: `SELECT * FROM "t" OFFSET $1`},
		{name: "mysql synthesizes max-bigint limit", dialect: platform.MySQL, wantSQL: "SELECT * FROM `t` LIMIT 18446744073709551615 OFFSET ?"},
		{name: "mariadb synthesizes max-bigint limit", dialect: platform.MariaDB, wantSQL: "SELECT * FROM `t` LIMIT 18446744073709551615 OFFSET ?"},
		{name: "sqlite synthesizes negative-one limit", dialect: platform.SQLite, wantSQL: `SELECT * FROM "t" LIMIT -1 OFFSET ?`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			offset := int64(10)
			stmt := &ast.SelectStatement{From: "t", Offset: &offset}
			sql, args, err := renderer.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(10)})
		})
	}
}

func TestRenderSelect_IdentifierQuotingAcrossClauses(t *testing.T) {
	// The projection, WHERE, and ORDER BY identifiers all route through the same
	// dialect quoting, so a quote-bearing name is escaped in every clause.
	tests := []struct {
		name    string
		stmt    *ast.SelectStatement
		wantSQL string
	}{
		{
			name: "projection column is escaped",
			stmt: &ast.SelectStatement{
				Columns: []ast.ResultColumn{{Name: `weird" col`}},
				From:    "t",
			},
			wantSQL: `SELECT "weird"" col" FROM "t"`,
		},
		{
			name: "order by column is escaped",
			stmt: &ast.SelectStatement{
				From:    "t",
				OrderBy: []ast.OrderByClause{{Column: `weird" col`, Direction: ast.SortDescending}},
			},
			wantSQL: `SELECT * FROM "t" ORDER BY "weird"" col" DESC`,
		},
		{
			name: "from table is escaped",
			stmt: &ast.SelectStatement{
				From: `weird" table`,
			},
			wantSQL: `SELECT * FROM "weird"" table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderSelect(tt.stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_NullTests(t *testing.T) {
	tests := []struct {
		name    string
		negated bool
		wantSQL string
	}{
		{name: "is null", negated: false, wantSQL: `SELECT * FROM "t" WHERE "deleted_at" IS NULL`},
		{name: "is not null", negated: true, wantSQL: `SELECT * FROM "t" WHERE "deleted_at" IS NOT NULL`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &ast.SelectStatement{
				From:  "t",
				Where: &ast.NullTest{Operand: &ast.ColumnRef{Name: "deleted_at"}, Negated: tt.negated},
			}
			sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_ValueNeverBecomesSQL(t *testing.T) {
	c := qt.New(t)

	payload := "x'; DROP TABLE users; --"
	stmt := &ast.SelectStatement{
		From: "users",
		Where: &ast.Comparison{
			Left:     &ast.ColumnRef{Name: "name"},
			Operator: ast.OpEqual,
			Right:    &ast.BoundValue{Value: payload},
		},
	}

	sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
	c.Assert(err, qt.IsNil)
	// The injection payload appears only as a bound argument, never in the SQL.
	c.Assert(sql, qt.Equals, `SELECT * FROM "users" WHERE "name" = $1`)
	c.Assert(sql, qt.Not(qt.Contains), "DROP TABLE")
	c.Assert(args, qt.DeepEquals, []any{payload})
}

func TestRenderSelect_IdentifierQuotingNeutralizesInjection(t *testing.T) {
	// An attacker-shaped identifier cannot terminate the quoted identifier: the
	// dialect quote character is doubled per the SQL standard.
	tests := []struct {
		name    string
		dialect string
		column  string
		wantSQL string
	}{
		{
			name:    "postgres doubles embedded double quote",
			dialect: platform.Postgres,
			column:  `name" FROM secrets; --`,
			wantSQL: `SELECT * FROM "t" WHERE "name"" FROM secrets; --" = $1`,
		},
		{
			name:    "mysql doubles embedded backtick",
			dialect: platform.MySQL,
			column:  "name` FROM secrets; --",
			wantSQL: "SELECT * FROM `t` WHERE `name`` FROM secrets; --` = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := &ast.SelectStatement{
				From: "t",
				Where: &ast.Comparison{
					Left:     &ast.ColumnRef{Name: tt.column},
					Operator: ast.OpEqual,
					Right:    &ast.BoundValue{Value: 1},
				},
			}
			sql, _, err := renderer.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
		})
	}
}

func TestRenderSelect_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *ast.SelectStatement
		dialect     string
		wantErrLike string
	}{
		{
			name:        "nil statement",
			stmt:        nil,
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil select statement",
		},
		{
			name:        "unsupported dialect",
			stmt:        &ast.SelectStatement{From: "t"},
			dialect:     platform.ClickHouse,
			wantErrLike: `renderer: SELECT rendering is not supported for dialect "clickhouse"`,
		},
		{
			name:        "missing from",
			stmt:        &ast.SelectStatement{Columns: []ast.ResultColumn{{Name: "a"}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: select statement requires a FROM table",
		},
		{
			name:        "empty in list",
			stmt:        &ast.SelectStatement{From: "t", Where: &ast.InExpr{Operand: &ast.ColumnRef{Name: "a"}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: IN requires at least one value",
		},
		{
			name:        "empty column name",
			stmt:        &ast.SelectStatement{Columns: []ast.ResultColumn{{Name: "  "}}, From: "t"},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: result column has an empty name",
		},
		{
			name: "unknown comparison operator",
			stmt: &ast.SelectStatement{From: "t", Where: &ast.Comparison{
				Left:     &ast.ColumnRef{Name: "a"},
				Operator: ast.ComparisonOperator(99),
				Right:    &ast.BoundValue{Value: 1},
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: unknown comparison operator 99",
		},
		{
			name: "unknown sort direction",
			stmt: &ast.SelectStatement{From: "t", OrderBy: []ast.OrderByClause{
				{Column: "a", Direction: ast.SortDirection(99)},
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: unknown sort direction 99",
		},
		{
			name: "typed-nil column reference leaf does not panic",
			stmt: &ast.SelectStatement{From: "t", Where: &ast.Comparison{
				Left:     (*ast.ColumnRef)(nil),
				Operator: ast.OpEqual,
				Right:    &ast.BoundValue{Value: 1},
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil column reference",
		},
		{
			name: "typed-nil bound value leaf does not panic",
			stmt: &ast.SelectStatement{From: "t", Where: &ast.Comparison{
				Left:     &ast.ColumnRef{Name: "a"},
				Operator: ast.OpEqual,
				Right:    (*ast.BoundValue)(nil),
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil bound value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := renderer.RenderSelect(tt.stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}
