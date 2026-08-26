package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/query"
)

// representativeSelect exercises columns, WHERE with AND/OR/NOT/IN/comparison/
// IS NOT NULL, ORDER BY with a tie-breaker, and LIMIT/OFFSET in a single tree.
func representativeSelect() *query.SelectStatement {
	limit := int64(24)
	offset := int64(0)
	return &query.SelectStatement{
		Columns: []query.ResultColumn{{Name: "id"}, {Name: "name"}},
		From:    "commodities",
		Where: &query.LogicalExpr{
			Operator: query.LogicalAnd,
			Operands: []query.Expression{
				&query.Comparison{
					Left:     &query.ColumnRef{Name: "draft"},
					Operator: query.OpEqual,
					Right:    &query.BoundValue{Value: false},
				},
				&query.InExpr{
					Operand: &query.ColumnRef{Name: "status"},
					Values: []query.Expression{
						&query.BoundValue{Value: "in_use"},
						&query.BoundValue{Value: "sold"},
					},
				},
				&query.LogicalExpr{
					Operator: query.LogicalOr,
					Operands: []query.Expression{
						&query.NullTest{Operand: &query.ColumnRef{Name: "deleted_at"}, Negated: true},
						&query.NotExpr{Operand: &query.Comparison{
							Left:     &query.ColumnRef{Name: "count"},
							Operator: query.OpGreaterThan,
							Right:    &query.BoundValue{Value: int64(10)},
						}},
					},
				},
			},
		},
		OrderBy: []query.OrderByClause{
			{Column: "name", Direction: query.SortAscending},
			{Column: "id", Direction: query.SortDescending},
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
			sql, args, err := query.RenderSelect(representativeSelect(), tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, wantArgs)
		})
	}
}

func TestRenderSelect_DialectAliasesNormalize(t *testing.T) {
	stmt := &query.SelectStatement{
		Columns: []query.ResultColumn{{Star: true}},
		From:    "users",
		Where: &query.Comparison{
			Left:     &query.ColumnRef{Name: "id"},
			Operator: query.OpEqual,
			Right:    &query.BoundValue{Value: int64(7)},
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
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.DeepEquals, []any{int64(7)})
		})
	}
}

func TestRenderSelect_ProjectionVariants(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *query.SelectStatement
		wantSQL string
	}{
		{
			name:    "no columns renders star",
			stmt:    &query.SelectStatement{From: "t"},
			wantSQL: `SELECT * FROM "t"`,
		},
		{
			name:    "explicit star",
			stmt:    &query.SelectStatement{Columns: []query.ResultColumn{{Star: true}}, From: "t"},
			wantSQL: `SELECT * FROM "t"`,
		},
		{
			name:    "named columns are quoted",
			stmt:    &query.SelectStatement{Columns: []query.ResultColumn{{Name: "a"}, {Name: "b"}}, From: "t"},
			wantSQL: `SELECT "a", "b" FROM "t"`,
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

func TestRenderSelect_LimitOffsetPlaceholderOrdering(t *testing.T) {
	c := qt.New(t)

	// With no WHERE clause, the LIMIT/OFFSET bounds take the first placeholders,
	// proving the bounds are numbered by emission order, not treated specially.
	limit := int64(5)
	offset := int64(10)
	stmt := &query.SelectStatement{
		From:   "t",
		Limit:  &limit,
		Offset: &offset,
	}

	sql, args, err := query.RenderSelect(stmt, platform.Postgres)
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
			stmt := &query.SelectStatement{From: "t", Offset: &offset}
			sql, args, err := query.RenderSelect(stmt, tt.dialect)
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
		stmt    *query.SelectStatement
		wantSQL string
	}{
		{
			name: "projection column is escaped",
			stmt: &query.SelectStatement{
				Columns: []query.ResultColumn{{Name: `weird" col`}},
				From:    "t",
			},
			wantSQL: `SELECT "weird"" col" FROM "t"`,
		},
		{
			name: "order by column is escaped",
			stmt: &query.SelectStatement{
				From:    "t",
				OrderBy: []query.OrderByClause{{Column: `weird" col`, Direction: query.SortDescending}},
			},
			wantSQL: `SELECT * FROM "t" ORDER BY "weird"" col" DESC`,
		},
		{
			name: "from table is escaped",
			stmt: &query.SelectStatement{
				From: `weird" table`,
			},
			wantSQL: `SELECT * FROM "weird"" table"`,
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
			stmt := &query.SelectStatement{
				From:  "t",
				Where: &query.NullTest{Operand: &query.ColumnRef{Name: "deleted_at"}, Negated: tt.negated},
			}
			sql, args, err := query.RenderSelect(stmt, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
			c.Assert(args, qt.HasLen, 0)
		})
	}
}

func TestRenderSelect_ValueNeverBecomesSQL(t *testing.T) {
	c := qt.New(t)

	payload := "x'; DROP TABLE users; --"
	stmt := &query.SelectStatement{
		From: "users",
		Where: &query.Comparison{
			Left:     &query.ColumnRef{Name: "name"},
			Operator: query.OpEqual,
			Right:    &query.BoundValue{Value: payload},
		},
	}

	sql, args, err := query.RenderSelect(stmt, platform.Postgres)
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
			stmt := &query.SelectStatement{
				From: "t",
				Where: &query.Comparison{
					Left:     &query.ColumnRef{Name: tt.column},
					Operator: query.OpEqual,
					Right:    &query.BoundValue{Value: 1},
				},
			}
			sql, _, err := query.RenderSelect(stmt, tt.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, tt.wantSQL)
		})
	}
}

func TestRenderSelect_Errors(t *testing.T) {
	tests := []struct {
		name        string
		stmt        *query.SelectStatement
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
			name: "unsupported dialect",
			stmt: &query.SelectStatement{From: "t"},
			// A dialect the renderer has never been taught. ClickHouse stood
			// here until stokaro/ptah#941 taught it, which is why the example
			// is now a name outside platform's set entirely: an example the
			// builder supports asserts nothing.
			dialect:     "db2",
			wantErrLike: `renderer: SELECT rendering is not supported for dialect "db2"`,
		},
		{
			name:        "missing from",
			stmt:        &query.SelectStatement{Columns: []query.ResultColumn{{Name: "a"}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: select statement requires a FROM table",
		},
		{
			name:        "empty in list",
			stmt:        &query.SelectStatement{From: "t", Where: &query.InExpr{Operand: &query.ColumnRef{Name: "a"}}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: IN requires at least one value or a subquery",
		},
		{
			name:        "empty column name",
			stmt:        &query.SelectStatement{Columns: []query.ResultColumn{{Name: "  "}}, From: "t"},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: result column has an empty name",
		},
		{
			name: "unknown comparison operator",
			stmt: &query.SelectStatement{From: "t", Where: &query.Comparison{
				Left:     &query.ColumnRef{Name: "a"},
				Operator: query.ComparisonOperator(99),
				Right:    &query.BoundValue{Value: 1},
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: unknown comparison operator 99",
		},
		{
			name: "unknown sort direction",
			stmt: &query.SelectStatement{From: "t", OrderBy: []query.OrderByClause{
				{Column: "a", Direction: query.SortDirection(99)},
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: unknown sort direction 99",
		},
		{
			name: "typed-nil column reference leaf does not panic",
			stmt: &query.SelectStatement{From: "t", Where: &query.Comparison{
				Left:     (*query.ColumnRef)(nil),
				Operator: query.OpEqual,
				Right:    &query.BoundValue{Value: 1},
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil column reference",
		},
		{
			name: "typed-nil bound value leaf does not panic",
			stmt: &query.SelectStatement{From: "t", Where: &query.Comparison{
				Left:     &query.ColumnRef{Name: "a"},
				Operator: query.OpEqual,
				Right:    (*query.BoundValue)(nil),
			}},
			dialect:     platform.Postgres,
			wantErrLike: "renderer: nil bound value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sql, args, err := query.RenderSelect(tt.stmt, tt.dialect)
			c.Assert(err, qt.ErrorMatches, tt.wantErrLike)
			c.Assert(sql, qt.Equals, "")
			c.Assert(args, qt.IsNil)
		})
	}
}
