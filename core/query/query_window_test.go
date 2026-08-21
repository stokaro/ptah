package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// windowSelect renders one window expression in a projection.
func windowSelect(c *qt.C, expr ast.Expression, dialect string) (string, error) {
	c.Helper()
	stmt := query.Select("id").From("t").ExprAs(expr, "v").Build()
	sql, _, err := renderer.RenderSelect(stmt, dialect)
	return sql, err
}

// TestWindow_RendersEachClauseCombination covers the OVER clause's four shapes.
//
// The empty spec is the one worth stating: `OVER ()` is valid and means the
// whole result set, and it is what makes the call a window function at all.
// Omitting the clause for an empty spec would silently turn SUM("total") OVER ()
// back into an ordinary aggregate over the GROUP BY group — a different query
// with a different answer (stokaro/ptah#941).
func TestWindow_RendersEachClauseCombination(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expression
		want string
	}{
		{
			name: "partition and order",
			expr: query.Over(query.Sum("total"), query.Partition("user_id"), query.OrderAsc("day")),
			want: `SELECT "id", SUM("total") OVER (PARTITION BY "user_id" ORDER BY "day" ASC) AS "v" FROM "t"`,
		},
		{
			name: "partition only",
			expr: query.Over(query.Sum("total"), query.Partition("user_id")),
			want: `SELECT "id", SUM("total") OVER (PARTITION BY "user_id") AS "v" FROM "t"`,
		},
		{
			name: "order only",
			expr: query.Over(query.Sum("total"), query.OrderDesc("day")),
			want: `SELECT "id", SUM("total") OVER (ORDER BY "day" DESC) AS "v" FROM "t"`,
		},
		{
			name: "empty spec still emits the clause",
			expr: query.Over(query.Sum("total")),
			want: `SELECT "id", SUM("total") OVER () AS "v" FROM "t"`,
		},
		{
			name: "a star call takes a window too",
			expr: query.Over(query.CountStar(), query.Partition("k")),
			want: `SELECT "id", COUNT(*) OVER (PARTITION BY "k") AS "v" FROM "t"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := windowSelect(c, test.expr, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestWindow_RankingFunctionsTakeNoArguments is the case that changed a rule.
//
// A zero-argument call is meaningless without a window — SUM() computes
// nothing — so the renderer refused one. With a window it is the whole shape of
// a ranking function: ROW_NUMBER, RANK and DENSE_RANK take no arguments and get
// their input from the OVER clause.
func TestWindow_RankingFunctionsTakeNoArguments(t *testing.T) {
	tests := []struct {
		name string
		expr ast.Expression
		want string
	}{
		{
			name: "row_number",
			expr: query.Over(query.Func("ROW_NUMBER"), query.Partition("k"), query.OrderAsc("t")),
			want: `SELECT "id", ROW_NUMBER() OVER (PARTITION BY "k" ORDER BY "t" ASC) AS "v" FROM "t"`,
		},
		{
			name: "rank",
			expr: query.Over(query.Func("RANK"), query.OrderDesc("score")),
			want: `SELECT "id", RANK() OVER (ORDER BY "score" DESC) AS "v" FROM "t"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := windowSelect(c, test.expr, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestWindow_ZeroArgumentCallWithoutAWindowIsStillRefused is the paired
// control. The rule was relaxed exactly where a window supplies the input, and
// nowhere else.
func TestWindow_ZeroArgumentCallWithoutAWindowIsStillRefused(t *testing.T) {
	c := qt.New(t)

	sql, err := windowSelect(c, query.Func("SUM"), "postgres")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "requires at least one argument, or a window")
	c.Assert(sql, qt.Equals, "")
}

// TestWindow_RendersOnEveryDialect keeps the clause portable. Window functions
// are one spelling everywhere, so the rows differ only in quoting.
func TestWindow_RendersOnEveryDialect(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: "postgres", want: `SELECT "id", ROW_NUMBER() OVER (PARTITION BY "k") AS "v" FROM "t"`},
		{dialect: "mysql", want: "SELECT `id`, ROW_NUMBER() OVER (PARTITION BY `k`) AS `v` FROM `t`"},
		{dialect: "mariadb", want: "SELECT `id`, ROW_NUMBER() OVER (PARTITION BY `k`) AS `v` FROM `t`"},
		{dialect: "sqlite", want: `SELECT "id", ROW_NUMBER() OVER (PARTITION BY "k") AS "v" FROM "t"`},
		{dialect: "clickhouse", want: "SELECT `id`, ROW_NUMBER() OVER (PARTITION BY `k`) AS `v` FROM `t`"},
		{dialect: "sqlserver", want: `SELECT [id], ROW_NUMBER() OVER (PARTITION BY [k]) AS [v] FROM [t]`},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := windowSelect(c, query.Over(query.Func("ROW_NUMBER"), query.Partition("k")), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestWindow_RefusesAnEmptyPartitionColumn keeps a blank identifier out of the
// clause, the same rule the statement's own ORDER BY has.
func TestWindow_RefusesAnEmptyPartitionColumn(t *testing.T) {
	c := qt.New(t)
	call := &ast.FuncCall{
		Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "x"}},
		Over: &ast.WindowSpec{PartitionBy: []ast.ColumnRef{{Name: "   "}}},
	}

	sql, err := windowSelect(c, call, "postgres")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "PARTITION BY term has an empty column")
	c.Assert(sql, qt.Equals, "")
}

// TestWindow_OrderTermsShareTheStatementsRules proves the window's ORDER BY and
// the statement's are the same code.
//
// Two copies of the loop would let one learn a rule the other did not — an
// empty column refused in one place and emitted in the other.
func TestWindow_OrderTermsShareTheStatementsRules(t *testing.T) {
	c := qt.New(t)
	call := &ast.FuncCall{
		Name: "SUM", Args: []ast.Expression{&ast.ColumnRef{Name: "x"}},
		Over: &ast.WindowSpec{OrderBy: []ast.OrderByClause{{Column: ""}}},
	}

	sql, err := windowSelect(c, call, "postgres")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "ORDER BY term has an empty column")
	c.Assert(sql, qt.Equals, "")
}

// TestWindow_AbsentSpecChangesNothing is the control: without Over the call is
// what it always was.
func TestWindow_AbsentSpecChangesNothing(t *testing.T) {
	c := qt.New(t)

	sql, err := windowSelect(c, query.Sum("total"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id", SUM("total") AS "v" FROM "t"`)
	c.Assert(sql, qt.Not(qt.Contains), "OVER")
}
