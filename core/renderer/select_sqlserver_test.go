package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// TestRenderSelect_SQLServerPagination pins T-SQL's row-limiting clause across
// the combinations the dialect matrix does not reach.
//
// The matrix renders one fixture per dialect, with a limit and no ORDER BY. The
// rows below are the other four states, and each pins a decision rather than a
// spelling: whether ORDER BY is synthesized, and whether OFFSET or FETCH is
// emitted (stokaro/ptah#941).
//
// SQL Server accepts OFFSET/FETCH only after an ORDER BY, and requires OFFSET
// before FETCH. `ORDER BY (SELECT NULL)` is the documented spelling for "no
// particular order", which is the semantics an unordered LIMIT already has
// everywhere else -- so it is synthesized rather than an order being invented,
// and it must NOT appear when the caller ordered the query themselves.
func TestRenderSelect_SQLServerPagination(t *testing.T) {
	limit := int64(10)
	offset := int64(5)
	ordered := []ast.OrderByClause{{Column: "id"}}

	tests := []struct {
		name    string
		limit   *int64
		offset  *int64
		orderBy []ast.OrderByClause
		want    string
		args    []any
	}{
		{
			name:  "limit only synthesizes an order and a zero offset",
			limit: &limit,
			want:  "SELECT [id] FROM [users] ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT @p1 ROWS ONLY",
			args:  []any{int64(10)},
		},
		{
			// A bare offset emits no FETCH at all: T-SQL allows OFFSET alone,
			// so there is no "no limit" sentinel to synthesize the way the
			// MySQL and SQLite paths need one.
			name:   "offset only emits no FETCH",
			offset: &offset,
			want:   "SELECT [id] FROM [users] ORDER BY (SELECT NULL) OFFSET @p1 ROWS",
			args:   []any{int64(5)},
		},
		{
			name:   "both bind in caller order",
			limit:  &limit,
			offset: &offset,
			want: "SELECT [id] FROM [users] ORDER BY (SELECT NULL) " +
				"OFFSET @p1 ROWS FETCH NEXT @p2 ROWS ONLY",
			args: []any{int64(5), int64(10)},
		},
		{
			// The caller's ordering is kept and nothing is synthesized beside
			// it. Synthesizing anyway emits two ORDER BY clauses, which T-SQL
			// rejects.
			name:    "a caller ordering is not replaced",
			limit:   &limit,
			orderBy: ordered,
			want:    "SELECT [id] FROM [users] ORDER BY [id] ASC OFFSET 0 ROWS FETCH NEXT @p1 ROWS ONLY",
			args:    []any{int64(10)},
		},
		{
			// No pagination, no clause. Synthesizing an ordering here would
			// change an unordered query into an ordered one for no reason.
			name: "no pagination synthesizes nothing",
			want: "SELECT [id] FROM [users]",
			args: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, args, err := renderer.RenderSelect(&ast.SelectStatement{
				Columns: []ast.ResultColumn{{Name: "id"}},
				From:    "users",
				OrderBy: test.orderBy,
				Limit:   test.limit,
				Offset:  test.offset,
			}, "sqlserver")

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
			c.Assert(args, qt.DeepEquals, test.args)
		})
	}
}
