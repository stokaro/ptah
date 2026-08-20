package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// cteDialectRow is one dialect and the WITH clause it must receive.
type cteDialectRow struct {
	name    string
	dialect string
	want    string
	args    []any
}

func TestSelectWithRendersACommonTableExpression(t *testing.T) {
	rows := []cteDialectRow{{
		name:    "postgres numbers across the whole statement",
		dialect: "postgres",
		want: `WITH "recent" AS (SELECT "id", "author_id" FROM "posts" WHERE "published" = $1) ` +
			`SELECT "id" FROM "recent" WHERE "author_id" = $2`,
		args: []any{true, 7},
	}, {
		name:    "mysql quotes with backticks",
		dialect: "mysql",
		want: "WITH `recent` AS (SELECT `id`, `author_id` FROM `posts` WHERE `published` = ?) " +
			"SELECT `id` FROM `recent` WHERE `author_id` = ?",
		args: []any{true, 7},
	}, {
		name:    "sqlserver uses its own placeholder style",
		dialect: "sqlserver",
		want: `WITH [recent] AS (SELECT [id], [author_id] FROM [posts] WHERE [published] = @p1) ` +
			`SELECT [id] FROM [recent] WHERE [author_id] = @p2`,
		args: []any{true, 7},
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			recent := query.Select("id", "author_id").From("posts").Where(query.Eq("published", true))
			stmt := query.Select("id").With("recent", recent).
				From("recent").Where(query.Eq("author_id", 7)).Build()

			sql, args, err := renderer.RenderSelect(stmt, row.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, row.want)
			c.Assert(args, qt.DeepEquals, row.args)
		})
	}
}

// TestSelectWithBindsTheSubqueryValuesFirst is the property the ordering exists
// for, stated on its own.
//
// A positional driver reads args by index, so a CTE whose values were appended
// after the outer query's would send the wrong value for $1 — and the query
// would still parse and still return rows, which is the failure worth a test of
// its own rather than an assertion buried in a rendering comparison.
func TestSelectWithBindsTheSubqueryValuesFirst(t *testing.T) {
	c := qt.New(t)
	inner := query.Select("id").From("posts").Where(query.Eq("author_id", "inner-value"))
	stmt := query.Select("id").With("recent", inner).
		From("recent").Where(query.Eq("id", "outer-value")).Build()

	_, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(args, qt.DeepEquals, []any{"inner-value", "outer-value"})
}

func TestSelectWithAccumulatesInOrder(t *testing.T) {
	c := qt.New(t)
	first := query.Select("id").From("posts")
	second := query.Select("id").From("first")
	stmt := query.Select("id").
		With("first", first).
		With("second", second).
		From("second").Build()

	sql, _, err := renderer.RenderSelect(stmt, "postgres")

	// A later CTE may read an earlier one, so the emitted order has to be the
	// order the caller declared.
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals,
		`WITH "first" AS (SELECT "id" FROM "posts"), "second" AS (SELECT "id" FROM "first") `+
			`SELECT "id" FROM "second"`)
}

func TestSelectWithIgnoresANilSubquery(t *testing.T) {
	c := qt.New(t)

	// The builder returns itself on every call, so a nil here would otherwise
	// surface as a nil-pointer panic at Build rather than at the call site.
	stmt := query.Select("id").With("recent", nil).From("posts").Build()

	sql, _, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id" FROM "posts"`)
}
