package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// subqueryRow is one builder helper and the SQL it must produce.
type subqueryRow struct {
	name  string
	where ast.Expression
	want  string
	args  []any
}

func publishedAuthors() *query.SelectBuilder {
	return query.Select("author_id").From("posts").Where(query.Eq("published", true))
}

func TestSubqueryHelpersRenderTheirClause(t *testing.T) {
	rows := []subqueryRow{{
		name:  "IN reads its candidates from a query",
		where: query.InQuery("id", publishedAuthors()),
		want: `SELECT "id" FROM "users" WHERE "id" IN ` +
			`(SELECT "author_id" FROM "posts" WHERE "published" = $1)`,
		args: []any{true},
	}, {
		name:  "EXISTS tests for any row",
		where: query.Exists(publishedAuthors()),
		want: `SELECT "id" FROM "users" WHERE EXISTS ` +
			`(SELECT "author_id" FROM "posts" WHERE "published" = $1)`,
		args: []any{true},
	}, {
		name:  "NOT EXISTS is the complement",
		where: query.NotExists(publishedAuthors()),
		want: `SELECT "id" FROM "users" WHERE NOT EXISTS ` +
			`(SELECT "author_id" FROM "posts" WHERE "published" = $1)`,
		args: []any{true},
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := query.Select("id").From("users").Where(row.where).Build()

			sql, args, err := renderer.RenderSelect(stmt, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, row.want)
			c.Assert(args, qt.DeepEquals, row.args)
		})
	}
}

// TestSubqueryBindsBeforeTheOuterPredicate pins the ordering for the nested
// form, the same property the WITH clause has.
//
// The subquery is rendered where it appears, so its values land in args at that
// position. A caller adding a predicate after the IN must still see its own
// value second.
func TestSubqueryBindsInRenderedOrder(t *testing.T) {
	c := qt.New(t)
	stmt := query.Select("id").From("users").
		Where(query.And(
			query.InQuery("id", publishedAuthors()),
			query.Eq("id", 7),
		)).Build()

	_, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(args, qt.DeepEquals, []any{true, 7})
}

func TestSubqueryHelpersIgnoreANilQuery(t *testing.T) {
	c := qt.New(t)

	// The helpers are used inline inside Where, so returning a typed nil
	// expression here would surface as a panic deep in the renderer.
	c.Assert(query.InQuery("id", nil), qt.IsNil)
	c.Assert(query.Exists(nil), qt.IsNil)
	c.Assert(query.NotExists(nil), qt.IsNil)
}
