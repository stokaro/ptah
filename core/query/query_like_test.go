package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// TestLike_RendersOnEveryDialect pins LIKE across the dialects the builder
// renders for.
//
// The rows differ only in quoting and placeholder style, which is the point:
// LIKE is one operator everywhere, so a row failing means that dialect's
// rendering changed rather than that LIKE did (stokaro/ptah#941).
func TestLike_RendersOnEveryDialect(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: "postgres", want: `SELECT "id" FROM "users" WHERE "name" LIKE $1`},
		{dialect: "mysql", want: "SELECT `id` FROM `users` WHERE `name` LIKE ?"},
		{dialect: "mariadb", want: "SELECT `id` FROM `users` WHERE `name` LIKE ?"},
		{dialect: "sqlite", want: `SELECT "id" FROM "users" WHERE "name" LIKE ?`},
		{dialect: "clickhouse", want: "SELECT `id` FROM `users` WHERE `name` LIKE ?"},
		{dialect: "sqlserver", want: "SELECT [id] FROM [users] WHERE [name] LIKE @p1"},
		{dialect: "cockroachdb", want: `SELECT "id" FROM "users" WHERE "name" LIKE $1`},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			stmt := query.Select("id").From("users").Where(query.Like("name", "ada%")).Build()

			sql, args, err := renderer.RenderSelect(stmt, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
			// The pattern is a bound VALUE, never text in the statement.
			c.Assert(args, qt.DeepEquals, []any{"ada%"})
		})
	}
}

// TestNotLike_RendersTheNegatedOperator is the paired case. Without it a build
// that emitted LIKE for both would pass every assertion above.
func TestNotLike_RendersTheNegatedOperator(t *testing.T) {
	c := qt.New(t)
	stmt := query.Select("id").From("users").Where(query.NotLike("name", "%test%")).Build()

	sql, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id" FROM "users" WHERE "name" NOT LIKE $1`)
	c.Assert(args, qt.DeepEquals, []any{"%test%"})
}

// TestLike_PatternIsBoundNotInterpolated is the safety property stated
// directly.
//
// A pattern is caller data and the classic injection is to concatenate it. Here
// a pattern that is entirely SQL travels as one bound argument and appears
// nowhere in the statement text, so it can only ever be matched against.
func TestLike_PatternIsBoundNotInterpolated(t *testing.T) {
	c := qt.New(t)
	hostile := "%' OR 1=1 --"
	stmt := query.Select("id").From("users").Where(query.Like("name", hostile)).Build()

	sql, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id" FROM "users" WHERE "name" LIKE $1`)
	c.Assert(sql, qt.Not(qt.Contains), "OR 1=1")
	c.Assert(args, qt.DeepEquals, []any{hostile})
}

// TestLike_WildcardsAreTheCallersToWrite pins that Ptah does not escape them.
//
// A caller matching a literal percent has to escape it themselves: Ptah cannot
// tell an intended wildcard from an accidental one, and escaping on their
// behalf would break every pattern that meant them. The assertion is that the
// pattern arrives unchanged.
func TestLike_WildcardsAreTheCallersToWrite(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{name: "trailing wildcard", pattern: "ada%"},
		{name: "single character wildcard", pattern: "ad_"},
		{name: "no wildcard at all", pattern: "ada"},
		{name: "an escaped percent stays escaped", pattern: `100\%`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := query.Select("id").From("t").Where(query.Like("c", test.pattern)).Build()

			_, args, err := renderer.RenderSelect(stmt, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(args, qt.DeepEquals, []any{test.pattern})
		})
	}
}

// TestLike_ComposesWithOtherPredicates keeps the operator usable in a real
// WHERE tree rather than only alone, and pins the argument order across it.
func TestLike_ComposesWithOtherPredicates(t *testing.T) {
	c := qt.New(t)
	stmt := query.Select("id").From("users").
		Where(query.And(query.Like("name", "ada%"), query.Gt("age", 30))).
		Build()

	sql, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT "id" FROM "users" WHERE ("name" LIKE $1 AND "age" > $2)`)
	c.Assert(args, qt.DeepEquals, []any{"ada%", 30})
}
