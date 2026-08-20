package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// archiveFromUsers is the statement the rows below render.
func archiveFromUsers() *query.InsertBuilder {
	return query.InsertInto("archive").Columns("id", "name").
		FromSelect(query.Select("id", "name").From("users").Where(query.Lt("age", 18)))
}

// TestInsertSelect_RendersOnEveryDialect is the acceptance case.
//
// The rows differ only in quoting and placeholder style: INSERT ... SELECT is
// one statement everywhere, unlike upsert, so a row failing means that
// dialect's rendering changed rather than that this construct did
// (stokaro/ptah#941).
func TestInsertSelect_RendersOnEveryDialect(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{
			dialect: "postgres",
			want:    `INSERT INTO "archive" ("id", "name") SELECT "id", "name" FROM "users" WHERE "age" < $1`,
		},
		{
			dialect: "mysql",
			want:    "INSERT INTO `archive` (`id`, `name`) SELECT `id`, `name` FROM `users` WHERE `age` < ?",
		},
		{
			dialect: "sqlite",
			want:    `INSERT INTO "archive" ("id", "name") SELECT "id", "name" FROM "users" WHERE "age" < ?`,
		},
		{
			dialect: "sqlserver",
			want:    "INSERT INTO [archive] ([id], [name]) SELECT [id], [name] FROM [users] WHERE [age] < @p1",
		},
		{
			dialect: "clickhouse",
			want:    "INSERT INTO `archive` (`id`, `name`) SELECT `id`, `name` FROM `users` WHERE `age` < ?",
		},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, args, err := renderer.RenderInsert(archiveFromUsers().Build(), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
			// The query's own bound values travel with it.
			c.Assert(args, qt.DeepEquals, []any{18})
		})
	}
}

// TestInsertSelect_RefusesAContradictoryOrUncheckableSource covers the three
// shapes that are refused, each for a different reason.
func TestInsertSelect_RefusesAContradictoryOrUncheckableSource(t *testing.T) {
	tests := []struct {
		name    string
		build   func() *query.InsertBuilder
		message string
	}{
		{
			// VALUES and SELECT are alternative row sources in the grammar.
			// Choosing one silently would insert rows the caller did not ask
			// for.
			name: "both sources",
			build: func() *query.InsertBuilder {
				return query.InsertInto("t").Columns("a").Values(1).
					FromSelect(query.Select("x").From("u"))
			},
			message: "they are alternatives",
		},
		{
			// The server enforces this too, but its message names neither side.
			name: "projection arity",
			build: func() *query.InsertBuilder {
				return query.InsertInto("t").Columns("a", "b").
					FromSelect(query.Select("x").From("u"))
			},
			message: "projects 1 column(s) for 2 target column(s)",
		},
		{
			// A star supplies whatever the source table has today, so a
			// statement that matches now breaks when someone adds a column
			// there, with nothing in this statement changed.
			name: "star projection",
			build: func() *query.InsertBuilder {
				return query.InsertInto("t").Columns("a").FromSelect(query.Select().From("u"))
			},
			message: "must project its columns explicitly",
		},
		{
			name: "neither source",
			build: func() *query.InsertBuilder {
				return query.InsertInto("t").Columns("a")
			},
			message: "requires at least one row or a SELECT source",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, _, err := renderer.RenderInsert(test.build().Build(), "postgres")

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.message)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestInsertSelect_ComposesWithUpsertAndReturning keeps the clauses in the
// order the engines require, and pins that the query's values are numbered
// before any clause that follows it.
func TestInsertSelect_ComposesWithUpsertAndReturning(t *testing.T) {
	c := qt.New(t)
	stmt := query.InsertInto("archive").Columns("id", "name").
		FromSelect(query.Select("id", "name").From("users").Where(query.Lt("age", 18))).
		OnConflictDoUpdate([]string{"id"}, "name").
		Returning("id").
		Build()

	sql, args, err := renderer.RenderInsert(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals,
		`INSERT INTO "archive" ("id", "name") SELECT "id", "name" FROM "users" WHERE "age" < $1 `+
			`ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name" RETURNING "id"`)
	c.Assert(args, qt.DeepEquals, []any{18})
}

// TestInsertSelect_ValuesStillWork is the control: without FromSelect the
// statement is what it always was, so the rows above measure the new source
// rather than the INSERT.
func TestInsertSelect_ValuesStillWork(t *testing.T) {
	c := qt.New(t)
	stmt := query.InsertInto("t").Columns("a", "b").Values(1, "x").Build()

	sql, args, err := renderer.RenderInsert(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `INSERT INTO "t" ("a", "b") VALUES ($1, $2)`)
	c.Assert(args, qt.DeepEquals, []any{1, "x"})
}
