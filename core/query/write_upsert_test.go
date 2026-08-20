package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// upsertInsert is the statement every row below renders: two columns, one row,
// so the cells differ only where the dialects differ.
func upsertInsert() *query.InsertBuilder {
	return query.InsertInto("users").Columns("id", "email").Values(int64(1), "a@b")
}

// TestUpsert_RendersTheDialectsOwnSpelling is the acceptance case.
//
// PostgreSQL and SQLite name the constraint and read the proposed row as
// `excluded`; MySQL and MariaDB read it with VALUES(). The rows are written out
// rather than derived, because the two spellings are different statements and
// not one statement quoted differently (stokaro/ptah#941).
func TestUpsert_RendersTheDialectsOwnSpelling(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		build   func() *query.InsertBuilder
		want    string
	}{
		{
			name:    "postgres do update",
			dialect: "postgres",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoUpdate([]string{"email"}, "email") },
			want: `INSERT INTO "users" ("id", "email") VALUES ($1, $2) ` +
				`ON CONFLICT ("email") DO UPDATE SET "email" = excluded."email"`,
		},
		{
			name:    "postgres do nothing",
			dialect: "postgres",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoNothing("email") },
			want:    `INSERT INTO "users" ("id", "email") VALUES ($1, $2) ON CONFLICT ("email") DO NOTHING`,
		},
		{
			name:    "postgres do nothing without a target",
			dialect: "postgres",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoNothing() },
			want:    `INSERT INTO "users" ("id", "email") VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		},
		{
			name:    "sqlite do update",
			dialect: "sqlite",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoUpdate([]string{"email"}, "email") },
			want: `INSERT INTO "users" ("id", "email") VALUES (?, ?) ` +
				`ON CONFLICT ("email") DO UPDATE SET "email" = excluded."email"`,
		},
		{
			name:    "mysql do update without a target",
			dialect: "mysql",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoUpdate(nil, "email") },
			want: "INSERT INTO `users` (`id`, `email`) VALUES (?, ?) " +
				"ON DUPLICATE KEY UPDATE `email` = VALUES(`email`)",
		},
		{
			// MySQL has no DO NOTHING. Holding the first column steady is the
			// documented idiom and keeps the row untouched; INSERT IGNORE would
			// also swallow unrelated errors such as a type conversion.
			name:    "mysql do nothing holds a column steady",
			dialect: "mysql",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoNothing() },
			want: "INSERT INTO `users` (`id`, `email`) VALUES (?, ?) " +
				"ON DUPLICATE KEY UPDATE `id` = `id`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, args, err := renderer.RenderInsert(test.build().Build(), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
			// The clause names identifiers only; the row's values stay bound.
			c.Assert(args, qt.DeepEquals, []any{int64(1), "a@b"})
		})
	}
}

// TestUpsert_RefusesWhatTheEngineCannotMean is the half that matters most.
//
// Each row is a combination the engine cannot express, and each is refused
// rather than rendered into something that runs and means something else.
func TestUpsert_RefusesWhatTheEngineCannotMean(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		build   func() *query.InsertBuilder
		message string
	}{
		{
			// The one that silently corrupts. ON DUPLICATE KEY UPDATE fires for
			// EVERY unique key, so honoring the clause while dropping the target
			// would overwrite on keys the caller never named -- correct in a test
			// with one unique index, wrong the day a second one exists.
			name:    "mysql cannot scope to a target",
			dialect: "mysql",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoUpdate([]string{"email"}, "email") },
			message: "cannot scope an upsert to named columns",
		},
		{
			name:    "mariadb cannot scope to a target",
			dialect: "mariadb",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoNothing("email") },
			message: "cannot scope an upsert to named columns",
		},
		{
			// A bare ON CONFLICT DO UPDATE is a syntax error on both engines
			// that have the clause: the server cannot know which index's
			// collision the SET applies to.
			name:    "postgres needs a target for do update",
			dialect: "postgres",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoUpdate(nil, "email") },
			message: "requires a conflict target for DO UPDATE",
		},
		{
			name:    "sqlite needs a target for do update",
			dialect: "sqlite",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoUpdate(nil, "email") },
			message: "requires a conflict target for DO UPDATE",
		},
		{
			name:    "sql server has no on conflict",
			dialect: "sqlserver",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoNothing() },
			message: "expresses an upsert as MERGE",
		},
		{
			name:    "clickhouse has no upsert statement",
			dialect: "clickhouse",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoNothing() },
			message: "deduplicates in the background rather than at insert time",
		},
		{
			name:    "an update that updates nothing",
			dialect: "postgres",
			build:   func() *query.InsertBuilder { return upsertInsert().OnConflictDoUpdate([]string{"email"}) },
			message: "updates no column",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, _, err := renderer.RenderInsert(test.build().Build(), test.dialect)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.message)
			// A refused statement renders nothing, so a caller cannot execute a
			// half-built one.
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestUpsert_AbsentClauseChangesNothing is the control: without an upsert the
// statement is what it always was, so the rows above measure the clause rather
// than the INSERT.
func TestUpsert_AbsentClauseChangesNothing(t *testing.T) {
	c := qt.New(t)

	sql, _, err := renderer.RenderInsert(upsertInsert().Build(), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `INSERT INTO "users" ("id", "email") VALUES ($1, $2)`)
	c.Assert(sql, qt.Not(qt.Contains), "ON CONFLICT")
}

// TestUpsert_ComposesWithReturning keeps the two optional clauses in the order
// the engines require: ON CONFLICT before RETURNING.
func TestUpsert_ComposesWithReturning(t *testing.T) {
	c := qt.New(t)
	stmt := upsertInsert().OnConflictDoUpdate([]string{"email"}, "email").Returning("id").Build()

	sql, _, err := renderer.RenderInsert(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `INSERT INTO "users" ("id", "email") VALUES ($1, $2) `+
		`ON CONFLICT ("email") DO UPDATE SET "email" = excluded."email" RETURNING "id"`)
}
