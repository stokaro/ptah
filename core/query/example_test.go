package query_test

import (
	"fmt"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/query"
)

// Example is the canonical first-use flow: build a SELECT fluently, compose the
// WHERE tree from And, Or, Not, and the comparison helpers, then hand the
// statement to RenderSelect for one dialect. Every value — including the IN
// list elements and the LIMIT/OFFSET bounds — comes back in args as a bound
// parameter, numbered in emission order; none of them appears in the SQL text.
func Example() {
	stmt := query.Select("id", "name").
		From("commodities").
		Where(query.And(
			query.Eq("draft", false),
			query.In("status", []string{"in_use", "sold"}),
			query.Or(
				query.IsNotNull("deleted_at"),
				query.Not(query.Gt("count", int64(10))),
			),
		)).
		OrderBy(query.Asc("name"), query.Asc("id")).
		Limit(24).
		Offset(0).
		Build()

	sql, args, err := query.RenderSelect(stmt, platform.Postgres)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// SELECT "id", "name" FROM "commodities" WHERE ("draft" = $1 AND "status" IN ($2, $3) AND ("deleted_at" IS NOT NULL OR NOT ("count" > $4))) ORDER BY "name" ASC, "id" ASC LIMIT $5 OFFSET $6
	// [false in_use sold 10 24 0]
}

// ExampleRenderSelect_dialects renders one statement for four dialects, which
// is where the per-dialect differences show up in one place: the placeholder
// styles ($1, ?, @p1, :1), the identifier quoting, and the pagination — SQL
// Server and Oracle page with OFFSET/FETCH instead of LIMIT, and only SQL
// Server needs the synthesized ORDER BY (SELECT NULL) in front of it.
func ExampleRenderSelect_dialects() {
	stmt := query.Select("id", "name").
		From("users").
		Where(query.Eq("id", int64(1))).
		Limit(10).
		Build()

	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.SQLServer, platform.Oracle} {
		sql, _ := must.Must2(query.RenderSelect(stmt, dialect))
		fmt.Printf("%s: %s\n", dialect, sql)
	}

	// Output:
	// postgres: SELECT "id", "name" FROM "users" WHERE "id" = $1 LIMIT $2
	// mysql: SELECT `id`, `name` FROM `users` WHERE `id` = ? LIMIT ?
	// sqlserver: SELECT [id], [name] FROM [users] WHERE [id] = @p1 ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT @p2 ROWS ONLY
	// oracle: SELECT id, name FROM users WHERE id = :1 OFFSET 0 ROWS FETCH NEXT :2 ROWS ONLY
}

// ExampleCol joins two aliased tables and qualifies every column with Col, so
// each renders as "alias"."col" with both parts quoted independently. The join
// ON condition is an ordinary expression — Col(…).EqCol is the equi-join
// predicate — and its bound values would be numbered before any WHERE value,
// because joins render first.
func ExampleCol() {
	stmt := query.Select().
		Columns(query.Col("u", "id"), query.Col("u", "name"), query.Col("o", "total")).
		FromAs("users", "u").
		InnerJoin("orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
		Where(query.And(
			query.Col("o", "status").Eq("paid"),
			query.Col("u", "active").Eq(true),
		)).
		OrderBy(query.Col("u", "name").Asc()).
		Limit(20).
		Build()

	sql, args := must.Must2(query.RenderSelect(stmt, platform.Postgres))
	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// SELECT "u"."id", "u"."name", "o"."total" FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id" WHERE ("o"."status" = $1 AND "u"."active" = $2) ORDER BY "u"."name" ASC LIMIT $3
	// [paid true 20]
}

// ExampleInsertInto builds a multi-row INSERT: one Values call per row, values
// numbered left to right, row by row, so args line up with the placeholders.
// Returning projects columns from the inserted rows on a dialect that can
// execute a RETURNING clause — here PostgreSQL.
func ExampleInsertInto() {
	stmt := query.InsertInto("users").
		Columns("id", "name").
		Values(int64(1), "alice").
		Values(int64(2), "bob").
		Returning("id").
		Build()

	sql, args := must.Must2(query.RenderInsert(stmt, platform.Postgres))
	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// INSERT INTO "users" ("id", "name") VALUES ($1, $2), ($3, $4) RETURNING "id"
	// [1 alice 2 bob]
}

// ExampleInsertBuilder_OnConflictDoUpdate renders one upsert twice. PostgreSQL
// requires the conflict target and overwrites the named columns from the
// proposed row (spelled excluded). MySQL cannot narrow ON DUPLICATE KEY UPDATE
// to a named index, so RenderInsert returns an error there rather than widen
// the statement to fire on every unique key the table happens to have. The
// error is the contract; its wording is not, so branch on the error being
// non-nil.
func ExampleInsertBuilder_OnConflictDoUpdate() {
	stmt := query.InsertInto("settings").
		Columns("key", "value").
		Values("theme", "dark").
		OnConflictDoUpdate([]string{"key"}, "value").
		Build()

	sql, args := must.Must2(query.RenderInsert(stmt, platform.Postgres))
	fmt.Println(sql)
	fmt.Println(args)

	_, _, err := query.RenderInsert(stmt, platform.MySQL)
	fmt.Println("refused on mysql:", err != nil)

	// Output:
	// INSERT INTO "settings" ("key", "value") VALUES ($1, $2) ON CONFLICT ("key") DO UPDATE SET "value" = excluded."value"
	// [theme dark]
	// refused on mysql: true
}

// ExampleUpdateBuilder_Unconditional shows the whole-table safety rule: a
// WHERE-less UPDATE is rejected at render time, so a forgotten filter cannot
// silently rewrite every row, and the same statement renders once the builder
// marks the missing filter deliberate. Only the refusal is contract; the
// message explaining it is free to change.
func ExampleUpdateBuilder_Unconditional() {
	forgotten := query.Update("sessions").Set("revoked", true).Build()
	_, _, err := query.RenderUpdate(forgotten, platform.Postgres)
	fmt.Println("refused without a WHERE:", err != nil)

	deliberate := query.Update("sessions").Set("revoked", true).Unconditional().Build()
	sql, args := must.Must2(query.RenderUpdate(deliberate, platform.Postgres))
	fmt.Println(sql)
	fmt.Println(args)

	// Output:
	// refused without a WHERE: true
	// UPDATE "sessions" SET "revoked" = $1
	// [true]
}
