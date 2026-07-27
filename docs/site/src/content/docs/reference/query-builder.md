---
title: Query Builder
description: Build parameterized, dialect-aware SELECT statements with the core/query package.
---

Ptah's `core/query` package is a fluent builder for parameterized `SELECT`
statements. It is the DML counterpart to the DDL AST: a builder produces an
`*ast.SelectStatement`, and `renderer.RenderSelect` turns that into a SQL string
plus its positional arguments for PostgreSQL, MySQL, MariaDB, and SQLite.

This is a bounded slice of the DML work in issue
[`#98`](https://github.com/stokaro/ptah/issues/98). It exists so callers can stop
hand-rolling dynamic `WHERE` / `ORDER BY` / `IN (…)` clauses with manual
placeholder counters.

## Scope

Implemented so far:

- `SELECT` with an explicit column list or `*`, and `SELECT DISTINCT`;
- `INNER`, `LEFT`, `RIGHT`, and `FULL OUTER` joins, with table aliases and
  columns qualified by a table or alias (see [Joins](#joins));
- a composable `WHERE` (and join `ON`) expression tree: `=`, `<>`, `<`, `<=`,
  `>`, `>=`, `IN`, `IS NULL`, `IS NOT NULL`, and the boolean combinators `AND`,
  `OR`, `NOT`;
- aggregate functions — `COUNT(*)`, `COUNT`, `COUNT(DISTINCT …)`, `SUM`, `AVG`,
  `MIN`, `MAX` — in the projection and in `HAVING` (see
  [Aggregates, GROUP BY, and HAVING](#aggregates-group-by-and-having));
- `GROUP BY` (bare or qualified columns) and `HAVING`;
- `ORDER BY` with per-column `ASC`/`DESC`;
- `LIMIT` and `OFFSET`.

Not yet implemented (follow-up phases): non-aggregate function calls, arithmetic,
`LIKE`, subqueries, window functions, and the `INSERT`/`UPDATE`/`DELETE` family.

## Safety model

The builder keeps identifiers and values in separate lanes, so the classic
"concatenate a value into SQL" injection cannot happen through this API:

- **Values are always bound.** Arguments to `Eq`, `In`, and the other
  comparison helpers are typed as `any` and travel to the database as bound
  parameters. They are never interpolated into the SQL text. `LIMIT` and
  `OFFSET` values are bound the same way. The renderer emits the dialect's
  placeholder (`$1`, `$2`, … for PostgreSQL; `?` for MySQL, MariaDB, and SQLite)
  and returns the values in a matching `[]any`.
- **Identifiers are always quoted.** Table and column names are emitted through
  dialect-aware identifier quoting, so an attacker-shaped identifier cannot
  terminate the quoted identifier and inject SQL. As with Ptah's DDL rendering,
  deciding *which* identifiers a caller may supply — for example, an allow-list
  of sortable columns — remains the caller's responsibility. The builder
  guarantees quoting and the absence of value interpolation.

Placeholder numbering is assigned by the renderer in a single left-to-right
pass, so argument order always matches placeholder order and callers never
manage indices by hand.

## Usage

```go
import (
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/query"
	"github.com/stokaro/ptah/core/renderer"
)

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

sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
```

The PostgreSQL output is:

```sql
SELECT "id", "name" FROM "commodities"
WHERE ("draft" = $1 AND "status" IN ($2, $3)
       AND ("deleted_at" IS NOT NULL OR NOT ("count" > $4)))
ORDER BY "name" ASC, "id" ASC
LIMIT $5 OFFSET $6
```

with `args` equal to `[]any{false, "in_use", "sold", int64(10), int64(24), int64(0)}`.

The same statement rendered with `platform.MySQL` uses backtick-quoted
identifiers and `?` placeholders; `platform.SQLite` uses double-quoted
identifiers and `?` placeholders. The argument slice is identical across
dialects.

## Shared WHERE fragments

Expression constructors return plain expression nodes, so a filter can be built
once and attached to more than one statement — for example, the paged list and
the `COUNT(*)` that share the same filter:

```go
filter := query.And(query.Eq("draft", false), query.Eq("tenant_id", tenantID))

page := query.Select("id", "name").From("commodities").
	Where(filter).OrderBy(query.Asc("name")).Limit(20).Offset(0).Build()

total := query.Select("id").From("commodities").
	Where(filter).Build()
```

## Joins

Alias the source table with `FromAs`, add joins with `InnerJoin`, `LeftJoin`,
`RightJoin`, or `FullJoin`, and qualify columns with `Col` so they render as
`"alias"."col"`. A qualified column works everywhere a column is accepted: the
projection (via `.Columns`), the join `ON` condition, `WHERE`, and `ORDER BY`.

A join `ON` is an ordinary expression, so an equi-join is
`Col(left).EqCol(Col(right))` and richer predicates compose with `And`, `Or`, and
`Not`. `Col(table, name)` also carries the comparison helpers (`Eq`, `Ne`, `Lt`,
`Le`, `Gt`, `Ge`, `IsNull`, `IsNotNull`) and the ordering helpers (`Asc`, `Desc`)
for the qualified column.

```go
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

sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
```

The PostgreSQL output is:

```sql
SELECT "u"."id", "u"."name", "o"."total"
FROM "users" "u"
INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id"
WHERE ("o"."status" = $1 AND "u"."active" = $2)
ORDER BY "u"."name" ASC
LIMIT $3
```

with `args` equal to `[]any{"paid", true, int64(20)}`. Tables render as
`table alias` (no `AS`), which every supported dialect accepts. A value inside a
join `ON` is bound **before** any `WHERE` value, because joins render first — so
placeholder numbering still follows left-to-right emission order across `ON`,
`WHERE`, and `LIMIT`/`OFFSET`.

### Join-type support by dialect

Not every dialect can express every join type. `RenderSelect` rejects an
unsupported join at render time — returning a clear error — rather than emit SQL
that fails at execution time against the database.

| Join type | PostgreSQL family | MySQL / MariaDB | SQLite |
| --- | --- | --- | --- |
| `INNER` | yes | yes | yes |
| `LEFT` | yes | yes | yes |
| `RIGHT` | yes | yes | no (added in 3.39) |
| `FULL OUTER` | yes | no (never supported) | no (added in 3.39) |

- **SQLite** gained `RIGHT` and `FULL OUTER JOIN` only in version 3.39 (2022).
  Because Ptah targets a range of SQLite versions and cannot assume 3.39+, both
  are rejected (`renderer: SQLite does not support RIGHT JOIN`).
- **MySQL and MariaDB** have no `FULL [OUTER] JOIN` in any version — it must be
  emulated with a `UNION` of a `LEFT` and a `RIGHT` join — so `FULL` is rejected
  (`renderer: mysql does not support FULL OUTER JOIN`). `RIGHT` renders normally.
- The **PostgreSQL family** (including CockroachDB and YugabyteDB) supports all
  four.

## Aggregates, GROUP BY, and HAVING

`Distinct()` renders `SELECT DISTINCT`. `GroupBy` adds `GROUP BY` columns — pass
`Col(table, name)` for a qualified column across joins, or `Col("", name)` for a
bare column. GROUP BY carries only identifiers, so it never binds a placeholder.

Aggregates are built with `CountStar`, `Count`, `CountDistinct`, `Sum`, `Avg`,
`Min`, and `Max` (bare-column free functions), or the matching methods on a
qualified column: `Col("o", "total").Sum()`, `Col("u", "id").Count()`, and so on.
Each returns an expression usable in two places:

- **the projection**, via `Exprs` (no alias) or `ExprAs` (with an `AS` alias);
- **a `HAVING` predicate**, by wrapping it with `Expr` to reach the comparison
  helpers — `Expr(query.CountStar()).Gt(int64(5))` — which compose with `And`,
  `Or`, and `Not` like any other expression.

A function name (`COUNT`, `SUM`, …) is a keyword emitted verbatim and never
quoted; its column arguments are quoted, and any value it is compared against is
bound. The renderer rejects a function name that is not a simple identifier
rather than emit it.

```go
stmt := query.Select("status").
	ExprAs(query.CountStar(), "n").
	From("orders").
	Where(query.Eq("tenant_id", tenantID)).
	GroupBy(query.Col("", "status")).
	Having(query.Expr(query.CountStar()).Gt(int64(5))).
	OrderBy(query.Asc("status")).
	Limit(10).
	Build()

sql, args, err := renderer.RenderSelect(stmt, platform.Postgres)
```

The PostgreSQL output is:

```sql
SELECT "status", COUNT(*) AS "n"
FROM "orders"
WHERE "tenant_id" = $1
GROUP BY "status"
HAVING COUNT(*) > $2
ORDER BY "status" ASC
LIMIT $3
```

with `args` equal to `[]any{tenantID, int64(5), int64(10)}`. A `HAVING` value is
bound **after** every `WHERE` value and **before** `LIMIT`/`OFFSET`, so
placeholder numbering still follows left-to-right emission order across `WHERE`,
`HAVING`, and `LIMIT`/`OFFSET`.

Aggregates work over qualified columns in join queries too:

```go
stmt := query.Select().
	Columns(query.Col("u", "name")).
	ExprAs(query.Col("o", "id").Count(), "orders").
	ExprAs(query.Col("o", "total").Sum(), "spent").
	FromAs("users", "u").
	InnerJoin("orders", "o", query.Col("o", "user_id").EqCol(query.Col("u", "id"))).
	GroupBy(query.Col("u", "name")).
	Build()

// SELECT "u"."name", COUNT("o"."id") AS "orders", SUM("o"."total") AS "spent"
// FROM "users" "u" INNER JOIN "orders" "o" ON "o"."user_id" = "u"."id"
// GROUP BY "u"."name"
```

## Builder reference

| Function | Result |
| --- | --- |
| `Select(cols ...string)` | Start a builder; `"*"` or no columns selects all. |
| `.Distinct()` | Render `SELECT DISTINCT`. |
| `.Columns(cols ...Column)` | Append qualified columns (from `Col`) to the projection. |
| `.Exprs(exprs ...ast.Expression)` | Append expression projections (for example aggregates). |
| `.ExprAs(expr, alias)` | Append one expression projection with an `AS` alias. |
| `.From(table)` | Set the source table (required); clears any alias. |
| `.FromAs(table, alias)` | Set the source table with an alias. |
| `.InnerJoin` / `.LeftJoin` / `.RightJoin` / `.FullJoin` `(table, alias, on)` | Append a join with an `ON` condition. |
| `.Where(expr)` | Set the filter expression; a later call replaces the earlier one. |
| `.GroupBy(cols ...Column)` | Append `GROUP BY` columns across calls. |
| `.Having(expr)` | Set the `HAVING` predicate; a later call replaces the earlier one. |
| `.OrderBy(terms ...)` | Append sort terms across calls. |
| `.Limit(n)` / `.Offset(n)` | Set bound row limit/offset. |
| `.Build()` | Produce the `*ast.SelectStatement`. |

Expression helpers: `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `In`, `IsNull`,
`IsNotNull`, `And`, `Or`, `Not`. Ordering helpers: `Asc`, `Desc`. Aggregate
helpers: `CountStar`, `Count`, `CountDistinct`, `Sum`, `Avg`, `Min`, `Max`, and
`Expr(expr)` with `.Eq`/`.Ne`/`.Lt`/`.Le`/`.Gt`/`.Ge` for `HAVING` comparisons.
Qualified columns: `Col(table, name)`, with `.Eq`/`.Ne`/`.Lt`/`.Le`/`.Gt`/`.Ge`,
`.EqCol` (column-to-column, for `ON`), `.IsNull`/`.IsNotNull`, `.Asc`/`.Desc`, and
the aggregate methods `.Count`/`.CountDistinct`/`.Sum`/`.Avg`/`.Min`/`.Max`.

`renderer.RenderSelect(stmt, dialect)` returns `(sql string, args []any, err error)`.
It returns an error for an unsupported dialect, a missing `FROM` table, an empty
`IN` list, or a malformed statement.

### OFFSET without LIMIT

MySQL, MariaDB, and SQLite only accept `OFFSET` as a suffix of `LIMIT`, so
setting `Offset` without `Limit` renders a dialect-specific "no limit" sentinel
in front of the bound `OFFSET`: `LIMIT -1` for SQLite and
`LIMIT 18446744073709551615` for MySQL and MariaDB. PostgreSQL accepts a bare
`OFFSET` and emits one. The sentinel is a structural constant, not caller data,
so it is emitted as a literal and the `OFFSET` value stays a bound parameter.
