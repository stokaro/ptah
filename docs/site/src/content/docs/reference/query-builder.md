---
title: Query Builder
description: Build parameterized, dialect-aware SELECT statements with the core/query package.
---

Ptah's `core/query` package is a fluent builder for parameterized `SELECT`
statements. It is the DML counterpart to the DDL AST: a builder produces an
`*ast.SelectStatement`, and `renderer.RenderSelect` turns that into a SQL string
plus its positional arguments for PostgreSQL, MySQL, MariaDB, and SQLite.

This is the first, bounded slice of the DML work in issue
[`#98`](https://github.com/stokaro/ptah/issues/98). It exists so callers can stop
hand-rolling dynamic `WHERE` / `ORDER BY` / `IN (…)` clauses with manual
placeholder counters.

## Scope

Implemented in this phase:

- single-table `SELECT` with an explicit column list or `*`;
- a composable `WHERE` expression tree: `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`,
  `IS NULL`, `IS NOT NULL`, and the boolean combinators `AND`, `OR`, `NOT`;
- `ORDER BY` with per-column `ASC`/`DESC`;
- `LIMIT` and `OFFSET`.

Not yet implemented (follow-up phases): `JOIN`, `GROUP BY`, `HAVING`, `DISTINCT`,
subqueries, function calls, arithmetic, `LIKE`, and the `INSERT`/`UPDATE`/
`DELETE` family.

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

## Builder reference

| Function | Result |
| --- | --- |
| `Select(cols ...string)` | Start a builder; `"*"` or no columns selects all. |
| `.From(table)` | Set the single source table (required). |
| `.Where(expr)` | Set the filter expression; a later call replaces the earlier one. |
| `.OrderBy(terms ...)` | Append sort terms across calls. |
| `.Limit(n)` / `.Offset(n)` | Set bound row limit/offset. |
| `.Build()` | Produce the `*ast.SelectStatement`. |

Expression helpers: `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `In`, `IsNull`,
`IsNotNull`, `And`, `Or`, `Not`. Ordering helpers: `Asc`, `Desc`.

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
