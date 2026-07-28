---
title: Go annotations
description: Use annotated Go structs as the desired database schema.
---

Use Go annotations when your Go application owns the schema and the database
should follow annotated model types. Ptah reads comments, not runtime Go tags,
so the model remains ordinary Go code.

## When to use them

| Use Go annotations when | Use another source when |
| --- | --- |
| The application structs already describe the domain. | A database team owns SQL or HCL directly. |
| You want code review to cover schema changes next to model changes. | You need an HCL schema construct Ptah has not implemented yet. |
| You want generated migrations from desired/live differences. | You only need to apply an existing migration directory. |

## Model the schema

The smallest annotation source that is still useful in a real project is a
table, a primary key, and a unique constraint:

```text
models/
  account.go
migrations/
```

Create `models/account.go`:

```go
package models

//migrator:schema:table name="accounts"
type Account struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string
}
```

Render the desired SQL before connecting to a database:

```bash
ptah schema render --root-dir ./models --dialect postgres
```

Expected output includes:

```sql
CREATE TABLE "accounts" (
  "id" SERIAL PRIMARY KEY NOT NULL,
  "email" TEXT UNIQUE NOT NULL
);
```

The exact type rendering depends on the selected dialect and field tags. To
smoke-check without any daemon, render the SQLite dialect to a file:

```bash
ptah schema render --root-dir ./models --dialect sqlite >/tmp/ptah-schema.sql
sed -n '1,80p' /tmp/ptah-schema.sql
```

## Compare before changing data

For an existing database, inspect and compare first:

```bash
ptah db read --db-url "$DATABASE_URL"
ptah schema compare --root-dir ./models --db-url "$DATABASE_URL"
ptah migrations plan --root-dir ./models --db-url "$DATABASE_URL"
```

Review the plan output before generating files. Destructive changes should be
explicit and gated in CI.

## Generate and apply

```bash
ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations

ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations --verify-sum
```

For shared environments, add these guards:

```bash
ptah migrations validate --dir ./migrations
ptah migrations lint --dir ./migrations --dialect postgres
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --verify-sum \
  --dry-run
```

Run without `--dry-run` only after reviewing the generated SQL and committed
`ptah.sum`.

## Compose multiple sources

`--root-dir` is repeatable, so a desired schema can be assembled from several
Go packages, and the same commands mix Go roots with YAML, HCL, and SQL files
through repeatable `--schema-file`. The merge semantics — identity-based
deduplication, conflict detection, and per-root type ownership — are described
once on [Composite desired schema](../composite/).

## Verify across dialects

When a model change is surprising, or annotations are meant to be portable,
render more than one dialect:

```bash
ptah schema render --root-dir ./models --dialect postgres >/tmp/schema.pg.sql
ptah schema render --root-dir ./models --dialect mysql >/tmp/schema.mysql.sql
```

This catches annotations that are valid but map differently across dialects,
such as enum storage, serial columns, constraints, or generated columns.
Dialect differences are expected; the important check is that each target
renders valid SQL for the capabilities it supports.

## Next steps

- Looking up a directive or attribute? [Go annotation reference](../../reference/go-annotations/).
- Modeling in files instead of Go? [YAML schema](../yaml/), [HCL schema](../hcl/), or [SQL schema](../sql/).
- Embedding the parser in your own tool? [Public API](../../extend/public-api/).
