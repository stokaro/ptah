---
title: Go schema workflow
description: Use annotated Go structs as the desired database schema.
---

Use this workflow when your Go application owns the schema and the database
should follow annotated model types. Ptah reads comments, not runtime Go tags,
so the model remains ordinary Go code.

## When to use it

| Use Go annotations when | Use another source when |
| --- | --- |
| The application structs already describe the domain. | A database team owns SQL or HCL directly. |
| You want code review to cover schema changes next to model changes. | You need an HCL schema construct Ptah has not implemented yet. |
| You want generated migrations from desired/live differences. | You only need to apply an existing migration directory. |

## Model the schema

Ptah scans Go packages for table annotations and field tags:

```go
package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string

	//migrator:schema:field name="first_name" type="TEXT"
	FirstName string

	//migrator:schema:field name="last_name" type="TEXT"
	LastName string
}
```

Render the desired SQL before connecting to a database:

```bash
ptah schema render --root-dir ./models --dialect postgres
```

Smoke-check the command before you involve a live database:

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

Review the plan output before generating files. Destructive changes should be explicit and gated in CI.

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

## Keep generated schema reviewable

When a model change is surprising, render more than one dialect:

```bash
ptah schema render --root-dir ./models --dialect postgres >/tmp/schema.pg.sql
ptah schema render --root-dir ./models --dialect mysql >/tmp/schema.mysql.sql
```

This catches annotations that are valid but map differently across dialects,
such as enum storage, serial columns, constraints, or generated columns.

## Keep references close

- Full native command tree: [Commands](../../reference/commands/).
- HCL authoring and migration path: [Schema files](../schema-files/) and
  [schema export](../api-schema-export/).
- Programmatic parser usage: [Public API](../../reference/public-api/).
