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

## Compose multiple sources

`--root-dir` is repeatable, so a desired schema can be assembled from several Go
packages — a shared `common` package plus per-service tables, for example. Every
root is parsed, merged, and finalized together, so a table in one root can
reference a table in another:

```bash
ptah schema render \
  --root-dir ./common \
  --root-dir ./services/orders \
  --dialect postgres
```

The same repeatable `--root-dir` works on `ptah schema compare`,
`ptah migrations plan`, and `ptah migrations generate`. Identical definitions
across roots are deduplicated; two roots that define the same named object
differently are an error for tables, columns, indexes, constraints, enums,
extensions, functions, sequences, user-defined types, views, triggers, RLS
objects, and roles. Ptah resolves table-scoped identities before comparing
definitions, so different Go struct names do not hide a database-object
conflict.

Conflict checks use the same database-object identities within one Go root and
across several roots. Separate `--root-dir` values preserve each source
boundary's Go type ownership while Ptah reconciles those parser names before
merging. This allows two roots to use the same Go type name for different
schema-qualified tables without mixing their columns. Source-local embedded
helper types are scoped the same way, including nested helpers, so two roots may
each define a `Metadata` type without either table receiving the other root's
fields.
Managed-data annotations also retain the absolute directory of their declaring
Go source, so equal relative `file=` paths in different roots continue to load
the correct row files after the schema is merged.

This is Ptah's open, local, no-account equivalent of Atlas's Pro-only
`composite_schema` data source. The render, compare, and migration commands also
accept repeatable `--schema-file` and can mix Go roots with YAML, HCL, and SQL
schema files — see [Schema files](../schema-files/).

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
