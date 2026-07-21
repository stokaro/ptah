---
title: Go model example
description: A tiny annotated Go model and the rendered SQL path.
---

This example shows the smallest Go-annotation source that is still useful in a
real project: a table, a primary key, a unique constraint, and a generated SQL
smoke check.

## Files

```text
models/
  account.go
migrations/
```

Create a model:

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

Render it from the project root:

```bash
ptah schema render --root-dir ./models --dialect postgres
```

Expected shape:

```sql
CREATE TABLE "accounts" (
  "id" SERIAL PRIMARY KEY NOT NULL,
  "email" TEXT UNIQUE NOT NULL
);
```

The exact type rendering depends on the selected dialect and field tags.

## Generate migration SQL

When a database is available, compare the desired Go model with the live state:

```bash
ptah migrations plan \
  --root-dir ./models \
  --db-url "$DATABASE_URL"
```

Generate files only after reviewing the plan:

```bash
ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations

ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

## Verify across dialects

Render multiple dialects when annotations are meant to be portable:

```bash
ptah schema render --root-dir ./models --dialect postgres >/tmp/accounts.pg.sql
ptah schema render --root-dir ./models --dialect sqlite >/tmp/accounts.sqlite.sql
```

Dialect differences are expected. The important check is that each target
renders valid SQL for the capabilities it supports.
