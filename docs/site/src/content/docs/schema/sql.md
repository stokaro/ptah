---
title: SQL schema
description: Use plain SQL DDL files as Ptah's desired schema.
---

Use SQL schema files when the desired schema is already written as local DDL
(Data Definition Language). Ptah parses the file through its compatibility SQL
parser; unsupported DDL fails explicitly instead of being skipped.

## Write a schema file

Create `schema.sql`:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
```

## Render it

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
```

Expected output includes:

```sql
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL
);
```

Rendering SQL back out of a SQL file is not a no-op: it proves the parser
understood every statement, and it can retarget the schema at another dialect.
`--schema-file` is accepted wherever Ptah needs a desired schema:
`ptah schema render`, `ptah schema compare`, `ptah schema drift`, and the
migration commands (`ptah migrations plan` / `ptah migrations generate`).

## Diff two SQL files locally

The Atlas-compatible command surface compares local SQL files directly. With
`old.sql` describing the deployed shape and `schema.sql` adding a `pets`
table, a dev database replays both sides:

```bash
ptah atlas schema diff \
  --from file://old.sql \
  --to file://schema.sql \
  --dev-url "sqlite://dev?mode=memory"
```

Expected output includes:

```sql
CREATE TABLE "pets" (
  "id" INTEGER PRIMARY KEY,
  "name" TEXT NOT NULL,
  "user_id" INTEGER NOT NULL CONSTRAINT "fk_pets_user_id" REFERENCES "users" ("id")
);
```

## Failure modes

- A change that a SQLite dev database cannot express as an in-place `ALTER`
  is refused loudly rather than turned into an incomplete diff. For example,
  adding a `NOT NULL` column to an existing table exits with
  `sqlite: adding column email to table users requires a table rebuild plan`.
- Unsupported DDL constructs fail with a parse error naming the statement.
  Treat the error as a compatibility gap and check the conformance reports.

## Next steps

- Combining SQL files with Go packages or other sources? [Composite desired schema](../composite/).
- Planning versioned migrations from this file? [Generate migrations](../../versioned/generate/).
- Using Atlas-style commands end to end? [Atlas-compatible CLI](../../workflows/atlas-cli/).
