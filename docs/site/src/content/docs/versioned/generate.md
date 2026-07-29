---
title: Generate migrations
description: Plan and generate migration files from the difference between your desired schema and a live database.
---

You have a desired schema — Go annotations, YAML, HCL, or SQL files — and a
database that should follow it. This page shows how to preview the migration
SQL, generate reviewed up/down files, and seal them, plus how to author a
migration by hand when generation is not the right tool.

Prerequisites: a built `ptah` binary and a schema source. The examples use a
Go model and a local SQLite file so they run without a daemon; substitute your
own `--db-url`.

## Starting state

One annotated model in `./models`, an empty `./migrations` directory, and an
empty target database:

```go
package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="INTEGER" primary="true" auto_increment="true" not_null="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string

	//migrator:schema:field name="name" type="TEXT"
	Name string
}
```

## Preview the SQL with plan

`ptah migrations plan` resolves the desired schema, reads the live database,
and prints the migration SQL with a safety classification — without writing
any files:

```bash
ptah migrations plan \
  --root-dir ./models \
  --db-url "sqlite://app.db"
```

Expected output includes:

```text
Safety classification:
  #  severity      subject                  reason
  1  safe         *ast.CreateTableNode     does not remove data or tighten constraints
=== MIGRATION SQL ===

CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT
);

Generated 1 migration statements.
```

Because the database is empty, the difference is the whole schema. On a
migrated database the plan contains only the delta.

## Generate the migration files

```bash
ptah migrations generate \
  --root-dir ./models \
  --db-url "sqlite://app.db" \
  --migrations-dir ./migrations \
  --name init
```

Expected output includes:

```text
Generated migration files for sqlite://app.db:
UP:   .../migrations/1785255952_init.up.sql
DOWN: .../migrations/1785255952_init.down.sql
```

The version prefix is a timestamp; `--name` becomes the description in the
file name. Ptah writes both directions — the generated down file reverses the
up file:

```sql
-- Migration rollback
-- Direction: DOWN

-- WARNING: This will delete all data!
DROP TABLE IF EXISTS "users";
```

When the desired schema and the database already match, `generate` writes
nothing and exits `0`.

## Hash the directory

Generation does not update the integrity file; sealing the directory is an
explicit step so the hash always reflects what you reviewed:

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

Expected output includes:

```text
Wrote ./migrations/ptah.sum
2 migration file(s) hashed
OK: migrations directory matches ptah.sum
```

Commit the migration pair and `ptah.sum` together, then continue with
[Apply migrations](../apply/).

## Generate from a composite desired schema

`plan` and `generate` resolve the same composite desired schema as
`ptah schema render` and `ptah schema compare`. Repeat `--root-dir` and
`--schema-file` in any combination:

```bash
ptah migrations generate \
  --root-dir ./common \
  --root-dir ./services/orders \
  --schema-file ./vendor/billing.hcl \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations
```

Ptah merges the sources before reading the live schema. Identical named
objects are deduplicated; a same-identity object with different desired
properties is a conflict, and no migration files are written. See
[Composite desired schema](../../schema/composite/) for source identity and
conflict rules.

## Verify on a shadow database

Add `--shadow-db` to replay the whole directory — including the new migration
— on a disposable
**[shadow database](../../concepts/database-urls-and-dev-databases/)** before
any files are kept. The shadow
database is dropped clean, migrated up, rolled back one step, and migrated up
again, so both directions of the new migration are proven executable:

```bash
ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --name add_posts \
  --shadow-db "sqlite://shadow.db"
```

Expected output includes the replay log before the generated files:

```text
INFO Applying migration version=1785255953 description=add_posts
INFO Rolling back migration version=1785255953 description=add_posts
INFO Applying migration version=1785255953 description=add_posts
Generated migration files for ...
```

The shadow database must be an ephemeral database of the same engine as the
target — never a real environment.

## Write a migration by hand

Create an empty pair when you want to author the SQL yourself — a data
backfill, an index rebuild, or anything the schema diff cannot express:

```bash
ptah migrations create add_invoices --migrations-dir ./migrations
```

Expected output includes:

```text
Generated empty migration files:
UP:   .../migrations/1785255954_add_invoices.up.sql
DOWN: .../migrations/1785255954_add_invoices.down.sql
```

Edit both files, then re-run `ptah migrations hash`. Keep the rollback real
even if the first consumer only applies migrations forward; the down half is
part of Ptah's migration contract. Pass `--edit` to open the created pair in
`$VISUAL`/`$EDITOR` immediately.

## Failure modes

**Destructive changes are classified and can be gated.** Removing a column or
table from the desired schema produces a plan whose classification names the
data risk:

```text
Safety classification:
  #  severity      subject                  reason
  2  safe         *ast.CreateTableNode     does not remove data or tighten constraints
  4  destructive  users                    DROP TABLE removes the table and all rows
  5  warning      *ast.RawSQLNode          rename can break deployed readers and writers
```

With `--check-destructive` set, `plan` and `generate` refuse to proceed and
exit `2`:

```text
error: destructive migration statements require AllowDestructive
```

Review the plan, then rerun with `--allow-destructive` to accept it. Use
`--report json` when CI needs the safety classification as structured data
instead of text — the GitHub Action's destructive-change check run is driven
by exactly that report ([CI](../../testing/ci/)). The apply-time gate on
`ptah migrations up` is separate — see
[Integrity and safety](../integrity-and-safety/).

**Conflicting sources stop generation.** When two schema sources disagree
about the same database object, Ptah fails with a
`conflicting field ... definitions` error before connecting to the database;
see [Composite desired schema](../../schema/composite/).

## Next steps

- Ready to run the files against a database? [Apply migrations](../apply/).
- Sealing and linting before they reach a shared environment?
  [Integrity and safety](../integrity-and-safety/).
- Need to reorder or rewrite a migration before it ships?
  [Maintain migration history](../maintain-history/).
