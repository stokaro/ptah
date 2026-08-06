---
title: SQLite
description: SQLite in Ptah - URL forms, the supported DDL surface, and which schema changes require a table rebuild.
---

SQLite is the engine to reach for when you want Ptah without a database
daemon: the quick start, most documentation examples, and the declarative
test runner's default databases all use it. Ptah uses a pure-Go driver, so no
CGO is required. SQLite's migration semantics differ deliberately from the
server engines — this page covers what renders natively and which changes
require a table rebuild.

## URLs

These URL forms are accepted:

```bash
sqlite://relative.db
sqlite:///absolute/path/app.db
sqlite:file:C:/absolute/windows/path/app.db
sqlite:///:memory:
sqlite:file:memdb1?mode=memory&cache=shared
```

Use forward slashes in the Windows file URI. The `file:` portion keeps the
drive-letter colon out of the URL host and lets reserved filename characters
be percent-encoded without changing the selected file.

Ptah adds `_pragma=foreign_keys(1)` unless the URL already supplies a
`foreign_keys` pragma, so declared foreign keys are enforced by default.
SQLite connections are limited to one open connection, which keeps in-memory
databases and connection-local `PRAGMA` state predictable.

## What renders natively

The SQLite renderer and planner support:

- `CREATE TABLE`, including the `STRICT` and `WITHOUT ROWID` table options,
  with inline `PRIMARY KEY`, `UNIQUE`, `CHECK`, and `FOREIGN KEY` constraints.
- Enum annotations as `TEXT` columns plus a generated
  `CHECK (<column> IN (...))` constraint.
- `CREATE INDEX`, including unique and partial indexes, and
  `DROP INDEX IF EXISTS` / `DROP TABLE IF EXISTS`.
- `ALTER TABLE ... ADD COLUMN`, `RENAME COLUMN`, and `RENAME TO`.
- Column and constraint changes ALTER TABLE cannot express, through a
  generated table-rebuild plan: create a rebuilt table in its desired shape,
  copy the retained columns, swap it in, and recreate the desired indexes and
  triggers when their metadata round-trips safely. One rebuild covers column
  drops, column type, nullability, default and generated-expression changes,
  added and removed table constraints (including enum-backed `CHECK`
  constraints), and a column drop combined with a column addition. A column the
  desired schema makes `NOT NULL` with a default is backfilled in the copy with
  `IFNULL(<column>, <default>)`, so rows already holding `NULL` survive.
- Views without `WITH CHECK OPTION`, and row-level triggers (SQLite has no
  statement-level triggers).

Introspection ignores SQLite system objects (names starting with `sqlite_`)
and Ptah's own revision table.

## Rebuild-required changes

Ptah's rebuild planning is intentionally conservative: where it cannot prove
the rebuild is safe, it reports an explicit rebuild-required error instead of
emitting unsafe or partial SQL. Changes that still report as rebuild-required
are:

- Adding a column, without any other change to the same table, in a shape
  SQLite cannot apply in place — a primary key, unique, or `AUTOINCREMENT`
  column, a `NOT NULL` column without a non-NULL literal default, an expression
  or parenthesized default, or a `STORED` generated column.
- Adding a `NOT NULL` column without a default as part of a rebuild: the copy
  step leaves the column out of the `INSERT`, so the first row would violate it.
- Rebuilding a table referenced by an inbound foreign key, whose retained
  triggers use syntax Ptah cannot round-trip, or whose rebuild scaffolding name
  `__ptah_rebuild_<table>` is already taken.
- A constraint change the diff cannot attribute to a table, so there is no
  table to rebuild.

Model such changes as a manual migration that performs the rebuild — see
[Generate migrations](../../versioned/generate/) for hand-written pairs.

PostgreSQL-only objects — extensions, materialized views, row-level security,
roles, grants, and `EXCLUDE` constraints — are rejected by the SQLite planner
rather than silently skipped.

## Next steps

- Running the whole loop on SQLite first: [Quick start](../../start/quick-start/).
- Testing against fresh ephemeral SQLite databases: [Test migrations and schemas](../../testing/migrations-and-schema/).
- Checking another engine's depth: [Database support matrix](../support-matrix/).
