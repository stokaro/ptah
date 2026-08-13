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

## Virtual tables

A virtual table is read as a virtual table, not as an ordinary one.
`ptah db read` emits the statement that created it:

```bash
ptah db read --db-url "sqlite://app.db"
```

```sql
CREATE VIRTUAL TABLE "docs" USING fts5(title, body);
```

On the compatibility surface the SQL format has to be asked for —
`ptah-compat schema inspect --url "sqlite://app.db" --format '{{ sql . }}'`.
Without it, `schema inspect` returns HCL, as the community CLI does, and HCL
has no virtual-table block: the table renders as
`table "docs" { schema = schema.main }` with no columns, which is what the
pinned community binary emits for the same object and which does not replay.

The module name and the text between its parentheses are carried verbatim, so
tokenizer options, quoted values and commas inside quoted arguments survive.
Applying that output to an empty database recreates the same object — a
full-text index that answers `MATCH`, not a plain table of the same name.

Nothing in the reader names a module. `PRAGMA table_list` classifies every
table as `table`, `virtual` or `shadow`, so `fts3`, `fts4`, `fts5`, `rtree`,
`rtree_i32`, `geopoly`, `fts5vocab`, `dbstat` and any module a build registers
are all read the same way.

The shadow tables a module maintains — `docs_data`, `docs_idx`, `docs_config`
and their siblings — are not reported at all. They are the module's own
bookkeeping, and applying a `CREATE TABLE` for one creates a table SQLite
would have created itself, which then collides when the virtual table is
created. Suppression comes from SQLite's classification rather than from the
names, so a `docs_data` an operator created is still reported as their table.

## Virtual tables in a comparison

No desired-state source can declare a virtual table. Go annotations, HCL, YAML
and `.sql` schema files have no syntax for one, and the native SQL schema
parser says so out loud: feeding it `ptah db read` output for a database
holding a virtual table fails with `unsupported CREATE target: VIRTUAL`.

A comparison therefore has two ways to be wrong about one, and Ptah refuses
both rather than planning them:

- **Absent from the desired state.** Read as intent, that plans
  `DROP TABLE "docs"` and deletes the index and its contents. The desired state
  could not have asked for the table to be kept, so the removal is refused and
  the table and its module are named.
- **Present in the desired state**, which it can only be as an ordinary table.
  Two different kinds of object have collided; the planner cannot convert one
  into the other, and `ALTER TABLE ... ADD COLUMN` is not something SQLite
  accepts on a virtual table. Refused rather than reported as no difference.

Every verb that compares a live database is covered — `ptah schema apply`,
`diff`, `compare`, `plan`, `drift`, and `ptah-compat schema diff`,
`schema apply` and `migrate diff`. Reading is untouched: `ptah db read` and
`ptah-compat schema inspect` compare nothing.

Say which one you meant to proceed:

```bash
# keep it: both sides ignore the table, the rest converges
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --exclude docs

# drop it: plans DROP TABLE, destroying the index and the module's shadow tables
PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP=1 \
  ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql
```

An unset variable and an explicit false both keep the refusal; a value that is
not a boolean is a configuration error. The opt-in covers only the removal — a
desired ordinary table colliding with a live virtual one stays refused however
it is set.

## Virtual table limitations

- Shadow tables belonging to a module the reading build does not register
  cannot be identified, because only that module knows which suffixes are its
  own. SQLite reports them as ordinary tables and so does Ptah. The virtual
  table itself is still recognized and still round-trips. This is permanent
  because it is SQLite's own answer: no catalog field distinguishes a shadow
  table without the module.
- Desired state cannot declare a virtual table, so a comparison can only refuse
  or be scoped past one; it can never converge a virtual table's definition.
  Declaring virtual tables in desired state is tracked in
  [#1028](https://github.com/stokaro/ptah/issues/1028).

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
