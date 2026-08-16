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
Without it, `schema inspect` returns HCL, as the community CLI does, and
neither HCL nor JSON has a virtual-table construct: the table renders as
`table "docs" { schema = schema.main }` with no columns, which is what the
pinned community binary emits for the same object and which does not replay.

The document is not changed — matching the community binary is what the surface
is for — but the loss is reported rather than left silent:

- ordinarily, a note on standard error names each virtual table the rendering
  dropped and points at `--format '{{ sql . }}'`, leaving standard output and
  the exit code untouched;
- under `PTAH_ATLAS_STRICT_COMPAT=1` the same condition is refused, because
  strict mode owns the process output contract. The SQL format is unaffected.

The module name and the text between its parentheses are carried verbatim, so
tokenizer options, quoted values and commas inside quoted arguments survive.
Applying that output to an empty database recreates the same object — a
full-text index that answers `MATCH`, not a plain table of the same name.

Nothing in the reader names a module. `PRAGMA table_list` classifies every
table as `table`, `virtual` or `shadow`, so `fts5`, `rtree`, `rtree_i32`,
`geopoly`, `fts5vocab`, `dbstat` and any module a build registers are all read
the same way.

Which modules this build registers is asked rather than assumed. It answers
`PRAGMA module_list` with exactly seven — `dbstat`, `fts5`, `fts5vocab`,
`geopoly`, `rtree`, `rtree_i32`, `sqlite_dbpage` — and `fts3` and `fts4` are
not among them. See [Virtual table limitations](#virtual-table-limitations) for
what follows from that.

The shadow tables a module maintains — `docs_data`, `docs_idx`, `docs_config`
and their siblings — are not reported at all. They are the module's own
bookkeeping, and applying a `CREATE TABLE` for one creates a table SQLite
would have created itself, which then collides when the virtual table is
created. Suppression comes from SQLite's classification rather than from the
names, so a `docs_data` an operator created is still reported as their table.

An explicit user-created index on a shadow table is refused during the read.
Ptah cannot expose the module-owned table as an ordinary desired object, and
omitting the index would claim a complete schema that cannot be replayed.
SQLite's own autoindexes have no stored `CREATE INDEX` statement and remain
internal to the module.

## Virtual tables in a comparison

A desired state can name a virtual table two ways. A native `.sql` schema file
may declare `CREATE VIRTUAL TABLE … USING …`, so `ptah db read` output is
readable back by the tool that wrote it; and `schema diff` accepts a **database
URL** as its desired side, read by the same reader that produced it. Go
annotations, HCL and YAML have no syntax for one.

The module arguments are preserved verbatim — the text between the module's
parentheses, quotes and embedded commas included — because SQLite stores and
compares them as written.

Each name either side calls virtual is classified, and only the answers a plan
cannot express are refused:

| desired side | database side | outcome |
| --- | --- | --- |
| does not name it, and **cannot** name it | virtual | **left alone** — the source has no virtual-table syntax, so its silence is not a request to drop |
| does not name it, but **could** | virtual | **refused** — dropping one deletes the index and everything in it, so Ptah does not plan that from an absence |
| ordinary table | virtual | **refused** — two kinds of object under one name |
| virtual | ordinary table | **refused** — the same collision, mirrored |
| virtual, different declaration | virtual | **refused** — no `ALTER VIRTUAL TABLE`, so converging destroys the index |
| virtual, same declaration | virtual | synced |
| virtual | absent | planned as `CREATE VIRTUAL TABLE` |

An object type a source cannot represent is outside the surface that source
manages. Go annotations, HCL and YAML have no `CREATE VIRTUAL TABLE`, so a
schema written in them never withheld one and a live index it does not mention
is left untouched — no refusal, and no drop. A native `.sql` document and a
database URL can both express one, so for those the ordinary declarative rule
holds and silence still means removal, which Ptah refuses rather than plans
because the drop is destructive.

The module name is folded the way SQLite folds an identifier; the module
arguments are compared verbatim, because only the module interprets them and
normalizing whitespace would equate two different tokenizers.
Catalog identifier bytes are preserved too: a quoted table named `" docs "`
is distinct from `docs`, and an authorized removal targets only the former.

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

An unset variable and an explicit false both keep the refusal. A value that is
not a boolean is a configuration error. An explicit SQLite URL or dialect is
validated before project configuration and command early returns. A SQLite URL
selected by project configuration is validated immediately after the effective
configuration merge and before source loading, path resolution, database or SQL
work. The public migration generator has no project-config merge and validates
before resolving filesystem paths.
`migrations checkpoint --dialect sqlite` validates before requiring the shadow
URL, then validates the URL-derived dialect again when the URL exists.
Non-SQLite operations do not consult the variable. The opt-in covers only the
first row of the table above — a kind collision and a changed declaration stay
refused however it is set.

A project that already configures the drop away is not asked for either
variable. With `diff.skip: [drop_table]` in `ptah.yaml`, or
`diff { skip { drop_table = true } }` in an Atlas project file, every table drop
and the dependent removals a dropped table carries are deleted from the diff
before any SQL is rendered, so the first row of the table above is not refused:
the `DROP TABLE` it warns about is never planned. Measured on an `fts5` database
whose desired state names only the ordinary table, `ptah schema apply` reported
`Schema is synced, no changes to be made.` at exit 0 with `MATCH` still
answering, where the same run without the policy exited 2. The other rows are
unaffected — `skip drop_table` filters removals, not modifications, and no
policy makes one kind of object convertible into another.

`drop_column` and `drop_index` are read the same way by the unregistered-module
guard below: a removed column is what makes SQLite rebuild a table, and a
removed index is a `DROP INDEX` against a table Ptah cannot classify, so a
project that skips either one is not refused for it. `drop_enum` is not read at
all, because SQLite has no enum type.

## Virtual table limitations

- Shadow tables belonging to a module the reading build does not register
  cannot be identified, because only that module knows which suffixes are its
  own. SQLite reports them as ordinary tables and so does Ptah. The virtual
  table itself is still recognized and still round-trips. This part is
  permanent because it is SQLite's own answer: no catalog field distinguishes a
  shadow table without the module.

  What is **not** permanent is planning against that description. Measured on
  `fts3` and `fts4`, excluding the virtual table left the module's storage in
  the comparison and `ptah schema apply` dropped every one of those tables at
  exit 0, after which `MATCH` answered `SQL logic error`; the `fts5` control
  reported a synced schema and changed nothing. A comparison whose database
  side holds a virtual table this build cannot load is now refused **when the
  plan could act on such a table** — when some live table in it is one the
  desired side does not name, or names and describes so differently that SQLite
  can only converge it by rebuilding, since a `DROP TABLE` and the rebuild
  SQLite uses in place of an `ALTER` destroy the module's storage equally, or
  loses an index or trigger the plan drops or replaces on it. A table that
  merely **gains** a column, an index or a trigger is none of those:
  `ALTER TABLE ... ADD COLUMN` is a statement SQLite has and a `CREATE` removes
  nothing, so that comparison runs at exit 0 with no opt-in.
  A generated **migration** is checked in both directions, because reversing an
  added column produces a removed one and SQLite converges that by rebuilding
  the table: `ptah migrations generate` is refused when its down file would
  rebuild such a table, even though its up file is the single
  `ALTER TABLE ... ADD COLUMN` the comparison admits. What the migration itself
  creates is discounted, so adding an ordinary table still generates both files
  and rolls back by dropping only that table. `ptah schema diff` and
  `ptah schema apply` plan no rollback and are unaffected.
  The refusal survives excluding the virtual table,
  because the tables at risk are not the one an operator would exclude; but a
  narrowing that leaves nothing the plan can touch, such as `--include users`,
  runs normally, and so do two databases that both hold the same index. A read
  still succeeds and prints a note, naming the virtual tables the rendered
  document still contains, or warning without a name where a projection dropped
  them and left their storage behind. `PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE=1`
  restores the old comparison for an operator who wants it — two databases that
  both already hold the same `fts4` index then compare normally and report a
  synced schema. A project that skips `drop_table` needs no opt-in for the
  removal half either: the drops are deleted from the diff before anything is
  rendered, so only a rebuild of a table the desired state names is still
  refused. Separately, **adding** such a table has no opt-in: a plan
  carrying `CREATE VIRTUAL TABLE ... USING fts4` is answered with
  `no such module: fts4`, so that one is refused before the plan, and only where
  the statement would actually be planned.
- A user-created index on a recognized shadow table is refused rather than
  omitted. Ptah cannot replay that index without exposing the module-owned
  table as an ordinary schema object.
- A desired side that does not name a live virtual table is refused rather than
  planned as a drop, because removing one deletes the index and everything in
  it. Name the table in a native `.sql` desired state, scope the comparison past
  it, or set `PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP=1` to plan the drop
  deliberately. Go annotations, HCL and YAML have no virtual-table syntax, so
  for those sources scoping past is the only way to leave one in place.
- A changed declaration is refused rather than converged. Recreating a virtual
  table destroys its contents, and Ptah compares module arguments as written
  rather than normalizing them, so it cannot tell an equivalent declaration from
  a changed one — `fts5(title, body)` and `fts5( title , body )` are two
  declarations here. Planning a recreate is tracked in
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
- Rebuilding a table whose retained triggers use syntax Ptah cannot round-trip,
  or whose rebuild scaffolding name `__ptah_rebuild_<table>` is already taken.
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
