---
title: Import from another tool
description: Convert a golang-migrate, Goose, Flyway, Liquibase, or dbmate migration directory into Ptah's native format.
---

Your project already has migration history in another tool, and you want Ptah
to own it from here — without hand-rewriting years of files. This page shows
how `ptah migrations import` converts that history, what the converted
directory looks like, and how to pick up the lifecycle afterward.

Prerequisites: a built `ptah` binary and the source tool's migration
directory. The example imports a golang-migrate directory; Goose, Flyway,
Liquibase, and dbmate work the same way.

## Starting state

A golang-migrate directory in which the second migration has no down file:

```text
db/migrations/
  000001_create_users.up.sql
  000001_create_users.down.sql
  000002_add_posts.up.sql
```

## Preview the conversion

`--dry-run` lists what would be written without writing it:

```bash
ptah migrations import \
  --source-dir ./db/migrations \
  --migrations-dir ./migrations \
  --dry-run
```

Expected output includes:

```text
Dry run: would write 4 migration file(s) to ./migrations
  0000000001_create_users.up.sql
  0000000001_create_users.down.sql
  0000000002_add_posts.up.sql
  0000000002_add_posts.down.sql
```

## Run the import

```bash
ptah migrations import \
  --source-dir ./db/migrations \
  --migrations-dir ./migrations
```

Expected output includes:

```text
Wrote 4 migration file(s) to ./migrations
Wrote ./migrations/ptah.sum
  0000000001_create_users.up.sql
  0000000001_create_users.down.sql
  0000000002_add_posts.up.sql
  0000000002_add_posts.down.sql
```

Import converts the source files into Ptah's native
`NNNNNNNNNN_name.up.sql` / `.down.sql` layout, preserving version order, and
rewrites `ptah.sum` itself — validation passes immediately:

```bash
ptah migrations validate --dir ./migrations
```

```text
OK: migrations directory matches ptah.sum
```

A source migration with no rollback gets a placeholder down file, so the
directory satisfies [the down-file contract](../rollback/):

```text
-- No rollback was provided by the source migration.
```

Review placeholder downs before relying on rollback through those versions.

## Supported source tools

The source tool is auto-detected from the directory layout; set `--from` to
assert it explicitly (`golang-migrate`, `goose`, `flyway`, `liquibase`,
`dbmate`).

| Tool | Notes |
| --- | --- |
| golang-migrate | `NNN_name.up.sql` / `.down.sql` pairs. |
| Goose | Annotated single files (`-- +goose Up` / `-- +goose Down`). |
| Flyway | Including dotted versions, undo `U__` scripts, and repeatable `R__` scripts. |
| Liquibase | Formatted-SQL changelogs (`--changeset` / `--rollback`); XML, YAML, and JSON changelogs are rejected with a message. |
| dbmate | Annotated single files (`-- migrate:up` / `-- migrate:down`); directive options such as `transaction:false` are dropped from the SQL. |

This is native Ptah-format import, distinct from the Atlas-compatible
`migrate import` verb of the `ptah-compat` binary, which writes an Atlas-format directory with
`atlas.sum`.

## After the import

The converted directory is ordinary Ptah history; what comes next depends on
the database:

- **The database already has the schema** (it ran the source tool's
  migrations): record the history as applied without executing it —
  `ptah migrations baseline`, covered step by step in
  [Adopt an existing database](../../start/adopt-an-existing-database/).
- **A fresh database**: [apply the directory](../apply/) from version zero.

Either way, future changes follow the regular loop starting at
[Generate migrations](../generate/).

## Failure modes

**Import never overwrites.** Running it into a directory that already
contains a converted file fails (exit `2`) and writes nothing:

```text
error: refusing to overwrite existing migration file "0000000001_create_users.up.sql" in ./migrations
```

Point `--migrations-dir` at an empty directory, or remove the partial result
and rerun.

**Unsupported changelog formats are rejected loudly.** Liquibase XML, YAML,
and JSON changelogs fail with a message naming the limitation; convert them
to formatted SQL first.

## Next steps

- Database already migrated by the old tool?
  [Adopt an existing database](../../start/adopt-an-existing-database/).
- Fresh database to bring up? [Apply migrations](../apply/).
- Long imported history slowing fresh setups? [Checkpoints](../checkpoints/).
