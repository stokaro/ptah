# Importing migrations from another tool

`ptah migrations import` converts an existing migration directory produced by
another versioned-migration tool into Ptah's native
`NNNNNNNNNN_description.up.sql` / `.down.sql` layout, preserving version order and
rewriting `ptah.sum`. A team already invested in another tool can adopt Ptah
without hand-rewriting its migration history.

This is distinct from **baseline** (`ptah migrations baseline`), which records
already-applied migrations as applied in the live database's revision table but
does not read another tool's files. Import is the "convert the files" half.

## Usage

```bash
ptah migrations import \
  --source-dir ./db/migrations \
  --migrations-dir ./migrations
```

| Flag | Meaning |
| --- | --- |
| `--source-dir` | Directory holding the source tool's migrations (required). |
| `--migrations-dir` | Output directory for the generated Ptah migrations (default `./migrations`). |
| `--from` | Source tool. Auto-detected from the directory layout when omitted. |
| `--dry-run` | Print the migrations that would be written without writing them. |

The source tool is auto-detected; pass `--from` to be explicit or to disambiguate.

## Supported tools

| Tool | Status | Notes |
| --- | --- | --- |
| golang-migrate | **Supported** | `<version>_<name>.up.sql` / `.down.sql`; integer or timestamp versions. |
| Goose | **Supported** | Single-file `<version>_<name>.sql` split by `-- +goose Up` / `-- +goose Down` (SQL only; `StatementBegin/End` directives are stripped, the exact line `-- +goose NO TRANSACTION` becomes `-- +ptah no_transaction` on both output directions, and Go-based migrations are rejected). |
| Flyway | **Supported** | Versioned `V<version>__<desc>.sql` (dotted versions such as `V2.1` are supported), undo `U<version>__<desc>.sql` (paired to its versioned migration by version and imported as the down), and repeatable `R__<desc>.sql` (imported as a one-time migration ordered after the versioned ones). |
| Liquibase | **Supported** (formatted SQL) | Formatted-SQL changelogs (a `.sql` file beginning with `--liquibase formatted sql`): each `--changeset <author>:<id>` becomes a migration, and its `--rollback` lines become the down. XML, YAML, and JSON changelogs are detected and rejected with a message — they are a follow-up. |

## Behavior

- **Version order is preserved.** Migrations are emitted in ascending source
  version order. A source version is kept as-is when it fits Ptah's version
  format (a 10-digit number, so up to `9999999999`). When any source version is
  wider than that — for example a 14-digit `YYYYMMDDHHMMSS` golang-migrate
  timestamp — all migrations are reassigned to sequential Ptah versions
  (`0000000001`, `0000000002`, …) in their existing order, and the original
  version is folded into the file name (`0000000001_v20230102030405_init...`) so
  history stays traceable. A duplicate or ambiguous source version fails the
  import loudly rather than silently dropping or reordering history.
- **Missing rollbacks** get a placeholder down migration, so every imported
  migration is a complete up/down pair.
- **Flyway specifics.** Dotted versions (`V2.1`) have no single-integer Ptah
  equivalent, so a Flyway directory that uses any dotted version is reassigned to
  sequential Ptah versions with the original version folded into the name
  (`0000000002_v2_1_add_email...`); a directory whose versions are all plain
  integers keeps them. An undo (`U<version>__…`) migration becomes the down of
  the versioned migration with the same version. A repeatable (`R__…`) migration
  is imported as a one-time migration ordered after every versioned one (named
  `repeatable_<desc>`), because Ptah-native migrations do not have Flyway-style
  reapply semantics — a later source change becomes a new Ptah migration rather
  than an automatic re-run.
- **Liquibase specifics.** Only formatted-SQL changelogs are read. A changeset
  has no numeric version (it is identified by `author:id` and applied in changelog
  order), so changesets are assigned sequential Ptah versions in file order — files
  are ordered by name, changesets within a file by appearance — with the
  `author:id` carried into the name (`0000000001_alice_create_users...`). Each
  `--rollback` line contributes the down; a normal `--` SQL comment is kept in the
  up. XML, YAML, and JSON changelogs are detected and rejected with an actionable
  message rather than silently skipped.
- **No clobbering.** The import refuses to overwrite an existing migration file
  in the output directory — point `--migrations-dir` at an empty or new
  directory.
- **Integrity.** After writing, `ptah.sum` is refreshed, so
  `ptah migrations validate` passes against the imported directory immediately.

## After importing

Verify the result, then treat the imported files as ordinary Ptah migrations:

```bash
ptah migrations validate --dir ./migrations
ptah migrations status --db-url "$DATABASE_URL" --migrations-dir ./migrations
```

If you are switching an already-migrated database over to Ptah, follow the import
with `ptah migrations baseline` so the revision table records the imported
migrations as already applied.
