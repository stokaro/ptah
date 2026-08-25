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
| `--allow-partial` | Import and write `ptah.sum` even though some source files were not converted. |

The source tool is auto-detected; pass `--from` to be explicit or to disambiguate.

## Supported tools

| Tool | Status | Notes |
| --- | --- | --- |
| golang-migrate | **Supported** | `<version>_<name>.up.sql` / `.down.sql`; integer or timestamp versions. |
| Goose | **Supported** | Single-file `<version>_<name>.sql` split by `-- +goose Up` / `-- +goose Down` (SQL only; `StatementBegin/End` directives are stripped, the exact line `-- +goose NO TRANSACTION` becomes `-- +ptah no_transaction` on both output directions, and Go-based migrations are rejected). |
| Flyway | **Supported** | Versioned `V<version>__<desc>.sql` (dotted versions such as `V2.1` are supported), undo `U<version>__<desc>.sql` (paired to its versioned migration by version and imported as the down), and repeatable `R__<desc>.sql` (imported as a one-time migration ordered after the versioned ones). |
| Liquibase | **Supported** | All four serializations: formatted SQL, XML, YAML and JSON. See [Liquibase specifics](#liquibase-specifics). |

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
- **Liquibase specifics.** All four serializations are read, and a changelog
  construct that cannot become a migration file is refused by name. See
  [Liquibase specifics](#liquibase-specifics) below.
- **No clobbering.** The import refuses to overwrite an existing migration file
  in the output directory — point `--migrations-dir` at an empty or new
  directory.
- **Integrity.** After writing, `ptah.sum` is refreshed, so
  `ptah migrations validate` passes against the imported directory immediately.
- **Every source file is accounted for.** See
  [What the import declines](#what-the-import-declines).

## What the import declines

Every file under `--source-dir` is either converted into a Ptah migration or
named on stderr with the reason it was not:

```console
$ ptah migrations import --from golang-migrate --source-dir ./src --migrations-dir ./out
Declined 2 source file(s):
  000002_add_index.sql: its name is not a golang-migrate migration file name (<version>_<name>.up.sql / .down.sql)
  tenant/000002_add_email.up.sql: it sits below the top level, and golang-migrate reads only the top level of the source directory
error: refusing to write ptah.sum for a partial import: 2 source file(s) were not converted (...); pass --allow-partial to import the rest anyway
```

Ptah cannot tell a `README` from a migration whose name missed the rule by one
character, so it reports both and lets you decide.

### Which directory levels each tool reads

| Tool | Depth | Why |
| --- | --- | --- |
| Flyway | The whole tree | A Flyway location is scanned recursively, so a project laid out per module or per release imports whole. This is also what `ptah-compat migrate import` does with the same directory. |
| golang-migrate | Top level only | golang-migrate's own reader does not descend into subfolders. |
| Goose | Top level only | Matches the tool's own reader. |
| Liquibase | Top level only | A master changelog names the files it includes; Ptah does not go looking beyond it. |
| dbmate | Top level only | Matches the tool's own reader. |

A file below the top level of a top-level-only source is declined by name rather
than dropped. If a directory holds all of its migrations one level down, the
import says so instead of reporting the layout as undetectable:

```console
error: no migration files at the top level of the source directory, but golang-migrate
migration files were found below it (v1/000001_create_users.up.sql); golang-migrate reads
only the top level, so point --source-dir at the directory that holds the migrations
```

### Files declined for a reason of their own

| File | Reason |
| --- | --- |
| Flyway `B<version>__<desc>.sql` | A baseline asserts a schema is already in place rather than describing a change to apply. Import the schema instead. |
| Flyway callbacks (`afterMigrate__…`, `beforeEachMigrate__…`, …) | A callback runs around migrations rather than being one. |
| `<version>_<name>.sql` with no `-- +goose Up` | The name is a Goose migration's and the content is not, which is more likely a missing marker than a file that was never a migration. |
| `<version>_<name>.sql` with no `-- migrate:up` | The same, for dbmate. |

### `ptah.sum` and partial imports

An import that declined a file which could carry SQL — anything ending in
`.sql`, in any case — is refused before `ptah.sum` is written. A checksum over
the subset that survived would make the truncated directory validate clean, and
nothing downstream could then establish that SQL had been lost.

Pass `--allow-partial` to accept the declined set and import the rest; the
declined files are still named. A declined file that cannot carry migration SQL
(a `README`, a `.gitkeep`) is reported but does not block the import.

## Liquibase specifics

All four serializations are read: formatted SQL, and the XML, YAML and JSON
changelogs. A changeset has no numeric version (it is identified by `author:id`
and applied in changelog order), so changesets are assigned sequential Ptah
versions in file order — files are ordered by name, changesets within a file by
appearance — with the `author:id` carried into the name
(`0000000001_alice_create_users...`).

In formatted SQL each `--rollback` line contributes the down, and a normal `--`
SQL comment is kept in the up. In a changelog the `sql` changes are the up and
`rollback` is the down, whether it is written as a change list, a nested
`<sql>`, or bare SQL text.

### Constructs that are refused

A construct that cannot become a migration file is **refused by name**, so an
import either carries the whole changelog or does not happen. A migration
directory that is not the changelog it claims to have imported applies cleanly
and is wrong, which is worse than an import that did not happen.

| Construct | Why it is refused |
| --- | --- |
| `include`, `includeAll` | They compose other changelog files. Ptah imports one changelog at a time, so the changesets those files hold would be left out. Import the referenced files instead. |
| `preConditions`, `contexts`, `labels` | They decide *whether* a changeset runs. A migration directory has no equivalent, so importing them would turn a conditional history into an unconditional one. Split the changelog, or import it by hand. |
| Typed refactorings (`createTable`, `addColumn`, …) | They are not SQL text, and rendering them would mean reimplementing Liquibase's generator for every dialect. Rewrite the changeset as a `sql` change. |

A directory holding both a changelog and formatted-SQL files is refused as well:
the two shapes order changesets by different rules, and a changelog may
`include` the SQL files outright, so importing both would reorder or duplicate
history. Import them separately.

`migrate apply --dir` still refuses a serialized changelog and names
`migrate import` as the verb that reads one. Direct apply takes each file's own
name as its version, and a changelog has none — its order lives inside the
document.

## After importing

Verify the result, then treat the imported files as ordinary Ptah migrations:

```bash
ptah migrations validate --dir ./migrations
ptah migrations status --db-url "$DATABASE_URL" --migrations-dir ./migrations
```

If you are switching an already-migrated database over to Ptah, follow the import
with `ptah migrations baseline` so the revision table records the imported
migrations as already applied.
