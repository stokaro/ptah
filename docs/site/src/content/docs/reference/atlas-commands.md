---
title: Atlas-compatible commands
description: Per-command status for every ptah atlas verb, with Atlas differences and known gaps.
---

This page is the lookup reference for the Atlas-compatible surface: what each
`ptah atlas <command>` does, where it differs from Atlas, and which inputs
fail explicitly. Usage, flags, and worked examples live on
[Atlas migrate commands](../../atlas/migrate-commands/) and
[Atlas schema commands](../../atlas/schema-commands/); the surfaces and
translation model are on the
[Atlas compatibility overview](../../atlas/overview/). Native verbs are on
[Native commands](../native-commands/).

The separate `ptah-compat` binary is a drop-in replacement for scripts that
need Atlas-style root commands. When it is copied or symlinked as `atlas`,
usage and help paths render as `atlas <command> ...` where Cobra can derive
them from the executable name.

## Utility commands

| Command | Behavior |
| --- | --- |
| `ptah atlas version` | Prints Ptah build information. |
| `ptah atlas license` | Prints Ptah's MIT license and the license-clean Atlas compatibility notice. |
| `ptah atlas completion <shell>` | Generates shell completion output for the full `ptah` command tree, including the Atlas-compatible namespace. |

## Migrate commands

### `ptah atlas migrate apply`

Applies Atlas-format migration directories with Atlas-compatible apply flags
and Atlas revision bookkeeping by default. With `--env`, reads `env.url`,
`migration`, and `format.migrate.apply` from `atlas.hcl`. Executes every Atlas
OSS directory format selected by `migration.format` or a `?format=` directory
URL query; non-`atlas` formats are converted in memory to up-only migrations.
Unknown formats, Flyway repeatable (`R__`) migrations, goose/dbmate files
missing their up directive, and colliding versions fail before the target
database is opened. Matching Atlas OSS, `--dir-format`, `--to-version`, and
`--lock-name` are rejected on this verb.

### `ptah atlas migrate status`

Reports Atlas-format migration status with Atlas revision-table metadata and
Atlas-format migration directories by default. Supports `--dir-format atlas`,
`--revisions-schema`, and Atlas Go-template `--format` output over `.Env`,
`.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, and `.Status`.

### `ptah atlas migrate hash`

Forwards to `ptah migrations hash` with Atlas `--dir-format` defaulting to
`atlas`, so the compatibility path writes `atlas.sum` by default.

### `ptah atlas migrate validate`

Silently verifies `atlas.sum` on success. Missing or mismatched checksum files
use Atlas-compatible exit-1 stdout/stderr diagnostics, and `--dev-url` cleans
the dev database and replays the migration directory to validate SQL
execution. Native `ptah migrations validate` keeps its own banner and exit
contract.

### `ptah atlas migrate lint`

Runs Ptah migration linting with Atlas `--dir-format` defaulting to `atlas`.
Maps `--latest N` and `--git-base`/`--git-dir` to native changeset linting,
infers the lint dialect from `--dev-url`, cleans and replays migrations on
directly connectable dev databases, prints Atlas's migration-analysis text
report by default, and supports Atlas Go-template `--format` output over
`.Env`, `.Steps`, and `.Files`. Docker dev databases and web reports remain
explicit gaps.

### `ptah atlas migrate new`

Creates an Atlas single-file skeleton migration and updates `atlas.sum` by
default; the native equivalent is `ptah migrations create`. Supports
`--dir-format atlas`, and `--edit` opens the created file in
`$VISUAL`/`$EDITOR` before `atlas.sum` is refreshed.

### `ptah atlas migrate set [version]`

Sets or rewrites the Atlas-format revision row for the positional version by
forwarding to `ptah migrations repair` with Atlas revision-table metadata and
Atlas-format migration directories by default. With `--env`, reads `env.url`,
`migration.dir`, and `migration.revisions_schema` from `atlas.hcl`; explicit
`--dir`, `--url`, and `--revisions-schema` flags keep CLI precedence.

### `ptah atlas migrate down`

Forwards to `ptah migrations down` with mapped Atlas flags. `--dev-url`
replays and verifies the rollback plan on the dev database before the target
is touched (native `--shadow-db`), and `--format` (flag or `PTAH_FORMAT`)
renders an Atlas Go-template report with the `YES` confirmation prompt on
stderr (`--dry-run` or the native `--confirm` pass-through skip it). The
forward defaults to Atlas revision bookkeeping (`--revision-format atlas`,
like `migrate set`), so a bare invocation reverts the revisions
`atlas migrate apply` wrote; the native `--revision-format ptah` pass-through
selects ptah bookkeeping. The registry-bound `--to-tag`, `--skip-checks`, and
`--plan` flags are recorded waivers that fail loudly with their rationale.

### `ptah atlas migrate diff`

Validates an existing `atlas.sum`, replays a local Atlas migration directory
on `--dev-url`, diffs it against local `.hcl`, `.yaml`, `.yml`, or `.sql`
`--to` schema files, writes a new Atlas single-file migration, and updates
`atlas.sum`. The Atlas-hidden `--dry-run` flag prints the generated SQL
instead of writing files. With `--env`, reads `env.schema.src`, `env.dev`,
`migration.dir`, `format.migrate.diff`, and supported non-concurrent `diff`
policy from `atlas.hcl`. Generated SQL uses Atlas-style two-space indentation
by default; `--format` renders it with `sql` and `.MarshalSQL` templates.
`--schema/-s` narrows both the replayed dev database state and the local
desired schema files. `--edit` opens the generated migration in
`$VISUAL`/`$EDITOR` before `atlas.sum` is finalized. `--lock-timeout` bounds
waiting for Ptah's local migration-directory lock. Atlas CE `--qualifier` is
registered for flag-surface parity and fails explicitly until custom qualifier
metadata is implemented. Database desired-schema URLs, `env://`, Docker dev
databases, and concurrent index migration-file metadata remain explicit gaps.

### `ptah atlas migrate import`

Imports local `file://` migration directories from `atlas`, `golang-migrate`,
`goose`, `flyway`, `liquibase`, or `dbmate` format into a separate Atlas
single-file directory and writes `atlas.sum`. Flyway repeatable migrations
fail explicitly until Ptah can execute Atlas R-suffixed imported migrations.
The native `ptah migrations import` converts the same source formats into
Ptah-native migrations instead.

### `ptah atlas migrate checkpoint [name]`

Forwards to `ptah migrations checkpoint`, replaying the migration directory on
the `--dev-url` dev database and writing a ptah-format cumulative-schema
checkpoint pair with `ptah.sum` refreshed. `--dir` maps to the native
migrations directory and the optional positional name to the checkpoint
description. Checkpoint output is ptah-format only, so this verb operates on
ptah-format directories: `--dir-format=ptah` passes through, and
`--dir-format=atlas` is a recorded waiver rejected loudly (Ptah marks
checkpoints via the ptah file-name convention; Atlas's `-- atlas:checkpoint`
directive has no reader support, so an Atlas-format checkpoint file would
replay as an ordinary migration). Atlas keeps `migrate checkpoint` in its Pro
build, so this is a free Ptah capability rather than an Atlas CE stub.

### `ptah atlas migrate test [paths]`

Forwards to `ptah migrations test`: `--dir` maps to the native migration
directory (Atlas-format by default via `--dir-format`), `--dev-url` to the
native throwaway database (an ephemeral SQLite database when omitted), `--run`
to the native case-name filter, and the optional positional path to the
directory of Ptah-native YAML test cases (default `./tests`). Exit codes match
the native runner: 0 when all cases pass, 1 on test failure. Atlas keeps
`migrate test` in its Pro build, so this is a free Ptah capability rather than
an Atlas CE stub.

### `ptah atlas migrate edit {name | version}`

Forwards to `ptah migrations edit`: the positional maps to the native
`--version` (a migration file name contributes its leading version digits),
`--dir` maps to the native migration directory (Atlas-format by default via
`--dir-format`), the editor resolves from `$VISUAL`, then `$EDITOR`, and the
directory checksum is rewritten afterwards. Atlas keeps `migrate edit` outside
its community build, so this is a free Ptah capability rather than an Atlas CE
stub.

### `ptah atlas migrate rebase {name | version}`

Forwards to `ptah migrations rebase`: re-timestamps the selected migration
past every existing version and rewrites the directory checksum. Multiple
positional values and `a...b` version ranges are rejected loudly; forward one
migration per run. Atlas keeps `migrate rebase` outside its community build,
so this is a free Ptah capability rather than an Atlas CE stub.

### `ptah atlas migrate rm {name | version}`

Forwards to `ptah migrations rm`: deletes the selected migration's files and
rewrites the directory checksum. Atlas keeps `migrate rm` outside its
community build, so this is a free Ptah capability rather than an Atlas CE
stub.

### `ptah atlas migrate push`

Registered Atlas CE boundary stub for a community-version unsupported command,
kept by decision: Atlas push targets the proprietary, account-bound Atlas
Registry protocol. `--help` prints the Atlas CE unsupported notice and exits
0; direct execution prints the Atlas CE abort text and exits 1. The open
replacement is the native `ptah migrations push` to any OCI registry.

## Schema commands

### `ptah atlas schema inspect`

Inspects a live database and writes Atlas-compatible schema output without
Ptah status banners. The default output is HCL; SQL output is supported with
`--format sql` or `--format '{{ sql . }}'`; JSON and custom templates are
supported through `--format json`, `{{ json . }}`, `{{ .MarshalHCL }}`,
`{{ hcl . }}`, `{{ sql . }}`, and `{{ mermaid . }}`. Basic
`{{ hcl . | split | write "schema" }}` and
`{{ sql . | split | write "schema" }}` exports are supported. `--schema/-s`
narrows inspection when supported by the database reader. The OSS `--exclude`
flag filters inspected resources with Atlas-style globs and `[type=...]`
selectors, including the Atlas-documented `*[type=extension].version` field
selector. Other field-level exclude selectors, include filtering, file-backed
inspection, advanced split/write configuration, and dev-database inference
remain explicit gaps.

### `ptah atlas schema apply`

Diffs a live database against local `file://` `.hcl`, `.yaml`, `.yml`, or
`.sql` desired schema files, prints the planned SQL, and applies it after
interactive confirmation or explicit `--auto-approve`; `--dry-run` prints the
plan without applying. With `--env`, reads `env.url`, `env.src`,
`env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`,
`format.schema.apply`, and supported `diff` policy from `atlas.hcl`, including
local variable defaults, locals, `getenv`, `file`, `fileset`, `format`,
`jsonencode`, and `data.hcl_schema.<name>.url` references. `--tx-mode=file`
and `--tx-mode=all` execute the generated plan in one transaction;
`--tx-mode=none` executes statements without transaction wrapping. `--format`
supports Atlas-style templates over planned changes with `sql` and
`.MarshalSQL`. `--exclude` and disabled `schema.mode` values filter matching
resources out of both sides of the comparison before planning. `--edit` opens
the planned SQL in `$VISUAL`/`$EDITOR` before approval, and the edited SQL is
what gets applied. `--plan file://<path>` executes a pre-approved local plan
file saved by `schema plan` after verifying the database still matches the
plan's source fingerprint; a drifted target refuses with a stale-plan error,
registry `atlas://` plan URLs are rejected, and `--plan` cannot be combined
with `--to`, `--file`, `--dev-url`, `--exclude`, or `--edit`. Atlas's hidden
`--file/-f` alias is accepted for local HCL or SQL paths; `--schema/-s` is
parsed for CLI compatibility but limited until database-URL desired schemas are
supported. `--lock-timeout` is registered for flag-surface parity and fails
explicitly until database lock waiting is implemented. Database desired-schema
URLs, migration directories, `env://` URL sources, include filters, and Atlas
dev-database simulation remain explicit gaps.

### `ptah atlas schema plan`

Computes the declarative migration from the `--from` target database to local
`--to` schema files and saves it as a fingerprinted local plan file: `--save`
writes `<name>.plan.json` (deterministic fingerprint-derived default name, or
`--name`), `--output <path>` chooses the location, and `--dry-run` prints the
plan document without saving. The JSON plan records the ordered SQL statements
with per-statement safety severity, the dialect, the exclude patterns, and
SHA-256 fingerprints of the source and desired schema states. With `--env`,
reads `url` (the plan target), `schema.src`, `dev`, `exclude`, `schema.mode`,
and supported `diff` policy from `atlas.hcl`. The registry-bound `--push`,
`--pending`, `--repo`, and `--auto-approve` flags are recorded waivers that
fail loudly; `--edit`, `--skip-lint`, `--format`, `--name-format`,
`--directive`, `--schema`, `--include`, and `--lock-timeout` fail explicitly
until implemented. The registry sub-verbs (`approve`, `lint`, `list`, `new`,
`pull`, `push`, `rm`, `test`, `validate`) stay Atlas CE boundary stubs. Atlas
keeps `schema plan` in its Pro registry flow, so this is a free Ptah
capability rather than an Atlas CE stub.

### `ptah atlas schema diff`

Diffs local `file://` schema files with `.hcl`, `.yaml`, `.yml`, or `.sql`
extensions, prints migration SQL, supports `--from/-f`, supports Atlas-style
`--format` templates with `sql` and `.MarshalSQL`, and applies `--exclude`
plus disabled `schema.mode` resource filters to both local inputs before
diffing. `--schema/-s` is parsed for CLI compatibility but limited until
database-URL schema diffs are supported. With `--env`, reads `env.schema.src`,
`env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and
supported `diff` policy from `atlas.hcl`. Database URLs, migration
directories, `env://`, and include filters remain explicit gaps.

### `ptah atlas schema fmt`

Formats local `.hcl` files using HCL canonical layout.

### `ptah atlas schema clean`

Cleans user-owned schema objects through Ptah's destructive database-cleanup
runtime: `--dry-run` prints the planned cleanup, interactive confirmation is
preserved unless `--auto-approve` is explicit, `--format` renders Atlas-style
templates over the cleanup plan, and `env.url` plus `format.schema.clean` are
read from `atlas.hcl`. Cleanup covers the object types Ptah cleanly models and
drops today: user tables across supported dialects, PostgreSQL enum types and
sequences, and SQL Server foreign-key constraints that must be dropped before
tables.

### `ptah atlas schema test [paths]`

Forwards to `ptah schema test`: `-u/--url` maps the desired schema URL (a
local `file://` directory of Go schema annotations) to the native
`--root-dir`, `--dev-url` to the native throwaway database (an ephemeral
SQLite database when omitted), `--run` to the native case-name filter, and the
optional positional path to the directory of Ptah-native YAML test cases. With
`--env`, `schema.src` supplies the desired schema URL and `dev` the dev
database. Exit codes match the native runner: 0 when all cases pass, 1 on test
failure. Atlas keeps `schema test` in its Pro build, so this is a free Ptah
capability rather than an Atlas CE stub.

### `ptah atlas schema push`

Registered Atlas CE boundary stub for a community-version unsupported command,
kept by decision: Atlas push targets the proprietary, account-bound Atlas
Registry protocol. `--help` prints the Atlas CE unsupported notice and exits
0; direct execution prints the Atlas CE abort text and exits 1. The open
replacement is the native `ptah schema push` to any OCI registry.

## Related pages

- Runnable migrate workflows and format template fields:
  [Atlas migrate commands](../../atlas/migrate-commands/).
- Runnable schema workflows and format template fields:
  [Atlas schema commands](../../atlas/schema-commands/).
- Measured compatibility evidence: [Conformance](../../atlas/conformance/).
