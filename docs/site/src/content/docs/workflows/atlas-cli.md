---
title: Atlas-compatible CLI
description: Use Atlas-style commands through ptah atlas.
---

Atlas-compatible command paths live under `ptah atlas <command> ...` inside the
native Ptah CLI tree.

The separate `ptah-compat` binary is a binary-level drop-in replacement for
scripts that need Atlas-style root commands, including scripts that call an
executable named `atlas`.

Ptah does not add root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect` to the native `ptah` binary. Those paths are intentionally
invalid because the native Ptah command tree is being designed separately before
GA. Use `ptah-compat` or a copied/symlinked executable named `atlas` when
existing scripts expect Atlas-style root commands.

## Translation model

Implemented Atlas-compatible commands either execute dedicated Atlas-shaped
behavior or translate Atlas-style flags into the closest native Ptah command
model. Unsupported flags fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR for supported `ptah atlas ... --env` defaults |
| `--config`, `-c` | Local Atlas project config path for `schema` and `migrate` commands |
| `--var name=value` | Atlas HCL variable override for supported local expressions |
| Atlas revision table mode | Ptah revision format and table settings |

Atlas project flags are persistent on the Atlas-compatible `schema` and
`migrate` command groups, so both of these forms are valid:

```bash
ptah atlas migrate --config project.hcl --env local hash
ptah atlas migrate hash --config project.hcl --env local
```

Atlas OSS shorthand aliases are part of the compatibility surface. Ptah accepts
`-u` for `--url`, `-c` for `--config`, `-s` for `--schema` on Atlas commands
that register schema selection, and `-f` for `schema diff --from`. `schema apply`
also accepts Atlas's hidden deprecated `--file/-f` input alias for local HCL or
SQL paths; prefer `--to` in new Ptah-authored scripts.

## Migration commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas migrate apply` | Atlas-format apply path equivalent to `ptah migrations up` |
| `ptah atlas migrate down` | Forwards to `ptah migrations down` with mapped Atlas flags. `--dev-url` replays and verifies the rollback plan on the dev database before the target is touched (native `--shadow-db`), and `--format` (flag or `PTAH_FORMAT`) renders an Atlas Go-template report over `.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, `.Start`, `.End`, and `.Error`, moving the YES confirmation prompt to stderr (`--dry-run` or the native `--confirm` pass-through skip it). The forward defaults to Atlas revision bookkeeping (`--revision-format atlas`, like `migrate set`), so a bare invocation reverts the revisions `atlas migrate apply` wrote; pass the native `--revision-format ptah` through to select ptah bookkeeping. The registry-bound `--to-tag`, `--skip-checks`, and `--plan` flags are recorded waivers that fail loudly with their rationale. |
| `ptah atlas migrate status` | Atlas-format migration status with Atlas revision-table metadata |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | Silently verifies `atlas.sum` on success; checksum failures use Atlas-compatible stdout/stderr diagnostics, and `--dev-url` cleans and replays migrations on the dev database to validate SQL execution. |
| `ptah atlas migrate lint` | `ptah migrations lint`; supports Atlas-style `--latest N`, infers lint dialect from `--dev-url`, cleans and replays migrations on directly connectable dev databases to validate SQL execution, and by default prints Atlas's migration-analysis text report (`--format`, `format.migrate.lint`, or `lint { log = "…" }` select custom output). |
| `ptah atlas migrate new` | `ptah migrations create`; `--edit` opens the created migration file in `$VISUAL`/`$EDITOR` and refreshes `atlas.sum` afterwards. |
| `ptah atlas migrate set [version]` | `ptah migrations repair` with Atlas revision metadata |
| `ptah atlas migrate diff` | Replays local Atlas migrations on `--dev-url`, diffs against local schema files, writes an Atlas single-file migration, and updates `atlas.sum`; `--schema/-s` scopes the diff, `--edit` opens the generated migration in `$VISUAL`/`$EDITOR` before `atlas.sum` is finalized, and the Atlas-hidden `--dry-run` flag prints the generated SQL instead of writing files. |
| `ptah atlas migrate import` | Imports local `file://` migration directories from Atlas-supported formats into a separate Atlas single-file directory and writes `atlas.sum`. |
| `ptah atlas migrate checkpoint [name]` | Forwards to `ptah migrations checkpoint`: replays the migration directory on the `--dev-url` dev database and writes a ptah-format cumulative-schema checkpoint pair (`ptah.sum` refreshed). `--dir` maps to the native migrations directory and the optional positional name to the checkpoint description. Checkpoint output is ptah-format only: `--dir-format=ptah` passes through, while `--dir-format=atlas` is a recorded waiver rejected loudly — Ptah marks checkpoints via the ptah file-name convention, and Atlas's `-- atlas:checkpoint` directive has no reader support, so an Atlas-format checkpoint file would replay as an ordinary migration. Atlas keeps `migrate checkpoint` in its Pro build; Ptah provides it free. |
| `ptah atlas migrate test [paths]` | Forwards to `ptah migrations test`: `--dir` maps to the native migration directory (read as Atlas-format by default via `--dir-format`), `--dev-url` to the native throwaway database (an ephemeral SQLite database when omitted), `--run` to the native case-name filter, and the optional positional path to the directory of Ptah-native YAML test cases (default `./tests`). Exit codes match the native runner: 0 when all cases pass, 1 on test failure. Atlas keeps `migrate test` in its Pro build; Ptah provides it free with Ptah-native test files as the executable payload. |
| `ptah atlas migrate edit {name \| version}` | Forwards to `ptah migrations edit`: the positional maps to the native `--version` (a migration file name contributes its leading version digits), `--dir` to the native migration directory (Atlas-format by default via `--dir-format`), and the editor resolves from `$VISUAL`, then `$EDITOR`. The directory checksum is rewritten afterwards so `ptah migrations validate` keeps passing. Atlas keeps `migrate edit` outside its community build; Ptah provides it free. |
| `ptah atlas migrate rebase {name \| version}` | Forwards to `ptah migrations rebase`: re-timestamps the selected migration past every existing version and rewrites the directory checksum. Atlas documents a repeatable positional; Ptah forwards one migration per run and rejects multiple values and `a...b` version ranges loudly. Atlas keeps `migrate rebase` outside its community build; Ptah provides it free. |
| `ptah atlas migrate rm {name \| version}` | Forwards to `ptah migrations rm`: deletes the selected migration's files and rewrites the directory checksum. Atlas keeps `migrate rm` outside its community build; Ptah provides it free. |
| `ptah atlas migrate push` | Registered Atlas CE boundary stub for a community-version unsupported command, kept by decision: Atlas push targets the proprietary, account-bound Atlas Registry protocol, which is not an open target. `--help` prints the Atlas CE unsupported notice and exits 0; direct execution prints the Atlas CE abort text and exits 1. `ptah migrations push` to any OCI registry is the open replacement. |

## Utility commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas version` | Prints Ptah build information. |
| `ptah atlas license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |
| `ptah atlas completion <shell>` | Generates Cobra completion output for the full `ptah` command tree, including the Atlas-compatible namespace. |

## Schema commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas schema inspect` | Inspects a live database and writes Atlas-shaped HCL by default, SQL with `--format sql` / `--format '{{ sql . }}'`, JSON with `--format json` / `--format '{{ json . }}'`, custom Go-template output, or basic `hcl`/`sql` split-write exports. `--schema/-s` narrows inspection, and the OSS `--exclude` flag filters inspected resources. |
| `ptah atlas schema apply` | Applies local desired schema files to a live database through Ptah schema diff and migration execution; supports `--env` project defaults, Atlas-style `--format` templates over the planned changes, `--schema/-s` parsing, the hidden Atlas `--file/-f` input alias, `--exclude` resource filters, and `--edit` for editing the planned SQL in `$VISUAL`/`$EDITOR` before approval and apply, and `--plan file://<path>` for executing a pre-approved local plan file saved by `schema plan` after verifying the database still matches the plan's source fingerprint (a drifted target refuses with a stale-plan error; registry `atlas://` plan URLs are rejected). |
| `ptah atlas schema diff` | Local `file://` schema-file diff for `.hcl`, `.yaml`, `.yml`, and `.sql` sources, including `--from/-f`, `--schema/-s` parsing, and `--exclude` resource filters. |
| `ptah atlas schema fmt` | Formats local `.hcl` files using HCL canonical layout. |
| `ptah atlas schema test [paths]` | Forwards to `ptah schema test`: `-u/--url` maps the desired schema URL (a local `file://` directory of Go schema annotations) to the native `--root-dir`, `--dev-url` to the native throwaway database (an ephemeral SQLite database when omitted), `--run` to the native case-name filter, and the optional positional path to the directory of Ptah-native YAML test cases. With `--env`, `schema.src` supplies the desired schema URL and `dev` the dev database. Exit codes match the native runner: 0 when all cases pass, 1 on test failure. Atlas keeps `schema test` in its Pro build; Ptah provides it free. |
| `ptah atlas schema plan` | Computes the declarative migration from the `--from` target database to local `--to` schema files and saves it as a fingerprinted local plan file (`--save`/`--output`, `--name`, `--dry-run`; JSON, `format_version` 1). `--env` supplies `url` (the plan target), `schema.src`, `dev`, `exclude`, `schema.mode`, and supported diff policy. The registry-bound `--push`, `--pending`, `--repo`, and `--auto-approve` flags are recorded waivers rejected loudly, and the registry sub-verbs (`approve`, `lint`, `list`, `new`, `pull`, `push`, `rm`, `test`, `validate`) stay Atlas CE boundary stubs. Atlas keeps `schema plan` in its Pro registry flow; Ptah provides the local plan-file workflow free. |
| `ptah atlas schema push` | Registered Atlas CE boundary stub for a community-version unsupported command, kept by decision: Atlas push targets the proprietary, account-bound Atlas Registry protocol, which is not an open target. `--help` prints the Atlas CE unsupported notice and exits 0; direct execution prints the Atlas CE abort text and exits 1. `ptah schema push` to any OCI registry is the open replacement. |

`ptah atlas schema inspect` accepts a live database `--url` and writes
machine-oriented schema output without native Ptah status banners. The default
format is Atlas-compatible HCL.

```bash
ptah atlas schema inspect --url "$DATABASE_URL" > schema.hcl
ptah atlas schema inspect --url "$DATABASE_URL" --format sql > schema.sql
ptah atlas schema inspect --url "$DATABASE_URL" --format json > schema.json
```

`--schema` / `-s` narrows inspection when the underlying database reader supports
schema scoping. `--dev-url` validates dialect compatibility only today; Ptah
does not yet run Atlas dev-database inference for inspection. `--format`
accepts Atlas-style Go templates with `.MarshalHCL`, `hcl`, `sql`, `json`,
`base64url`, `mermaid`, `split`, and `write`. Basic split-write exports are
supported for HCL and SQL output:

```bash
ptah atlas schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ hcl . | split | write "schema" }}'

ptah atlas schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ sql . | split | write "schema" }}'
```

`--exclude` accepts repeated or comma-separated
Atlas-style glob patterns, including `[type=...]` selectors, and removes
matching resources from HCL, SQL, JSON, and custom-template output. Field-level
exclude selector support includes the Atlas-documented
`*[type=extension].version` form. Other field-level selectors fail explicitly
until Ptah models those fields as independently filterable resources.
Schema-qualified function and enum filters remain limited by Ptah's current
introspection model, which does not retain schema names for those resource types
yet. `--include` is not part of the pinned Atlas CE inspect flag surface.
File-backed inspection, exporter blocks, and advanced split/write configuration
remain explicit gaps.

`ptah atlas schema apply` accepts one or more local `--to` schema file URLs and
a live database `--url`. With `--env`, Ptah can read `env.url`, `env.src`,
`env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`,
`format.schema.apply`, and supported `diff` policy from the selected
`atlas.hcl` environment, including local variable defaults, locals, `getenv`,
`file`, `fileset`, `format`, `jsonencode`, and `data.hcl_schema.<name>.url`
references. Explicit CLI flags still take precedence. Ptah reads the current
database schema, diffs it against the desired local schema files, prints the
planned SQL, and applies it after interactive confirmation. Use `--dry-run` to
print the plan without applying it, or `--auto-approve` to skip the prompt
explicitly. Use `--tx-mode=file` or `--tx-mode=all` to execute the generated
plan in one transaction, or `--tx-mode=none` to execute statements without
transaction wrapping. With `--edit`, the planned SQL opens in `$VISUAL` or
`$EDITOR` before the plan is shown and approved, and the edited SQL is what
gets applied.

For Atlas script compatibility, `schema apply` also accepts the hidden
deprecated `--file/-f` alias for local HCL or SQL paths and maps it to the same
local desired-schema loading path as `--to`. `--file` and `--to` are mutually
exclusive.

```bash
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
```

`ptah atlas schema plan` is the open local replacement for Atlas's Pro
registry-gated plan workflow. It computes the same declarative plan `schema
apply` would generate — from the `--from` target database to the local `--to`
schema files — and saves it as a local JSON plan file that records the ordered
SQL statements with per-statement safety severity, the dialect, and the
SHA-256 fingerprints of the source and desired schema states (the plan-file
format is documented in the repository's `docs/native_cli.md`). `schema apply
--plan file://<path>` then executes exactly the reviewed statements after
verifying the live database still matches the plan's source fingerprint; a
drifted database refuses with a stale-plan error instead of running reviewed
SQL against unreviewed state.

```bash
# Compute and save the plan for review (or --save for ./<name>.plan.json).
ptah atlas schema plan \
  --from "$DATABASE_URL" \
  --to file://schema.sql \
  --output add-orders.plan.json

# Later, execute exactly the reviewed plan; drift refuses loudly.
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --plan file://add-orders.plan.json \
  --auto-approve
```

```hcl
data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = getenv("DATABASE_URL")
  dev = getenv("DEV_DATABASE_URL")
  schema {
    src = data.hcl_schema.app.url
    mode {
      funcs = false
    }
  }
  format {
    schema {
      apply = "{{ sql . \"  \" }}"
    }
  }
}
```

```bash
ptah atlas schema apply --env local --dry-run
```

`--dev-url` is accepted for dialect validation only in this path today. It must
match the target database dialect; Ptah does not yet execute Atlas's
dev-database simulation for declarative apply.

`--exclude` accepts repeated or comma-separated Atlas-style glob patterns,
including `[type=...]` selectors. Ptah applies the filter to both the current
live schema and the desired local schema files before planning, so excluded
objects are ignored rather than dropped.

Disabled `schema.mode` values are mapped to the same resource-exclusion system
for object kinds represented in Ptah's schema IR. `diff.skip.drop_table = true`
removes table drops from supported local plans. For non-dry-run PostgreSQL
`schema apply` plans that actually emit `CREATE INDEX CONCURRENTLY`,
`diff.concurrent_index.create = true` requires `--tx-mode none`;
`diff.concurrent_index.drop` and `diff.skip.drop_schema` fail explicitly.

`--format` accepts Atlas-style Go templates over the planned apply changes. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run \
  --format '{{ sql . "  " }}'
```

`ptah atlas schema diff` accepts one or more `--from` and `--to` local schema
file URLs and requires `--dev-url` so Ptah can choose the SQL dialect. With
`--env`, Ptah can read `env.schema.src`, `env.dev`, `env.exclude`,
`env.schema.mode`, `format.schema.diff`, and supported `diff` policy from
`atlas.hcl`. The current implementation does not execute Atlas's dev-database
simulation; it uses the dev URL for dialect selection only.

```bash
ptah atlas schema diff \
  -f file://old.hcl \
  --to file://schema.hcl \
  --dev-url "postgres://localhost/dev"
```

`--format` accepts Atlas-style Go templates over Ptah's local diff report. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah atlas schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "postgres://localhost/dev" \
  --format '{{ sql . "  " }}'
```

Remote database URLs, migration directory URLs, `env://` project attributes,
and include filters fail explicitly until their semantics are implemented.
Non-Atlas-CE flags such as `--tx-mode` are rejected as unknown. `--exclude` and
disabled `schema.mode` values filter both local `--from` and `--to` schema files
before diffing.

## Migration Apply

`ptah atlas migrate apply` reads a local Atlas migration directory and records
runtime history in Atlas revision-table format by default. The optional
positional `amount` applies only the first N pending migrations. Use
`--baseline` to mark earlier migration files as applied without executing their
SQL bodies before applying the remaining pending migrations.

```bash
ptah atlas migrate apply 2 \
  --url "$DATABASE_URL" \
  --dir file://migrations
```

Supported Atlas apply flags include `--dry-run`, `--tx-mode`, `--exec-order`,
`--allow-dirty`, `--baseline`, `--revisions-schema`, `--lock-timeout`, and
`--format`. `--format` executes a Go template against a Ptah apply result that
mirrors Atlas's public apply-template fields: `Pending`, `Applied`, `Current`,
`Target`, `Start`, `End`, `Driver`, `URL`, and `Dir`; `{{ json . }}` emits the
same result as JSON with database credentials redacted. With `--env`, Ptah can
read `env.url`, `migration`, and `format.migrate.apply` from `atlas.hcl`.
The apply path executes every Atlas OSS migration directory format selected by
`migration.format` or the directory URL `?format=` parameter: `atlas`,
`golang-migrate`, `goose`, `flyway`, `liquibase`, and `dbmate`. The native
`atlas` format is read from disk unchanged, preserving `atlas.sum` verification
and down migrations. Every other format is read and converted in memory to Atlas
single-file, up-only migrations, so apply executes only the source tool's
forward (up) SQL and never its down, rollback, undo, or metadata section. This
reuses the same format-loading layer as `ptah atlas migrate import`, so apply
and import agree on every format's semantics. An explicit `?format=` query on
the effective directory URL, from either `migration.dir` or CLI `--dir`,
overrides the `migration.format` project default, matching Atlas; an empty query
value selects the native `atlas` format.

```bash
# Apply a Goose directory directly — no separate import step.
ptah atlas migrate apply --url "$DATABASE_URL" \
  --dir "file://migrations?format=goose"
```

Flyway versions are compared component-wise like Flyway itself (`V1.5` sorts
before `V2`, `V1.10` after `V1.9`) and are encoded to a stable Atlas version that
depends only on the version — never on the other files in the directory — so
inserting a mid-sequence migration (a hotfix, an out-of-order merge) never
renumbers the others and existing revision checksums stay valid. Each version
maps to a fixed-width `major.minor.patch` int64 (minor and patch `0`–`99`); this
covers semantic and `yyyyMMddHHmmss` timestamp schemes. A version with more than
three components, or a minor/patch of `100` or more, cannot be represented in an
int64 and is rejected before the database is opened.

Several inputs still fail before Ptah opens the target database rather than guess
at semantics: unknown formats; goose/dbmate files missing their up directive
(never falling back to executing the whole file); Flyway repeatable (`R__`)
migrations, which Ptah cannot yet execute as versioned migrations (matching how
`ptah atlas migrate import` handles them); and two source files that resolve to
the same version. See
[`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742).
Atlas OSS does not register `migrate apply --dir-format`, `--to-version`, or
`--lock-name`; Ptah follows that surface and rejects those flags on
`migrate apply`.

## Migration Diff

`ptah atlas migrate diff` accepts a local `--dir` migration directory, one or
more local `--to` schema files, and a directly connectable `--dev-url`. With
`--env`, Ptah can read `env.schema.src`, `env.dev`, `migration.dir`,
`format.migrate.diff`, and supported non-concurrent `diff` policy from
`atlas.hcl`. Ptah drops all tables in the dev database, replays the migration
directory into it, compares that state to the desired schema files, and writes
an Atlas-style single `.sql` migration plus `atlas.sum` when changes exist. Use
a disposable dev database. If `atlas.sum` already exists, Ptah validates it
before replaying migrations and fails on checksum drift instead of silently
rehashing edited files.

```bash
ptah atlas migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db"
```

Atlas OSS registers `migrate diff --dry-run` as a hidden flag. Ptah accepts the
same hidden flag and prints the generated SQL instead of writing a migration
file or updating `atlas.sum`:

```bash
ptah atlas migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db" \
  --dry-run
```

Use `--lock-timeout` to bound waiting for Ptah's local migration-directory lock
while the command validates checksums and writes the new migration. The default
migration-file format matches Atlas's two-space SQL indentation template. Use
`--format` to render the generated migration SQL through Atlas-style Go
templates with `sql` and `.MarshalSQL`, for example to disable indentation:

```bash
ptah atlas migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db" \
  --format '{{ sql . "" }}'
```

With `--edit`, the generated migration file opens in `$VISUAL` or `$EDITOR`
before `atlas.sum` is finalized, so hand-tuned SQL still validates; `--edit`
cannot be combined with the hidden `--dry-run` flag because dry runs write no
migration file to edit. Atlas CE also registers `migrate diff --qualifier`;
Ptah accepts that flag name for surface parity and fails explicitly until
custom qualifier metadata is implemented.

`--schema` accepts repeated or comma-separated schema names and narrows the
replayed dev database state plus local desired schema files before the diff is
planned. `diff.concurrent_index.create` is rejected in this command until Ptah
can write matching no-transaction metadata into generated migration files.
Database desired-state URLs, `env://` project attributes, and Docker dev
databases fail explicitly until their semantics are implemented.

## Migration Validate

`ptah atlas migrate validate` verifies the migration directory against
`atlas.sum`. When `--dev-url` is set, Ptah first checks integrity
and then treats the dev database as scratch space: it drops user tables and
replays the migration directory to validate SQL execution semantics. If
integrity drift is found, Ptah reports the drift and does not connect to the dev
database.

A successful validation is silent, including a successful `--dev-url` replay.
Checksum mismatches exit `1`, print Atlas-compatible recovery guidance to
stdout, and print `Error: checksum mismatch` to stderr. If `atlas.sum` is
missing, the command prints the same recovery guidance and writes
`Error: checksum file not found` to stderr. For added, edited, or removed
migration files, the stdout guidance includes the first mismatched `atlas.sum`
line, file name, and reason.

This compatibility behavior is scoped to `ptah atlas` and `ptah-compat`.
Native `ptah migrations validate` keeps Ptah's success banner and native error
output; missing or malformed sum files remain exit-`2` usage failures.

```bash
ptah atlas migrate validate \
  --dir file://migrations \
  --dir-format atlas \
  --dev-url "sqlite://dev.db"
```

## Example

```bash
ptah atlas migrate apply \
  --url "$DATABASE_URL" \
  --dir ./migrations

ptah atlas schema inspect --url "$DATABASE_URL"
ptah atlas schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
ptah atlas schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "postgres://localhost/dev"
ptah atlas migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db"
ptah atlas schema fmt schema.hcl
ptah atlas migrate import \
  --from "file://flyway?format=flyway" \
  --to "file://migrations"

ptah atlas migrate lint \
  --dir ./migrations \
  --dev-url "sqlite://dev.db" \
  --latest 1
```

`migrate lint --dev-url` treats the dev database as scratch space: it drops user
tables, replays the migration directory, and then runs static lint
reporting. Docker `--dev-url` values remain an explicit gap; use a directly
connectable database URL.

With no `--format` and no project template, `ptah atlas migrate lint` prints
Atlas's default migration-analysis text report: an `Analyzing changes …` header,
a per-version block listing each analyzer group's diagnostics (with a suggested
fix where the analyzer provides one), a `-- ok (…)` line per version, and a
summary of version statuses, semantic schema changes, and diagnostics. The
report is written to stdout even when findings fail, and error-severity findings
still exit with code 1. The native `ptah migrations lint` output is unchanged.
Custom output is selected by `--format`, by `format.migrate.lint`, or by Atlas's
`lint { log = "…" }` template; an explicit CLI `--format` wins over a project
template, and a selected `--env` `lint.log` overrides a global one.

The command captures the migration directory once before checking `atlas.sum`,
selecting `--latest` versions, replaying migrations, and rendering reports.
Checksum status, findings, statement metadata, and formatted output therefore
describe the same immutable inputs.

The `Replay Migration Files` step reports the number of semantic schema changes
the selected migrations express, recovered from Ptah's dialect-aware parse of
their DDL — the same parser the replay and planner use — not a count of
statements or files. One statement can contribute zero changes (an operational
`INSERT`/`SELECT` or a construct outside the DDL grammar), exactly one (a single
`CREATE`), or several (a multi-action `ALTER TABLE`, or a `DROP TABLE` naming
several tables), so this change count and the new-migration-file count differ in
general.

Ptah also validates and fully loads the migration provider, including Atlas
templates, before dropping any objects from the dev database. A malformed
migration directory therefore leaves the existing dev database state intact;
cleanup starts only after the replay plan is valid.

For a code-by-code audit of the analyzer checks Atlas marks as Pro against
Ptah's native lint rules, see
[Comparison: Atlas Pro analyzer coverage](../../reference/comparison/#atlas-pro-analyzer-coverage).

Atlas-compatible lint directives are enabled only under the
`ptah atlas migrate lint` compatibility profile. A statement-local
`-- atlas:nolint <selector>` suppresses the following statement. A first
nonempty `-- atlas:nolint <selector>` header followed by a blank line applies
to the whole file. A bare file header ignores the file completely, so it is
absent from `.Files` and per-file analysis steps. Supported analyzer selectors
are `destructive`, `data_depend`, `concurrent_index`, `incompatible`, and
`nestedtx`; supported Atlas diagnostic aliases are `DS102`, `DS103`, and
`MF103`. Native lint and migrate-up safety keep their native directive
semantics unless the Atlas compatibility profile is selected explicitly.

Atlas-compatible migration metadata commands default to Atlas directory format.
`ptah atlas migrate hash`, `lint`, `new`, `set`, `status`, and `validate`
register `--dir-format` with Atlas's default value `atlas`. The supported value
is `atlas`; Atlas's external migration-tool formats (`golang-migrate`, `goose`,
`flyway`, `liquibase`, and `dbmate`) fail explicitly on those commands until
they are imported with `ptah atlas migrate import` or implemented natively.
`ptah atlas migrate set [version]` maps the positional version to Ptah's
native repair version, uses Atlas revision-table metadata, and internally
rewrites or creates the revision row for that version. With `--env`, it reads
`env.url`, `migration.dir`, and `migration.revisions_schema` from `atlas.hcl`;
explicit `--url`, `--dir`, and `--revisions-schema` flags keep CLI precedence.
`ptah atlas migrate status` also accepts `--revisions-schema` and runs against
Atlas revision-table metadata.

`ptah atlas migrate status --format` renders Atlas-style Go templates over
`.Env`, `.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, and
`.Status`. `ptah atlas migrate lint --format` renders over `.Env`, `.Steps`,
and `.Files`, so Atlas-style templates such as `{{ json .Files }}` work for
the supported local lint subset. Formatted output redacts credentials from URLs
before rendering.

Atlas-compatible format reports use the same data shape for `ptah atlas` and
`ptah-compat`. URL fields render as redacted URL strings in Go templates such as
`{{ .Env.URL }}`, but `{{ json . }}` emits an Atlas-like URL object with
`Scheme`, `User`, `Host`, `Path`, `RawQuery`, `Fragment`, `RawPath`,
`RawFragment`, `ForceQuery`, `OmitHost`, and, for SQLite URLs, `Schema`. Query
keys that look like passwords, tokens, secrets, or API keys are replaced with
`xxxxx`; URL userinfo passwords are removed.

| Command | Format data fields |
| --- | --- |
| `ptah atlas schema inspect --format` | `.Realm`, `.Schema`, `.MarshalHCL`, `.MarshalSQL`, `.MarshalJSON`, plus `hcl`, `sql`, `json`, `base64url`, `mermaid`, `split`, and `write` template helpers. |
| `ptah atlas schema apply --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for the planned SQL statements. |
| `ptah atlas schema clean --format` | `.Env.Driver`, `.Env.URL`, `.DryRun`, `.Applied`, `.Objects`, and `.Changes`. |
| `ptah atlas schema diff --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for generated migration SQL. |
| `ptah atlas migrate apply --format` | `.Driver`, `.URL`, `.Dir`, `.Env`, `.Pending`, `.Applied`, `.Current`, `.Target`, `.Start`, `.End`, `.Error`, and JSON `.Message` for successful or no-op reports. `.Pending` and `.Applied` entries expose `.Name`, `.Version`, `.Description`; applied entries also expose `.Applied`, `.Skipped`, `.Checks`, and statement `.Error`. |
| `ptah atlas migrate diff --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for generated migration SQL. |
| `ptah atlas migrate lint --format` | `.Env.Driver`, `.Env.URL`, `.Env.Dir`, `.Steps`, and `.Files`. Step entries expose `.Name`, `.Text`, `.Error`, and `.Result`; file entries expose `.Name`, `.Text`, `.Error`, and `.Findings`. |
| `ptah atlas migrate status --format` | `.Env.Driver`, `.Env.URL`, `.Env.Dir`, `.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, and `.Status`. Available and pending migration file entries expose `.Name`, `.Version`, and `.Description`. Applied revision entries expose `.Version`, `.Description`, `.Type`, `.Applied`, `.Total`, `.ExecutedAt`, `.ExecutionTime`, `.Error`, `.ErrorStmt`, and `.OperatorVersion`. |

For existing scripts that already call `atlas`, install or copy the
`ptah-compat` drop-in replacement under that executable name:

```bash
install_dir="$(go env GOPATH)/bin"
ln -sf "$(command -v ptah-compat)" "$install_dir/atlas"
atlas migrate apply --url "$DATABASE_URL" --dir ./migrations
```

Ptah translates or implements supported Atlas-style flags. Unsupported Atlas
flags should fail clearly instead of being ignored.

`ptah atlas migrate import` is intentionally fail-closed: use a destination
directory different from the source directory, and start with a destination that
does not already contain `.sql` migration files or `atlas.sum`.
Flyway repeatable migrations currently fail explicitly because Ptah does not yet
execute Atlas R-suffixed imported migrations.

## Check before migration

```bash
ptah atlas migrate hash --dir ./migrations
ptah atlas migrate new add_users --dir ./migrations
ptah atlas migrate validate --dir ./migrations --dev-url "sqlite://dev.db"
ptah atlas migrate status --url "$DATABASE_URL" --dir ./migrations
```

When converting scripts, keep the `atlas` namespace in the Ptah command:

| Do | Do not |
| --- | --- |
| `ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations` | `ptah migrate apply --url "$DATABASE_URL" --dir ./migrations` |
| `ptah atlas schema inspect --url "$DATABASE_URL"` | `ptah schema inspect --url "$DATABASE_URL"` |

When replacing an existing Atlas binary in scripts, use the `ptah-compat`
drop-in replacement instead of adding root-level Atlas spellings to `ptah`:

```bash
atlas schema apply --url "$DATABASE_URL" --to file://schema.sql --dry-run
atlas schema inspect --url "$DATABASE_URL"
```

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../../operate/conformance/) for current evidence and
[Comparison](../../reference/comparison/) for tracked product, coverage, and
documentation gaps.
