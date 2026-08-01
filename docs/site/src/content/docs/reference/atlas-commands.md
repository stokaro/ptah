---
title: Atlas-compatible commands
description: Per-command status for every ptah-compat verb, with Atlas differences and known gaps.
---

This page is the lookup reference for the Atlas-compatible surface: what each
`ptah-compat <command>` does, where it differs from Atlas, and which inputs
fail explicitly. Usage, flags, and worked examples live on
[Atlas migrate commands](../../atlas/migrate-commands/) and
[Atlas schema commands](../../atlas/schema-commands/); the surfaces and
translation model are on the
[Atlas compatibility overview](../../atlas/overview/). Native verbs are on
[Native commands](../native-commands/).

The Atlas-compatible commands are hosted by the separate `ptah-compat` binary,
a drop-in replacement for scripts that need Atlas-style root commands; the main
`ptah` binary has no Atlas command paths. The invocations on this page are
written as `ptah-compat <command> ...`, the name the binary ships under. Each
verb section names its native `ptah` twin.

## Utility commands

| Command | Behavior |
| --- | --- |
| `ptah-compat version` | Prints Ptah build information. |
| `ptah-compat license` | Prints Ptah's MIT license and the license-clean Atlas compatibility notice. |
| `ptah-compat completion <shell>` | Generates shell completion output for the Atlas-compatible command tree under the invoked executable name. |

## Migrate commands

### `ptah-compat migrate apply`

Applies Atlas-format migration directories with Atlas-compatible apply flags
and Atlas revision bookkeeping by default. With `--env`, reads `env.url`,
`migration`, and `format.migrate.apply` from `atlas.hcl`.

Executes every Atlas OSS directory format selected by `migration.format` or a
`?format=` directory URL query; non-`atlas` formats are converted in memory to
up-only migrations.

Honors the `-- atlas:checkpoint` file directive: a fresh database applies
only the latest checkpoint plus post-checkpoint migrations, and a database
that already applied pre-checkpoint history silently skips the checkpoint,
matching measured Atlas behavior.

**Fails before the target database is opened:** unknown formats, Flyway
repeatable (`R__`) migrations, goose/dbmate files missing their up directive,
colliding versions, and a hashed directory that fails `atlas.sum`
verification — the checksum-mismatch refusal is byte-identical to
`ptah-compat migrate validate` and nothing is applied.

**Rejected on this verb, matching Atlas OSS:** `--dir-format`, `--to-version`,
and `--lock-name`.

Native twin: [`ptah migrations up`](../native-commands/).

### `ptah-compat migrate status`

Reports Atlas-format migration status with Atlas revision-table metadata and
Atlas-format migration directories by default. Supports `--dir-format atlas`,
`--revisions-schema`, and Atlas Go-template `--format` output over `.Env`,
`.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, and `.Status`.
Native twin: [`ptah migrations status`](../native-commands/).

### `ptah-compat migrate hash`

Forwards to `ptah migrations hash` with Atlas `--dir-format` defaulting to
`atlas`, so the compatibility path writes `atlas.sum` by default.

### `ptah-compat migrate validate`

Silently verifies `atlas.sum` on success. Missing or mismatched checksum files
use Atlas-compatible exit-1 stdout/stderr diagnostics, and `--dev-url` cleans
the dev database and replays the migration directory to validate SQL
execution. Native `ptah migrations validate` keeps its own banner and exit
contract.

### `ptah-compat migrate lint`

Runs Ptah migration linting with Atlas `--dir-format` defaulting to `atlas`.

| Flag | Behavior |
| --- | --- |
| `--latest N` | Maps to native changeset linting. |
| `--git-base`, `--git-dir` | Map to native changeset linting. |
| `--dev-url` | Infers the lint dialect, and cleans and replays migrations on directly connectable dev databases. |
| `--format` | Atlas Go-template output over `.Env`, `.Steps`, and `.Files`. The default is Atlas's migration-analysis text report. |

Docker dev databases and web reports remain explicit gaps.
Native twin: [`ptah migrations lint`](../native-commands/).

### `ptah-compat migrate new`

Creates an Atlas single-file skeleton migration and updates `atlas.sum` by
default; the native equivalent is `ptah migrations create`. Supports
`--dir-format atlas`, and `--edit` opens the created file in
`$VISUAL`/`$EDITOR` before `atlas.sum` is refreshed.

### `ptah-compat migrate set [version]`

Moves Atlas revision history to the positional version without executing
migration SQL, with Atlas revision-table metadata and Atlas-format migration
directories by default. With `--env`, reads `env.url`, `migration.dir`, and
`migration.revisions_schema` from `atlas.hcl`; explicit `--dir`, `--url`, and
`--revisions-schema` flags keep CLI precedence. Native twin:
[`ptah migrations set`](../native-commands/).

### `ptah-compat migrate down`

Forwards to `ptah migrations down` with mapped Atlas flags.

| Flag | Behavior |
| --- | --- |
| `--dev-url` | Replays and verifies the rollback plan on the dev database before the target is touched (native `--shadow-db`). |
| `--format` | Flag or `PTAH_FORMAT`; renders an Atlas Go-template report with the `YES` confirmation prompt on stderr. `--dry-run` and the native `--confirm` pass-through skip the prompt. |
| `--revision-format` | Defaults to `atlas`, like `migrate set`. The native `ptah` pass-through selects ptah bookkeeping. |

Because the forward defaults to Atlas revision bookkeeping, a bare invocation
reverts the revisions `ptah-compat migrate apply` wrote.

The registry-bound `--to-tag`, `--skip-checks`, and `--plan` flags are recorded
waivers that fail loudly with their rationale.

### `ptah-compat migrate diff`

Validates an existing `atlas.sum`, replays a local Atlas migration directory on
`--dev-url`, diffs it against `--to`, and writes new Atlas-style migration
files. `atlas.sum` updates only after every file was written; a failed write
rolls the whole generation back.

**Desired state (`--to`)** accepts one of: local `.hcl`, `.yaml`, `.yml`, or
`.sql` files; one directly connectable database URL; one local Atlas migration
directory; or one `env://` reference into the evaluated `atlas.hcl`
environment. Source kinds cannot be mixed, the database source must use the
`--dev-url` dialect, and a desired database must not identify the same database
as `--dev-url`.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--dry-run` | Atlas-hidden; prints the generated SQL instead of writing files. |
| `--format` | Renders generated SQL with `sql` and `.MarshalSQL` templates. The default is Atlas-style two-space indentation. |
| `--schema`/`-s` | Narrows the current and desired schemas used for comparison and output. |
| `--edit` | Opens the generated migrations in `$VISUAL`/`$EDITOR` before `atlas.sum` is finalized. |
| `--env` | Reads `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and supported `diff` policy from `atlas.hcl`. |

`--schema` narrows comparison only: migration replay and cleanup still own the
complete [dev database realm](../../concepts/database-urls-and-dev-databases/).

**Concurrent indexes.** With `diff.concurrent_index.create`, new indexes are
planned as `CREATE INDEX CONCURRENTLY` and their files are tagged with the
Atlas `-- atlas:txmode none` directive, splitting mixed plans into a
transactional file followed by a concurrent-index file. Unsplittable mixes are
refused.

**`--lock-timeout`** bounds waiting for both Ptah's local migration-directory
lock and the exclusive dev-database lock:

- PostgreSQL, YugabyteDB, MySQL, MariaDB, and SQL Server use session advisory
  locks;
- SQLite, ClickHouse, and CockroachDB use an operating-system lock keyed by
  normalized database identity;
- dialects without a safe dev-database lock fail before cleanup.

Cross-host ClickHouse and CockroachDB replay is unsupported.

**`--qualifier`** prefixes every object in the generated statements with a
custom schema qualifier on PostgreSQL-family, MySQL, and MariaDB dev databases.
Invalid values, unsupported dialects, multi-schema plans, and statement kinds
Ptah cannot re-qualify yet (for example enum types) fail explicitly before any
file or checksum is written.

Docker dev databases remain an explicit gap.
Native twin: [`ptah migrations generate`](../native-commands/).

### `ptah-compat migrate import`

Imports local `file://` migration directories from `atlas`, `golang-migrate`,
`goose`, `flyway`, `liquibase`, or `dbmate` format into a separate Atlas
single-file directory and writes `atlas.sum`. Flyway repeatable migrations
fail explicitly until Ptah can execute Atlas R-suffixed imported migrations.
The native `ptah migrations import` converts the same source formats into
Ptah-native migrations instead.

### `ptah-compat migrate checkpoint [name]`

Forwards to `ptah migrations checkpoint`, replaying the migration directory on
the `--dev-url` dev database and writing a ptah-format cumulative-schema
checkpoint pair with `ptah.sum` refreshed.

| Argument | Maps to |
| --- | --- |
| `--dir` | The native migrations directory. |
| Positional name (optional) | The checkpoint description. |
| `--dir-format=ptah` | Passes through. |
| `--dir-format=atlas` | A recorded waiver, rejected loudly. |

Checkpoint output is ptah-format only, so this verb operates on ptah-format
directories: Ptah marks the checkpoints it writes via the ptah file-name
convention and does not emit Atlas-format checkpoint files yet. The read side
does honor Atlas's `-- atlas:checkpoint` directive: applying an externally
produced Atlas checkpoint directory bootstraps a fresh database from the
latest checkpoint and silently skips the checkpoint on a database that
already applied the pre-checkpoint history, matching measured Atlas
behavior.

Atlas keeps `migrate checkpoint` in its Pro build, so this is a free Ptah
capability rather than an Atlas CE stub.

### `ptah-compat migrate test [paths]`

Forwards to `ptah migrations test`.

| Atlas flag | Native equivalent |
| --- | --- |
| `--dir` | The native migration directory, Atlas-format by default via `--dir-format`. |
| `--dev-url` | The native throwaway database; an ephemeral SQLite database when omitted. |
| `--run` | The native case-name filter. |
| Positional path (optional) | The directory of Ptah-native YAML test cases, default `./tests`. |

Exit codes match the native runner: 0 when all cases pass, 1 on test failure.

Atlas keeps `migrate test` in its Pro build, so this is a free Ptah capability
rather than an Atlas CE stub.

### `ptah-compat migrate edit {name | version}`

Forwards to `ptah migrations edit`: the positional maps to the native
`--version` (a migration file name contributes its leading version digits),
`--dir` maps to the native migration directory (Atlas-format by default via
`--dir-format`), the editor resolves from `$VISUAL`, then `$EDITOR`, and the
directory checksum is rewritten afterwards. Atlas keeps `migrate edit` outside
its community build, so this is a free Ptah capability rather than an Atlas CE
stub.

### `ptah-compat migrate rebase {name | version}`

Forwards to `ptah migrations rebase`: re-timestamps the selected migration
past every existing version and rewrites the directory checksum. Multiple
positional values and `a...b` version ranges are rejected loudly; forward one
migration per run. Atlas keeps `migrate rebase` outside its community build,
so this is a free Ptah capability rather than an Atlas CE stub.

### `ptah-compat migrate rm {name | version}`

Forwards to `ptah migrations rm`: deletes the selected migration's files and
rewrites the directory checksum. Atlas keeps `migrate rm` outside its
community build, so this is a free Ptah capability rather than an Atlas CE
stub.

### `ptah-compat migrate push`

Registered Atlas CE boundary stub for a community-version unsupported command,
kept by decision: Atlas push targets the proprietary, account-bound Atlas
Registry protocol. `--help` prints the Atlas CE unsupported notice and exits
0; direct execution prints the Atlas CE abort text and exits 1. The open
replacement is the native `ptah migrations push` to any OCI registry.

## Schema commands

### `ptah-compat schema inspect`

Inspects the `--url` source and writes Atlas-compatible schema output without
Ptah status banners.

**Sources (`--url`)** accepts one of: a live database URL; a local `.hcl`,
`.yaml`, `.yml`, or `.sql` schema file; a migration directory (a directory
containing `atlas.sum`); or an `env://` reference resolved through the
evaluated `atlas.hcl` env.

Non-database sources require `--dev-url` and are evaluated on the dev database:
it is reset, the source is materialized on it (schema files executed, migration
directories replayed), and the result is introspected. Inspecting a file
without `--dev-url` fails with Atlas's `--dev-url cannot be empty` message.

**Output formats**

| Output | How to request it |
| --- | --- |
| HCL | The default. |
| SQL | `--format sql` or `--format '{{ sql . }}'`. |
| JSON | `--format json` or `{{ json . }}`. |
| Custom templates | `{{ .MarshalHCL }}`, `{{ hcl . }}`, `{{ sql . }}`, `{{ mermaid . }}`. |

**Split-write exports.** `{{ hcl . | split | write "schema" }}` and
`{{ sql . | split | write "schema" }}` support the documented Atlas split
strategies: per object (the default, with a `main.sql` `atlas:import` entry
point for SQL), `split "schema"`, and `split "type"`, plus an optional
file-extension argument.

Exports render one output plan applied by a single writer. Duplicate output
paths, traversal or escape from the output directory, planned file/directory
collisions, and existing-directory destinations fail explicitly before anything
is written. The pinned Atlas CE binary rejects `split`, `write`, and `hcl` as
non-community template functions, so these exports are an open Ptah extension.

**Filtering**

- `--schema`/`-s` narrows inspection when supported by the database reader.
- The OSS `--exclude` flag filters inspected resources with Atlas-style globs
  and `[type=...]` selectors, including the Atlas-documented
  `*[type=extension].version` field selector with schema-qualified globs.
- Other field-level exclude selectors and type selectors on non-final pattern
  segments fail explicitly; include filtering and exporter blocks remain
  explicit gaps.

Native twin: [`ptah schema inspect`](../native-commands/).

### `ptah-compat schema apply`

Diffs a live database against the `--to` desired state, prints the planned SQL,
and applies it after interactive confirmation or explicit `--auto-approve`.

**Desired state (`--to`)** accepts one of:

- local `file://` `.hcl`, `.yaml`, `.yml`, or `.sql` schema files;
- one directly connectable database URL;
- one migration directory (a `file://` directory containing `atlas.sum`)
  replayed on the required `--dev-url` dev database;
- one `env://<attribute>` reference (`src`, `schema.src`, `url`, `dev`,
  `migration.dir`) resolved through the evaluated `atlas.hcl` env.

All `--to` values must be one source kind, database and migration-directory
sources accept one URL, and unsupported schemes such as `atlas://` fail before
the target database is contacted.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--dry-run` | Prints the plan without applying. |
| `--tx-mode` | `file` and `all` execute the generated plan in one transaction; `none` executes statements without transaction wrapping. |
| `--format` | Atlas-style templates over planned changes with `sql` and `.MarshalSQL`. |
| `--exclude` | Filters matching resources out of both sides of the comparison before planning, as do disabled `schema.mode` values. |
| `--edit` | Opens the planned SQL in `$VISUAL`/`$EDITOR` before approval; the edited SQL is what gets applied. |
| `--file`/`-f` | Atlas's hidden alias, accepted for local HCL or SQL paths. |
| `--env` | Reads `env.url`, `env.src`, `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff` policy from `atlas.hcl`. |

`--env` evaluation includes local variable defaults, locals, `getenv`, `file`,
`fileset`, `format`, `jsonencode`, and `data.hcl_schema.<name>.url` references.

**`--schema`/`-s` and `--include`** scope both sides of the comparison.
`--schema` restricts them to the named schema scopes; `--include` positively
selects top-level resources with Atlas-style glob selectors and `[type=...]`
filters. Repeated values union deterministically, `--exclude` plus disabled
`schema.mode` values subtract afterward, cross-scope dependencies refuse the
plan with explicit diagnostics, and an empty selection reports a synced schema.

**`--plan file://<path>`** executes a pre-approved local plan file instead of
re-planning. Both plan formats are accepted, detected by content: the Atlas
`.plan.hcl` shape and Ptah's native format_version-1 `.plan.json`.

- A JSON plan is verified against its recorded source fingerprint — a drifted
  target refuses with a stale-plan error — and may run without `--to`.
- An Atlas-format plan requires `--to`, matching the official binary: its
  hashes are Atlas-computed with no local recipe, so the plan is replayed on
  a dev database from the target's current schema, and the reached state must
  equal the `--to` desired state before the target is touched. SQLite targets
  get a throwaway dev database automatically; every other dialect requires
  `--dev-url`.
- Before replaying, statements matching a deny-list of known escape
  constructs are refused by name before anything executes. The lint covers
  SQLite (`ATTACH`/`DETACH`, `VACUUM INTO`, storage-directory pragmas,
  `load_extension`), PostgreSQL (`DO` blocks, routine bodies and dynamic SQL
  calling file-access or `dblink` functions, `COPY ... PROGRAM` or `COPY` with
  a file path, `postgres_fdw`, `file_fdw`), MySQL/MariaDB
  (`LOAD DATA INFILE`, `INTO OUTFILE`/`DUMPFILE`, `LOAD_FILE`,
  `ENGINE=FEDERATED`, `CREATE SERVER`, `INSTALL PLUGIN`/`COMPONENT`,
  `DATA`/`INDEX DIRECTORY`), SQL Server (`xp_cmdshell`, `xp_dirtree`,
  `OPENROWSET`, `OPENDATASOURCE`, `BULK INSERT`, `sp_addlinkedserver`), and
  ClickHouse (`URL`, `File`, `S3`, `HDFS`, `MySQL`, `PostgreSQL` table
  engines).
- **The lint is best-effort, not exhaustive, and it is not a sandbox.** String
  concatenation alone defeats any scanner, so a `--dev-url` must point at a
  database you are willing to have a foreign plan file execute arbitrary SQL
  against.
- **Real enforcement exists only on the ephemeral SQLite dev database** Ptah
  creates for SQLite targets: a throwaway file in a private temp directory
  whose session refuses `ATTACH`, `DETACH`, and `VACUUM INTO` at the engine
  level and cannot load extensions. Ptah verifies the restriction is in force
  before rehearsing and refuses to rehearse if it is not. See
  [Save and execute plan files](../../atlas/schema-commands/#where-enforcement-is-real).
- The replay also runs under `--dry-run`, so a plan can be verified without
  committing to apply it.
- Whenever a desired state is available, the end state is verified again on
  the target after the apply and a mismatch fails loudly; the verification is
  always on, like Atlas's.
- Registry `atlas://` plan URLs are rejected. `--plan` cannot be combined
  with `--file`, `--exclude`, `--schema`, `--include`, or `--edit`, and
  `--dev-url` combines with `--plan` only together with `--to`.

**`--lock-timeout`** bounds waiting for the session advisory lock that
serializes concurrent schema applies against one target. The lock is acquired
before target inspection and planning, held through simulation, confirmation,
and execution, and released on every exit path. Empty waits indefinitely, an
elapsed timeout fails before the target is inspected, and dialects without
advisory locks (SQLite, ClickHouse, CockroachDB, YugabyteDB, Spanner) proceed
unlocked with a stderr note.

**`--dev-url` rehearsal.** Before a non-dry-run apply, `--dev-url` rehearses the
exact ordered plan on the dev database — reset, the target's current schema
recreated, then the planned (or edited) statements executed under the same
transaction mode. A failed rehearsal refuses the apply with the target
unchanged; the dev database must not be the target and must share its schema
scope.

Native twin: [`ptah schema apply`](../native-commands/).

### `ptah-compat schema plan`

Computes the declarative migration from the `--from` target database to local
`--to` schema files and saves it as a local plan file. The default format is
the Atlas `.plan.hcl` shape — one `plan` block with `from`/`to` fingerprints
and the migration SQL — so the saved file is readable by Atlas's plan reader;
an `--output` path ending in `.json` writes the native fingerprinted JSON
plan (format version 1) instead. Without `--save`/`--output`/`--dry-run`, the
plan document prints to stdout.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--save` | Writes `<name>.plan.hcl`, using an Atlas-style UTC timestamp default name or `--name`. Refuses to overwrite an existing default-named file, since the timestamp has one-second granularity. |
| `--output <path>`/`-o` | Chooses the location, and a `.json` path selects the native JSON plan format. The plan name recorded inside a JSON plan stays fingerprint-derived unless `--name` is given. |
| `--dry-run` | Prints the plan document without saving. |
| `--auto-approve` | Accepted for Atlas CLI compatibility; a locally saved plan file is approved by operator review, so there is no prompt to skip. |
| `--env` | Reads `url` (the plan target), `schema.src`, `dev`, `exclude`, `schema.mode`, and supported `diff` policy from `atlas.hcl`. |

The JSON plan records the ordered SQL statements with per-statement safety
severity, the dialect, the exclude patterns, and SHA-256 fingerprints of the
source and desired schema states. The `.plan.hcl` shape carries only the
name, the fingerprints, and the migration SQL; Ptah writes its own sha256
fingerprints there (the official binary parses the file but verifies its own
base64 hashes, which have no local recipe), re-derives statement severity at
read time, and refuses to save a plan computed with `--exclude` as
`.plan.hcl` because the shape cannot record the patterns.

**Not implemented**

- Registry-bound `--push`, `--pending`, and `--repo` are recorded waivers
  that fail loudly.
- `--edit`, `--skip-lint`, `--format`, `--name-format`, `--directive`,
  `--schema`, `--include`, and `--lock-timeout` fail explicitly until
  implemented.
- The registry sub-verbs (`approve`, `lint`, `list`, `new`, `pull`, `push`,
  `rm`, `test`, `validate`) stay Atlas CE boundary stubs.

Atlas keeps `schema plan` in its Pro registry flow, so this is a free Ptah
capability rather than an Atlas CE stub.
Native twin: [`ptah schema plan`](../native-commands/).

### `ptah-compat schema diff`

Diffs two desired-state sources and prints migration SQL.

**Sources.** Each of `--from`/`-f` and `--to` accepts one of:

- local `file://` schema files with `.hcl`, `.yaml`, `.yml`, or `.sql`
  extensions;
- one directly connectable database URL, whose live schema is introspected;
- one migration directory (a `file://` directory containing `atlas.sum`)
  replayed on the required `--dev-url` dev database;
- one `env://<attribute>` reference resolved through the evaluated `atlas.hcl`
  env.

Unsupported schemes such as `atlas://` fail during validation. The SQL dialect
is pinned by `--dev-url` first, then by `--from` and `--to` database URLs; local
schema files alone still require `--dev-url`.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--format` | Atlas-style templates with `sql` and `.MarshalSQL`. |
| `--exclude` | Filters resources out of both sides before diffing, as do disabled `schema.mode` values. |
| `--schema`/`-s`, `--include` | Positively scope both sides, with the same selection semantics as `schema apply`. |
| `--env` | Reads `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and supported `diff` policy from `atlas.hcl`. |

Selection order matches `schema apply`: schema universe first, include selection
inside it, exclusion last, cross-scope dependency diagnostics, and synced output
for empty selections.

Native twin: [`ptah schema diff`](../native-commands/).

### `ptah-compat schema fmt`

Formats local `.hcl` files using HCL canonical layout. Native twin:
[`ptah schema fmt`](../native-commands/).

### `ptah-compat schema clean`

Cleans user-owned schema objects through Ptah's destructive database-cleanup
runtime.

| Flag | Behavior |
| --- | --- |
| `--dry-run` | Prints the planned cleanup. |
| `--auto-approve` | Skips the interactive confirmation, which is otherwise preserved. |
| `--format` | Renders Atlas-style templates over the cleanup plan. |
| `--env` | Reads `env.url` and `format.schema.clean` from `atlas.hcl`. |

Cleanup covers the object types Ptah cleanly models and drops today: user
tables across supported dialects, PostgreSQL enum types and sequences, and SQL
Server foreign-key constraints that must be dropped before tables.

Native twin: [`ptah schema clean`](../native-commands/).

### `ptah-compat schema test [paths]`

Forwards to `ptah schema test`.

| Atlas flag | Native equivalent |
| --- | --- |
| `-u`/`--url` | `--root-dir`. The desired schema URL is a local `file://` directory of Go schema annotations. |
| `--dev-url` | The native throwaway database; an ephemeral SQLite database when omitted. |
| `--run` | The native case-name filter. |
| Positional path (optional) | The directory of Ptah-native YAML test cases. |

With `--env`, `schema.src` supplies the desired schema URL and `dev` the dev
database. Exit codes match the native runner: 0 when all cases pass, 1 on test
failure.

Atlas keeps `schema test` in its Pro build, so this is a free Ptah capability
rather than an Atlas CE stub.

### `ptah-compat schema push`

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
