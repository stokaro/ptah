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
colliding versions, an Atlas directory that fails `atlas.sum` verification, and
an Atlas directory that carries no `atlas.sum` at all while holding at least one
`.sql` file anywhere in its tree — both checksum refusals are byte-identical to
`ptah-compat migrate validate` and nothing is applied. A directory with no
`.sql` file anywhere reports `No migration files to execute` and exits `0`.

The scan is recursive because Ptah's registrar executes migrations in
subdirectories. Atlas CE ignores subdirectories, so for that layout CE reports
nothing to execute while Ptah refuses an unhashed directory rather than running
migrations it has not verified — see [#976](https://github.com/stokaro/ptah/issues/976).

Directories in an external tool's format are gated on the `atlas.sum` the source
directory carries, verified before the source layout is parsed and before the
database is opened. The covered file set is Atlas's for that layout, so a
golang-migrate down file and a Flyway undo file are not covered, and a layout
that carries no `atlas.sum` and whose covered set is empty is not a checksum
error. What executes is what the verified checksum covers, for every layout.

**Rejected on this verb, matching Atlas OSS:** `--dir-format`, `--to-version`,
and `--lock-name`.

Pre-migration checks — `-- +ptah check` directives and Atlas txtar
`checks.sql` / `checks/*.sql` sections — are enforced here as they are natively.
Atlas registers no `--skip-checks` on `migrate apply` (measured: CE v1.2.0
answers with `unknown flag`, and the licensed v1.2.4 surface registers it only
on `migrate down`), so the emergency bypass is the `PTAH_SKIP_CHECKS`
environment variable rather than a flag this surface must not grow:

```bash
PTAH_SKIP_CHECKS=1 ptah-compat migrate apply --url "$DB" --dir file://migrations
```

It parses as a boolean, rejects a non-boolean value outright, warns on stderr
while active, and bypasses checks only — `atlas.sum` verification and revision
bookkeeping are unaffected. See
[Pre-migration checks](../../versioned/integrity-and-safety/).

Native twin: [`ptah migrations up`](../native-commands/).

### `ptah-compat migrate status`

Reports Atlas-format migration status with Atlas revision-table metadata and
Atlas-format migration directories by default. Supports `--dir-format atlas`,
`--revisions-schema`, and Atlas Go-template `--format` output over `.Env`,
`.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, and `.Status`.
Native twin: [`ptah migrations status`](../native-commands/).

### `ptah-compat migrate hash`

Writes `atlas.sum` for the migration directory. `--dir-format` defaults to
`atlas`, so the compatibility path writes `atlas.sum` by default, and the
atlas layout forwards to `ptah migrations hash`.

### `ptah-compat migrate validate`

Silently verifies `atlas.sum` on success. Missing or mismatched checksum files
use Atlas-compatible exit-1 stdout/stderr diagnostics, and `--dev-url` cleans
the dev database and replays the migration directory to validate SQL
execution. Native `ptah migrations validate` keeps its own banner and exit
contract.

### Source directory layouts on `hash` and `validate`

Both verbs read a migration directory written by another tool. The layout is
selected with either spelling Atlas accepts, and both produce the same
`atlas.sum`:

```bash
ptah-compat migrate hash --dir "file://migrations?format=goose"
ptah-compat migrate hash --dir file://migrations --dir-format goose
```

Accepted values are `atlas` (the default), `golang-migrate`, `goose`, `flyway`,
`liquibase`, and `dbmate`. `migration.format` in `atlas.hcl` selects the same
thing under `--env`. When the query and the flag disagree, the query wins: an
empty `?format=` selects the atlas layout whatever the flag says.

Each layout covers a different set of source files, matching Atlas:

| Layout | Files the checksum covers |
| --- | --- |
| `atlas`, `goose`, `liquibase`, `dbmate` | every top-level `*.sql`, ordered by name |
| `golang-migrate` | every top-level `*.up.sql`. The down file of a pair is not covered, so editing it is invisible to `validate` |
| `flyway` | the whole tree: `V` and `B` files, then `R` repeatables last. `U` undo files are dropped, and everything at or below the highest baseline is squashed |

Format names are matched exactly, so `GOOSE` and `" goose "` are unknown
formats rather than `goose`. A directory with nothing for the layout to cover —
an empty directory, or a `golang-migrate` directory holding only a down file —
hashes to the empty-set checksum and validates clean.

Inputs that stay refused where Atlas CE exits 0, all of them loudly:

- an empty `--dir-format` value;
- a query parameter other than `format`;
- a repeated `format` parameter;
- a semicolon in the query, such as `?format=flyway;x=1`, which Atlas drops
  whole and reads as the atlas layout;
- a query on a `--dir` that a later `--dir` overrides;
- a stray positional argument, including one after `--`.

None of them can produce a wrong checksum. They are tracked in
[#990](https://github.com/stokaro/ptah/issues/990); the query rules are shared
with `migrate apply`, so relaxing one widens what a future integrity gate
accepts.

`migrate apply` registers no `--dir-format` at all, matching Atlas. It gates a
directory read through `?format=` over the same per-layout file set these two
verbs hash, so what `migrate hash` writes is what `migrate apply` verifies.

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
waivers that fail loudly with their rationale. `--to-tag` and `--plan` are also
settable through their `PTAH_<FLAG>` twins, and refusing them is the point:
setting `PTAH_TO_TAG` is a request for a capability Ptah lacks, and discarding
it would leave an empty rollback target that reverts the whole history.

`--skip-checks` is the single exception, and it is explicit-only. `migrate
apply` reads `PTAH_SKIP_CHECKS` as its pre-migration check bypass, so on this
verb the variable is not a request for Atlas Cloud down checks; it neither
refuses a rollback nor appears as an `[env: ...]` suffix in `--help`.

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
the `--dev-url` dev database and writing a cumulative-schema checkpoint.

| Argument | Maps to |
| --- | --- |
| `--dir` | The native migrations directory. |
| Positional name (optional) | The checkpoint description, used as the file-name stem. |
| `--dir-format=atlas` | Writes the Atlas single-file checkpoint (default). |
| `--dir-format=ptah` | Writes the ptah reversible checkpoint pair. |

`--dir-format` selects the checkpoint convention, and **on this verb it
defaults to `atlas`**, matching the default Atlas registers and every other
compat migrate verb:

- **`atlas`** writes one up-only file, `<version>_<name>.sql`, whose first line
  is the `-- atlas:checkpoint` directive, and refreshes `atlas.sum`. The version
  is a UTC timestamp, bumped past the newest existing migration. There is no
  down file: the Atlas format is up-only, so an Atlas checkpoint is not
  reversible.
- **`ptah`** writes the reversible pair
  `NNNNNNNNNN_<name>.checkpoint.up.sql` / `.checkpoint.down.sql` and refreshes
  `ptah.sum`.

The native `ptah migrations checkpoint` keeps `ptah` as its default; only the
compat surface defaults to `atlas`. `--dir-format=auto` is refused on both,
because writing under it would have to guess the file convention and which
integrity file to refresh.

Each convention refreshes its own integrity file, so a checkpoint that would
leave a directory holding both `ptah.sum` and `atlas.sum` is refused up front —
`--dir-format auto` cannot read such a directory, and the failure would
otherwise only surface on a later command. Re-hash the directory into one
format first.

The read side honors the `-- atlas:checkpoint` directive either way: applying a
checkpoint directory bootstraps a fresh database from the latest checkpoint and
silently skips the checkpoint on a database that already applied the
pre-checkpoint history, matching measured Atlas behavior.

Atlas keeps `migrate checkpoint` in its Pro build — the pinned CE binary
registers the verb but aborts with "not supported by the community version" and
registers none of its own flags — so this is a free Ptah capability rather than
an Atlas CE stub.

`?format=` on this verb's `--dir` URL is still refused, as it is on `migrate
lint`, `new`, `set` and `status`; use `--dir-format`.

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
- `--include` positively selects the top-level resources that survive, with
  Atlas-style globs and `[type=...]` selectors. Repeated and comma-separated
  values union. Composition order is `--schema`, then `--include`, then
  `--exclude`. A selection that matches nothing renders no objects; an empty
  value carries no selection and leaves inspection unfiltered.
- The OSS `--exclude` flag filters inspected resources with Atlas-style globs
  and `[type=...]` selectors, including the Atlas-documented
  `*[type=extension].version` field selector with schema-qualified globs.
- Child resources (columns, indexes, constraints, triggers, policies, grants)
  cannot be included on their own, in either the `[type=column]` or the
  literal-dot `table.column` spelling; both fail before any database is
  contacted.
- Depth counts separators outside quotes, so an identifier holding a dot is
  selected as `main."my.table"` or `a\.b\.c`. The bare `a.b.c` spelling is
  refused, because it cannot be told apart from `schema.table.column`.
- Glob metacharacters — `*`, `?`, and character classes — match a dot, so
  `table*column`, `table?column`, and `table[.]column` are not caught by the
  depth check and select nothing
  ([#979](https://github.com/stokaro/ptah/issues/979)).
- A selection that drops a dependency of a selected object is refused rather
  than rendered.
- Other field-level exclude selectors and type selectors on non-final pattern
  segments fail explicitly; exporter blocks remain an explicit gap.

The pinned Atlas CE binary rejects `schema inspect --include` with
`unknown flag: --include`; the licensed build registers it. The measured
behavioral differences are tabulated in
[the Atlas comparison](../../atlas/comparison/#schema-inspect---include).

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
- **Real enforcement exists only on SQLite dev databases** — the ephemeral one
  Ptah creates for SQLite targets, and an operator-supplied SQLite
  `--dev-url`, since the restriction keys on the dev dialect. Their sessions
  refuse `ATTACH`, `DETACH`, and `VACUUM INTO` at the engine level and cannot
  load extensions; Ptah verifies the restriction is in force before rehearsing
  and refuses to rehearse if it is not. Storage-directory pragmas and
  `writable_schema` are not covered, so the converges-to-`--to` verdict is a
  good-faith check rather than an adversarial one. See
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
| `--edit` | Opens the planned SQL in `$VISUAL`, then `$EDITOR`, and saves the plan rebuilt from the edited text. Statement severity and the destructive marker are re-derived from what you wrote. An edit leaving no statement is refused, and nothing is written. |
| `--name-format <template>` | Computes the plan name from a Go template over `.FromHash` and `.ToHash`, this plan's own `sha256:` fingerprints. The Atlas template helpers (`json`, `upper`, `add`, `indent_ln`, …) are available. Cannot be combined with `--name`. |
| `--skip-lint` | Accepted as an explicit no-op: `schema plan` runs no lint step, so there is nothing to skip. |
| `--env` | Reads `url` (the plan target), `schema.src`, `dev`, `exclude`, `schema.mode`, and supported `diff` policy from `atlas.hcl`. |

The JSON plan records the ordered SQL statements with per-statement safety
severity, the dialect, the exclude patterns, and SHA-256 fingerprints of the
source and desired schema states. The `.plan.hcl` shape carries only the
name, the fingerprints, and the migration SQL; Ptah writes its own sha256
fingerprints there (the official binary parses the file but verifies its own
base64 hashes, which have no local recipe), re-derives statement severity at
read time, and refuses to save a plan computed with `--exclude` as
`.plan.hcl` because the shape cannot record the patterns.

Editing changes the statements, never the fingerprints. `from` still describes
the live source database, so apply-time staleness detection keeps working. `to`
still describes the schema the plan was computed against, which edited SQL may
no longer reach: `schema apply` replays an Atlas-format plan on a dev database
and requires it to converge on `--to` before touching the target, but a native
`.json` plan carries no such replay, so an edited JSON plan is only as good as
its review.

Everything that can refuse the plan without reading its statements — the
`--exclude`/`.plan.hcl` incompatibility, and every `--name-format` failure —
happens before the editor opens, so an edit is never thrown away over a problem
that was decidable beforehand.

A plan name becomes a file name, so both `--name` and `--name-format` refuse
path separators, control characters, `.`/`..`, and the characters Windows
forbids in a file name (`:*?"<>|`). That last rule means Atlas's own documented
example, `plan_{{ slice .ToHash 0 8 }}`, is refused here: Ptah fingerprints are
`sha256:<hex>`, so slicing from `0` keeps the prefix and yields `plan_sha256:`.
Slice from `7` to skip it — `plan_{{ slice .ToHash 7 15 }}` gives the digest
characters the Atlas example is after. The colon matters because it is NTFS's
alternate-data-stream separator: accepted, it would write an empty
`plan_sha256` file with the plan document hidden in a stream.

**Not implemented**

- Registry-bound `--push`, `--pending`, and `--repo` are recorded waivers
  that fail loudly.
- `--format` fails explicitly. Atlas's plan report payload was never executed
  on the licensed trial, so its field names are unknown; an invented shape
  would silently break Pro templates that reference the real ones.
- `--directive` fails explicitly. The measured Atlas `.plan.hcl` carries only
  `from`, `to`, and `migration`, so a directive would have to ride inside the
  migration heredoc in an unmeasured spelling — and Ptah's own reader ignores
  `-- atlas:checkpoint` today, so emitted directives would be silent no-ops.
- `--schema`, `--include`, and `--lock-timeout` fail explicitly until
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
