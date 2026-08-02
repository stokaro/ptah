---
title: Atlas migrate commands
description: Run Atlas-style migration workflows with ptah-compat migrate apply, down, status, diff, lint, and the directory-maintenance verbs.
---

You have an Atlas-format migration directory — or scripts that manage one with
`atlas migrate ...` — and want to run that workflow through Ptah. This page
covers the `ptah-compat migrate` verbs: what each one does, a worked example,
and the behavior details that differ from a first guess. Every invocation on
this page uses the separate `ptah-compat` drop-in binary; the install steps
plus the flag translation rules are on the
[Atlas compatibility overview](../overview/).

## Command behavior

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah-compat migrate apply` | Atlas-format apply path equivalent to `ptah migrations up`; executes every Atlas OSS directory format and refuses an Atlas directory whose `atlas.sum` is missing or stale. |
| `ptah-compat migrate down` | Forwards to `ptah migrations down` with mapped Atlas flags and Atlas revision bookkeeping by default; `--dev-url` verifies the rollback plan first. |
| `ptah-compat migrate status` | Atlas-format migration status with Atlas revision-table metadata. |
| `ptah-compat migrate hash` | Forwards to `ptah migrations hash`; writes `atlas.sum` by default. |
| `ptah-compat migrate validate` | Silently verifies `atlas.sum` on success; `--dev-url` replays migrations to validate SQL execution. |
| `ptah-compat migrate lint` | Forwards to `ptah migrations lint` with Atlas changeset selectors, dev-database replay, and Atlas report output. |
| `ptah-compat migrate new` | Creates an Atlas single-file skeleton migration; equivalent to `ptah migrations create`. |
| `ptah-compat migrate set [version]` | Moves Atlas revision history to the selected version without executing migration SQL. |
| `ptah-compat migrate diff` | Replays local Atlas migrations on `--dev-url`, diffs them against the desired state, and writes Atlas-style migration files with `atlas.sum` updated atomically. |
| `ptah-compat migrate import` | Imports local `file://` migration directories from Atlas-supported formats into a separate Atlas single-file directory. |
| `ptah-compat migrate checkpoint [name]` | Forwards to `ptah migrations checkpoint`; writes a cumulative-schema checkpoint, Atlas single-file format by default or the ptah pair under `--dir-format ptah`. |
| `ptah-compat migrate test [paths]` | Forwards to `ptah migrations test` with Ptah-native YAML test cases. |
| `ptah-compat migrate edit {name \| version}` | Forwards to `ptah migrations edit` and rewrites the directory checksum. |
| `ptah-compat migrate rebase {name \| version}` | Forwards to `ptah migrations rebase`; one migration per run. |
| `ptah-compat migrate rm {name \| version}` | Forwards to `ptah migrations rm` and rewrites the directory checksum. |
| `ptah-compat migrate push` | Atlas CE boundary stub; the native `ptah migrations push` to any OCI registry is the open replacement. |

Per-verb status detail — Atlas differences, waivers, and the inputs that fail
explicitly — is on [Atlas-compatible commands](../../reference/atlas-commands/).

## Worked example: an Atlas-format directory

Atlas-style migration files can include `migration.sql`, `down.sql`, the
default `checks.sql`, and ordered `checks/*.sql` sections inside txtar archives.
Ptah executes `migration.sql` on apply and `down.sql` on rollback, and enforces
every check file as a pre-migration gate. Each assertion must be a top-level
`SELECT` returning exactly one column and one row with a truthy scalar. All
assertions in a file must pass unless a file-level `-- atlas:assert oneof`
requires at least one; an empty `oneof` file fails closed.

Dialect-aware splitting preserves PostgreSQL escape strings and MySQL/MariaDB
semantic comments instead of rewriting assertion SQL. Checks use a dedicated
physical session that Ptah discards afterward. Transaction-capable drivers roll
back, while ClickHouse uses the disposable session directly because its driver
does not implement transactions.

For MySQL/MariaDB executable comments, Ptah validates the effective SQL and
evaluates version guards against the connected server. Numeric prefixes shorter
than five digits remain part of the executable SQL body. Hidden statement
delimiters and non-`SELECT` effective bodies fail closed before query execution.

A failure aborts before any body statement, matching the licensed Atlas build's
enforcement point. Ptah ignores unrelated embedded files.

Create an Atlas-style migration:

```text
-- atlas:txtar

-- migration.sql --
CREATE TABLE users (
  id integer PRIMARY KEY,
  email text NOT NULL UNIQUE
);

-- down.sql --
DROP TABLE users;
```

Name the file with a migration version, for example:

```text
migrations/20260721120000_create_users.sql
```

Hash and validate the directory:

```bash
ptah-compat migrate hash --dir file://migrations
ptah-compat migrate validate --dir file://migrations
```

Expected output includes:

```text
Wrote migrations/atlas.sum
1 migration file(s) hashed
```

A successful `validate` is silent.

Apply it, then check status:

```bash
ptah-compat migrate apply \
  --url "$DATABASE_URL" \
  --dir file://migrations

ptah-compat migrate status \
  --url "$DATABASE_URL" \
  --dir file://migrations
```

Expected output includes:

```text
Migrating to version 20260721120000 from 1 pending migrations.
Migration complete. Current version: 20260721120000
```

```text
=== MIGRATION STATUS ===
Current Version: 20260721120000
Total Migrations: 1
Applied Migrations: 1
Pending Migrations: 0
Status: Database is up to date
```

Roll back using the `down.sql` section. A bare `ptah-compat migrate down` reads
the Atlas revision rows `migrate apply` wrote, and asks for a `YES`
confirmation before touching the database (`--dry-run` skips both):

```bash
ptah-compat migrate down \
  --url "$DATABASE_URL" \
  --dir file://migrations \
  --to-version 0
```

Expected output ends with:

```text
✅ Migration rollback completed successfully!
Database is now at version: 0
```

Ptah validates every selected down body before rollback starts. If one is
missing, the command leaves both the schema and Atlas revision rows unchanged.
Dry runs use the same dirty-state, checksum, checkpoint, and down-body
validation path as real rollbacks, while suppressing schema and revision writes.

Add `--dev-url` to reset a disposable dev database, replay the migration
directory to the target's current version, and verify the rollback there before
the target is touched:

```bash
ptah-compat migrate down \
  --url "$DATABASE_URL" \
  --dev-url "$DEV_DATABASE_URL" \
  --dir file://migrations \
  --to-version 0
```

The dev database must select a different live database or catalog from
`--url`. Ptah rejects equivalent URL aliases first, then connects and verifies
the actual dialects and selected database/catalog names before resetting the
dev database. Equal live names fail closed across different endpoints.

Native `ptah migrations` commands read the same directory when `--dir-format
atlas` is passed; the native lifecycle is documented under
[Versioned migrations](../../versioned/overview/). If parsing fails, force
`--dir-format atlas` and inspect the migration file for section names: Ptah
recognizes `migration.sql` and `down.sql`, and other section names are not
executed.

## Apply a migration directory

`ptah-compat migrate apply` reads a local Atlas migration directory and records
runtime history in Atlas revision-table format by default. The optional
positional `amount` applies only the first N pending migrations. Use
`--baseline` to mark earlier migration files as applied without executing their
SQL bodies before applying the remaining pending migrations.

The directory's integrity is checked before anything executes, matching
official Atlas:

- An Atlas directory whose `atlas.sum` does not verify is refused with
  `Error: checksum mismatch`.
- An Atlas directory that carries no `atlas.sum` at all is refused with
  `Error: checksum file not found`; run `ptah-compat migrate hash` once and
  commit the file. A directory holding no `.sql` file anywhere in its tree is
  not a checksum error — it reports `No migration files to execute` and exits
  `0`, matching Atlas. The scan is recursive because Ptah executes migrations in
  subdirectories, which Atlas ignores
  ([#976](https://github.com/stokaro/ptah/issues/976)).
- Directories read through `?format=` (goose, flyway, liquibase, dbmate,
  golang-migrate) are gated on the `atlas.sum` the **source** directory carries,
  verified before the source layout is parsed. The covered file set is the one
  Atlas uses for that layout, so a golang-migrate down file and a Flyway undo
  file are not covered, and a layout whose covered set is empty is not refused.
  Run `ptah-compat migrate hash --dir 'file://migrations?format=goose'` to write
  it. "Not covered" also means "not executed": for every layout the set apply
  runs is the set the verified checksum covers.

Both refusals exit `1` with output identical to `ptah-compat migrate validate`
on that directory, no migration runs, and the target database is never created.

Migrations whose first line is the `-- atlas:checkpoint` directive get
measured Atlas checkpoint semantics — a fresh database applies only the
latest checkpoint plus later migrations, and a database that already applied
pre-checkpoint history skips the checkpoint silently.

```bash
ptah-compat migrate apply 2 \
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
Dry-run plans read the stored Atlas revision rows and include only migrations
that a real apply would select. They also run the same dirty-state, checksum,
execution-order, and transaction-mode validations as a real apply.

A successful `ptah-compat migrate apply` writes nothing to stderr, matching
Atlas CE: there is no progress narration, in a dry run or otherwise, so
`--format` output survives the usual CI idiom of folding both streams together.

```bash
ptah-compat migrate apply --url "$DATABASE_URL" --dir file://migrations \
  --dry-run --format '{{ json . }}' 2>&1 | jq
```

Two things still reach stderr, by design. A command that fails prints its
`Error: …` diagnostic there and exits `1`. And a Warn-level diagnostic that
exists on no other channel — such as function ordering or a dev database that
would not close — is still reported, because dropping it would let an apply
claim success while quietly degrading. Valid circular foreign keys are rendered
in two phases and do not produce a warning. Neither diagnostic appears on a
clean run.

:::caution
`ptah-compat migrate down` is the exception. Without `--format` it forwards to
the native `ptah migrations down`, which starts its own run log and writes it to
stderr — eight lines on a successful dry run, four on a successful rollback.
The Atlas surface exposes no `--log-level` to quiet it. Use `--format` when you
need a clean stderr from `migrate down`; that path renders the report directly
and stays quiet. Tracked in
[`stokaro/ptah#969`](https://github.com/stokaro/ptah/issues/969).
:::

The equivalent native command, `ptah migrations up --dry-run`, does narrate each
statement it would execute through its run log; that narration is selected by
the native `--log-level` and `--log-format` flags, which the Atlas surface does
not expose. See [Apply migrations](../../versioned/apply/).

The apply path executes every Atlas OSS migration directory format selected by
`migration.format` or the directory URL `?format=` parameter: `atlas`,
`golang-migrate`, `goose`, `flyway`, `liquibase`, and `dbmate`. The native
`atlas` format is captured unchanged, preserving `atlas.sum` verification and
down migrations.

Every other format is captured first and then converted in memory to Atlas
single-file, up-only migrations, so apply executes only the source tool's
forward (up) SQL and never its down, rollback, undo, or metadata section. This
reuses the same format-loading layer as `ptah-compat migrate import`, so apply
and import agree on every format's semantics.

An explicit `?format=` query on the effective directory URL, from either
`migration.dir` or CLI `--dir`, overrides the `migration.format` project
default, matching Atlas; an empty query value selects the native `atlas`
format.

The local directory is opened through a rooted handle and captured twice before
the target database is opened. The command aborts when the captures differ or a
migration symlink escapes the root. Apply planning, execution, `atlas.sum`
verification, and `--format` output all use the resulting immutable filesystem
instead of reopening the path.

```bash
# Apply a Goose directory directly — no separate import step.
ptah-compat migrate apply --url "$DATABASE_URL" \
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
`ptah-compat migrate import` handles them); and two source files that resolve to
the same version. See
[`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742).
Atlas OSS does not register `migrate apply --dir-format`, `--to-version`, or
`--lock-name`; Ptah follows that surface and rejects those flags on
`migrate apply`.

The direct `migrate down --format` path uses the same snapshot for rollback
planning, optional `--dev-url` shadow verification, target execution, and the
rendered report. `migrate status`, `migrate lint`, and `migrate set` also capture
their local directory before database work and do not reopen it later.

## Generate a migration with migrate diff

`ptah-compat migrate diff` accepts a local `--dir` migration directory, a
directly connectable `--dev-url`, and one desired schema source through
`--to`. The source can be one or more local schema files, one directly
connectable database URL, one local Atlas migration directory, or one `env://`
reference into the evaluated `atlas.hcl` environment. With `--env`, Ptah can
read `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and
supported `diff` policy from `atlas.hcl`.

Ptah snapshots the desired schema first, cleans the dev database, and replays
the migration directory into it. It compares the replayed state to the
snapshot, cleans the dev database again, and only then writes Atlas-style
`.sql` migration files plus `atlas.sum` when changes exist. The final cleanup
also runs after replay, introspection, comparison, or context-cancellation
failures.

Use a disposable dev database. Ptah only reads a database used as `--to`; it
never cleans or mutates that database. Ptah rejects a desired database that
identifies the same host, port, and database as `--dev-url`, even when
credentials, connection options, scheme aliases, or an explicit default port
differ. Repeated `--schema` values filter both sides of the comparison and the
generated output.

They do not narrow the [dev database
realm](../../concepts/database-urls-and-dev-databases/) replayed and cleaned
by Ptah.

If `atlas.sum` already exists, Ptah validates it before replaying migrations
and fails on checksum drift instead of silently rehashing edited files. Ptah
holds the migration-directory lock while it captures and verifies one
immutable directory snapshot, replays that exact snapshot, and publishes the
result.

It also holds an exclusive dev-database lock from desired-schema resolution
through final cleanup. PostgreSQL, YugabyteDB, MySQL, MariaDB, and SQL Server
use session advisory locks. SQLite, ClickHouse, and CockroachDB use an
operating-system lock keyed by normalized database identity. The latter
coordinates Ptah processes on one host; cross-host ClickHouse and CockroachDB
replay is unsupported.

A dialect without a safe locking mechanism fails before Ptah cleans the dev
database. Generated migration files are staged before publication, and
`atlas.sum` is atomically replaced last as the batch commit marker. An
OS-backed lock prevents cooperating processes from planning against the same
directory concurrently and is released by the operating system if a process
exits unexpectedly.

The generated versions and checksum are derived from the captured snapshot.
Ptah rejects a migration added after that capture instead of publishing above
an unreplayed file. `atlas.sum` is updated only after every migration file of
the run was written. A failed write rolls the generation back immediately.

If the process exits between publishing the SQL files and publishing
`atlas.sum`, the durable publication journal remains next to the migration
directory; the next lock holder compares the checksum commit marker and either
finalizes the committed batch or removes only the hard-linked files owned by
the interrupted batch.

```bash
ptah-compat migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db"
```

Expected output includes:

```text
Created migration file: .../migrations/20260721120001_add_users.sql
Updated migration checksum: .../migrations/atlas.sum
```

Use a live database as the desired schema:

```bash
ptah-compat migrate diff mirror_schema \
  --dir file://migrations \
  --to "$DESIRED_DATABASE_URL" \
  --dev-url "$DEV_DATABASE_URL"
```

Use an evaluated project attribute as the desired schema:

```bash
ptah-compat migrate diff mirror_schema \
  --config file://atlas.hcl \
  --env local \
  --to env://url
```

`env://src` and `env://schema.src` resolve the selected environment's schema
sources. `env://url` and `env://dev` resolve database URLs, while
`env://migration.dir` resolves the configured migration directory. When
`--to` is omitted, `migrate diff` resolves the selected environment's
`schema.src` through the same typed path, including database-valued defaults.
An `env://` reference must be the only `--to` value. Mixed source kinds,
multiple database or migration-directory sources, nested `env://` references,
dialect mismatches, and source/dev aliases fail before Ptah connects to the
dev database.

Use an Atlas migration directory as the desired state:

```bash
ptah-compat migrate diff import_history \
  --dir file://migrations \
  --to file://desired-migrations \
  --dev-url "$DEV_DATABASE_URL"
```

The desired directory is checksummed and replayed on the dev database to
capture a schema snapshot. Ptah cleans the dev database before it evaluates
the output directory. A source loading failure therefore does not create the
output migration directory, and a successful source replay does not leave its
objects behind.

Atlas OSS registers `migrate diff --dry-run` as a hidden flag. Ptah accepts the
same hidden flag and prints the generated SQL instead of writing a migration
file or updating `atlas.sum`:

```bash
ptah-compat migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db" \
  --dry-run
```

Use `--lock-timeout` to bound waiting for both the migration-directory lock and
the exclusive dev-database lock. The default migration-file format matches
Atlas's two-space SQL indentation template. Use `--format` to render the
generated migration SQL through Atlas-style Go templates with `sql` and
`.MarshalSQL`, for example to disable indentation:

```bash
ptah-compat migrate diff add_users \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "sqlite://dev.db" \
  --format '{{ sql . "" }}'
```

With `--edit`, the generated migration files open in `$VISUAL` or `$EDITOR`
before `atlas.sum` is finalized, so hand-tuned SQL still validates; `--edit`
cannot be combined with the hidden `--dry-run` flag because dry runs write no
migration file to edit.

`--qualifier` matches Atlas's single-schema semantics: every object named by
the generated statements is prefixed with the custom schema qualifier, so the
file can be applied to a schema other than the one it was planned against:

```bash
ptah-compat migrate diff add_items \
  --dir file://migrations \
  --to file://schema.sql \
  --dev-url "postgres://user:pass@localhost:5432/dev" \
  --qualifier tenant
# generates: CREATE TABLE "tenant"."items" (...)
```

The qualifier is supported on PostgreSQL, CockroachDB, YugabyteDB, MySQL, and MariaDB dev
databases and applies to tables, columns' foreign-key references, table-level
constraints, indexes, and drops. Invalid values (control characters, `.`,
quotes), unsupported dialects, plans spanning several schemas, and statement
kinds Ptah cannot re-qualify yet (for example enum types) fail explicitly
before any migration file or checksum is written. As with Atlas, replaying a
directory that contains qualified migrations requires the qualifier schema to
exist on the dev database.

`--schema` accepts repeated or comma-separated schema names and narrows the
replayed dev database state plus the resolved desired state before the diff is
planned. With `diff.concurrent_index.create = true` in the selected
`atlas.hcl` env, newly added indexes are planned as PostgreSQL `CREATE INDEX
CONCURRENTLY` statements.

Files carrying such statements start with the Atlas `-- atlas:txmode none`
file directive, which both Atlas and Ptah honor by executing the file outside
a transaction.

Because those statements must not silently strip transaction safety from
ordinary DDL, a mixed plan is split the same way Ptah's native generator
splits it: a `<name>_transactional` file with the transactional statements
followed by a `<name>_concurrent_indexes` file tagged `-- atlas:txmode none`;
mixes that cannot be split automatically (for example enum value additions
alongside table changes) are refused.

Docker dev databases fail explicitly until their provisioning semantics are
implemented.

## Validate integrity

`ptah-compat migrate validate` verifies the migration directory against
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

This compatibility behavior is scoped to the `ptah-compat` binary.
Native `ptah migrations validate` keeps Ptah's success banner and native error
output; missing or malformed sum files remain exit-`2` usage failures.

```bash
ptah-compat migrate validate \
  --dir file://migrations \
  --dir-format atlas \
  --dev-url "sqlite://dev.db"
```

## Lint migrations

```bash
ptah-compat migrate lint \
  --dir file://migrations \
  --dev-url "sqlite://dev.db" \
  --latest 1
```

`migrate lint --dev-url` treats the dev database as scratch space: it drops user
tables, replays the migration directory, and then runs static lint
reporting. Docker `--dev-url` values remain an explicit gap; use a directly
connectable database URL.

With no `--format` and no project template, `ptah-compat migrate lint` prints a
compatibility report: an `Analyzing changes …` header, a per-version block
listing each analyzer group's diagnostics and mapped rule IDs, a `-- ok (…)`
line per version, and a summary of version statuses, semantic schema changes,
and diagnostics. Diagnostic messages are written by Ptah and do not include
upstream documentation links or copied suggested-fix prose.

The report is written to stdout even when findings fail, and error-severity
findings still exit with code 1. The native `ptah migrations lint` output is
unchanged. Custom output is selected by `--format`, by `format.migrate.lint`,
or by Atlas's `lint { log = "…" }` template; an explicit CLI `--format` wins
over a project template, and a selected `--env` `lint.log` overrides a global
one.

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
[Comparison: Atlas Pro analyzer coverage](../comparison/#atlas-pro-analyzer-coverage).

Atlas-compatible lint directives are enabled only under the `ptah-compat
migrate lint` compatibility profile. A statement-local `-- atlas:nolint
<selector>` suppresses the following statement. A first nonempty `--
atlas:nolint <selector>` header followed by a blank line applies to the whole
file.

A bare file header ignores the file completely, so it is absent from `.Files`
and per-file analysis steps. Supported analyzer selectors are `destructive`,
`data_depend`, `concurrent_index`, `incompatible`, and `nestedtx`; supported
Atlas diagnostic aliases are `DS102`, `DS103`, and `MF103`.

Native lint and migrate-up safety keep their native directive semantics unless
the Atlas compatibility profile is selected explicitly.

## Metadata commands and directory formats

Atlas-compatible migration metadata commands default to Atlas directory
format. `ptah-compat migrate hash`, `lint`, `new`, `set`, `status`, and
`validate` register `--dir-format` with Atlas's default value `atlas`.

`hash` and `validate` read a directory rather than rewrite one, so they accept
every Atlas source layout — `golang-migrate`, `goose`, `flyway`, `liquibase`,
and `dbmate` — under either spelling Atlas accepts, and produce the same
`atlas.sum` from both:

```bash
ptah-compat migrate hash --dir "file://migrations?format=goose"
ptah-compat migrate hash --dir file://migrations --dir-format goose
```

Each layout covers a different set of source files, matching Atlas; see
[the compat command reference](../../reference/atlas-commands/) for the
per-layout rules and for the inputs that stay refused.

On `lint`, `new`, `set`, and `status` the supported value is still `atlas`;
the external migration-tool formats fail explicitly there until the directory
is imported with `ptah-compat migrate import` or the format is implemented
natively. `migrate apply` registers no `--dir-format` at all, matching Atlas,
and selects a converted source directory through `?format=` on `--dir`.

`ptah-compat migrate set [version]` moves Atlas revision history to the
selected boundary without executing migration SQL. It preserves existing clean
rows through the target, inserts missing rows as manually set, keeps dirty-row
diagnostics with the combined applied and manually-set type, and removes rows
above the target.

With `--env`, it reads `env.url`, `migration.dir`, and
`migration.revisions_schema` from `atlas.hcl`; explicit `--url`, `--dir`, and
`--revisions-schema` flags keep CLI precedence. `ptah-compat migrate status`
also accepts `--revisions-schema` and runs against Atlas revision-table
metadata.

A pre-apply check sequence for CI looks like:

```bash
ptah-compat migrate hash --dir file://migrations
ptah-compat migrate new add_users --dir file://migrations
ptah-compat migrate validate --dir file://migrations --dev-url "sqlite://dev.db"
ptah-compat migrate status --url "$DATABASE_URL" --dir file://migrations
```

## Import from other tools

`ptah-compat migrate import` converts a local `file://` migration directory from
an Atlas-supported format into a separate Atlas single-file directory and
writes `atlas.sum`:

```bash
ptah-compat migrate import \
  --from "file://flyway?format=flyway" \
  --to "file://migrations"
```

The command is intentionally fail-closed: use a destination directory different
from the source directory, and start with a destination that does not already
contain `.sql` migration files or `atlas.sum`. Flyway repeatable migrations
currently fail explicitly because Ptah does not yet execute Atlas R-suffixed
imported migrations.

The native `ptah migrations import` converts the same source formats into
Ptah-native migrations instead; see
[Import from another tool](../../versioned/import/).

## Format template fields

**`ptah-compat migrate apply --format`**

- Top level: `.Driver`, `.URL`, `.Dir`, `.Env`, `.Pending`, `.Applied`,
  `.Current`, `.Target`, `.Start`, `.End`, `.Error`, and JSON `.Message` for
  successful or no-op reports.
- Each `.Pending` and `.Applied` entry: `.Name`, `.Version`, `.Description`.
  Applied entries also expose `.Applied`, `.Skipped`, `.Checks`, and statement
  `.Error`.

**`ptah-compat migrate diff --format`**

- `.Changes`, `.MarshalSQL`, plus the `sql` helper for generated migration SQL.

**`ptah-compat migrate lint --format`**

- Top level: `.Env.Driver`, `.Env.URL`, `.Env.Dir`, `.Steps`, and `.Files`.
- Each step entry: `.Name`, `.Text`, `.Error`, and `.Result`.
- Each file entry: `.Name`, `.Text`, `.Error`, and `.Findings`.

**`ptah-compat migrate status --format`**

- Top level: `.Env.Driver`, `.Env.URL`, `.Env.Dir`, `.Available`, `.Applied`,
  `.Pending`, `.Current`, `.Next`, and `.Status`.
- Each available and pending migration file entry: `.Name`, `.Version`, and
  `.Description`.
- Each applied revision entry: `.Version`, `.Description`, `.Type`, `.Applied`,
  `.Total`, `.ExecutedAt`, `.ExecutionTime`, `.Error`, `.ErrorStmt`, and
  `.OperatorVersion`.

**`ptah-compat migrate down --format`**

- `.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, `.Start`,
  `.End`, and `.Error`.

The shared report shape and URL redaction rules are described on the
[Atlas compatibility overview](../overview/#format-reports-and-redaction).

## Next steps

- Inspecting, diffing, or applying schemas instead of migrations:
  [Atlas schema commands](../schema-commands/).
- Deciding whether the native lifecycle fits better:
  [Versioned migrations](../../versioned/overview/).
- Checking what this surface does and does not prove:
  [Conformance](../conformance/).
