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
| `ptah-compat migrate apply` | Atlas-format apply path equivalent to `ptah migrations up`; executes every Atlas OSS directory format. |
| `ptah-compat migrate down` | Forwards to `ptah migrations down` with mapped Atlas flags and Atlas revision bookkeeping by default; `--dev-url` verifies the rollback plan first. |
| `ptah-compat migrate status` | Atlas-format migration status with Atlas revision-table metadata. |
| `ptah-compat migrate hash` | Forwards to `ptah migrations hash`; writes `atlas.sum` by default. |
| `ptah-compat migrate validate` | Silently verifies `atlas.sum` on success; `--dev-url` replays migrations to validate SQL execution. |
| `ptah-compat migrate lint` | Forwards to `ptah migrations lint` with Atlas changeset selectors, dev-database replay, and Atlas report output. |
| `ptah-compat migrate new` | Creates an Atlas single-file skeleton migration; equivalent to `ptah migrations create`. |
| `ptah-compat migrate set [version]` | Moves Atlas revision history to the selected version without executing migration SQL: existing clean rows through the target are preserved, missing rows are recorded as manually set, dirty rows retain diagnostics with the combined applied and manually-set type, and rows above the target are removed. |
| `ptah-compat migrate diff` | Replays local Atlas migrations on `--dev-url`, diffs against local files, a live database, a migration directory, or `env://`, writes Atlas-style migration files (split when concurrent index builds require `-- atlas:txmode none`), and updates `atlas.sum` atomically. |
| `ptah-compat migrate import` | Imports local `file://` migration directories from Atlas-supported formats into a separate Atlas single-file directory. |
| `ptah-compat migrate checkpoint [name]` | Forwards to `ptah migrations checkpoint`; writes a ptah-format cumulative-schema checkpoint pair. |
| `ptah-compat migrate test [paths]` | Forwards to `ptah migrations test` with Ptah-native YAML test cases. |
| `ptah-compat migrate edit {name \| version}` | Forwards to `ptah migrations edit` and rewrites the directory checksum. |
| `ptah-compat migrate rebase {name \| version}` | Forwards to `ptah migrations rebase`; one migration per run. |
| `ptah-compat migrate rm {name \| version}` | Forwards to `ptah migrations rm` and rewrites the directory checksum. |
| `ptah-compat migrate push` | Atlas CE boundary stub; the native `ptah migrations push` to any OCI registry is the open replacement. |

Per-verb status detail — Atlas differences, waivers, and the inputs that fail
explicitly — is on [Atlas-compatible commands](../../reference/atlas-commands/).

## Worked example: an Atlas-format directory

Atlas-style migration files can include `migration.sql` and `down.sql` sections
inside txtar archives. Ptah executes those known sections and ignores unrelated
embedded files.

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

The apply path executes every Atlas OSS migration directory format selected by
`migration.format` or the directory URL `?format=` parameter: `atlas`,
`golang-migrate`, `goose`, `flyway`, `liquibase`, and `dbmate`. The native
`atlas` format is read from disk unchanged, preserving `atlas.sum` verification
and down migrations. Every other format is read and converted in memory to Atlas
single-file, up-only migrations, so apply executes only the source tool's
forward (up) SQL and never its down, rollback, undo, or metadata section. This
reuses the same format-loading layer as `ptah-compat migrate import`, so apply
and import agree on every format's semantics. An explicit `?format=` query on
the effective directory URL, from either `migration.dir` or CLI `--dir`,
overrides the `migration.format` project default, matching Atlas; an empty query
value selects the native `atlas` format.

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
failures. Use a disposable dev database. Ptah only reads a database used as
`--to`; it never cleans or mutates that database. Ptah rejects a desired
database that identifies the same host, port, and database as `--dev-url`,
even when credentials, connection options, scheme aliases, or an explicit
default port differ. Repeated `--schema` values filter both sides of the
comparison and the generated output. They do not narrow the
[dev database realm](../../concepts/database-urls-and-dev-databases/) replayed
and cleaned by Ptah.

If `atlas.sum` already exists, Ptah validates it before replaying migrations
and fails on checksum drift instead of silently rehashing edited files. Ptah
holds the migration-directory lock while it captures and verifies one immutable
directory snapshot, replays that exact snapshot, and publishes the result.
It also holds an exclusive dev-database lock from desired-schema resolution
through final cleanup. PostgreSQL, YugabyteDB, MySQL, MariaDB, and SQL Server
use session advisory locks. SQLite, ClickHouse, and CockroachDB use an
operating-system lock keyed by normalized database identity. The latter
coordinates Ptah processes on one host; cross-host ClickHouse and CockroachDB
replay is unsupported. A dialect without a safe locking mechanism fails before
Ptah cleans the dev database.
Generated migration files are staged before publication, and `atlas.sum` is
atomically replaced last as the batch commit marker. An OS-backed lock prevents
cooperating processes from planning against the same directory concurrently
and is released by the operating system if a process exits unexpectedly.
The generated versions and checksum are derived from the captured snapshot.
Ptah rejects a migration added after that capture instead of publishing above
an unreplayed file. `atlas.sum` is updated only after every migration file of
the run was written.
A failed write rolls the generation back immediately. If the process exits
between publishing the SQL files and publishing `atlas.sum`, the durable
publication journal remains next to the migration directory; the next lock
holder compares the checksum commit marker and either finalizes the committed
batch or removes only the hard-linked files owned by the interrupted batch.

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
`atlas.hcl` env, newly added indexes are planned as PostgreSQL
`CREATE INDEX CONCURRENTLY` statements. Files carrying such statements start
with the Atlas
`-- atlas:txmode none` file directive, which both Atlas and Ptah honor by
executing the file outside a transaction. Because those statements must not
silently strip transaction safety from ordinary DDL, a mixed plan is split the
same way Ptah's native generator splits it: a `<name>_transactional` file with
the transactional statements followed by a `<name>_concurrent_indexes` file
tagged `-- atlas:txmode none`; mixes that cannot be split automatically (for
example enum value additions alongside table changes) are refused. Docker dev
databases fail explicitly until their provisioning semantics are implemented.

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

With no `--format` and no project template, `ptah-compat migrate lint` prints
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
[Comparison: Atlas Pro analyzer coverage](../comparison/#atlas-pro-analyzer-coverage).

Atlas-compatible lint directives are enabled only under the
`ptah-compat migrate lint` compatibility profile. A statement-local
`-- atlas:nolint <selector>` suppresses the following statement. A first
nonempty `-- atlas:nolint <selector>` header followed by a blank line applies
to the whole file. A bare file header ignores the file completely, so it is
absent from `.Files` and per-file analysis steps. Supported analyzer selectors
are `destructive`, `data_depend`, `concurrent_index`, `incompatible`, and
`nestedtx`; supported Atlas diagnostic aliases are `DS102`, `DS103`, and
`MF103`. Native lint and migrate-up safety keep their native directive
semantics unless the Atlas compatibility profile is selected explicitly.

## Metadata commands and directory formats

Atlas-compatible migration metadata commands default to Atlas directory format.
`ptah-compat migrate hash`, `lint`, `new`, `set`, `status`, and `validate`
register `--dir-format` with Atlas's default value `atlas`. The supported value
is `atlas`; Atlas's external migration-tool formats (`golang-migrate`, `goose`,
`flyway`, `liquibase`, and `dbmate`) fail explicitly on those commands until
they are imported with `ptah-compat migrate import` or implemented natively.
`ptah-compat migrate set [version]` moves Atlas revision history to the selected
boundary without executing migration SQL. It preserves existing clean rows
through the target, inserts missing rows as manually set, keeps dirty-row
diagnostics with the combined applied and manually-set type, and removes rows
above the target. With `--env`, it reads `env.url`, `migration.dir`, and
`migration.revisions_schema` from `atlas.hcl`; explicit `--url`, `--dir`, and
`--revisions-schema` flags keep CLI precedence.
`ptah-compat migrate status` also accepts `--revisions-schema` and runs against
Atlas revision-table metadata.

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

| Command | Format data fields |
| --- | --- |
| `ptah-compat migrate apply --format` | `.Driver`, `.URL`, `.Dir`, `.Env`, `.Pending`, `.Applied`, `.Current`, `.Target`, `.Start`, `.End`, `.Error`, and JSON `.Message` for successful or no-op reports. `.Pending` and `.Applied` entries expose `.Name`, `.Version`, `.Description`; applied entries also expose `.Applied`, `.Skipped`, `.Checks`, and statement `.Error`. |
| `ptah-compat migrate diff --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for generated migration SQL. |
| `ptah-compat migrate lint --format` | `.Env.Driver`, `.Env.URL`, `.Env.Dir`, `.Steps`, and `.Files`. Step entries expose `.Name`, `.Text`, `.Error`, and `.Result`; file entries expose `.Name`, `.Text`, `.Error`, and `.Findings`. |
| `ptah-compat migrate status --format` | `.Env.Driver`, `.Env.URL`, `.Env.Dir`, `.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, and `.Status`. Available and pending migration file entries expose `.Name`, `.Version`, and `.Description`. Applied revision entries expose `.Version`, `.Description`, `.Type`, `.Applied`, `.Total`, `.ExecutedAt`, `.ExecutionTime`, `.Error`, `.ErrorStmt`, and `.OperatorVersion`. |
| `ptah-compat migrate down --format` | `.Env`, `.Planned`, `.Reverted`, `.Current`, `.Target`, `.Total`, `.Start`, `.End`, and `.Error`. |

The shared report shape and URL redaction rules are described on the
[Atlas compatibility overview](../overview/#format-reports-and-redaction).

## Next steps

- Inspecting, diffing, or applying schemas instead of migrations:
  [Atlas schema commands](../schema-commands/).
- Deciding whether the native lifecycle fits better:
  [Versioned migrations](../../versioned/overview/).
- Checking what this surface does and does not prove:
  [Conformance](../conformance/).
