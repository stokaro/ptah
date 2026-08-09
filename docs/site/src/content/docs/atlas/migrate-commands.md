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
| `ptah-compat migrate down` | Forwards to `ptah migrations down` with mapped Atlas flags and the Atlas revision-table layout by default; `--dev-url` verifies the rollback plan first. Failed rollbacks retain Ptah's recoverable dirty state. |
| `ptah-compat migrate status` | Atlas-format migration status with Atlas revision-table metadata; refuses an Atlas directory whose `atlas.sum` is missing or stale. |
| `ptah-compat migrate hash` | Forwards to `ptah migrations hash`; writes `atlas.sum` by default. |
| `ptah-compat migrate validate` | Silently verifies `atlas.sum` on success; `--dev-url` replays migrations to validate SQL execution. |
| `ptah-compat migrate lint` | Forwards to `ptah migrations lint` with Atlas changeset selectors, dev-database replay, and Atlas report output. |
| `ptah-compat migrate new` | Creates an Atlas single-file skeleton migration; equivalent to `ptah migrations create`. |
| `ptah-compat migrate set [version]` | Moves Atlas revision history to the selected version without executing migration SQL; refuses an Atlas directory whose `atlas.sum` is missing or stale. |
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

### The `--dir` default

Every `migrate` verb that registers `--dir` defaults it to
`file://migrations`, so `ptah-compat migrate apply --url "$DATABASE_URL"` run
from a project root reads `./migrations` with no flag at all. That is
`apply`, `new`, `diff`, `status`, `set`, `lint`, `hash` and `validate` — the
same eight verbs Atlas documents the default on, and the same value
([#1241](https://github.com/stokaro/ptah/issues/1241)). `migrate apply` and
`migrate new` used to refuse a run with no `--dir`, and `migrate hash` and
`migrate validate` used the directory without printing the default in `--help`.

The default is a default, not a fallback. Every layer that names a directory
outranks it — `--dir`, `PTAH_DIR`, `PTAH_MIGRATIONS_DIR`, and `atlas.hcl`
`migration.dir` — and a `--dir` naming a directory that is not there fails
rather than quietly reading `./migrations`:

```bash
ptah-compat migrate apply --url "$DATABASE_URL" --dir file://migrtions
# Error: atlas migrate apply --dir: open migrations directory: openat migrtions: no such file or directory
```

The default also does not skip anything. A defaulted directory reaches the
`atlas.sum` gate exactly as an explicit one does, so an unhashed or drifted
`./migrations` is still refused with `Error: checksum file not found` or
`Error: checksum mismatch`.

The writing verbs create the directory they are pointed at, including missing
parents: `ptah-compat migrate new add_users --dir file://db/migrations` creates
`db` and `db/migrations`. A path component that already exists and is not a
directory is still refused, and nothing is written.

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

A failure aborts before any body statement, matching Atlas's
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

Successful `hash` and `validate` commands are silent. The hash command writes
`migrations/atlas.sum`; commit that file with the migration.

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
Migration Status: OK
  -- Current Version: 20260721120000
  -- Next Version:    Already at latest version
  -- Executed Files:  1
  -- Pending Files:   0
```

`migrate status` is the one compatibility verb whose output a pipeline parses
with a machine rather than reads, so it mirrors the Atlas report shape by
default — field names, sentinel strings and value encodings included. A deploy
gate written as `grep -q 'Migration Status: OK'` works unchanged, `-- Current
Version:` matches, and a database with nothing applied reports the sentence
`No migration applied yet` rather than `0`. Native
[`ptah migrations status`](../../versioned/apply/) keeps its own block: the two
surfaces are allowed to differ and only the compatibility one is a contract.

## Recovering from a migration body that failed part-way

When a transactional body fails and `Rollback` succeeds, `ptah-compat` removes
the zero-progress revision row. The next apply retries the whole file without
`--allow-dirty`, matching Atlas. Native `ptah migrations up` keeps its durable
failure record instead.

On MySQL and MariaDB a `file` rollback does not always reach zero progress: the
server may commit around DDL. `ptah-compat` does not guess the boundary from SQL
keywords. The Atlas revision row is an InnoDB witness updated on the same
physical transaction before and after each statement. An implicit server commit
makes the matching `applied` count and `partial_hashes` durable with the body;
an ordinary rollback removes both the user DML and its witness. A plain-DML body
that rolls back leaves no committed prefix and is retried whole. A witnessed
DDL/DML prefix stays dirty because retrying it would repeat committed SQL.

This recovery mode requires InnoDB for the Atlas revision table, the session's
default storage engine, and every existing base table in the selected database.
An explicit non-InnoDB `CREATE`, `ALTER`, or storage-engine setting is refused
before the migration runs. `CREATE TABLE ... LIKE` is also refused because the
new table inherits an engine that the session default does not prove.

MySQL-family `file` bodies fail closed on SQL whose effects Ptah cannot tie to
the InnoDB witness:

- Transaction controls such as `BEGIN`, `COMMIT`, and `SET autocommit`.
- Durable server-state operations such as `SET GLOBAL`, `SET PERSIST`, `RESET`,
  and `CREATE`, `ALTER`, or `DROP DATABASE` or `SCHEMA`.
- `USE` and qualified references to another database. The connection URL must
  name the database whose engines Ptah validates.
- Executable comments, nested or dynamic SQL, and table locks.
- Definitions of indirect database objects, references to existing views or
  trigger-bearing tables, and stored-routine calls.
- Custom migration functions, whose inner statements are opaque to Ptah.
- Statement interceptors, which can replace inspected SQL with another
  execution path.

Rejection errors identify the statement number and safety class without echoing
the SQL.

MySQL and MariaDB do not support `--tx-mode all`. Ordinary session settings
remain valid. Ptah runs the body on one pinned session, discards it afterward,
and replays safe settings such as `SET SESSION sql_mode` from a verified
committed prefix before an automatic retry. A durable unknown-outcome witness
blocks automatic retry until the database is inspected and the revision is
repaired.

When a statement committed under `--tx-mode none`, or its outcome is unknown,
`ptah-compat` preserves the revision row. `migrate status` reports that row as
a half-applied file, because `Current Version` counts it:

```text
Migration Status: PENDING
  -- Current Version: 20260721120100 (1 statements applied)
  -- Next Version:    20260721120100 (1 statements left)
  -- Executed Files:  2 (last one partially)
  -- Pending Files:   1

Last migration attempt had errors:
  -- SQL:   ALTER TABLE users ADD COLUMN email TEXT
  -- ERROR: failed to execute migration SQL: ...
```

Fix the migration, rerun `ptah-compat migrate hash`, and rerun the apply with
`--allow-dirty`. The retry reuses the dirty row instead of recording a second
one and skips the statements the earlier attempt committed — under
`--tx-mode none` above, statement 1 — only after proving that committed source
prefix is unchanged. Atlas-format rows use the cumulative `partial_hashes`
entry at `applied`. Editing the unapplied suffix is allowed, and a later retry
failure cannot lower `applied` below that committed prefix even when the
transaction mode changed. Atlas needs no flag here; `--allow-dirty` stays
required so a half-applied migration is never resumed by accident.

Automatic resume refuses and names `ptah migrations repair --version <v>` when
a run was interrupted mid-statement, the statement count changed, the committed
source prefix changed, or `partial_hashes` is malformed or disagrees with
`applied`. It also refuses negative `applied` or `total` values and
`applied > total`. Legacy Atlas rows without `partial_hashes` may resume only
while the stored full-file hash still matches. Revision listing, status, and
version operations reject the same invalid counters instead of classifying an
equal negative pair as clean.

## Rolling back

Roll back using the `down.sql` section. A bare `ptah-compat migrate down` reads
the Atlas revision rows `migrate apply` wrote and starts the rollback without
reading stdin:

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

Review the URL, migration directory, and target before running the command.
The Atlas-compatible surface has no confirmation prompt and does not accept
the native `--confirm` flag. Native `ptah migrations down` keeps its prompt.

Ptah validates every selected down body before rollback starts. If one is
missing, the command leaves both the schema and Atlas revision rows unchanged.
Dry runs use the same dirty-state, checksum, checkpoint, and down-body
validation path as real rollbacks, while suppressing schema and revision writes.

If a rollback fails after execution starts, the Atlas-format revision row stays
dirty and records `Ptah/down` in `operator_version`. Resume through the native
repair command because the drop-in surface intentionally has no repair verb:

```bash
ptah migrations repair \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --dir-format atlas \
  --revision-format atlas \
  --version 2 \
  --resume-from 2
```

Use the failed version and next down-statement number reported by status. If
the compat command used `--revisions-schema`, pass that value as
`--migrations-schema` here.

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
  commit the file. A directory holding no top-level `.sql` file is not a
  checksum error — it reports `No migration files to execute` and exits `0`,
  matching Atlas. The scan is top-level-only because the executed set is the set
  `atlas.sum` covers: a `.sql` file in a subdirectory, or a top-level `.SQL`, is
  not a migration on either tool
  ([#976](https://github.com/stokaro/ptah/issues/976)). Each such file is named
  on stderr as declined, which Atlas does not do — see
  [Integrity and safety](../../versioned/integrity-and-safety/).
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

### Adopting a database that already has objects

`migrate apply` refuses to adopt a database that already holds objects no
migration recorded, matching Atlas. What counts as such an object depends on
what the URL pinned, and the two answers are different.

**A URL that pins a schema** — `?search_path=public` on PostgreSQL, or a
database on a MySQL URL — puts the run in schema scope. The refusal names a
table in that one schema:

```text
Error: sql/migrate: connected database is not clean: found table "legacy_stuff" in schema "public". baseline version or allow-dirty is required
```

On SQLite the same refusal reports a count instead of a name:
`found multiple tables: 2`. Views, sequences, and tables in other schemas are
not tables in the connected schema and do not trigger it, and neither does the
revision table itself.

**A plain PostgreSQL URL that pins no `search_path`** puts the run in realm
scope, where the whole database is under review and the operand is schemas
rather than tables. An empty extra schema is enough:

```text
Error: sql/migrate: connected database is not clean: found schema "extra". baseline version or allow-dirty is required
```

At that scope only two things are tolerated: an empty `public`, and the schema
holding this run's revision table. Anything else — another schema, empty or
not, or a table in `public` — refuses, and the refusal names the first offender
by name. A `public` holding a table is reported as `found schema "public"`, not
as a table. A revisions schema holding more than the revision table reports a
count: `found 2 tables in schema "atlas_schema_revisions"`.

Only the `search_path` query parameter selects schema scope. A search path set
through libpq's `options=-c search_path=…` moves the session but leaves the run
at realm scope, which is what Atlas does.

The check is an adoption gate, not a standing drift check. It runs only while
the revision table holds no rows, so it fires on the first apply against a
database somebody else's tooling owns and never again — a managed database that
later grows an unmanaged table applies its next migration normally. The refusal
also fires under `--dry-run` and on a directory with nothing pending, because
the question it answers is about the database rather than about the work.

Two flags opt in, and they cannot be combined:

- `--allow-dirty` applies every pending migration against the existing schema.
- `--baseline <version>` records history as starting at that version and applies
  only what comes after it.

Passing both exits `1` with
`Error: sql/migrate: baseline and allow-dirty are mutually exclusive` before
anything is recorded.

The gate is enforced on PostgreSQL, MySQL, MariaDB, and SQLite. Other dialects
are not gated, because the behavior to match has not been measured on them.
Realm scope is enforced on PostgreSQL only: a MySQL URL that names no database
is refused by the connection before the gate is reached, so that combination
never applies anything either. Native
[`ptah migrations up`](../../versioned/apply/) has no equivalent gate; see
[#1231](https://github.com/stokaro/ptah/issues/1231).

At realm scope this implementation keeps its own revision table in `public`
while Atlas keeps its in a schema named `atlas_schema_revisions`. Both databases
are adopted at exit `0`; the difference is where the bookkeeping lands, and it
is tracked in [#1257](https://github.com/stokaro/ptah/issues/1257).

```bash
ptah-compat migrate apply 2 \
  --url "$DATABASE_URL" \
  --dir file://migrations
```

Supported Atlas apply flags include `--dry-run`, `--tx-mode`, `--exec-order`,
`--allow-dirty`, `--baseline`, `--revisions-schema`, `--lock-timeout`,
`--lock-name`, `--skip-lock`, and `--format`. `--format` executes a Go template
against a Ptah apply result that mirrors Atlas's public apply-template fields:
`Pending`, `Applied`, `Current`, `Target`, `Start`, `End`, `Driver`, `URL`, and
`Dir`; `{{ json . }}` emits the same result as JSON with database credentials
redacted. With `--env`, Ptah can read `env.url`, `migration`, and
`format.migrate.apply` from `atlas.hcl`. Dry-run plans read the stored Atlas
revision rows and include only migrations that a real apply would select. They
also run the same dirty-state, checksum, execution-order, and transaction-mode
validations as a real apply.

An env `for_each` can select several database targets. `migrate apply` runs
them sequentially in stable expansion order and stops at the first failure.
Formatted output contains one document per attempted target with one newline
between adjacent documents. A structured execution failure stays in that
target's report; stderr remains empty and the process exits `1`.

`--lock-name` replaces the name of the session advisory lock that serializes
migration runs (`ptah_migrate` by default). Two runs serialize only when they
name the same lock, so this is how a Ptah run coordinates with another tool on
the same database. A lock another process holds makes the run wait, bounded by
`--lock-timeout`; an elapsed timeout fails the run before any migration
executes. An empty value is refused rather than falling back to the default
name.

`--skip-lock` acquires no lock at all: no wait, no timeout, and no
serialization against another runner. It cannot be combined with `--lock-name`,
because there is no lock to name. On dialects with no advisory-lock semantics —
SQLite, ClickHouse, CockroachDB, and Spanner — an explicit `--lock-name` prints
a note on stderr naming the lock that was not acquired.

Atlas migration files may override global `file` or `none` with a leading
header followed by a blank line:

```sql
-- atlas:txmode file

ALTER TABLE users ADD COLUMN email TEXT;
```

The accepted file values are `file` and `none`. File-level `all`, unknown
values, duplicate values, and any explicit file mode under global `all` fail
before the affected migration body or revision row changes. Validation follows
the selected plan: global `all` validates the complete selected batch first;
global `file` and `none` validate each file as execution reaches it. An amount
or baseline that excludes a malformed file does not validate that file.
On `ptah-compat`, these failures use Atlas's leaf diagnostic without Ptah's
internal `error applying migrations:` wrapper. The native command keeps its
`error running migrations:` context.

Inside a Ptah-supported txtar migration, `migration.sql` and `down.sql` carry
independent modes. Ptah rejects a transaction-mode header placed before
`-- atlas:txtar`; this safety check prevents the archive from being executed as
one plain SQL stream. Atlas CE `v1.3.0` instead ignores section-local modes and
can classify that malformed outer-header shape as plain SQL, so this contour is
an intentional safety difference rather than a parity claim.

A clean successful `ptah-compat migrate apply` writes nothing to stderr,
matching Atlas CE: there is no progress narration, in a dry run or otherwise,
so `--format` output survives the usual CI idiom of folding both streams
together.

```bash
ptah-compat migrate apply --url "$DATABASE_URL" --dir file://migrations \
  --dry-run --format '{{ json . }}' 2>&1 | jq
```

Three things still reach stderr, by design. A command that fails prints its
`Error: …` diagnostic there and exits `1`. A Warn-level runtime diagnostic that
exists on no other channel — such as function ordering or a dev database that
would not close — is still reported. An `atlas.hcl` name that Atlas CE accepts
without acting on also produces a location-aware warning that the construct has
no effect. Valid circular foreign keys are rendered in two phases and do not
produce a warning. None of these diagnostics appears on a clean run.

`ptah-compat migrate down` holds the same contract, with or without `--format`.
Without `--format` it forwards to the native `ptah migrations down`, which
starts its own run log; the Atlas surface pins that log to the same Warn
threshold the rest of this binary uses, so a successful dry run and a successful
rollback both leave stderr empty (stokaro/ptah#969).

The equivalent native command, `ptah migrations up --dry-run`, does narrate each
statement it would execute through its run log; that narration is selected by
the native `--log-level` and `--log-format` flags. The Atlas surface does not
model those flags, but a forwarded verb passes through any flag its native
target registers, so `ptah-compat migrate down --log-level info` is accepted and
restores the full narration. See [Apply migrations](../../versioned/apply/).

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

That numeric order governs `atlas.sum` and execution, and it is **not** the
comparison that decides whether a migration was added out of order. Flyway
answers that on the version token as text, where `"10"` sorts below `"2"`, so a
project's tenth migration added beside an already-applied `V2__x.sql` is refused
rather than executed:

```text
Error: error applying migrations: out-of-order pending migrations for current
version 4611686018427510315 (source version "2"): 4611686018427836747
(source version "10") (use --exec-order=non-linear to apply or
--exec-order=linear-skip to ignore)
```

The refusal names the **source version** because the int64 beside it appears in
no file name. `--exec-order=non-linear` runs the migration, `--exec-order=linear-skip`
leaves it pending, and a directory with no recorded history is unaffected —
applying `V2__x.sql` and `V10__y.sql` together for the first time runs both, in
that order. A repeatable (`R__`) is version `""` to Atlas, which sorts below
every token, so one added to a database that already has history is refused by
the same rule.

Several inputs still fail before Ptah opens the target database rather than guess
at semantics: unknown formats; goose files whose directives are out of order;
dbmate files missing their up directive; and two source files that resolve to the
same version. Flyway repeatable migrations are **not** in that list — they are
converted and executed, on a reserved version slot above every versioned
migration, which is where Atlas emits them. See
[`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742) and
[`stokaro/ptah#1098`](https://github.com/stokaro/ptah/issues/1098).

### Goose directive parsing

Goose directives are parsed as a state machine, not filtered line by line, and
the accepted set matches Atlas. Directive names are case- and space-sensitive
exactly as Atlas matches them: `-- +goose Up` is a directive, `-- +goose up`,
`-- +goose  Up` and `-- +Goose Up` are not. A name may carry trailing text
(`-- +goose Up extra` is still `Up`). Lines Atlas does not recognize as
section directives — including `-- +goose NO TRANSACTION` — stay in the
migration body and are executed with it.

A goose file that contains **no** section directive at all is executed in full:
the whole file is the migration. Such a file has no rollback section that could
leak onto the apply path, so there is nothing to protect against. A file with a
**broken** directive set is a different thing and is still refused — `Down`
before any `Up`, a second `Up`, `StatementBegin` outside a section, an
unmatched `StatementEnd`, or any section directive inside a `StatementBegin`
block. See [`stokaro/ptah#981`](https://github.com/stokaro/ptah/issues/981).

The up section runs from the **start of the file** through the first `Down`, so
SQL written above the `Up` directive is executed rather than silently dropped.
An intentionally empty up section is recorded as an applied revision with zero
statements rather than being skipped.

### Deliberate divergences

Three behaviors differ from Atlas on purpose. All three are cases where matching
would mean reproducing a defect, so Ptah is stricter — never looser — than Atlas.

#### A Goose directive with the wrong case

`-- +goose down` instead of `-- +goose Down`.

Atlas exits 0. The typo is not recognized, so the line folds into the body as a
comment and the rollback SQL under it executes — the migration is created,
dropped, and recorded as successful.

Ptah refuses, naming the line and the correct spelling. A case error in a
directive must not silently roll back a migration.

#### A dbmate file with no `-- migrate:up`

Atlas exits 0, records the revision with 0 of 0 statements and creates nothing,
so the migration is marked done and never runs. `migrate import` then writes a
zero-byte file over the authored SQL and hashes it into `atlas.sum`.

Ptah refuses, because nothing in the file would execute.

#### An Atlas directory holding an R-suffixed migration

For example `1R_view.sql` or `R__view.sql`.

Atlas exits 0 and executes it, keyed on the opaque version string the file name
spells (`1R`, `R`).

Ptah refuses, naming every such file. Ptah's migration identity is an `int64`
version and a repeatable has none, so the only alternatives were executing it
under a version no other tool records, or dropping it — which is what Ptah used
to do, silently.

An R-suffixed file only reaches a Ptah directory from outside: the community
binary's own `migrate import` writes `1R_name.sql`, and `ptah-compat migrate
import` writes the same migration on a reserved numeric slot instead, so a
directory Ptah imported is unaffected. Rename the file to `<version>_<name>.sql`
and re-run `ptah-compat migrate hash` to execute it. Ordering is not preserved by
that rename: Atlas sorts directory entries by file name, so `1R_view.sql` runs
*before* `1_users.sql`, while `2_view.sql` runs after it.

Ptah refuses only exact near-miss spellings of the four section directives.
Prose that merely begins with one (`-- +goose up to date`) and unrecognized
names (`-- +goose Frobnicate`) stay comments, as they do in Atlas.
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

### The publication boundary

`migrate diff` opens the migration directory and its parent once, before
anything is staged, and keeps both handles for the rest of the run. The parent
is held as well because the publication journal and the commit marker sit
beside the directory, so an interrupted run stays recoverable even when the
directory itself was left half-built.

Every later step names a direct child of one of those two handles: the staged
files, the published migrations, `atlas.sum`, the journal, the commit marker,
the rollback quarantine, and orphan cleanup. Recovery runs through the same
handles, so an interrupted batch is withdrawn from the objects the run opened.
The directory pathname survives only in reported paths and error text, and no
write step resolves it a second time.

This draws the boundary against a directory replaced after the run validated
it. Replacing the pathname, or re-pointing a symlink on the way to it, no
longer selects where the run writes: a migration or an `atlas.sum` can land
only in the directory that was captured and verified. When the directory came
from `atlas.hcl`, both handles are opened through the project root, so a
replacement cannot move the write outside that root either.

Two things stay keyed to the pathname on purpose. The cross-process lock file
is created beside the directory before any handle exists, because it is
cooperative mutual exclusion between Ptah processes rather than a boundary
against a hostile writer, and every verb has to agree on its identity. The
`--edit` callback also receives absolute staged paths, because an external
editor cannot take a handle; the files it returns are re-validated through the
retained handle before anything is published.

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

An **empty** migration directory is the one shape where a missing `atlas.sum`
is not drift: there is nothing for it to cover, so `ptah-compat migrate validate`
exits `0` with no output, matching the pinned Atlas community binary v1.3.0 and
matching what `ptah-compat migrate apply` already does on the same directory
(`No migration files to execute`). The moment the directory holds a migration
file the refusal returns, byte-identically. `ptah-compat migrate lint --latest`
follows the same rule: an empty directory selects nothing and exits `0`, which
is what a repository linting its migrations in CI does before the first
migration exists. The scope selector is what makes that `0` legitimate — an
empty directory exits `0` only when `--latest` or `--git-base` was given. With
neither, the run is refused before the directory is read (see
[Lint migrations](#lint-migrations) below).

This compatibility behavior is scoped to the `ptah-compat` binary.
Native `ptah migrations validate` keeps Ptah's success banner and native error
output; missing or malformed sum files remain exit-`2` usage failures, and
native `ptah migrations lint` still refuses an empty directory with
`no *.sql migration files found`.

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

Both flags in that example are required, and they are **two separate
requirements** with two separate refusals, each matching the pinned Atlas
community binary v1.3.0 byte for byte:

| invocation | exit | message |
| --- | --- | --- |
| no `--dev-url` | 1 | `required flag(s) "dev-url" not set` |
| no `--latest` and no `--git-base` | 1 | `--latest or --git-base is required` |

An argv missing both answers the `--dev-url` sentence, because that is the one
the pinned binary answers on the same argv.

A **scope** may come from the selected `atlas.hcl` environment instead of the
command line: a `lint { latest = 1 }` or a `lint { git { base = … } }` satisfies
the requirement with nothing spelled on the command line. `--latest N` with an N
larger than the directory analyzes every migration. The scope refusal comes
before the migration directory is read and before `--dev-url` is contacted,
which is the part that matters: `--dev-url` is scratch space and the run
**cleans** it, so an unscoped invocation that answered would drop tables in a
database the pinned binary never connects to.

Two environment variables relax these requirements. They relax **different**
ones, and neither implies the other:

- `PTAH_ATLAS_LINT_WITHOUT_DEV_URL=1` drops the dev-database requirement.
  Ptah's analyzers reach a verdict from the migration files alone; the run still
  needs a scope.
- `PTAH_ATLAS_LINT_ALL_VERSIONS=1` drops the scope requirement and lints the
  whole directory. The run still needs a dev database unless the variable above
  is also set.

Both default to off so a pipeline written against the community CLI gets the
same refusal here that it gets there, and both are environment variables rather
than flags because the conformance `cli-surface` tier asserts flag parity with
the pinned binary. Native `ptah migrations lint` needs neither a dev database
nor a scope and is unaffected by both.

`migrate lint --dev-url` treats the dev database as scratch space: it drops user
tables, replays the migration directory, and then runs static lint
reporting. Docker `--dev-url` values remain an explicit gap; use a directly
connectable database URL.

With no `--format` and no project template, `ptah-compat migrate lint` prints a
compatibility report: an `Analyzing changes …` header, a per-version block
listing each analyzer group's diagnostics and mapped rule IDs, a `-- ok (…)`
line per version, and a summary of version statuses, semantic schema changes,
and diagnostics. For mapped Atlas diagnostics, the compatibility renderer
reproduces the measured wording, analyzer documentation links, wrapping, and
suggested-fix layout. Ptah-only diagnostics remain visibly labeled and do not
link to unproven Atlas analyzer codes. Native `ptah migrations lint` keeps
Ptah's more detailed diagnostic prose and remediation guidance.

A statement affecting several objects reports per object, and the two
destructive shapes are not the same:

- A `DROP TABLE` naming several tables produces one diagnostic and one suggested
  fix per dropped table, ordered by table name compared byte-wise, so the
  suggested-fix header pluralizes and the diagnostic count rises with the number
  of tables.
- One `ALTER TABLE` dropping several columns produces one diagnostic naming
  every dropped column in clause order, under a single suggested fix.
- One `ALTER TABLE` adding several non-nullable columns without a default
  produces one diagnostic per column, in clause order.

Native `ptah migrations lint` splits its findings the same way, so the two
surfaces never disagree about how many objects a statement affects. Each
per-object finding names its object in the native message, which is also what
keeps each SARIF result's fingerprint distinct when several of them share a
rule, a file, and a line.

### Which objects are under review

The dev database is what a lint run compares against, so it also decides which
objects the run analyzes. A PostgreSQL-family `--dev-url` carrying
`?search_path=<schema>` puts exactly that one schema under review:

- an object in a different schema raises no diagnostic and counts as no schema
  change, because it was never part of the state the run compares against;
- an unqualified reference resolves into the reviewed schema, so it stays under
  review, and so does a reference written out with the reviewed schema's own
  name;
- one statement is measured per object: `DROP TABLE users, other.audit_log;`
  under `search_path=public` reports `users` and counts one schema change;
- an index is measured by the schema of the table it is on whenever the
  statement names a table, because the index name is bare there:
  `CREATE INDEX idx ON app.users (id);` under `search_path=public` counts no
  schema change and raises no diagnostic, while the same statement on a
  `public` table counts one and raises `PG101`. `DROP INDEX idx ON app.t;` —
  MySQL's, MariaDB's and SQL Server's spelling — is measured the same way;
- a `DROP INDEX` that names no table is measured by the qualifier on the index
  itself, which is the only one the statement has: `DROP INDEX app.idx;` under
  `search_path=public` counts no schema change and raises no diagnostic, while
  `DROP INDEX public.idx;` and the unqualified `DROP INDEX idx;` each count one
  and raise `PG106`.

A `--dev-url` that names no schema puts the whole connected database under
review and filters nothing, which is also what every non-PostgreSQL dev URL
does. A `search_path` naming more than one schema is not a scope: it is read as
a single schema name, so it scopes nothing and every object stays under review.

An `ALTER TABLE`'s schema changes belong to its table's schema, never to the
column or constraint it names, and a `CREATE SCHEMA` is measured against the
reviewed schema by its own name.

The scope decision is made once per statement and drives both outputs, so a
statement the scope removed contributes neither a schema change nor a
diagnostic, and one it kept can contribute either. A diagnostic that names no
object at all — the rules that report a statement rather than the objects in
it — follows the decision already taken for the statement it belongs to: dropped
when the scope removed that statement, and otherwise reported, since there is
nothing to measure a scope against and a hazard must not be silenced on an
unestablished boundary. `ALTER TABLE app.users ADD CONSTRAINT …` under
`search_path=public` therefore reports nothing, where it used to raise `PG105`
about a change it had already counted as zero.

A statement is removed only when **every** table it names is out of review, not
only the one it alters. `ALTER TABLE app.child ADD CONSTRAINT c FOREIGN KEY
(pid) REFERENCES public.parent (id);` under `search_path=public` names two, and
validating that key holds a `SHARE ROW EXCLUSIVE` lock on `public.parent` for
the duration, so `PG306` is reported: the hazard lands on a table the run is
responsible for. The same statement referencing `app.parent` reports nothing.
This is one place the two outputs deliberately describe different statements —
the constraint lands on `app.child`, so the reviewed schema still counts zero
changes for it. A count of zero is not a statement of safety.

Silence is not exclusion. A statement outside Ptah's SQL grammar is left under
review whatever schema it names, because a boundary that could not be read must
not be able to drop a diagnostic. That grammar boundary is why
`TRUNCATE app.users;` and `DROP FUNCTION app.recalc();` are reported under
`search_path=public` while counting no schema change.

`DROP INDEX` used to be the largest example of it and no longer is: the parser
models the statement, so `DROP INDEX public.idx;` counts one schema change,
matching the pinned community binary v1.3.0, and `DROP INDEX app.idx;` is
scoped out whole — no schema change and no `PG106`, which is also what the
community binary reports. Ptah raised `PG106` for the `app` form before
[`stokaro/ptah#1296`](https://github.com/stokaro/ptah/issues/1296); nothing
about the reviewed schema became quieter, since the `public` form still raises
it.

Two `DROP INDEX` forms are still outside the grammar. Both are refused by the
parser rather than half-recorded, so each counts no schema change and keeps its
diagnostics under every scope:

- PostgreSQL's multi-index `DROP INDEX a, b;`, which one `DROP INDEX` node
  cannot hold;
- SQL Server's backward-compatible `DROP INDEX t.idx;`, where the qualifier
  names the table rather than a schema. Reading it the way every other dialect
  spells the same text would scope the drop to a schema nobody wrote. Ptah
  renders SQL Server index drops as `DROP INDEX idx ON t`, which is read
  exactly.

Three constructs are still measured by a name that carries no schema, because
Ptah's parser records none for them. Measured on PostgreSQL 17.10 under
`search_path=public`, `CREATE SEQUENCE app.s;` counts one schema change,
`CREATE TRIGGER trg … ON app.t;` counts one and raises `PG308`, and
`CREATE POLICY p ON app.t;` counts one — where the pinned community binary
v1.3.0 counts no schema change and raises no diagnostic for all three. They
over-report rather than under-report, and each is a warning at exit 0, so a
scoped run is never told less than the truth about its database. They are listed
here rather than left to be discovered.

The boundary applies to `ptah-compat migrate lint` only. Native
`ptah migrations lint` keeps every object under review, whatever the dev URL
selects, so the two surfaces deliberately disagree about scope.

The reason the boundary exists on the compatibility surface is that the tool it
replaces reviews only what the dev URL covers, and matching that is the whole
point of the surface. The reason it does not exist natively is that the
justification for it — an object outside the dev URL's reach was never in the
before-state the run compares against — describes a diff-based analyzer, and
Ptah's linter reads SQL text. That is why it reports `TRUNCATE` and
`DROP SCHEMA`, neither of which produces a diff. Scoped natively,
`DROP TABLE app."Users", app.audit_log;` under `search_path=public` reports
nothing and exits 0, on the one surface where no other tool can be consulted
about whether that is right.

### Renames

A rename retires one logical name and introduces another. The two surfaces
describe that single event at different altitudes:

- `ptah-compat migrate lint` reports it as a destructive change to the retired
  name — `DS102` for `ALTER TABLE … RENAME TO`, `RENAME TABLE …` and MySQL's
  bare `ALTER TABLE … RENAME new_name`; `DS103` for `RENAME COLUMN` and its form
  without the `COLUMN` keyword. The diagnostic names the old name rather than the
  new one, carries the matching pre-migration-check suggested fix, is
  error-severity, and exits 1.
- Native `ptah migrations lint` reports `BC101`, which explains the operational
  hazard and prescribes add-new/backfill/drop-old across releases. It stays a
  warning there.

Exactly one of the two is emitted per rename, so neither surface reports one
statement twice.

A statement renaming several objects follows the same two shapes the destructive
drops do. One `ALTER TABLE` renaming several columns — the MySQL multi-clause
form — produces one diagnostic naming every renamed column in clause order,
under a single suggested fix. One `RENAME TABLE` naming several pairs produces
one diagnostic and one suggested fix per renamed table, ordered by table name
compared byte-wise. Ordering is per statement: consecutive `ALTER TABLE … RENAME
TO` statements report in the order they are written.

Renaming an object the same migration file created reports nothing on either
surface: no deployed application version ever saw a name this migration itself
introduced.

Because the retired name is a destructive change, `-- atlas:nolint destructive`
suppresses a rename on the compatibility surface and `-- atlas:nolint
incompatible` does not.

Index, key and constraint renames are not reported at all: deployed application
code does not name them.

On the compatibility surface a rename also reports the column it **introduces**.
Renaming a `NOT NULL` column with no `DEFAULT` produces a second diagnostic —
`MF103`, "adding a non-nullable column will fail in case the table is not
empty" — naming the new column and the retired column's type. A `RENAME COLUMN`
statement carries neither that type nor its nullability, so this one is not read
from the migration text: `ptah-compat migrate lint` reads the schema state the
version starts from off the `--dev-url` database during the replay it already
performs, before the version runs and while the retired column still exists. A
run without `--dev-url` reports the retirement alone.

Three consequences follow from where the facts come from:

- The type is spelled the way the database canonicalizes it, not the way the
  migration writes it: `int` reports as `integer` and `varchar(20)` as
  `character varying(20)`.
- A retired column that is nullable, or that carries a `DEFAULT`, has no such
  diagnostic — the introduced column cannot fail on existing rows.
- A type whose diagnostic spelling Ptah has not measured keeps the diagnostic but
  prints Ptah's own labeled wording for it rather than a guess at the other
  tool's.

The two halves belong to different analyzers, so `-- atlas:nolint destructive`
silences the retirement and leaves the addition, and `-- atlas:nolint
data_depend` does the reverse.

Native `ptah migrations lint` reports no addition. It models a rename as a
rename, and a rename does not fail on a populated table, so `BC101` stays its
single finding.

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
general. A table rename counts as two changes — the old name stops naming an
object and the new one starts — while a column rename counts as one, because the
table it modifies stays the same object.

Ptah also validates and fully loads the migration provider, including Atlas
templates, before dropping any objects from the dev database. A malformed
migration directory therefore leaves the existing dev database state intact;
cleanup starts only after the replay plan is valid.

For a code-by-code audit of the analyzer checks Atlas marks as Pro against
Ptah's native lint rules, see
[Comparison: Atlas Pro analyzer coverage](../comparison/#atlas-pro-analyzer-coverage).

A statement-local `-- atlas:nolint <selector>` suppresses the statement
directly below it. A blank line between the directive and the statement
detaches the two: the directive then suppresses nothing, exactly as the
community binary treats it.

The whole-file header form — a first nonempty `-- atlas:nolint <selector>`
line followed by a blank line — is enabled only under the `ptah-compat migrate
lint` compatibility profile. A bare file header ignores the file completely, so
it is absent from `.Files` and per-file analysis steps.

Supported analyzer selectors are `destructive`, `data_depend`,
`concurrent_index`, `incompatible`, and `nestedtx`. They name rule families, so
they mean the same thing on both surfaces.

A code selector names the code the running surface prints: every code
`ptah-compat migrate lint` emits is suppressed by that code, and every code
`ptah migrations lint` emits is suppressed by that code. The three codes the
compatibility surface renames are the only place the two differ — a column drop
prints `DS102` natively and `DS103` on the compatibility surface, so `--
atlas:nolint DS103` silences it there and `-- atlas:nolint DS102` silences it
natively. Where two native rules share one printed code, the selector reaches
both: `-- atlas:nolint DS103` silences a `DROP COLUMN` and a column type change
alike.

A code selector matches one code exactly and never widens into a family, on
both surfaces: `-- atlas:nolint DS` and `-- atlas:nolint D` suppress nothing,
while the native `-- ptah:nolint DS` still silences the whole family. An
unrecognized
selector is accepted and suppresses nothing, without a warning — the pinned
community binary behaves the same way, and matching it was chosen over
diagnosing the typo. Ptah's own `.ptah-lint.yaml` `disabled-rules` remains
strict and rejects a selector matching no registered rule.

Migrate-up safety keeps its native directive semantics: the whole-file Atlas
header never reopens the apply-time destructive gate.

## Metadata commands and directory formats

Atlas-compatible migration metadata commands default to Atlas directory
format. `ptah-compat migrate hash`, `lint`, `new`, `set`, `status`, and
`validate` register `--dir-format` with Atlas's default value `atlas`.

All six accept every Atlas source layout — `golang-migrate`, `goose`, `flyway`,
`liquibase`, and `dbmate` — under either spelling Atlas accepts, and produce the
same `atlas.sum` from both:

```bash
ptah-compat migrate hash --dir "file://migrations?format=goose"
ptah-compat migrate hash --dir file://migrations --dir-format goose
```

Each layout covers a different set of source files, matching Atlas; see
[the compat command reference](../../reference/atlas-commands/) for the
per-layout rules and for the inputs that stay refused.

When both spellings are given, the `?format=` query wins, which is what Atlas
does. Values are matched verbatim on every one of those verbs: `--dir-format
ATLAS` and `--dir-format " atlas "` are refused rather than normalized, and an
empty value selects the Atlas layout.

`format` is the only `--dir` query key that selects anything. On the eight verbs
that accept a `--dir` query — `apply`, `diff`, `hash`, `lint`, `new`, `set`,
`status` and `validate` — any other key is ignored, as Atlas ignores it, and
named on standard error so a misspelled `?fromat=goose` does not quietly read
the directory in the Atlas layout. The exit code and standard output are
unchanged. Set `PTAH_STRICT_DIR_QUERY=1` to refuse an unrecognized key instead.
`migrate checkpoint`, `down`, `edit`, `rebase`, `rm` and `test` register `--dir`
as well and refuse any query on it, so the note never appears there; see
[the compat command reference](../../reference/atlas-commands/) for that split.

`migrate new` writes the selected layout rather than only reading it. The
created file names and their contents follow the source tool's own convention,
and `atlas.sum` is rewritten over the set that layout covers:

| `--dir-format` | files created | covered by `atlas.sum` |
| --- | --- | --- |
| `atlas` | `<version>_<name>.sql`, empty | the file |
| `golang-migrate` | `<version>_<name>.up.sql` and `.down.sql`, both empty | the `.up.sql` only |
| `flyway` | `V<version>__<name>.sql` and `U<version>__<name>.sql`, both empty | the `V` file only |
| `goose` | `<version>_<name>.sql` holding `-- +goose Up` / `-- +goose Down` | the file |
| `dbmate` | `<version>_<name>.sql` holding `-- migrate:up` / `-- migrate:down` | the file |
| `liquibase` | `<version>_<name>.sql` holding `--liquibase formatted sql` | the file |

Two inputs are refused on a non-`atlas` layout. A migration name is required,
because a file named by the version alone is one Ptah's own `migrate apply`
cannot read back on four of the five layouts. `--edit` is refused, which is what
Atlas does for a non-Atlas directory as well.

`migrate diff` still refuses a non-`atlas` layout under both spellings: it emits
planned SQL, and nothing writes a migration body in a foreign tool's convention
yet. Import the directory with `ptah-compat migrate import` before diffing into
it. `migrate apply` registers no `--dir-format` at all, matching Atlas, and
selects a converted source directory through `?format=` on `--dir`.

`ptah-compat migrate set [version]` moves Atlas revision history to the
selected boundary without executing migration SQL. It preserves existing clean
rows through the target, inserts missing rows as manually set, keeps dirty-row
diagnostics with the combined applied and manually-set type, and removes rows
above the target.

### Which verbs enforce `atlas.sum`

`apply`, `status`, `set`, `validate`, `new` and `diff` all verify the
directory's integrity file before doing anything else, with the same output on
all six ([#974](https://github.com/stokaro/ptah/issues/974),
[#1086](https://github.com/stokaro/ptah/issues/1086)). The refusal precedes the
database connection, so an unreachable `--url` or `--dev-url` does not hide it;
on `set` it precedes the positional-version check too, and on `diff` it precedes
`--to` and `--dev-url` being required at all. The empty-directory and
non-SQL-directory exemptions described under
[Apply a migration directory](#apply-a-migration-directory) apply identically,
so a CI bootstrap that creates an empty `migrations/` keeps working.

`new` and `diff` are the two that WRITE, so for them the gate is a preflight
rather than a check alongside the work: nothing is created, and no `atlas.sum`
is rewritten, on a directory the gate refuses. A `--dir` naming a directory that
does not exist yet is not a checksum error on either tool — both verbs create
it, which is how the first migration of a project gets written.

Because they create it, those same two verbs require `--dir` to name a scheme,
which is what Atlas requires on every verb:

```bash
ptah-compat migrate new add_users --dir migrations
# Error: missing scheme for dir url. Did you mean "file://migrations"?
```

Nothing is created. Ptah still accepts a bare path on the verbs that only read a
directory, and on a directory named by `atlas.hcl` `migration.dir` — both remain
looser than Atlas and are tracked in
[#1186](https://github.com/stokaro/ptah/issues/1186). The requirement is a
`PTAH_DIR` rule as much as a flag rule; `PTAH_MIGRATIONS_DIR`, which is the
native `--migrations-dir` under its environment name, still takes a plain path.

### A migration name cannot contain a path separator

The name becomes part of the file name, so a `/` in it selects a directory that
is not there. Both writing verbs refuse it and write nothing
([#1231](https://github.com/stokaro/ptah/issues/1231)):

```bash
ptah-compat migrate new "sub/dir_name" --dir file://migrations
# Error: atlas migrate new "sub/dir_name": migration name must be a single file
# name element, without a path separator
```

The community CLI refuses the same input, with the raw `open …: no such file or
directory` of the write it did not expect to fail; Ptah names the rule instead,
in the same words the foreign-layout path has always used. Nothing else about a
name is refused on an Atlas-layout directory: a space, a backslash and `..` are
all accepted, because they are accepted there.

The refusal lands where the file would be written, which matters on
`migrate diff`: a diff that finds no changes writes nothing and never reaches
the name, so it still exits 0 — the same as the community CLI.

`lint` deliberately does not enforce it, but only for a *missing* integrity
file: linting a directory that has never been hashed is how you inspect one
before adopting it. It does not tolerate drift. On a hashed directory whose
migration was edited, added to, or removed from, both `lint` implementations
exit 1 on the checksum mismatch, so this is not a route for inspecting a
directory that has already drifted.

`down` does not enforce it either, for a different reason than `lint`: the
community binary refuses that verb outright, so there is no behavior to match.
It still reads a native Atlas directory and executes rollback SQL, so on a
hashed directory whose migration was edited, `status` exits 1 while `down`
reports normally and exits 0. No issue tracks gating it yet.

`rm`, `rebase`, `checkpoint` and `edit` remain divergent — they write an
`atlas.sum` over a directory whose previous contents were never verified,
turning drift into apparent cleanliness. All four are verbs the community binary
refuses outright, so like `down` they have no behavior to match; they are listed
because the hazard is the same one, not because a comparison exists. No issue
tracks closing that gap yet. `new` and `diff` used to be on this list and were
gated by [#1086](https://github.com/stokaro/ptah/issues/1086); the predicate
they now share is the one described above, so the remaining four need a decision
about the missing-`atlas.sum` case rather than new machinery.

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

A successful compatibility import is silent. Inspect the destination directory
and its `atlas.sum` to see what was written, rather than reading a progress
message off stdout. Rejections stay loud on stderr.

The command is intentionally fail-closed: use a destination directory different
from the source directory, and start with a destination that does not already
contain `.sql` migration files or `atlas.sum`. Flyway repeatable migrations are
not in that list: they are converted onto a reserved version slot above every
versioned migration, and the destination file name carries that slot rather than
an R suffix, because Ptah cannot execute an R-suffixed Atlas migration.

**The source directory's `atlas.sum` is verified first.** If the source carries
one, it must cover the source before anything is converted, and the source
checksum is checked ahead of the destination rules above — a tampered source is
refused whatever the destination looks like, and no destination directory is
created. If the source carries no `atlas.sum` at all, the import proceeds: a
directory another tool wrote has never been hashed, and importing it is what
this verb is for. See
[Integrity and safety](../../versioned/integrity-and-safety/).

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
