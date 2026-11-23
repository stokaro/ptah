---
title: Apply migrations
description: Run pending migrations with integrity verification, inspect status, wire operational hooks, and apply from OCI artifacts.
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I apply pending migrations and verify their state?"
goal: "Apply pending migrations and verify their state."
sourceOfTruth:
  - "internal/cli/migrations"
  - "migration"
generated: false
quickstart: true
searchAliases:
  - "apply migrations"
overlaps: []
disposition: keep
owns:
  - cli-ptah-migrations-up
---

You have a hashed migration directory and a database that needs to catch up.
Preview the apply, run it with integrity verification, read the resulting state
in scripts and CI, and add the operational controls the target needs. To apply
one release to many databases, a canary first and the rest in stages, see
[Roll a release out in stages](../../operate/staged-rollout/).

## Prerequisites

A migration directory sealed with `ptah migrations hash` (see
[Generate migrations](../generate/)). The examples use a local SQLite file;
substitute your own `--db-url`.

If you do not have one, build it here. Start in an empty directory:

```console
mkdir ptah-apply
cd ptah-apply
```

Save the pair as `migrations/1785255952_init.up.sql`:

```sql
CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    email TEXT NOT NULL
);
```

and `migrations/1785255952_init.down.sql`:

```sql
DROP TABLE users;
```

Seal the directory:

```console
ptah migrations hash --dir ./migrations
```

Expected output on standard output:

```text
2 migration file(s) hashed
```

## Preview with a dry run

On a fresh target, `--dry-run` prints every statement that would execute,
without touching the database:

```console
ptah migrations up --db-url "sqlite://app.db" --migrations-dir ./migrations --dry-run
```

Expected output on standard output:

```text
=== DRY RUN MODE ===
No actual changes will be made to the database

✅ Dry run completed successfully!
Would have applied 1 migrations
```

The statement-by-statement narration is a log record, not report output, so it
goes to the log stream — stderr by default:

```text illustration
level=INFO msg="[DRY RUN] Would begin transaction"
level=INFO msg="[DRY RUN] Would execute SQL" sql="CREATE TABLE \"users\" ..."
level=INFO msg="[DRY RUN] Would commit transaction"
```

Both `--log-level` and `--log-format` apply to it: `--log-level warn` drops the
narration and leaves only the stdout report, and `--log-format json` moves every
record — report lines included — onto stdout as one JSON object per line, which
keeps `ptah migrations up ... | jq` parseable. See
[Execution controls](#execution-controls).

:::note
The dry run reads an existing revision table and walks only migrations pending
on this target. On a fresh target, it treats the absent revision table as empty
without creating it. A legacy three-column Ptah revision table is read without
upgrading its layout. A partially upgraded table is rejected with a missing
column diagnostic and is not modified.
:::

:::note
A dry run predicts, it does not evaluate everything. Pre-migration checks are
reads, so a preview evaluates a migration's assertions only where the state it
observes is the state a real apply would give it — the first migration executed
in the run. Later migrations' assertions are still parsed and statically
validated, but their database evaluation is deferred and the run says so on
stderr. See
[Checks in a dry run](../integrity-and-safety/#checks-in-a-dry-run).
:::

:::caution
The per-statement narration is native-only: `ptah-compat migrate apply
--dry-run` does not emit it, matching Atlas CE. Compat still writes
safety-relevant notes to stderr — an active `PTAH_SKIP_CHECKS` bypass, and any
deferred checks — so keep the streams apart rather than folding
them together: `ptah-compat ... --format '{{ json . }}' | jq` sees exactly one
JSON document, while `2>&1` would mix those notes into it. See
[Atlas migrate commands](../../atlas/migrate-commands/).
:::

## Apply with integrity verification

```console
ptah migrations up --db-url "sqlite://app.db" --migrations-dir ./migrations --verify-sum
```

Expected output on standard output:

```text
=== MIGRATE UP ===
Dialect: sqlite
Transaction mode: file

Current version: 0
Total migrations: 1
Pending migrations: 1

✅ Migrations completed successfully!
Database is now at version: 1785255952
```

A hashed directory (one that carries `ptah.sum` or `atlas.sum`) is always
verified before anything runs, and the apply aborts on drift — the same check
`ptah migrations validate` performs. A directory without a sum file is not
gated on this native surface. `--verify-sum` additionally makes a missing sum
file itself an error, so set it for shared environments where the directory
must be hashed. `ptah-compat migrate apply` refuses a never-hashed Atlas
directory outright, because Atlas does; see
[Integrity and safety](../integrity-and-safety/).

`--verify-sum` is registered on `migrations up`, `down`, `status` and `push`.
The requirement is the same on each — carry a sum, and match it — but on the
first three the subject is the directory the run pulled, while on `push` it is
the local directory about to be published.

Either check compares the directory against the sum stored beside it, so what
it establishes depends on where the directory came from. See
[Apply from an OCI registry](#apply-from-an-oci-registry) for what that means
when the directory is a registry artifact.

Applied versions land in the revision table. Rerunning the same command is a
safe no-op:

```console
ptah migrations up --db-url "sqlite://app.db" --migrations-dir ./migrations --verify-sum
```

Expected output on standard output:

```text
✅ Database is already up to date!
```

## Locking and `--migration-lock-timeout`

A session advisory lock keeps two runners from advancing one migration history
at the same time, and `--migration-lock-timeout` bounds how long a run waits for
it. The revision table is created under the lock too, so runners that start
together against an empty database wait for each other, and a run with nothing
to apply leaves an empty database empty. Not every engine gives Ptah such a
lock, and a run against one that does not proceeds unlocked.

A lock timeout aimed at a target that cannot lock is refused. The message itself
names the engines that do lock, so no copy of that list lives here:

```console exits=2
ptah migrations up --db-url "sqlite://app.db" --migrations-dir ./migrations --migration-lock-timeout 10s
```

Expected output on standard error:

```text
error: --migration-lock-timeout requested the migration advisory lock, and dialect "sqlite" has none: only postgres, mysql, mariadb, sqlserver, yugabytedb, oracle take a session advisory lock. Remove --migration-lock-timeout to run without a lock
```

Nothing is applied and the revision table is untouched. A URL that names its
dialect is refused before the connection opens, which is why a `sqlite://`
target that does not exist yet is left uncreated. A PostgreSQL-wire URL names no
product: `postgres://` can reach CockroachDB, and that target is
decided once the server has named itself — after the connection, still before
the first migration runs. Remove the flag to run unlocked, which is what such a
target does anyway.

`ptah migrations down` and `ptah migrations baseline` answer the same way: all
three take the same lock on the same history.

The lock is what serializes runs, retries included. A run without it, on an
engine that has none or under `ptah-compat migrate apply --skip-lock`, is
protected only where its read of the revision table is stale by the time it
writes:

- A run that found no row for a version, and whose INSERT of the revision row
  then fails on the key because another run recorded the version meanwhile,
  stops with
  `another run recorded this migration after this run read the revision table`
  before any statement of the body. Any other failure of the INSERT is reported
  as the server gave it.
- Under `--tx-mode all` the revision rows are written inside the run's
  transaction, each after its migration's body, so that INSERT fails after the
  body ran. The run stops with `a revision row for this migration appeared
  after this run read the revision table, and the tx-mode all transaction rolls
  back`, and the rollback takes every body of the run with it, since `--tx-mode
  all` runs only where schema changes roll back.
- A retry with `--allow-dirty` rewrites the dirty row only while the row is as
  the retry read it: in Ptah's layout while its state, applied count and
  checksum are unchanged, and in the Atlas layout, which has no state column,
  while its applied count is unchanged and it still carries its error. A retry
  that read the row before another retry rewrote it stops with the same error.

Two retries that both read the row before either writes it both pass the check,
and both run the rest of the body. A retry that records inside the migration's
own transaction is not checked, and neither is one on ClickHouse, because
neither reports how many rows the rewrite changed.

YugabyteDB 2024.x cannot take the session advisory lock. Ptah refuses a
migration run on that server before changing the schema. YugabyteDB 2025.1
and later support the lock.

On YDB the provider takes the lock on a coordination node, and the server can
take it away while the run goes on; see
[The migration lock](../../extend/providers/ydb/migrations/#the-migration-lock).

On a built-in engine the lock belongs to a database session the run opens for
the lock alone, and the migrations run on other connections. The server
releases the lock when that session ends, and another runner can take it at
once: `pg_terminate_backend` or `KILL` ends it, and so does a broken
connection. The run pings that session every second, and asks
it again before a statement once the last answer is a second old. A session
that does not answer is treated as a lost lock:
the statement running then is canceled, nothing more runs, and the run fails
with the loss.

Every spelling refuses. `PTAH_MIGRATION_LOCK_TIMEOUT` fills the flag on each of
those commands, and `migration.migration_lock_timeout` in
[the project config](../../reference/configuration/) fills it on `up` and
`down`; all of them name this one lock, so a value arriving from any of them
asks for a lock the target cannot give. The message says which spelling carried
the value.

`ptah migrations checkpoint` registers the same flag for a different lock: the
one its replay takes on the **shadow** database, not on a target. A checkpoint
writes files rather than advancing a history, so a shadow database that cannot
lock earns a note on standard error and the replay goes ahead.

`ptah-compat migrate down --lock-timeout` reaches the same command and is not
refused. That surface answers to the Atlas contract, and what the Atlas CLI does
with a lock timeout on a dialect that cannot lock is not measured here.

### Choose the lock name

The lock is named `ptah_migrate` unless you choose another name with
`--migration-lock-name`, `PTAH_MIGRATION_LOCK_NAME`, or `migration.migration_lock_name`
in [the project config](../../reference/configuration/). The flag wins over the
variable, and the variable wins over the config file. `up`, `down`, `baseline`,
`repair` and `set` all read it, and every one of them holds the lock under that
name for the whole run, including the creation of the revision table. `down`
takes it before it first reads the history and keeps it until it has reported,
except while its confirmation prompt waits: an unanswered prompt keeps no other
runner of that name waiting. The config key reaches every one of them except
`baseline`, which reads no project config.

Once its prompt is answered, `down` takes the lock again and reads the applied
migrations under it. When the ones above the target are no longer the ones the
prompt showed, it refuses before any statement runs, and nothing is rolled
back.

On PostgreSQL the lock key comes from the name alone. The schema, the database
name and the revision table do not enter it, so two histories in one database
wait for each other under the default name, however their schemas are named.
Runners of one history must therefore agree on one name, and two histories may
run at the same time only when each has a name of its own. Give each history its
own name when its migrations never read or change objects of the other. If a
migration of one history reads a table, a type or a function that the other
history creates or changes, keep both under one name, so they apply one after
the other.

```bash illustration
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir migrations/billing \
  --migrations-schema billing \
  --migration-lock-name billing
```

Leaving the setting out keeps `ptah_migrate`, so a pipeline that sets nothing
keeps serializing on that name. A name is an explicit request for the lock. An empty or
whitespace-only value is refused before the run connects, and the message names
the spelling that carried it, `PTAH_MIGRATION_LOCK_NAME` included: read as
unset, an empty variable would take `ptah_migrate`, which a runner holding the
named lock does not hold. A name given on a
dialect that has no session advisory lock is refused as a lock timeout is, with
the same message and the same dialects.

When a name was chosen, the report of `up` and `down` and the dry run of
`baseline` print `Migration lock: <name>`. A lock timeout names the lock it
waited for in every case. `ptah-compat migrate down` forwards to
`ptah migrations down` and does not read this setting: Atlas has no lock-name
flag on that verb, so it refuses `--migration-lock-name` and ignores the variable
and the config key. It keeps `ptah_migrate`, the name `ptah-compat migrate
apply` takes unless it is given `--lock-name`.

## Run only the approved sequence

A caller that approves a plan and applies it later reads the history twice:
once when it computes the plan, and again when `up` selects under the migration
lock. If the history moves in between, for example because a backup restored
the database to an earlier version, `up` selects more than was approved and runs
all of it.

`--expect-sequence` names a JSON file that lists the approved migrations in
order. `up` compares its selection under the lock with that list, and any
difference refuses the run before the schema or the revision table changes. The
message names both lists. The order is part of the approval, so the same
migrations in another order are refused too. An empty selection is compared as
well: a history that already moved past the approval is refused rather than
reported up to date.

Each entry names a migration by `version_key` when it carries one, and by
`version` otherwise; the number beside a key takes no part in the comparison.
An entry with a key needs no `version`. The file is read strictly: an unknown
field, a second document, a missing `migrations` list or an entry with neither a
key nor a version of one or more is an error, because a field this build did
not read would be a constraint the caller believes it set.

A directory with [repeatable migrations](../repeatable/) approves their work in
a `repeatables` list, each entry an `identity`, a `checksum` and optionally the
`applied_checksum` the work runs over. Without the list, an approval approves
no repeatable work.

The database this page migrated already records `1785255952`, so an approval
naming it again describes a history that has since moved: `up` selects nothing
under the lock, and the approval says something should run. Write the approval:

```bash
printf '{"migrations":[{"version":1785255952}]}\n' > expected.json
```

```powershell
Set-Content expected.json '{"migrations":[{"version":1785255952}]}'
```

Run it. The command exits with status 2 and changes nothing:

```console exits=2
ptah migrations up --db-url "sqlite://app.db" --migrations-dir ./migrations --expect-sequence expected.json
```

Expected output on standard error:

```text
error: error running migrations: the migrations selected under the migration lock are [], and the approved sequence is [1785255952]: the history moved after the sequence was approved, so nothing was run
```

With `--json`, the document records the refusal as a `failed` run with nothing
applied.

## Check the schema against its history

Checksums prove the files are the ones the database ran, but a column added by
hand or an index dropped during an incident leaves every checksum intact.
`--check-drift` compares the schema.

Under the migration lock, after reading the history and selecting what is
pending, `up` replays the migrations the revision table records as applied on
the `--shadow-db` database and compares that schema with the target's, before
anything runs. Each [repeatable migration](../repeatable/) is replayed after
them with the content in effect: its file when the file hashes to the digest
recorded as applied, and otherwise the body the record keeps for that digest.
The baseline is the applied
history, not the directory, so what a pending migration is about to create is
not drift. The revision table, its log and the record of repeatable
migrations, under the names this run is configured with, are left out of the
comparison.

The replay takes no transaction mode from this run. Each applied migration
replays in a transaction of its own, unless its directive names a mode or a
transaction refuses it, and then it replays without one;
[How each applied migration replays](../drift/#how-each-applied-migration-replays)
gives the rule. A file applied with `--tx-mode none` that a transaction accepts
but changes, such as one whose `SET LOCAL` did nothing outside a transaction,
replays differently. Give such a file `-- atlas:txmode none` before it is
applied. Once it is applied, the directive changes its checksum and the check
refuses the history, so leave what the file created out with `--drift-exclude`.

Run it against the database this page migrated, with a shadow database that
does not exist yet:

```console
ptah migrations up --db-url "sqlite://app.db" --migrations-dir ./migrations --check-drift --shadow-db "sqlite://shadow.db"
```

Expected output on standard output:

```text
Drift check: the database matches the schema its 1 applied migration produces
```

A clean check lets the run go on and apply what is pending. A target that
differs stops the run before any pending migration, and the command exits with
status 1. Standard error lists each difference by object, and never a row:

```text
Drift check: the database differs from the schema its 1 applied migration produces:
  - extra column users.nickname
No pending migration was run. Bring the database back to its history, or record the change in a migration, then run again.
```

A check that cannot decide is never reported as clean. It exits with status 2
and applies nothing, when:

- the history cannot be replayed: an applied revision whose file is gone, an
  Atlas `R` version, whose row records only its last run, a history that
  applied a lower version after a higher one, a repeatable migration whose
  last attempt ran outside a transaction and did not finish, or one whose
  record keeps no body that hashes to the digest it applied;
- the shadow database cannot be used: it is the target under another name, it
  is not empty, or the replay of the applied migrations fails on it;
- the target read cannot describe an object the applied migrations create, so
  whether the target holds it is unknown.

A dirty revision stops `up` before the check, as it always does, and so does
an applied file that changed after it ran, whose checksum no longer matches.
With `--allow-dirty`, the check reports the dirty row as a history it cannot
replay.
An Atlas baseline stands for every file at or below it, and those files are
replayed. A database bootstrapped from a checkpoint replays from that
checkpoint.

The shadow database must be empty and must not be the target, which the check
recognizes under another URL too, and the check empties it again when it ends.
A `docker://` URL starts a disposable container for the run, and a project's
`dev_url` is the default. `--shadow-db` and `--drift-exclude` without
`--check-drift` are refused.

`--drift-exclude`, which can be repeated, leaves out objects the migrations do
not manage, such as a table another tool owns, with the selectors
`schema diff --exclude` takes. The `exclude` list of `ptah.yaml` leaves its
objects out of the check too, and `--drift-exclude` adds to it.

A selector that matches no object in the database or in the replayed history
protects nothing, so a mistyped name leaves its table in the check. The check
warns on standard error, naming where the selector was written, and goes on;
the `--json` document on standard output is unchanged:

```text
Warning: the --drift-exclude selection matched no objects: "audit_log".
```

The lock keeps a cooperating runner from moving the history between the check
and the apply; DDL from a session that does not take it can still land after
the check. The check never repairs
drift. `--dry-run` runs the check against the shadow database too, without the
lock, since a dry run takes none, and changes nothing on the target.

With `--json`, the run document carries the result in `drift_check`. Its
`outcome` is `clean`, `drifted` or `unable`; `replayed` lists the versions the
check replayed, `replayed_repeatables` the identities of the repeatable
migrations it replayed after them, `findings` the differences, and an `unable`
check names its `stage` and `reason`:

```json
{
  "outcome": "failed",
  "drift_check": {
    "outcome": "drifted",
    "replayed": [1785255952],
    "replayed_keys": ["1785255952"],
    "findings": [
      {
        "kind": "extra_column",
        "object": "users.nickname",
        "table": "users",
        "column": "nickname",
        "message": "extra column users.nickname"
      }
    ]
  }
}
```

An `atlas.hcl` env can ask for the check without the flag, in a
`check "migrate_apply"` block holding a `drift` block:

```hcl
env "prod" {
  dev = "docker://postgres/17/dev"
  check "migrate_apply" {
    drift {
      on_error = CONTINUE
    }
  }
}
```

`ptah migrations up --config atlas.hcl --env prod` then runs the check, with
`--shadow-db` or the env's `dev` database as the shadow database. `on_error =
FAIL`, the default, refuses on drift as `--check-drift` does, and `CONTINUE`
reports the differences and applies. An `exclude` list in the `drift` block
replaces the project's for the check, and for `ptah migrations drift` run with
the same env. A `--check-drift` value, from the flag or
from `PTAH_CHECK_DRIFT`, wins over the block. `ptah-compat migrate apply` reads
the same block; see
[Pre-apply drift check](../../atlas/project-config/#pre-apply-drift-check).

`ptah migrations drift` makes the same comparison on its own and applies
nothing: it reports the differences, the diff and the statements that would
bring the database back to its history. See
[Report drift from the applied history](../drift/).

`ptah schema drift` answers a different question: whether a database matches a
declared desired schema. `--check-drift` asks whether it matches the history it
claims to have run.

## Check status

```console
ptah migrations status --db-url "sqlite://app.db" --migrations-dir ./migrations
```

Expected output on standard output:

```text
Current Version: 1785255952
Total Migrations: 1
Applied Migrations: 1
Pending Migrations: 0
Out-of-order Migrations: 0
Status: ✅ Database is up to date
```

Status only reads. Against a database with no tracking table it reports every
migration pending and creates nothing, so an account that may only read the
database can run it.

For scripts, `--json` prints the same state as one object:

```console
ptah migrations status --db-url "sqlite://app.db" --migrations-dir ./migrations --json
```

```json
{
  "current_version": 1785255952,
  "applied_migrations": [
    1785255952
  ],
  "applied_migration_keys": [
    "1785255952"
  ],
  "pending_migrations": [],
  "out_of_order_migrations": [],
  "total_migrations": 1,
  "has_pending_changes": false,
  "current_version_key": "1785255952",
  "contract_version": 2,
  "command": "migrations status",
  "migrations": [
    {
      "version": 1785255952,
      "version_key": "1785255952",
      "description": "Create users",
      "checksum": "h1:Ow4bDq9rLuGUoQxXSbCJn/y4Q1oZbLvBg9wEQ8B4V0M=",
      "applied_checksum": "h1:Ow4bDq9rLuGUoQxXSbCJn/y4Q1oZbLvBg9wEQ8B4V0M=",
      "applied_at": "2026-07-28T16:25:53.104771Z",
      "execution_time_ms": 4,
      "state": "applied"
    }
  ]
}
```

`contract_version` is the version of the document, not of Ptah. A consumer that
does not know the version it reads should refuse the document rather than read
the fields it recognizes out of a shape that means something else. A new field
an existing reader can ignore does not raise it.

In an Atlas directory whose files run under their positions in file-name order,
because a version such as `00` or `v1` is not a decimal number above zero, the
numbers the migrator works with are positions. A position moves when a file is
added before it, so it names no migration to another process. There the status
document and the run document leave out every numeric version member:
`current_version` beside `current_version_key`, `applied_migrations`,
`pending_migrations` and `out_of_order_migrations` beside their `*_keys` lists,
`version` beside `version_key` in each entry, `checkpoint_version`, and an
empty numeric list. The members are absent, never `null`, and the key members name each migration by the
version its file spells. A directory of numbered files prints the numbers as
above.

A status that could not be read prints a document of its own, in the same
contract version: the version, the command and an `error` object, and none of
the fields above. A document that carries `error` is never a status, so a
reader checks for it before reading anything else. `error.message` is the
sentence printed on standard error, and `error.kind` says why the target
database could not be opened; it is absent for every other failure.
[Why a command failed](../../reference/exit-codes/#why-a-command-failed)
lists the kinds. Against a server that refuses the connection:

```json
{
  "contract_version": 2,
  "command": "migrations status",
  "error": {
    "kind": "database_unreachable",
    "message": "error connecting to database: failed to ping database: failed to connect to `user=app database=app`: 10.0.0.5:5432 (db.internal): dial error: dial tcp 10.0.0.5:5432: connect: connection refused"
  }
}
```

The `migrations` array is what a caller plans from. Each entry carries the
migration's version and exact revision identity, its description, the checksum
of the file as it stands, the checksum the revision row recorded, whether the
file is a checkpoint, the transaction mode the file declares resolved for this
dialect, and one state. An entry whose revision row exists also carries
`applied_at`, when the row was written, in UTC, and `execution_time_ms`, how
long the migration ran as the row recorded it. A pending entry carries neither,
and a revision table from before Ptah recorded durations has no
`execution_time_ms`.

| State | Meaning |
| --- | --- |
| `applied` | A clean revision row whose file still accounts for it |
| `modified` | A clean revision row whose file no longer accounts for it; nothing may apply while one exists |
| `dirty` | The version a failed or interrupted run left behind |
| `pending` | Not applied |
| `out-of-order` | Pending, below the current version |
| `checkpoint-covered` | Below the checkpoint that covers it, so it will never run here |

A seventh state, `missing`, never appears in that array: it belongs to a
revision the database recorded and the directory holds no file for, and there is
no entry to carry it. Those are listed in `missing_migrations`, in recorded
order, each with an empty `checksum` because there is nothing to hash.

`checkpoint_version` names that checkpoint: the one a fresh database
bootstraps from, or the newest one the database has applied. A migration below
it reads as covered on both sides of the bootstrap, because applying the
checkpoint is what the bootstrap does.

Comparing `checksum` against `applied_checksum` is not the rule that decides
`modified`, and a caller must not reimplement it: an Atlas history records a
running hash over every preceding file, so the two strings differ for reasons
that are not an edit. `state` is that rule's own answer.

Set `--exit-code` in CI when a database that is not up to date should fail the
job: the command exits `1` while pending work exists, while any migration is
`modified`, and while `missing_migrations` is non-empty. Those last two are
states a deployment must not run into. It exits `0` once none holds.

`ptah migrations up` and `ptah migrations down` refuse in both of them,
including the run that has nothing else to do, and refuse before anything is
applied.

A `modified` migration stays refused until the file carries the bytes the
revision row recorded, so the recovery has an order: put those bytes back, then
carry the change you wanted in a new migration. A new migration on its own
cannot run, because the refusal precedes it.

A missing one has no file to compare, so nothing can say what it did or roll it
back. Two situations reach it and they read the same to the database: a file
removed from the directory, where restoring it is the way back, and an older
release running in front of a database a newer one migrated, where the release
holding the file is. Ptah does not guess which.

**Which of the two refuses depends on the revision format.** A native history
refuses, because nothing writes a row there without a file. An Atlas-format
history does not: it is a history the Atlas community binary also writes, that
binary applies such a directory rather than refusing it, and retiring a
migration file while its recorded history stays readable is something Ptah
supports on purpose for converted directories. So on an Atlas history the state
is reported and the apply proceeds. `missing_migrations` and the
`migrations status` report carry it either way, and `--exit-code` treats it as
not up to date either way; only the apply is scoped.

Adoption is not scoped, whatever the format. `ptah project adopt --check
--preflight` refuses a history whose directory cannot account for it, because
after a takeover the history is native and a row with no file has nothing to be
read from.

## The evidence a run leaves

`ptah migrations up --json` prints one document describing the run, on the
successful path and on the failing one. Under this flag the document is standard
output and everything written for a person goes to standard error, so a caller
parses one stream and reads the other. `ptah migrations down --json` prints the
same document for a rollback, which
[Rollback](../rollback/#the-document-a-rollback-leaves) describes.

```json
{
  "contract_version": 2,
  "command": "migrations up",
  "direction": "up",
  "outcome": "failed",
  "planned": [1785255952, 1785255999],
  "applied": [1785255952],
  "error": { "message": "failed to apply migration 1785255999: ..." },
  "failure": {
    "version": 1785255999,
    "version_key": "1785255999",
    "statement": 2,
    "statements": 3,
    "sqlstate": "42P01"
  },
  "migration_log": { "written": 4 },
  "status": { "…": "the history after the run" }
}
```

The outcome is read from the revision table rather than from the exit status,
because a run that died between its last statement and its answer exits the same
way as one that never started:

| Outcome | Meaning |
| --- | --- |
| `up-to-date` | Nothing was pending, so nothing was selected |
| `applied` | Every selected migration is recorded applied, and no row is dirty |
| `dry-run` | The run was asked to change nothing, and did not |
| `failed` | The run stopped, and the migration that failed committed nothing: its dirty row counts no applied statements. A run that stopped before it selected anything, such as one that could not open the target, is `failed` with nothing planned and no `status` |
| `partial` | The migration that failed committed some of its statements and not the rest |
| `unknown` | The evidence could not be read, or a dirty row does not say how far it got |

`failed` and `partial` are different instructions. A `failed` run may be retried
once the cause is fixed. A `partial` run must not: the recovery is
`ptah migrations repair --resume-from`, which has the row's partial digest to
prove which statements it may skip. `unknown` is for a person.

`planned` is what the migrator selected while holding the migration lock, which
a `--limit`, a target version or a checkpoint narrows; `applied` names the
selected migrations the history records afterwards, so a migration that was
already applied before the run is not counted as this run's work. In an Atlas
directory whose files run under their positions, `planned`, `applied` and
`failure.version` are absent, as the status members are, and `planned_keys`,
`applied_keys` and `failure.version_key` name the migrations.

`error.message` is the message written for a person, and it quotes what the
database said. That can include a row of data, such as the key in a
duplicate-key error, and the statement itself. A caller that must not publish
data reads `error.kind` and `failure` instead, which carry no text. `error.kind`
says why the run could not open the target database, as
[Why a command failed](../../reference/exit-codes/#why-a-command-failed)
lists, and is absent for every other failure. `failure` names the statement the
run stopped at:

| Field | Meaning |
| --- | --- |
| `version`, `version_key` | The migration that failed, spelled as `planned` and `planned_keys` spell it |
| `statement`, `statements` | The failed statement's 1-based position in the migration, and how many statements it holds. `statement` is `0` when `commit` is present |
| `commit` | Present and `true` when the statements ran and the commit that ends them failed, as a deferred constraint does |
| `sqlstate` | The SQLSTATE the server answered with, on PostgreSQL-protocol and MySQL-protocol servers |
| `error_number` | The server's error number on MySQL-protocol, SQL Server, Oracle and ClickHouse servers, such as `1146` for a table that does not exist on MySQL |

`failure` is present only when a statement reached the database and failed, or the
commit did. With `--tx-mode all` one commit covers the run, and a failure of it
names the last migration. A run that stopped before any statement ran has none: a
lock timeout, a checksum mismatch, or a statement refused before execution because
it cannot run inside a transaction. Neither has one that stopped at something other
than a statement of a migration, such as a write to the revision table. `sqlstate`
is absent when the server gave no answer, such as after a lost connection or a
canceled statement. [Where a run stopped](../../reference/exit-codes/#where-a-run-stopped)
lists what each engine gives.

### What the server said

`failure` holds codes alone, so it says where the run stopped and not why. The
server's own account, such as the DETAIL that names the key a foreign key
refused, is shown only when you ask for it with `--server-details`. A migration
that inserts an order for a customer who does not exist fails on its foreign key:

```bash illustration
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir migrations --server-details
```

The details follow the error line on standard error:

```text illustration
error: error running migrations: failed to apply migration 2: failed to execute migration SQL: SQL execution failed: ERROR: insert or update on table "orders" violates foreign key constraint "orders_customer_fkey" (SQLSTATE 23503)
SQL: INSERT INTO orders (id, customer) VALUES (1, 4242)
server details (may contain application data):
  failed at: statement 1 of 1 of migration 2 (0000000002_orders.up.sql)
  sqlstate: 23503
  severity: ERROR
  message: insert or update on table "orders" violates foreign key constraint "orders_customer_fkey"
  detail: Key (customer)=(4242) is not present in table "customers".
  schema: public
  table: orders
  constraint: orders_customer_fkey
```

With `--json` the document carries the same details as `server_details`, next
to `failure`, which stays as it is:

```json
{
  "outcome": "failed",
  "failure": {
    "version": 2,
    "version_key": "2",
    "statement": 1,
    "statements": 1,
    "sqlstate": "23503"
  },
  "server_details": {
    "file": "0000000002_orders.up.sql",
    "server": {
      "severity": "ERROR",
      "sqlstate": "23503",
      "message": "insert or update on table \"orders\" violates foreign key constraint \"orders_customer_fkey\"",
      "detail": "Key (customer)=(4242) is not present in table \"customers\".",
      "schema": "public",
      "table": "orders",
      "constraint": "orders_customer_fkey"
    }
  },
  "…": "the rest of the document"
}
```

| Field | Meaning |
| --- | --- |
| `file` | The migration file the failed statement came from. Absent for a commit, which belongs to the transaction |
| `location` | Where the server's `position` falls: `line` and `column` in the statement as sent, and `file_line` and `file_column` in the file. A column counts characters, as the server does |
| `server` | What the server answered: `severity`, `sqlstate`, `message`, `detail`, `hint`, `position`, `where`, `schema`, `table`, `column`, `data_type` and `constraint`. A field the server did not send is absent |

**The details may contain application data.** A DETAIL can quote the row a
constraint refused, and a WHERE can quote a statement. That is why no default
output carries them and why `failure` never does: a caller that must not publish
data, such as the [Kubernetes operator](../../operate/kubernetes-operator/),
reads `failure` and sees no change. Leave the flag off where the output is
published, such as in a public CI log.

Some rules hold whatever the server sends:

- The password of every database address the command opened, and a role
  password Ptah wrote into a statement, are taken out of every field. Each field
  is then cut to 1024 bytes at a character boundary, and `server.truncated`
  says that one was.
- The text form writes control characters other than a newline or a tab, format
  characters such as the right-to-left override U+202E, and the line and
  paragraph separators as escapes such as `\x1b` and `\u202e`, while the JSON
  form keeps the server's text. The escapes and the indentation of a
  continuation line count toward the 1024 bytes of the text form, so a field
  the escapes lengthen is cut again, and the note
  `a field longer than 1024 bytes is cut` follows.
- Only servers that speak the PostgreSQL protocol send these fields:
  PostgreSQL, CockroachDB and YugabyteDB. On another engine `server` is absent
  and `file` still names the file.
- A statement with no answer, such as after a lost connection or a client-side
  timeout, has no `server`. The text says `server: no details reported`, and
  nothing is filled in from another source.
- A deferred constraint fails at the commit, after every statement ran. Its
  details say `the commit of the transaction that ran migration 2` and carry no
  file and no location. Under `--tx-mode all` the commit names the last
  migration, as `failure` does.
- A migration in an Atlas txtar file is named by that file, and its `location`
  has a line and a column in the statement but no file line or column.
- `location` is present only when it is certain. A statement Ptah changed before
  sending it, such as one it wrote a role password into, has none. The file
  line and column are present only when the statement appears in the file
  exactly once, as it was sent.

The flag changes neither the outcome nor the exit code. It reads
`PTAH_SERVER_DETAILS` like every boolean `PTAH_*` variable, and
`ptah migrations down` takes it too. `ptah-compat migrate apply` and
`ptah-compat migrate down` take no new flag, so they read only the variable; see
[Atlas-compatible commands](../../reference/atlas-commands/#ptah-compat-migrate-apply).

`migration_log` says what the run's writes to the
[migration log](../migration-log/) came to. A failed write does not stop the
migration, so it changes neither `outcome` nor the exit status, and the table
cannot say what it was never given. This document is the only record that an
entry was lost, and a rerun cannot bring it back.

| `migration_log` | Meaning |
| --- | --- |
| absent | The run keeps no log: `migration.log` is `false`, the revision format is the Atlas-compatible one, the run is a dry run, or it failed before the migrator ran, such as one that could not open the target |
| `{ "written": 0 }` | The run had nothing to record |
| `{ "written": 4 }` | The server accepted all four entries the run tried to write |
| `{ "written": 4, "run_reference": "sha256:…" }` | As above, and every written entry carries the caller's `--run-reference`. See [the run reference](../migration-log/#the-run-reference) |
| `{ "written": 2, "lost": [ … ] }` | The server accepted two entries, and `lost` names the rest |

Each attempt tries to write two entries, one when it starts and one when it ends,
so a migration can appear in `lost` twice:

```json
{
  "operation": "up",
  "version": 1785255999,
  "version_key": "1785255999",
  "state": "started",
  "sqlstate": "22001",
  "error_number": 1406,
  "message": "SQL execution failed: Error 1406 (22001): Data too long for column 'actor' at row 1 ..."
}
```

| Field | Meaning |
| --- | --- |
| `operation` | The verb the entry belongs to, `up` or `down` |
| `version`, `version_key` | The migration the entry is about, spelled as `planned` and `planned_keys` spell it |
| `state` | The outcome the entry would have recorded |
| `sqlstate`, `error_number` | The server's answer, as in `failure`. Absent when the server gave none, and on an engine with neither |
| `message` | The failure written for a person. It can quote what the database said, including part of the value it refused, so a caller that must not publish data reads the other fields |

## Execution controls

Each statement reaches the server with the comments inside it, and without
the comments before its first token. MySQL, MariaDB and SQL Server store a
trigger's or a routine's body as it arrives, so a comment written in the body
is still there when the database is read back. A statement that holds only
comments is not sent.

The defaults are safe for most runs. These controls matter once several people
and pipelines share a directory:

- **Transaction mode** (`--tx-mode`): `file` (default) wraps each migration
  in its own transaction; `all` runs the selected batch in one; `none`
  disables wrapping. A migration may select `file` or `none` with a leading
  `-- atlas:txmode <mode>` header, or with `-- +ptah no_transaction`. A blank
  line after that header is accepted but not required. Atlas CE reads
  `atlas:txmode` anywhere in a header line that a blank line follows, and Ptah
  does the same. A header that runs straight into the first statement is a Ptah
  extension, and there the directive counts only on a line that begins with
  it. The heading `-- Create "atlas:txmode none" table`, which Atlas and
  `ptah-compat migrate diff` write directly above a statement, is therefore not
  a directive. Explicit file modes conflict with global `all`.

  Ptah transaction, timeout, and online-DDL directives are significant only
  **before the first executable statement**. Atlas transaction mode uses its
  own stricter header rule described below. A directive written below the
  statements it claims to govern is not honored — and it is not dropped in
  silence either. Ptah reports it at `WARN` on stderr, naming the file, the
  line, the directive and how to fix it, because an operator who writes
  `txmode none`, sees exit `0`, and believes the file ran outside a transaction
  is the failure this rule exists to prevent. The `-- atlas:` spelling keeps
  Atlas's own stricter acceptance inside that region: only the unbroken run of
  line comments that begins on line 1, each starting in column 1. A leading
  blank line, an indented directive, or a blank line between the directive and
  an earlier comment all put it outside that block, so Atlas CE ignores it and
  so does Ptah — with the same warning. `-- +ptah` directives accept the whole
  region, blank lines and indentation included.

  The region uses the target database's comment grammar, read from the same
  lexer options Ptah splits the file's statements with — never from a separate
  list of dialects, which is how the two would drift apart. A leading `#`
  comment therefore stays part of the header wherever that reader treats it as
  a comment, which is MySQL, MariaDB and ClickHouse, and does not hide the
  `no_transaction`, `lock_timeout` or `statement_timeout` directives that
  follow it. Those three read every `#` as the start of a comment, whatever
  character follows it. On the other targets `#` is not a comment (it is the
  XOR operator on PostgreSQL, and the first character of a temporary table
  name on SQL Server), so a directive below one really does sit below the
  first non-comment line and Ptah reports it.

  The same grammar decides where a `--` comment begins. MySQL and MariaDB start
  one only when a whitespace or control character follows the second dash, so a
  `-- ` separator line stays in the header — including its `-- \r\n` form and a
  `--` line carrying nothing but its line terminator — while `--x`, which those
  two read as SQL rather than as a comment, ends the header there.

  Before a connection exists the dialect is unresolved, and there Ptah reads
  the header the widest way any target would. That direction is deliberate:
  loading a file happens first and its verdict is final, so a header cut short
  there would refuse a correctly placed directive as being below the first SQL
  statement and offer a remedy — move it up — that the line already satisfies.
  Ptah then resolves the effective timeouts and transaction mode with the
  execution dialect before it validates or runs the migration.

  Position and value are separate facts, and a bad value is not demoted to a
  position warning. A `-- +ptah` directive whose key Ptah recognizes but whose
  **value** it cannot read — `no_transaction=maybe`, `lock_timeout=soon` — fails
  the run wherever the line sits, and the refusal names the position too, so you
  are not told the value is nonsense, told nothing about the line being in the
  wrong place, and left to discover that on the next run. A timeout key written
  with no value at all — `-- +ptah lock_timeout` — fails the same way, and it
  does so whatever else shares the line: a neighboring field Ptah can read says
  nothing about whether the timeout has a value. A bare word Ptah does not
  recognize as a directive (`-- +ptah revisit this`) is an ordinary comment and
  is neither refused nor reported, and neither is a word inside an ordered
  `-- +ptah check` line, whose quoted arguments are that directive's own grammar
  rather than `key=value` fields.

  The `-- atlas:` spelling deliberately has no equivalent refusal. Measured on
  Atlas CE `v1.3.0`, `migrate apply` over a directory carrying
  `-- atlas:txmode bogus` exits `1` when the line is the header and `0` when it
  sits below the statement. Refusing the second would exit non-zero where Atlas
  CE exits `0`, so Ptah reports it and applies the directory, as CE does.

- **Runner** (`--runner`, or `migration.runner` in `ptah.yaml`): the tool the
  directory is written for. `ptah` (default) wraps each file in a transaction
  of the migrator's, as described above. `golang-migrate` opens none: a
  PostgreSQL file that opens and closes its own transaction runs those
  transactions as written, a file of one statement runs without a
  transaction, and a failed file records the statements its own `COMMIT` made
  permanent, so a retry with `--allow-dirty` resumes after them. Under the
  default, Ptah refuses a file that controls the transaction it owns before
  running that file. The apply gate and `ptah migrations lint --runner`
  judge `transaction/explicit-transaction` by the same runner, `ptah migrations down` rolls a down
  file back by it, and `ptah migrations repair` reads the file a dirty
  revision names by it
  ([A directory written for golang-migrate](../rollback/#a-directory-written-for-golang-migrate)).
  [Transactions in a golang-migrate directory](../../atlas/migrate-commands/#transactions-in-a-golang-migrate-directory)
  describes how such a file runs.

- **Batch limit** (`--limit`): apply only the first N pending migrations —
  useful for staged rollouts and verifying one step at a time. `--allow-dirty`
  is the explicit recovery escape hatch that proceeds past a dirty revision
  row. When the dirty row belongs to a migration that is still pending — the
  usual case, a body that failed part-way — the retry reuses that row rather
  than recording a second one, and skips the statements the earlier attempt
  committed. Before it skips anything, Ptah verifies that the committed source
  prefix is unchanged. Native rows carry a `partial:h1:` prefix checksum;
  Atlas-format rows carry cumulative `partial_hashes`. Editing only the
  unapplied suffix is allowed. A failed retry cannot reduce `applied` below the
  previously committed prefix, even when the transaction mode changed.
  A run executes every migration body on one pinned physical database
  session, so a statement such as `SET search_path` remains effective for the
  statements that follow and for the later migrations of the run, as
  golang-migrate and Atlas apply a directory. On server databases, revision
  checkpoints stay on the original connection or are qualified with the
  connection's schema, and cannot be redirected by that session state. SQLite
  pins a session for each `none` attempt instead and qualifies its revision
  table in `main`, so in-memory databases do not deadlock and temporary objects
  cannot shadow the metadata. A provider's connection runs the bodies on its
  own session, where the provider holds the migration lock and guards each
  commit, so what they leave there outlives the run. A resumed attempt starts a
  run on a fresh session: Ptah replays recognized session-control statements
  from the verified prefix of the resumed migration, skips recognized durable
  DDL and DML, and refuses prefixes that created temporary objects or whose
  session-local effect cannot be classified safely.
  A `none` body cannot contain top-level transaction-control statements such as
  `BEGIN`, `COMMIT`, `ROLLBACK`, savepoint commands, MySQL or MariaDB
  `SET autocommit`, or SQL Server `SET IMPLICIT_TRANSACTIONS`. Ptah validates
  the complete body and pins its physical session before changing revision
  metadata, so either failure leaves a new version absent and preserves an
  existing dirty row unchanged.
  A `file` or `all` body also cannot control the transaction Ptah owns. Ptah
  validates each up body before running it, or the whole batch before `all`
  runs, and validates a down body before changing its revision row. On MySQL
  and MariaDB, changing
  `autocommit` inside a `file` body can also make a post-DDL checkpoint
  ambiguous. Their revision metadata, default
  storage engine, and every existing base table in the selected database must
  use InnoDB. A migration that explicitly selects another storage engine,
  resets an engine setting to `DEFAULT`, or inherits one through
  `CREATE TABLE ... LIKE` is refused before its first statement. `DEFAULT` can
  resolve differently from the session default that preflight verified. On
  MySQL, the migration account must hold `TRIGGER` at database or
  global scope so Ptah can see a complete trigger catalog. MariaDB exposes each
  trigger's identity and target table without `TRIGGER`, so it does not require
  that privilege for this catalog check. Ptah refuses MySQL `file` mode when it
  cannot prove complete visibility. Ptah reads a grant as the server applies it:
  a pattern such as ``GRANT ALL ON `app%`.*`` covers `app`, a more specific row
  that leaves `TRIGGER` out hides the pattern, and with `partial_revokes` on a
  name in a grant is literal. Every `sql_mode` assignment is also refused because changing grammar or quoting rules after
  preflight can make the server execute SQL that Ptah did not inspect. A session
  that already enables parser-changing `ANSI_QUOTES`, `MSSQL`, or
  `NO_BACKSLASH_ESCAPES` behavior through the connection DSN or server default
  is refused for the same reason. Durable server-state operations such as
  `SET GLOBAL`, `SET PERSIST`, `RESET`, and `CREATE`,
  `ALTER`, or `DROP DATABASE` are refused for the same reason: their effects do
  not share the InnoDB transaction containing the witness. `SELECT` or `TABLE`
  with `INTO OUTFILE` or `INTO DUMPFILE` is also refused because it writes
  outside that transaction. The equivalent `SCHEMA` statements and `USE` are rejected as
  well; select the target database in `--db-url` so Ptah can validate the
  database it will modify. A statement that names another database is inspected
  rather than refused: the engine preflight and the object catalog cover every
  database a migration names. Ptah also refuses executable comments, `CALL`,
  prepared or dynamic SQL, table locks, definitions of views, triggers,
  routines, and events, references to existing views or trigger-bearing tables,
  and calls to stored routines. Those forms can hide work that does not share
  the witness transaction. A custom `MigrationFunc` is opaque for the same
  reason and must use `none`; a `StatementInterceptor` is also opaque because
  it can replace the inspected statement with different SQL. MySQL-family
  `file` mode accepts directly executed SQL-backed migrations only.
  Statement-level rejection diagnostics identify the statement number and
  safety class without echoing the SQL, which may contain credentials.
  Migration-function and interceptor refusals identify the affected direction
  instead because their inner statements are opaque.
  The migration advisory lock serializes Ptah clients that use the same lock
  name. It cannot freeze DDL from a client that ignores that lock. Do not run
  out-of-band DDL from the safety preflight until the migration finishes.
  Pre-migration checks are not rerun after committed progress because they
  describe the original pre-migration state. Automatic continuation is
  up-direction only: a row left dirty by an interrupted rollback is refused so
  up SQL cannot be resumed from a down-statement offset. Use
  `ptah migrations repair` after inspecting the database when the row cannot
  be resumed automatically: an
  interrupted rollback, a process whose last statement has an unknown outcome,
  changed or unverifiable committed-prefix metadata, an edit that changed the
  file's statement count, or a dirty row for a migration whose file was rebased
  away. Legacy dirty rows without prefix metadata may resume only while their
  full-file checksum still matches. Repairing an incomplete up row without
  running its missing SQL requires `--force` after manual reconciliation.
- **Version bound** (`--to-version`): apply pending migrations up to and
  including one exact version, and stop there. An approved plan names the
  versions it covers before the run starts, and this is how the run is held to
  them. It is equally how you take half a backlog deliberately and watch what
  happens.

  ```bash illustration
  ptah migrations up \
    --db-url "$DATABASE_URL" \
    --migrations-dir migrations \
    --to-version 1785255952
  ```

  The version is written the way the migration file writes it, so leading zeros
  are accepted. In an Atlas directory whose files run under their positions in
  file-name order, the bound is the version string a file spells: `2` names
  `2_x.sql` and `v1` names `v1_x.sql`, and the run stops after that file in
  file-name order. `0` is refused before the command connects, because it names
  the empty history rather than a migration, and so is text over a directory
  whose every file carries a number, because no file there can spell it. A
  version the directory does not carry is refused, and so is a
  version the database has already passed: nothing pending reaches the target
  there, and an exit `0` would report a state the database never arrived at. A
  database recorded at exactly the named version is not that case, and applies
  nothing and succeeds. Under `--exec-order linear-skip` a target the execution
  order leaves pending is refused as well, naming the order rather than the
  recorded version, because `--exec-order non-linear` is what reaches it.

  Passing `--limit` as well is refused: the two select different prefixes of the
  pending list and neither outranks the other. The refusal reads the values, not
  the spelling, so `PTAH_TO_VERSION` beside `PTAH_LIMIT` is refused before the
  run connects, while `--to-version ""` and `--limit 0` ask for no bound and
  leave the other flag to decide. A flag typed beside the other's variable wins,
  and the variable is withdrawn.

  The bound narrows what `--dry-run` reports and what `--json` names under
  `planned`, because both read the plan the migrator selected while holding the
  migration lock rather than the pending list read before it.
- **Execution order** (`--exec-order`): `linear` (default) fails when a merge
  landed a pending migration below the current version; `linear-skip` warns
  and leaves it pending; `non-linear` applies it. Status reports such
  versions as out-of-order.
- **Timeouts and locks**: `--statement-timeout`, `--lock-timeout`, and
  `--migration-lock-timeout` bound long DDL and the session-level advisory
  lock that keeps two migrators from racing. A target whose dialect has no such
  advisory lock refuses `--migration-lock-timeout`; see
  [Locking and `--migration-lock-timeout`](#locking-and---migration-lock-timeout).
  A migration that resolves to `none` takes its statement and lock timeouts
  on the database session that runs its statements, because `SET LOCAL`
  outside a transaction changes nothing. That session belongs to the one
  migration, so the settings end with it. YDB has no such setting, and its
  provider bounds each query with the statement timeout; see
  [Statement timeouts on YDB](../../extend/providers/ydb/migrations/#statement-timeouts). On a target with no
  way to honor a timeout, a migration that declares one is refused in every
  transaction mode, before any SQL runs or the revision row changes.
  SQL-backed non-transactional migrations record a durable progress marker
  before and after each autocommit statement. A custom Go `MigrationFunc`
  remains opaque and is recorded only when it returns.
### What `--tx-mode all` cannot carry

`--tx-mode all` runs every selected migration in one transaction. Three other
features are scoped to a single migration, so they do not compose with it, and
each is refused before any SQL runs rather than discovered one migration at a
time.

| Combined with `--tx-mode all` | Result |
| --- | --- |
| A target with no transactional DDL | refused: `tx-mode all is not supported for dialect "…": this target commits schema changes as they run, so a failed migration cannot be rolled back as a unit` |
| A migration declaring pre-migration checks | refused: `migration N declares pre-migration checks, which cannot run with tx-mode all` |
| A migration declaring timeouts | refused: `migration N declares timeouts, which cannot run with tx-mode all` |
| A migration selecting its own transaction mode | refused: an explicit file-level `file` or `none` conflicts with global `all` |

The reason is the same in the middle two cases: one transaction spans the whole
run, so a per-migration timeout would bound the entire batch rather than the
file that asked for it, and a pre-migration check would read state the batch has
already changed. The remedy is the default per-file mode, or removing the
directive from that migration.

The first row is the engine rather than a Ptah policy, and it is decided by the
target's transactional-DDL capability rather than by a list of dialect names.
MySQL, MariaDB, ClickHouse and Oracle commit DDL as it runs, and Spanner and
YDB, which their providers serve, refuse DDL inside a transaction at all.
YugabyteDB commits schema statements separately at its default setting, so
Ptah also refuses `--tx-mode all` there. Use the default per-file mode to run
migrations on YugabyteDB. Ptah does not inspect the server's transactional-DDL
flag, so the refusal also applies when that flag is enabled.

CockroachDB refuses for a narrower reason: a target named with no server version
in hand resolves to the newest measured line, where a schema statement inside a
transaction commits itself first, so the rollback has nothing left to undo. A
connected CockroachDB server on an older line reaches the capability through the
version ladder and is accepted.

Timeouts themselves are not tied to that capability. Each has a capability of
its own, `migration_lock_timeout` and `migration_statement_timeout`, and a
target can carry one and not the other. Where a target lacks one,
`migrations up` and `migrations down` refuse the timeout it lacks, naming the
flag or the `ptah.yaml` key that set it, the engine and the capability, even
when no migration is pending. Where the URL settles the answer the refusal comes
before the command connects, so a `sqlite://` file is not created. Spanner,
through its provider, takes neither, and the refusal comes before anything is
read or written. YDB takes the statement timeout and refuses the lock timeout,
because no statement there waits for a lock.

A value from `PTAH_LOCK_TIMEOUT` or `PTAH_STATEMENT_TIMEOUT` is refused by the
migrator instead, on the first migration that would run under it:
`ptah schema apply` reads `PTAH_LOCK_TIMEOUT` as a lock wait, so an exported
value is not taken as addressed to the versioned commands alone.

- **Run logging** (`--log-level`, `--log-format`): `--log-level`
  debug\|info\|warn\|error selects how much of the run is narrated —
  `warn` silences the per-statement dry-run narration — and `--log-format`
  text\|json selects the encoding. `json` moves every record, the stdout
  report included, onto stdout as one JSON object per line.

See [Configuration](../../reference/configuration/) for the `ptah.yaml`
equivalents of every control.

### `--allow-dirty` means two different things

One spelling, two surfaces, two unrelated safety questions. The collision is
permanent — the compatibility surface registers the flag Atlas registers, and
the native surface registers the one Ptah has always had — so read the flag
against the command it was typed after:

| command | what is dirty | what the flag asks for |
| --- | --- | --- |
| `ptah migrations up` | a **revision row**, left by a migration body that failed part-way | a verified retry of that body, skipping only an unchanged committed prefix |
| `ptah-compat migrate apply` | the **schema**, which already holds objects this history did not create | adopt that database anyway and apply into it |

Neither can be expressed in terms of the other, and neither implies the other.
Measured against the pinned community binary, its `--allow-dirty` releases no
dirty-revision guard at all: an operator who passes it there has said nothing
about revision rows, and one who passes it to `ptah migrations up` has said
nothing about adopting a populated database.

Native `ptah migrations up` has no adoption gate, so nothing refuses a database
it did not create; running it against one fails on the first object that
already exists. `ptah migrations baseline` is the native way to adopt an
existing database, and `--shadow-db` verifies that the baselined history
reproduces the schema it was pointed at. Without `--shadow-db` it compares the
Go entities with the database as `ptah schema compare` does. On a PostgreSQL
URL that pins no `search_path`, a table the entities name without a schema
matches only a table in the connection's current schema, where the migrations
would create it.

## Migrations on YDB

A YDB migration runs through the YDB provider, which splits it into the queries
YDB runs, takes the migration lock on a coordination node and bounds each query
by the statement timeout. Its
[Migrations on YDB](../../extend/providers/ydb/migrations/)
page describes each of them and the files it refuses.

## Operational hooks

Production-like runs should be configured, not wrapped in ad hoc shell
scripts. `ptah migrations up` supports:

- `--pre-up-hook` — a shell command that must exit `0` before anything is
  applied (rollback runs have `--pre-down-hook`).
- `--webhook` — a URL that receives migration metadata and must return
  HTTP 200 before the run proceeds.
- `--pg-dump-to` / `--mysqldump-to` — a directory where a backup is written
  before migrations are applied.
- Revision-table placement (`--migrations-table`, `--migrations-schema`) and
  Prometheus metrics (`--metrics-addr`). OpenTelemetry traces need a build
  with the `observability` tag; see [Trace migration runs](../../operate/trace-migrations/).
- `--migrations-engine` — the storage engine the revision table is created with.
  It exists for ClickHouse, where a replicated deployment needs
  `ReplicatedMergeTree(...)` or the migration history lives on one node while
  every replica reports itself consistent; see
  [the ClickHouse revision table's storage engine](../../databases/clickhouse/#the-revision-tables-storage-engine).
  An engine the revision table cannot be is refused before any statement runs.

A command hook runs with `PTAH_DB_URL`, `PTAH_DIALECT`, `PTAH_CURRENT_VERSION`
and `PTAH_TARGET_VERSION` in its environment: the version the database records
and the version the run ends at. The webhook receives a JSON object with
`direction`, `dialect`, `current_version`, `target_version` and
`database_url`, the URL with its password and secret query parameters hidden.
A backup is named `ptah_pre_v<current>_to_v<target>_<UTC time>.dump`, or
`.sql` for `--mysqldump-to`.

In an Atlas directory whose files run under their positions in file-name order,
the numbers are positions, which name no migration outside the run, so the
hooks receive the version keys alone. The command hook gets
`PTAH_CURRENT_VERSION_KEY` and `PTAH_TARGET_VERSION_KEY`, the webhook gets
`current_version_key` and `target_version_key`, and a backup is named
`ptah_pre_<current>_to_<target>_<UTC time>`, as the files spell the versions.
An empty current key means no migration is applied, and a backup name spells it
`none`.

All of these can live in `ptah.yaml` instead of the command line; see
[Configuration](../../reference/configuration/). A hook written in `ptah.yaml`
is a program the project file names, so it runs only with
`--allow-config-exec` or `PTAH_ALLOW_CONFIG_EXEC=1`; without either,
`migrations up` refuses it before it connects to the database. The same hook
passed as `--pre-up-hook` needs no permission. See
[Programs a project file names](../../reference/configuration/#programs-a-project-file-names).

## Apply from an OCI registry

`up`, `status`, and `down` accept an `oci://` reference as the migrations
directory, so the artifact your CI published is exactly what production runs
— pin it by immutable digest:

```bash illustration
ptah migrations push \
  oci://ghcr.io/acme/app-migrations \
  --migrations-dir ./migrations \
  --verify-sum

ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir oci://ghcr.io/acme/app-migrations@sha256:<digest> \
  --verify-sum
```

Pin the digest deliberately: `--verify-sum` checks the pulled directory against
the sum shipped inside the same artifact, so over a movable tag it proves the
files are internally consistent, not that they are the reviewed ones. `up`
prints that qualification, along with the digest the tag resolved to and the
reference that pins it, whenever a sum verifies over a tag-resolved artifact.
The run still succeeds; a digest reference gets no such line.

To keep the readable name and the pin together, write both:

```bash illustration
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir oci://ghcr.io/acme/app-migrations:release@sha256:<digest> \
  --verify-sum
```

The digest selects the bytes and is verified against what the registry returns;
the tag is a label. Repointing `:release` afterwards changes nothing about what
this command runs, and this reference counts as a digest pin, so it gets no
movable-tag qualification.

See [OCI registry artifacts](../../operate/oci-registry/) for
authentication, tag and digest semantics, referrer reports, and CI wiring.

## Failure modes

**Integrity drift aborts before anything runs** (exit `2`):

```text illustration
error: migration sum verification failed:
migration directory does not match ptah.sum:
  changed: 0000000002_add_posts.up.sql
```

**Destructive pending migrations are refused by default** (exit `2`); rerun
with `--allow-destructive` after review:

```text illustration
error: error running migrations: pending migrations contain destructive statements; rerun with --allow-destructive after review:
- 0000000003_drop_users.up.sql:1 destructive/drop-table error: DROP TABLE permanently deletes table users and every row in it; ...
```

The integrity gate is covered in depth on
[Integrity and safety](../integrity-and-safety/), the destructive gate on
[Lint and gate unsafe SQL](../lint/).

**A file its transaction would refuse stops the run before it starts** (exit
`2`). A statement the engine refuses inside a transaction block, such as a
`CREATE INDEX CONCURRENTLY` in a file that runs in the per-file transaction or
under `--tx-mode all`, is found in every file of the run before the first
migration is applied, in a dry run too. No migration of the run is applied and
no revision row is written:

```text illustration
error: error running migrations: 0000000002_add_index.up.sql cannot run inside a transaction: line 1 CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; mark the file `-- +ptah no_transaction`, or move the concurrent index into a migration of its own
SQL: CREATE INDEX CONCURRENTLY idx_posts_author ON posts (author_id)
```

A file that runs without a transaction around it is not refused: one marked
`-- +ptah no_transaction` or `-- atlas:txmode none`, every file under
`--tx-mode none`, and, under `--runner golang-migrate`, a file of one
statement, which that runner sends alone.

The other refusals Ptah makes from a file's text stop the run before it starts
in the same way, in both directions and in a dry run: transaction control such
as `BEGIN` or `COMMIT` in a file Ptah runs in a transaction or without one, an
`online_ddl_tool` or `online_ddl_fallback` value Ptah does not know, and on
MySQL and MariaDB under `--tx-mode file`, a statement the transaction witness
refuses, such as a table on a non-InnoDB engine. What needs the database's
answer, such as a pre-migration check or the storage engines the witness
verifies, is asked when the run reaches the file.

**A migration failed partway.** The revision table records a dirty state and
every later run refuses to continue until it is repaired — see
[Maintain migration history](../maintain-history/). A failed *rollback* records
the same recoverable state in both revision-table formats;
`ptah-compat migrate down` keeps the Atlas table layout but does not copy
Atlas's hidden failed-down state. [Roll back migrations](../rollback/) shows
how to resume it through the native repair command.

**What a failed body records depends on the transaction mode — and, on the
MySQL family, on the statements themselves.** The committed prefix a resume
skips is only ever the prefix that really survived:

- Under `none`, every statement commits on its own. The revision row is
  checkpointed after each one, so `applied` and the cumulative digests name
  exactly the statements that ran, and the retry continues at the next one.
- Under `file` or `all` on PostgreSQL, SQLite and SQL Server, DDL is
  transactional and the failure rolls the whole body back. Nothing is recorded
  as committed, and the retry runs the body from its first statement. An
  Atlas-format row written for such a failure is removed rather than left
  claiming progress that no longer exists.
- Under `file` on CockroachDB from v25, `autocommit_before_ddl` is on by
  default. A schema statement commits the transaction around it before it runs,
  and the statements after it commit one by one. The server reports that the
  session left its transaction, and Ptah reads that report: `applied` counts
  every statement that ran before the failure. A body that reached no schema
  statement still rolls back and records zero.
- Under `file` on YugabyteDB, `yb_ddl_transaction_block_enabled` is off by
  default. A schema statement commits on its own and the transaction around it
  stays open, so the rollback undoes the data and leaves the schema statements.
  `applied` counts the leading statements that stayed, and a retry runs the data
  after them again. Data ahead of a schema statement is the exception: no count
  describes a statement that stayed behind one the rollback undid, so the row
  says so and the outcome is `unknown`. The same goes for a statement Ptah
  cannot read, such as a `DO` block or a `CALL`. With the setting on, the
  rollback takes the whole body.

  [What a failed migration leaves](../../databases/distributed/#what-a-failed-migration-leaves)
  shows each shape on each engine.
- Under `file` on Oracle, Ptah opens no transaction around the body. Oracle
  commits before every schema statement, so each statement commits as it runs,
  as under `none`, and `applied` counts the statements that ran before the one
  that failed. Oracle does not support `all`.
- Under `file` on Spanner, Ptah opens no transaction around the body. Spanner's
  PostgreSQL interface refuses a schema statement inside an explicit
  transaction, so its [provider](../../extend/providers/spanner/) splits the
  body and each statement commits as it runs, as under `none`. `applied` counts
  the statements that ran before the one that failed, and those statements stay
  in the database. Spanner does not support `all`; see
  [Migrations on Spanner](../../extend/providers/spanner/migrations/).
- On YDB, `file` runs a body the way `none` does. YDB runs no schema statement
  inside a transaction, so a file transaction could hold only data statements,
  and those already commit query by query. The unit is a query rather than a
  statement; see [Migrations on YDB](#migrations-on-ydb). `applied` counts the
  queries that committed before the one that failed. YDB does not support
  `all`.
- Under `file` on MySQL and MariaDB, the server may commit the open transaction
  around DDL, so part of a failed body can survive its final rollback. Ptah does
  not infer that prefix from SQL keywords. Before and after each statement it
  updates the InnoDB revision row on the same physical transaction as the body.
  A server-side implicit commit therefore makes the matching witness durable;
  an ordinary rollback removes both user DML and its witness. Plain DML that
  rolls back retries from the first statement, while a durable DDL/DML prefix
  resumes at the first statement not witnessed as complete. Ptah pins the
  run's physical session and discards it when the run ends; a retry replays
  safe session settings from the verified prefix before it continues. Temporary
  tables remain available to the later migrations of the run: their DDL does
  not make permanent InnoDB work durable by itself, and discarding the session
  prevents temporary state from leaking into a retry. If a later implicit
  commit makes a prefix containing a temporary object durable, automatic resume
  refuses because that object cannot be reconstructed safely.
  The configured migration metadata table name is reserved. Before reading or
  writing revision metadata, Ptah refuses and discards a pinned session that
  already contains a same-named temporary table. It also rejects statements
  that directly reference the metadata relation, including through a
  schema-qualified name.
  If a witness committed before a failing statement whose lack of side effects
  cannot be proven, the row remains marked unknown and automatic retry stops for
  manual inspection. On MySQL with InnoDB target tables, a server refusal of
  `CREATE INDEX` or `CREATE UNIQUE INDEX` proves that the index was not created;
  the dirty row records a failed statement without an unknown-outcome marker.
  A lost server reply still leaves the outcome unknown. MariaDB keeps the
  conservative unknown outcome for a refused index build. MySQL and MariaDB
  do not support `all`.

Recorded progress therefore excludes transactional work that a rollback undid,
while non-transactional mode retains work that no rollback could undo.
On MySQL and MariaDB, non-transactional mode keeps revision bookkeeping on a
separate checked metadata session while the migration body runs on the run's
session, so body-local temporary objects cannot shadow the metadata table.

**A non-transactional statement was interrupted.** If the process exits, the
context is canceled, its deadline expires, or the connection breaks while an
autocommit statement is in flight, the revision row preserves the last known
completed statement and marks the interrupted statement's outcome as unknown.

A broken connection is a reset, an end of stream or a network error that
arrives after the statement was sent, and on YDB a transport error: the server
may have applied the statement and lost the way to say so. A refusal the server
sent is an answer, and is recorded as an ordinary failure.

Inspect the database before repair. Both `repair --resume-from` and `up
--allow-dirty` refuse the row while this marker is present, because the SQL may
already have committed and neither verb can tell. The refusal holds when the
marker sits on the first statement, where the row records no completed
statement at all: zero says no checkpoint was written, not that nothing ran.

**A concurrent index build failed on PostgreSQL** (exit `2`). The invalid index
left behind keeps the name. The build `generate` writes has no `IF NOT EXISTS`,
so `up --allow-dirty` stops at it with `relation "idx_members_email" already
exists` (SQLSTATE `42P07`). Drop the index so the statement can build it, then
run the migration again, or rebuild it with `REINDEX INDEX CONCURRENTLY` and
record the migration with `migrations repair --force`. `up` cannot run past a
rebuilt index, which still holds the name.

A build written with `IF NOT EXISTS` skips the name rather than retrying it and
reports no error. Ptah refuses to run the migration while an index such a
conditional create expects is unusable, and names the `REINDEX INDEX
CONCURRENTLY` that rebuilds it:

```text illustration
error: error running migrations: migration 1785756328 cannot be applied: PostgreSQL reports index "public"."idx_members_email" (indisvalid=false, indisready=false) unusable, and CREATE INDEX ... IF NOT EXISTS finds the name taken and skips it rather than rebuilding it, so this run would record the migration applied over a constraint that is not enforced; run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", or drop the index, then run the migration again
```

`--allow-dirty` does not bypass this — retrying the body is what the refusal is
about. An invalid *unique* index enforces nothing, so without the refusal the
run would exit `0`, report the database up to date, and keep accepting duplicate
rows. Rebuild the index with the `REINDEX` the message names, or drop it so the
name is free and the statement builds it, then run again. Only indexes named by
`CREATE INDEX ... IF NOT EXISTS` are checked, and only on PostgreSQL; ordinary
creates retain PostgreSQL's normal error semantics and may be renamed or removed
by later statements. Other dialects have no concurrent index build to leave
half-finished.

The check opens a database session of its own and closes it when it ends; a
migration with no conditional create and no `DROP INDEX` opens no such
session. A dry run is exempt, because it records nothing. `ptah migrations
repair` refuses on the same grounds — see [Maintain migration history](../maintain-history/).

An intentional `DROP INDEX` followed by a matching create is allowed when the
drop will execute in the current attempt. A statement skipped by dirty-resume
cannot serve as that cleanup. Ptah resolves both unqualified drops and target
tables through `search_path`, then checks the schema-level relation name. An
index on another table or a non-index relation with that name is a conflict.

So is a usable index on the target whose definition differs from the declared
one: Ptah builds the declared index on an empty copy of the table and compares
the two definitions PostgreSQL renders, so the same index spelled another way
still applies. That copy is a temporary table, so the comparison needs the
`TEMPORARY` privilege on the database, and a role without it is refused rather
than let past the check. PostgreSQL renders a storage parameter such as
`fillfactor` whenever one is set, even to its default, so an index created with
one differs from the same index declared without it.

After the body, Ptah positively verifies on the active transaction or connection
that every conditional create's statement-local schema, target table, and index
name still describe a usable result. Equal raw names under different
`search_path` values remain distinct checks.

`ALTER TABLE ... ADD CONSTRAINT name UNIQUE USING INDEX index` and
`PRIMARY KEY USING INDEX index` rename the index to the constraint name.
Ptah follows that name on the same target table when checking the result,
including after a retry. Removing the constraint and its index still fails
the check.

Repair that cannot reconstruct an
explicit original path checks every same-named target in PostgreSQL user
schemas, so the repair session's current path cannot hide another candidate.

A partitioned parent index created with `CREATE INDEX ... ON ONLY` is accepted
in its expected ready-but-incomplete catalog state.

**A pre-migration check blocked the migration.** Nothing is applied and no
revision row is written, so the run is recorded as never started. Fix the data
the check guarded and re-run; no repair step and no bypass flag is involved.
`ptah migrations up --skip-checks` exists as an emergency override, and the
Atlas-compatible `ptah-compat migrate apply` has no such flag, matching Atlas.

## Next steps

- Need to undo an applied migration? [Roll back migrations](../rollback/).
- Hardening the pipeline that runs this command?
  [Integrity and safety](../integrity-and-safety/).
- Fresh databases replaying years of history? [Checkpoints](../checkpoints/).
