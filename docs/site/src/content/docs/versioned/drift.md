---
title: Report drift from the applied history
description: "Use `ptah migrations drift` to check whether a database still holds the schema its applied migrations produce, without applying anything."
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "Has anyone changed this database outside its migrations?"
goal: "Compare a database with its applied migration history and read the differences, the diff and the statements that would undo them."
sourceOfTruth:
  - "internal/cli/migrationsdrift"
  - "internal/driftgate"
  - "internal/cli/atlas"
generated: false
searchAliases:
  - "migrate drift"
  - "migration drift"
overlaps: []
disposition: keep
owns:
  - cli-ptah-migrations-drift
---

Checksums prove that the migration files are the files the database ran. They
cannot see a column added by hand or an index dropped during an incident.
`ptah migrations drift` compares the schema itself, and applies nothing.

The command reads the revision table under the migration lock, replays the
migrations it records as applied on a disposable shadow database, and compares
the database with the schema the replay produced. The baseline is the applied
history, not the directory, so a pending migration is listed and never read as
drift. It is the comparison
[`ptah migrations up --check-drift`](../apply/#check-the-schema-against-its-history)
makes before it applies, reported on its own.

## Run the report

Give the database, the migration directory and a shadow database. The shadow
database must be empty and must not be the target under another name; the
report empties it again when it ends. A `docker://` URL starts a disposable
container for the run, and a project's `dev_url` is the default.

```console
ptah migrations drift --db-url "$DATABASE_URL" --migrations-dir ./migrations --shadow-db "$SHADOW_URL"
```

A database that matches its history prints where it stands and exits with
status 0:

```text
Dialect: postgres
Revision table: "schema_migrations"
Current version: 1
Pending migrations: 1
  - 2

No drift: the database matches the schema its 1 applied migration produces.
```

A database that differs lists each difference by object, never by row, prints
the statements that would bring it back to its history, and exits with status
1. Here a column and a table were added by hand:

```text
Dialect: postgres
Revision table: "schema_migrations"
Current version: 1
Pending migrations: 1
  - 2

Drift: the database differs from the schema its 1 applied migration produces:
  - extra table audit_log
  - extra column users.nickname

Statements that would bring the database back to its history (review them; nothing ran):
ALTER TABLE "users" DROP COLUMN "nickname";

DROP TABLE "audit_log";
```

Nothing runs those statements. Read them before you run any: they drop what no
applied migration created, and the data it holds goes with it. When the change
is one to keep, record it in a new migration instead.

## Leave objects out

`--exclude`, which can be repeated, leaves out objects the migrations do not
manage, such as a table another tool owns. It takes the selectors
`schema diff --exclude` takes, and adds to the project's list: the `exclude`
list of `ptah.yaml`, or of the `atlas.hcl` env. An `atlas.hcl`
`check "migrate_apply"` block whose `drift` block sets its own `exclude` list
replaces the env's list, for this report as for `ptah migrations up`, so the
report and the check before an apply leave out the same objects. A selector
that matches nothing in the database or in the replayed history is a warning
on standard error, and the report goes on.

```console
ptah migrations drift --db-url "$DATABASE_URL" --migrations-dir ./migrations --shadow-db "$SHADOW_URL" --exclude audit_log
```

## Read the JSON report

`--json` prints one document on standard output. Its fields:

| Field | What it holds |
| --- | --- |
| `check` | The result, in the shape the run document of `migrations up --check-drift` carries in `drift_check`: `outcome` is `clean`, `drifted` or `unable`, `replayed` lists the versions replayed, and `findings` lists the differences. |
| `revision_table` | The table the history was read from, quoted and qualified as the engine names it. |
| `current_version`, `pending` | Where the database stands. |
| `diff`, `statements` | The change that brings the database back, and the statements that make it. Present only on drift. |
| `plan_error` | Replaces `statements` when the change could not be planned; the drift still stands. |
| `undecided` | The objects the database read could not describe. Present only when there are some. |
| `warnings` | What to know before acting on the report; see [below](#a-history-the-report-cannot-find). Present only when there are some. |

A change the plan leaves out is a warning on standard error, as
`ptah schema compare` reports it. The `diff` below is shortened to the part
that changed:

```json
{
  "contract_version": 1,
  "command": "migrations drift",
  "dialect": "postgres",
  "revision_table": "\"schema_migrations\"",
  "current_version": "1",
  "pending": ["2"],
  "check": {
    "outcome": "drifted",
    "replayed": [1],
    "replayed_keys": ["1"],
    "findings": [
      {
        "kind": "extra_column",
        "object": "users.nickname",
        "table": "users",
        "column": "nickname",
        "message": "extra column users.nickname"
      }
    ]
  },
  "diff": {
    "tables_modified": [{ "table_name": "users", "columns_removed": ["nickname"] }]
  },
  "statements": ["ALTER TABLE \"users\" DROP COLUMN \"nickname\""]
}
```

A run that could not start prints the failure document instead, with an
`error` object and no `check`; see
[Why a command failed](../../reference/exit-codes/#why-a-command-failed).

## How each applied migration replays

The revision table does not record the transaction mode a migration ran in, so
the replay decides it for each file and takes no mode from the run.
`ptah migrations up --tx-mode`, `PTAH_TX_MODE` and the project's
`migration.tx_mode` name the mode pending migrations run in, and the replay
reads none of them.

- A file whose `-- atlas:txmode` or `-- +ptah no_transaction` directive names
  a mode replays in that mode.
- A file that a transaction refuses replays without one. It ran, so only
  `--tx-mode none` applied it.
- Every other file replays in a transaction of its own.

A transaction refuses a file in one of these ways:

- Before the file runs, Ptah finds a statement that cannot run in a
  transaction, such as PostgreSQL `CREATE INDEX CONCURRENTLY` or SQLite
  `VACUUM`, or on MySQL and MariaDB a statement Ptah cannot verify inside one.
- Before the file's first statement, on MySQL and MariaDB, Ptah finds a table
  it cannot verify a transaction over, such as a `MyISAM` table an earlier
  migration created.
- PostgreSQL refuses a statement for running in a transaction, with SQLSTATE
  `25001`, `2D000` or `55P04`: a `CALL` of a procedure that runs `COMMIT`, or
  an enum value used in the transaction that added it. The transaction rolls
  back, and the replay runs the file again without one. Sequence values and
  session state a rollback does not undo, such as a prepared statement or an
  advisory lock, stay, so a file that depends on them may not replay: a row
  the first attempt inserted takes a new `serial` value the second time, and a
  later foreign key to the first value fails. A pending file that PostgreSQL
  refuses this way stays refused.

CockroachDB and YugabyteDB do not roll schema changes back with a transaction,
so the replay does not run a refused file a second time, and the report fails
with the server's refusal. Such a file replays only if it carried
`-- atlas:txmode none` when it was applied.

A file that a transaction accepts but changes is the other class, and the
replay cannot reproduce it from the file. Applied with `--tx-mode none`, it
behaved as it did only because no transaction surrounded it, and it replays in
a transaction. PostgreSQL ignores `SET LOCAL` outside a transaction, so
`SET LOCAL search_path TO s2; CREATE TABLE t2 (id int);` creates `public.t2`
under `--tx-mode none` and `s2.t2` in a transaction, and the report finds an
extra table `t2` that is not drift.

Give such a file `-- atlas:txmode none` before it is applied, and the apply and
the replay both run it without a transaction. A file that is already applied
cannot take the directive: the edit changes its checksum, and the report and
`ptah migrations up --check-drift` refuse the history. Leave the object it
created out of the comparison instead:

```console
ptah migrations drift --db-url "$DATABASE_URL" --migrations-dir ./migrations --shadow-db "$SHADOW_URL" --exclude public.t2
```

`ptah migrations up --check-drift` takes the same selector as
`--drift-exclude public.t2`. A run with the selector does not compare the
table at all, so it does not report a later change made to `public.t2` by hand
either. A file applied with `--tx-mode all` shared one transaction with the
rest of its run, and replays in a transaction of its own.

## Report every target of an env

An `atlas.hcl` env with `for_each` names several databases. Select it with
`--env`, and the report runs over each target in the order the env expands,
one after the other, each under its own migration lock:

```hcl
env {
  for_each = ["postgres://db1.example/app", "postgres://db2.example/app"]
  name     = atlas.env
  url      = each.value
  dev      = "docker://postgres/17/dev"
}
```

```console
ptah migrations drift --config atlas.hcl --env tenants --migrations-dir ./migrations
```

Each target is the database its instance's `url` names. `--db-url` and
`PTAH_DB_URL` would replace every one of those URLs with one database, so the
run refuses either one over such an env, before it opens anything. Leave both
unset to report each target. An env without `for_each` takes `--db-url` over
its `url`.

A target that drifted, one whose comparison could not decide, and one the run
could not open are each reported, and the run goes on to the next. The text
report names each target before its report, with the password and the secret
parameters of its URL hidden, prints `Error:` and the reason for a target that
made no report, and closes with a count:

```text
Instance 1 of env "tenants": postgres://db1.example/app
Dialect: postgres
...
No drift: the database matches the schema its 3 applied migrations produce.

Instance 2 of env "tenants": postgres://db2.example/app
Error: error connecting to database: failed to ping database: ...

Summary: 2 targets: 1 without drift, 0 with drift, 1 not compared.
```

The run exits with the most serious status of any target: 2 when a target
could not be compared, otherwise 1 when a target drifted, otherwise 0.

With `--json` the run prints one document with the `env` it selected and a
`targets` list. Each entry holds:

- `instance`: the target's position, from 1, in the `for_each` expansion;
- `url`: the target database, with its password and secret parameters hidden;
- `exit_code`: the status a run over that target alone exits with;
- `report`: the document that run prints, or `error`, the reason it made none.

The run of the text example above prints:

```json
{
  "contract_version": 1,
  "command": "migrations drift",
  "env": "tenants",
  "targets": [
    {
      "instance": 1,
      "url": "postgres://db1.example/app",
      "exit_code": 0,
      "report": {
        "contract_version": 1,
        "command": "migrations drift",
        "dialect": "postgres",
        "revision_table": "\"schema_migrations\"",
        "current_version": "3",
        "check": { "outcome": "clean", "replayed": [1, 2, 3], "replayed_keys": ["1", "2", "3"] }
      }
    },
    {
      "instance": 2,
      "url": "postgres://db2.example/app",
      "exit_code": 2,
      "error": {
        "kind": "database_unreachable",
        "message": "error connecting to database: failed to ping database: ..."
      }
    }
  ]
}
```

`contract_version` 1 covers the report over one database, which carries
`check`; the report over the targets of an env, which carries `targets` and no
`check`; and the failure document, which carries `error` and no `check`.

A `for_each` env whose `name` selects one instance prints this shape too, so a
script reads the same document however many targets the env holds.

## When the report cannot decide

A comparison that cannot decide is never reported as a match. The report names
the `stage` and `reason` it stopped at and exits with status 2, when:

- the history cannot be replayed: an applied revision whose file is gone, an
  applied file that changed after it ran, a revision that is not cleanly
  applied, or a history that applied a lower version after a higher one. A
  changed file is refused whatever the edit, a comment included, as
  `ptah migrations up` refuses it, because the directory no longer holds what
  the database ran. A history out of order is refused even where
  `ptah migrations up` accepts it, because the report replays versions in
  order and that order is not the one the database ran;
- the shadow database cannot be used: it is the target under another name, it
  is not empty, or the replay of the applied migrations fails on it;
- the database read cannot describe an object the applied migrations create,
  so whether the database holds it is unknown.

Drift found beside an object the read could not describe is still drift, and
the report exits with status 1. It lists the object it could not decide on,
and the statements do not cover that object.

## A history the report cannot find

A report that reads the wrong revision table finds no applied migration. Every
object in the database then reads as drift, and the statements drop all of
them, the revision table that does hold the history included. The report
still exits with status 1, and when no applied migration is recorded and the
database holds objects it says so first, in the text report and in
`warnings`.

The likeliest cause is a history recorded in the other revision format: one
written with `--revision-format atlas` and read with the default `ptah`, or the
reverse. The report then reads the other format's revision table where that
format keeps it, and when the table records applied migrations the warning
names it and the `--revision-format` value that reads it:

```text
Warning: the revision table "schema_migrations" records no applied migration, or does not exist, but the atlas-format revision table "atlas_schema_revisions"."atlas_schema_revisions" records 1 applied migration: run the report again with --revision-format atlas; the statements below would drop every object the report compares.
```

Otherwise the warning names the flags to check: `--migrations-table` and
`--migrations-schema` for where the history is, and `--revision-format` for
its format.

## The migration lock

The report holds the migration lock while it reads the history and the
database, so a cooperating runner cannot move the history in between. A runner
that holds the lock makes the report wait. `--migration-lock-timeout` bounds
the wait, and `--migration-lock-name` selects the lock, as on
`ptah migrations up`. DDL from a session that does not take the lock can still
land after the report. The revision state is only read: on a database no
migration ran on, the report creates no revision table, and every migration is
pending.

On Oracle the migration lock is a row in the table `ptah_locks`, beside the
revision table, and taking it writes. The first lock in a schema creates the
table, which needs `CREATE TABLE` (`CREATE ANY TABLE` in a schema another
account owns), and a new lock name adds its row. Where the account may not
create it, a DBA creates the table and grants `SELECT` and `INSERT`; see
[the migration lock on Oracle](../../databases/oracle/#the-migration-lock).

## From `ptah-compat`

`ptah-compat migrate drift` runs this report under the Atlas flag names. It is
off by default: until
[`PTAH_ATLAS_MIGRATE_DRIFT=1`](../../reference/extension-variables/#ptah_atlas_migrate_drift)
is set, it answers as the community CLI does, with the `migrate` group help,
and a flag after it is refused as unknown. Strict mode refuses the variable.

```console
PTAH_ATLAS_MIGRATE_DRIFT=1 ptah-compat migrate drift --env tenants
```

The report reads the Atlas revision table, `atlas_schema_revisions`, and a
directory in the Atlas layout, as `ptah-compat migrate apply` writes them.
`--dir` is read relative to the directory that holds `atlas.hcl`, and a
`data.template_dir` directory is read as rendered. `-u`/`--url` is the
database and `--dev-url` the dev database the history replays on. `--exclude`
leaves objects out as [above](#leave-objects-out). `--no-cache` is accepted and
changes nothing, because Ptah keeps no cache of the expected state. `--format`
is refused: the output is the text report on this page.

`--lock-timeout` and `--lock-name` bound and name the migration lock: the
report waits for a `migrate apply` that holds the lock of that name. A blank
`--lock-name` or `PTAH_LOCK_NAME` is refused, as `migrate apply` refuses it. On
an engine with no advisory lock, such as SQLite, the report takes no lock under
that name and prints a note on standard error that says so, as `migrate apply`
does; the native `--migration-lock-name` is refused there. `--skip-lock` is
refused, because the history is read under the migration lock.
`--skip-lock=false` and `PTAH_SKIP_LOCK=false` ask for that lock, and are
accepted.

In `atlas.hcl`, `format { migrate { drift = ... } }` and
`migration { skip_lock = true }` are refused for the same reasons.
`skip_lock = false` asks for the lock the report takes, and is accepted. A
`skip_lock` the targets of an env disagree on is refused.

`migration { exclude = ... }` and `migration { lock_name = ... }` are refused
too, because the report does not read them yet: pass the patterns with
`--exclude` and the name with `--lock-name`. The flag takes precedence over the
setting, as an Atlas flag overrides an env attribute, so with the flag given,
or its variable `PTAH_EXCLUDE` or `PTAH_LOCK_NAME`, the setting is not refused
and the flag's value is the one the report uses. A project file that
`migrate apply` also reads can keep `lock_name` that way.

An env with `for_each` is reported target by target, as in
[Report every target of an env](#report-every-target-of-an-env), and each target
takes its own `url`, `dev` and migration `dir`. `--dir` names one directory for
every target. This surface forwards the `migration` block's `format`,
`revisions_schema` and `lock_timeout` once for the run, so they must be the
same on every target or be given as a flag. `--url` or `PTAH_URL` over such an
env is refused, because it would point every target at one database; leave both
unset to report each target. The variables of the native flags, such as
`PTAH_DB_URL`, `PTAH_SHADOW_DB` and `PTAH_JSON`, are not read here. The run
exits 1 when any target drifted or could not be compared, the status the
community CLI gives every failure, and 0 otherwise.

## What it does not cover

`ptah schema drift` answers a different question: whether a database matches a
declared desired schema. `ptah migrations drift` asks whether it matches the
history it claims to have run.
