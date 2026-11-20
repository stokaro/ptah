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
`schema diff --exclude` takes, and adds to the `exclude` list of `ptah.yaml`.
A selector that matches nothing in the database or in the replayed history is
a warning on standard error, and the report goes on.

```console
ptah migrations drift --db-url "$DATABASE_URL" --migrations-dir ./migrations --shadow-db "$SHADOW_URL" --exclude audit_log
```

## Read the JSON report

`--json` prints one document on standard output. `check` is the result in the
shape the run document of `migrations up --check-drift` carries in
`drift_check`: its `outcome` is `clean`, `drifted` or `unable`, `replayed`
lists the versions the check replayed, and `findings` lists the differences.
`current_version` and `pending` say where the database stands. `diff` is the
change that brings the database back, and `statements` are the statements that
make it; both are present only on drift. `plan_error` replaces `statements`
when the change could not be planned, and the drift still stands. The `diff`
below is shortened to the part that changed:

```json
{
  "contract_version": 1,
  "command": "migrations drift",
  "dialect": "postgres",
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

## When the report cannot decide

A comparison that cannot decide is never reported as a match. The report names
the `stage` and `reason` it stopped at and exits with status 2, when:

- the history cannot be replayed: an applied revision whose file is gone, a
  revision that is not cleanly applied, or a history that applied a lower
  version after a higher one;
- the shadow database cannot be used: it is the target under another name, it
  is not empty, or the replay of the applied migrations fails on it;
- the database read cannot describe an object the applied migrations create,
  so whether the database holds it is unknown.

## The migration lock

The report holds the migration lock while it reads the history and the
database, so a cooperating runner cannot move the history in between. A runner
that holds the lock makes the report wait. `--migration-lock-timeout` bounds
the wait, and `--migration-lock-name` selects the lock, as on
`ptah migrations up`. DDL from a session that does not take the lock can still
land after the report. The report writes nothing to the database: on a
database no migration ran on, it creates no revision table, and every migration
is pending.

## What it does not cover

`ptah schema drift` answers a different question: whether a database matches a
declared desired schema. `ptah migrations drift` asks whether it matches the
history it claims to have run.

The report runs against one database. A multi-target environment, and a
`migrate drift` spelling in `ptah-compat`, are not covered yet
(stokaro/ptah#6326).
