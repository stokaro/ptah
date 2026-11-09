---
title: The migration log
description: "What `ptah migrations log` records, what it cannot be used for, and why a rollback no longer erases the fact that a version was applied."
type: reference
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I find out what happened to this database, beyond where it stands now?"
goal: "Read the recorded history of migration operations and know its limits."
sourceOfTruth:
  - "migration/migrator"
generated: false
overlaps: []
disposition: keep
---

The revision table answers what is applied. It cannot answer what happened: a
completed rollback deletes the row, so a database that was on version 43
yesterday and is on 42 today reads exactly like one that never reached 43.

The migration log answers the second question. It is an append-only table
beside the revision table, written by `ptah migrations up` and `ptah migrations
down`, and read by `ptah migrations log`.

```bash
ptah migrations log --db-url "$DATABASE_URL"
```

```text title="stdout"
STARTED               VERSION  OPERATION  OUTCOME       ACTOR         SOURCE
2026-09-21T06:11:04Z  43       down       rolled_back   release-bot   provided
2026-09-21T05:58:12Z  43       up         applied       release-bot   provided
2026-09-21T05:57:40Z  43       up         failed        release-bot   provided
```

## What a row means

Each attempt writes two entries: one when it starts and one when it ends. The
reader pairs them, so an attempt that never gained an outcome is reported as
`undetermined` — a run whose process was killed, lost its connection or had its
container stopped says nothing about whether its work landed, and inferring
success from a missing failure would be a claim nobody can support.

A write to the log can fail on its own, so an attempt can keep its outcome and
lose its start. The reader keeps that attempt, because the outcome is the only
record it has. The `STARTED` column prints `-`, and `--json` sets `start_lost` and
leaves out `started_at`. The other fields come from the outcome, which names the
same run, version, actor and checksum.

The entries are written on the pool connection rather than inside the
migration's transaction, which is the point: a migration that failed and rolled
back still leaves the record that it was attempted. A log written inside the
transaction would describe only the runs that succeeded.

`--json` prints the same attempts with the start and finish timestamps, the run
identifier, the checksum of the migration that ran, and the error. Start and
finish timestamps are recorded in UTC, including when the applying process
uses another time zone.

A MySQL or MariaDB URL does not need `parseTime=true`. Without it the driver hands
back a time as the server's text, which carries no zone, and Ptah reads that text
the way the driver does with the parameter: in the zone the URL's `loc` parameter
names, or UTC without one. `ptah migrations log`, `ptah migrations status` and
`ptah migrations tag` print the same values with or without the parameter. Measured
on MySQL 8.4 and 26.10 and on MariaDB 10.11 and 12.3.

Under `tx-mode all` the whole batch is one transaction, so every migration in it
gets an entry opened before the transaction starts and they all settle with the
same outcome. Recording one of them applied when a later one rolls the batch
back would describe a database that never existed.

`ptah migrations down --plan` derives its rollback from the schema difference
instead of running the authored down bodies. It deletes the same revision rows,
so it records the same attempts.

## The actor, and what the name is worth

`--actor` names the run. It is unverified by construction — a name on a command
line is what somebody wrote — so the log stores where the name came from beside
it:

| Source | Meaning |
| --- | --- |
| `provided` | the caller supplied the name, through `--actor` or `PTAH_ACTOR` |
| `process-user` | nobody supplied one, so the user the process runs as was recorded |
| `unknown` | neither was available |

Storing an unverified name and an observed one the same way is how a log
becomes evidence for something it cannot support.

## When an entry cannot be written

A write to the log can fail: the server refuses the value, the account may not
create the table, or the connection is gone. The migration goes on, because
stopping a migration over its note would turn an investigation aid into an
outage. Nothing in the table can record that it was not written to, so the run
reports it:

- `ptah migrations up --json` and `ptah migrations down --json` name each lost
  entry under `migration_log` in
  [the run document](../apply/#the-evidence-a-run-leaves), against the migration
  it belongs to. A table with no rows then has two different readings, and the
  document says which one applies.
- Without `--json`, both commands close with a notice that names the
  migrations, after the line that says the run succeeded.
- A program using the library asks `Migrator.MigrationLogReport`.

The exit status does not change. A lost entry says nothing about the migration:
the revision table says what is applied, and a run that exits `0` applied what it
selected. A nonzero status would send a caller to retry, the retry would find
nothing left to do, and the entry would stay lost. A caller that treats a lost
record as a problem reads `migration_log.lost`.

An entry is sent as the run holds it. Ptah does not shorten an actor to fit the
256 characters of its column, or replace a character the database's charset cannot
hold, because a record that names somebody else is worse than no record. The
server refuses such an entry, and it is lost and reported like any other. The
measured answers are `22001` on PostgreSQL for a name that is too long, `1406` on
MySQL and MariaDB, `3988` on MySQL and `1366` on MariaDB for a character the
charset cannot hold.

A MySQL or MariaDB session whose `sql_mode` is not strict is the exception, and
Ptah cannot see it. The server stores the name shortened to 256 characters or with
`?` in place of the character, and answers success, so the entry counts as written
and the record is wrong. Measured with `sql_mode=''` on MySQL 8.4 for a long name
and on MariaDB 12.3 for a character the charset cannot hold. Keep the mode strict,
which is the default on both, on a database whose log you rely on.

## What this is not

**It is not an audit trail.** The table lives in the database it describes and
is writable by the account that runs migrations: whoever can apply a migration
can edit the record of having done so. Tamper evidence is a different design.

**It is not a second source of truth.** `ptah migrations status` remains the
answer to where the database stands, and nothing reads the log to decide the
current version. A reader that had to reconcile two tables to learn that would
be worse off than with no log at all.

**It keeps everything.** Rows are small and nothing prunes them. A deployment
that needs a retention rule applies its own `DELETE`.

**A destructive clean destroys it.** `ptah db drop-all` names the log table in
the plan it asks you to confirm, beside the revision table, and drops it with
the rest.

## Turning it off

`migration.log: false` in the project config keeps the table out of a database
entirely: nothing creates it, and `ptah migrations log` says logging is off for
the run rather than printing an empty table. The Atlas-compatible revision
format keeps no log whatever the setting says: that contract defines one table,
and a second one Ptah added would appear in a database an Atlas user believes
only Atlas writes. The two are different settings, so the command names the one
that applies.

The table is created by the first entry written, not by the initialization that
creates the revision table. An account that may read and not create can run
`ptah migrations log` against a database that has no log yet.
