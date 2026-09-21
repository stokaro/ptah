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

The entries are written on the pool connection rather than inside the
migration's transaction, which is the point: a migration that failed and rolled
back still leaves the record that it was attempted. A log written inside the
transaction would describe only the runs that succeeded.

`--json` prints the same attempts with the start and finish timestamps, the run
identifier, the checksum of the migration that ran, and the error.

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

## Turning it off

`migration.log: false` in the project config keeps the table out of a database
entirely. The Atlas-compatible revision format keeps no log whatever the
setting says: that contract defines one table, and a second one Ptah added
would appear in a database an Atlas user believes only Atlas writes.
