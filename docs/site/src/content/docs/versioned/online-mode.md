---
title: The online mode
description: "What `online: require` proves about a migration, what it refuses, and what no reading of the SQL can answer."
type: reference
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "What does `online: require` prove about a migration, and what does it leave to me?"
goal: "Read the online mode's guarantee and its limits before relying on it."
sourceOfTruth:
  - "migration/lint"
generated: false
overlaps: []
disposition: keep
---

`online: require` in `.ptah-lint.yaml` selects a mode that refuses any
migration it cannot prove runs without blocking reads and writes. It is the
inversion of every other rule family here: those report hazards somebody wrote
a rule for, and this one reports everything it has not proven, including every
statement it does not recognize.

It does not guarantee zero downtime, and must not be read as doing so.

The mode is refused on an engine it has no measurement for, rather than
reporting a clean directory nothing proved. Where the run names a dialect the
refusal comes from the configuration; where only a dev URL names one, it comes
when the connection says which product answered, because a `postgres://` URL
reaches CockroachDB, YugabyteDB and Spanner too.

## What it proves

`online: require` states one property about the SQL, and it is worth quoting
exactly because a mode that implies more is worse than no mode:

> Every statement in this migration is, on this engine at this version, a
> catalog-only change or an operation whose lock does not conflict with reads
> and writes for longer than it takes to acquire.

Two rules carry it, each proving a different thing.

- **`ON101`, PostgreSQL**: the statement is in the set measured to take no
  conflicting lock. A concurrent index build or drop, a plain column addition,
  a column or constraint drop, `SET DEFAULT`, `DROP NOT NULL`, a constraint
  added `NOT VALID`, the `VALIDATE CONSTRAINT` that completes it, and a
  statement that locks nothing that already exists.
- **`ON102`, MySQL and MariaDB**: the statement asked the server to refuse what
  it cannot do online, by carrying `ALGORITHM=INPLACE, LOCK=NONE` or
  `ALGORITHM=INSTANT`. That is a stronger answer than a list, because the
  server decides rather than a model of it.

The property says nothing about scans under a lock that blocks nobody.
PostgreSQL's `VALIDATE CONSTRAINT` reads the whole table under `SHARE UPDATE
EXCLUSIVE`, which blocks neither readers nor writers, and it is the second half
of the form `diff.online_alter` generates to make a constraint addition online
at all. What that scan costs depends on how many rows there are, which is the
first of the things below that nothing here can see.

## What it does not prove

- **Table size.** A dev database has no rows. A scan that is free on ten
  thousand rows is an outage on a billion.
- **Concurrent load.** Whether blocked writes matter depends on traffic no tool
  observes.
- **Replication.** An online change on the primary can still stall a replica.
- **Application rollout.** `DROP COLUMN` is cheap and breaks deployed code,
  which is expand and contract across two deploys rather than a property of the
  DDL. `BC103` and `BC104` name the retired identifier; they do not sequence a
  rollout.

## The lock timeout the mode requires

On PostgreSQL the mode refuses to run without one, and this is the requirement
no reading of the SQL can carry. An `ADD COLUMN` that edits only the catalog
still takes `ACCESS EXCLUSIVE` for an instant, and if anything holds a
conflicting lock the `ALTER` waits — with every later reader and writer of that
table waiting behind it, because the lock queue is first in, first out. A
statement the mode calls online can therefore take an application down for as
long as somebody else's `SELECT` runs. Set `--lock-timeout`, or
`migration.lock_timeout` in the project config, and a wait becomes a failed
statement somebody can retry.

The requirement lifts where nothing can take a timeout: a run under
`--tx-mode none`, or a directory whose every migration opted out of the
transaction, because the migrator refuses a timeout on those outright and
requiring one would leave the configuration with no successful invocation.
