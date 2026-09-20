---
title: Verify a release against the database
description: Write release requirements as `-- +ptah check` assertions and evaluate them against a live database with ptah db verify, which reads and changes nothing.
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I confirm a change produced the result it was for, not only that it ran?"
goal: "Write assertions about the data a release must leave behind, and run them against the database as a gate."
sourceOfTruth:
  - "internal/cli/dbverify"
  - "migration/migrator"
generated: false
overlaps:
  - /versioned/apply/
  - /direct/compare-and-drift/
disposition: keep
owns:
  - cli-ptah-db-verify
---

A migration that adds a column and backfills it can succeed statement by
statement, match the declaration, and report no drift, while the `UPDATE`
predicate was wrong and a subset of rows kept a null. `ptah schema drift`
compares structure and finds nothing, because the structure is right.

The requirement that would catch it is about the data:

> no row in the set this release covers is left without the new value

`ptah db verify` evaluates requirements like that against a database that is
already running, and changes nothing in it.

## Write the requirements

A requirement is a `-- +ptah check` directive, the same one a migration carries
as a precondition. Put them in a `.sql` file, or a directory of them:

```sql title="release-checks/010_tier_backfill.sql"
-- +ptah check name="every user has a tier" assert="SELECT COUNT(*) = 0 FROM users WHERE tier IS NULL"
-- +ptah check name="no user lost an email" assert="SELECT COUNT(*) = 0 FROM users WHERE email IS NULL"
```

Each `assert` is one read-only `SELECT` returning a single truthy scalar. That
is the same grammar a pre-migration check uses, so versioned migrations and
direct schema changes express a requirement the same way rather than growing
two spellings for one idea.

## Run them

```bash
ptah db verify --db-url "$DATABASE_URL" --checks release-checks/
```

```text title="stdout"
STATUS    NAME                  SOURCE
verified  every user has a tier  release-checks/010_tier_backfill.sql
failed    no user lost an email  release-checks/010_tier_backfill.sql

failed: no user lost an email
  assert: SELECT COUNT(*) = 0 FROM users WHERE email IS NULL

Verdict: failed (1 verified, 1 failed, 0 errored of 2)
```

A directory contributes its `.sql` files in file-name order, and each file its
directives in the order written, so two runs produce lists you can diff. Files
with any other extension are left alone.

Add `--format json` for a machine-readable report with the same four counts and
one entry per assertion.

## What the four outcomes mean

| Outcome | What it says | Exit code |
| --- | --- | --- |
| `verified` | The assertion ran and held. | `0` when every assertion is verified |
| `failed` | The assertion ran and did not hold. The database answered, and the answer is not the one the author required. | `1` |
| `errored` | The assertion could not run: it is not a well-formed read-only `SELECT`, or the server refused the query. Nothing is established about the requirement. | `2` |
| `not verified` | The run found no assertions at all. | `1` |

`not verified` is deliberately not a pass. A gate that exited `0` over an empty
checks path would let through a release whose requirements someone deleted.

An assertion that could not run outranks one that failed in the verdict line:
a run that never evaluated one of its requirements cannot claim the failures
are the only violations.

## What this verb does not do

It applies no schema, runs no migration, loads no seed data and drops nothing.
That is what separates it from `ptah migrations test` and `ptah schema test`,
whose contract is a throwaway database they may destroy. Point those at a live
database and they would write to it; this verb is meant for the database the
release actually runs on.

Two rules hold it to reading. Every assertion is proved to be a single `SELECT`
from its text before any query is sent, and the session is opened read-only
wherever the engine has such a mode. On PostgreSQL that second rule is what
refuses a `SELECT` that writes by calling a function that writes — a shape no
reading of the statement can catch. On Oracle the driver cannot open a
read-only transaction, so Ptah asks the server for one instead, which refuses a
`SELECT ... FOR UPDATE` that would otherwise hold a row lock every writer waits
on. Where an engine has no read-only mode, the static proof is the whole of the
protection, which is the guarantee a pre-migration check already carries there.

Each assertion is evaluated in its own session, so one the server refuses
cannot decide the outcome of the next.

### What a read-only session cannot reach

A routine that runs in a transaction of its own is not undone with the session
that called it, on any engine, because it was never part of it. Oracle is where
that is reachable: a function declared `PRAGMA AUTONOMOUS_TRANSACTION` may
insert and commit while a `SELECT` calls it, and the row is there afterwards.
An ordinary Oracle function that writes is refused by the server before that
matters — `ORA-14551`, a DML operation inside a query — so the shape that gets
through is one an author wrote deliberately.

So the promise is precise: Ptah sends nothing but a statement it proved is a
read `SELECT`, and opens the strictest session the engine offers. An assertion
that calls a routine to do the reading answers for what that routine does.

## Where this sits beside the other checks

- `ptah schema drift` answers whether the structure matches the declaration.
- `ptah migrations status` answers whether the recorded history matches the
  directory.
- `ptah db verify` answers whether the database holds the result the release
  was for.

The first two are about Ptah's own bookkeeping. This one is about your data,
and only you can write it down.
