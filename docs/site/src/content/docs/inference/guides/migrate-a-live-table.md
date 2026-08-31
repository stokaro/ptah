---
title: Migrate a live table
description: Run a generation change against a table your application is inserting into, updating, and deleting from throughout.
type: how-to
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I migrate a table while my application continues writing to it?"
goal: "Migrate a table while my application continues writing to it."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

The table keeps changing while the backfill runs. This page is about not losing
those changes.

## The mechanism

`prepare` does two things that matter here. It installs a companion table and two
triggers on your source, and it records a **boundary** — the point in the
database's transaction ordering that the backfill will embed the source as of.

From then on, every committed change to a source row writes a row into the
companion table **in the same transaction as the change**. That is the guarantee:
a change that committed has an event, because one transaction committed both.

```yaml
consistency:
  mode: outbox
source:
  mutable: true
```

Both lines are required. `mutable: true` says the source changes; `mode: outbox`
says how that is accounted for.

## Run it

```bash
ptah inference prepare  --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id "$RUN"
```

The backfill embeds the source as it was at the boundary. Rows inserted after it
are not in that set, and rows updated after it have vectors computed from the old
text. Both are the catch-up's work.

```bash
ptah inference catchup --spec spec.yaml --db-url "$DB" --run-id "$RUN"
```

Run it until it reports nothing left:

```text
caught up to transaction 8842: 0 changed rows, 0 tombstoned
```

The numbers are **this pass's** work, not the run's, which is what makes the
line a stop condition at all: a count that included the backfill could never
reach zero.

On a busy table this takes several passes, because rows keep changing while
catch-up is running. That is expected. Each pass has less to do than the last as
long as your write rate is below the rate Ptah can embed.

## What happens to deletions

A row deleted from the source becomes a **tombstone** rather than a deleted
target row. The tombstone is what stops a late-arriving embedding — one already
in flight when the delete happened — from recreating a row your source no longer
has.

Tombstones are counted separately in the catch-up output, and coverage
verification treats them as accounted for.

## What happens to a row you skip

`preprocessing.empty_policy: skip` means a row whose model input comes out empty
is recorded as a deliberate skip rather than embedded. Coverage counts it as
accounted for; a rollback to the generation does not treat it as a gap.

A row that *nothing* ever embedded is a real gap and is reported as one. The two
are one finding apart in the same layer, and Ptah keeps them distinct.

## Verify with the writes still running

```bash
ptah inference verify --spec spec.yaml --db-url "$DB" --run-id "$RUN"
```

Verification measures a moving target, and the consistency layer is where that
shows up: it refuses if the backfill has not finished or catch-up has not reached
the barrier. A freshness finding on a busy table is often the last few
seconds of writes — run `catchup` again and re-verify.

## Cut over

The cutover checks the same things again at the moment it runs, so a table that
moved between your verification and your approval is caught there rather than
after.

```bash
ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id "$RUN" \
  --approve <digest> --approver "your name" --stabilize-for 24h
```

## Keep catching up afterwards

The generation you switched *away* from stops receiving changes the moment your
queries stop reading it. If you want a rollback to be possible, it has to keep
being caught up:

```bash
ptah inference catchup --spec previous-spec.yaml --db-url "$DB" \
  --run-id previous-run --maintain-for 1h
```

Put that on a schedule for the length of the window. Without it, the window
elapses over a generation that drifted, and `rollback` refuses it.

## When the triggers go away

`retire` removes the generation and its bookkeeping. Until then the outbox table
and its triggers stay on your source table — that is the cost of the guarantee,
and it is worth knowing it is not free.

The outbox belongs to the source **table**, not to one generation, so retiring a
generation while another still reads that table leaves it in place. `retire`
says which it did:

```text
generation 3e0df4e18980 is gone, with 3 vectors
  - the outbox is gone: its triggers, capture function and event table were the
    last thing Ptah had on articles
```

or, where something still needs it:

```text
  - the outbox stays: 1 other generation(s) still read articles
```
