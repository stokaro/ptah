---
title: The lifecycle
description: What each phase of a generation change does, why they are separate commands, and how a run records where it got to.
---

A generation change is a sequence of steps. Each is a separate command, because
each is a decision somebody takes separately.

## The phases

| Phase | Command | What it does |
| --- | --- | --- |
| boundary captured | `prepare` | Creates the target column and its bookkeeping, installs the change-capture mechanism, and records where the source was |
| backfilling | `backfill` | Embeds the source as it was at that point |
| caught up | `catchup` | Embeds what changed since |
| indexed | `index` | Builds the vector index |
| verified | `verify` | Runs the deterministic checks |
| cut over | `cutover` | Makes the new generation the one queries read |
| rolled back / retired | `rollback`, `retire` | Go back, or destroy |

`ptah inference status` reports the furthest phase a run reached.

## Why they are separate

Because none of them implies another, and treating them as one command would
hide the places where you are meant to look.

- A **finished backfill** means every source row that existed at the boundary
  has a vector. It says nothing about whether those vectors are any good.
- A **passing verification** means the deterministic checks found nothing. It
  does not move a single query.
- A **completed cutover** means your queries read the new generation. It does
  not make the old one disposable.

The expensive step is the backfill, and it is the one you may want to run
overnight. The irreversible step is `retire`, and it is the one you want to run
much later, if at all.

## The boundary, and why catch-up exists

Your application keeps writing while the backfill runs. A row inserted after the
backfill read past it would have no vector; a row updated after the backfill
embedded it would have a stale one.

So `prepare` records a **boundary** — a point in the database's own transaction
ordering — and `backfill` embeds the source as it was at that point. Everything
that happened after is `catchup`'s work.

Run `catchup` until it reports nothing left. A cutover is refused while the
backfill is unfinished or the change log has a backlog, which is what stops a
half-built corpus from becoming the one your users search.

How changes are captured, and what each mode can promise, is
[Consistency](../consistency/).

## The phase is a high-water mark

A run's phase records the furthest point it reached, not where it is now.
Running `catchup` again after a verification is ordinary — the source keeps
moving — and it leaves the phase at `verified` rather than dragging it back to
`caught_up`.

A phase is only ever reached one step at a time, so nothing arrives at a cutover
without having passed through verification.

## Resuming

`backfill` and `catchup` are resumable. An interrupted run continues from its
checkpoint when you run the same command again, because the checkpoint and the
vectors it describes are written in one transaction — there is no state in which
the vectors landed and the record of them did not.

`prepare` is idempotent too, so a run that was interrupted before it finished
preparing is repaired by running it again.

## What a failure leaves behind

A run that stops leaves everything it committed and nothing it did not. The
provider being unreachable, the process being killed, the lease being taken by
another worker — each stops the run and none of them corrupts the generation
being built. [Resume and recover](../../guides/resume-and-recover/) is the
practical page.
