---
title: The lifecycle
description: What each phase of a generation change does, why they are separate commands, and how a run records where it got to.
type: concept
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How does Ptah model the lifecycle?"
goal: "Explain Ptah's model for the lifecycle."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

A generation change is a sequence of steps. Each is a separate command, because
each is a decision somebody takes separately.

## The phases

| Phase | Command | What it does |
| --- | --- | --- |
| boundary captured | `prepare` | Creates the target column and its bookkeeping, installs the change-capture mechanism, and records where the source was |
| backfilling / backfilled | `backfill` | Embeds the source as it was at that point, and records that the walk reached the end |
| caught up | `catchup` | Embeds what changed since |
| indexed | `index` | Builds the vector index |
| verified | `verify` | Runs the deterministic checks |
| cut over | `cutover` | Makes the new generation the one queries read |
| rolled back | `rollback` | Returns queries to an earlier generation. Reversible: cutting over again returns the run to cut over |
| retired | `retire` | Destroys the generation and makes every run for it complete. Runs already at cut over or rolled back advance to retired; earlier attempts keep their truthful high-water phase |

`ptah inference status` reports the furthest phase a run reached. `abandoned` is
a terminal run status rather than a phase: it ends one attempt without
destroying the generation or its vectors.

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
