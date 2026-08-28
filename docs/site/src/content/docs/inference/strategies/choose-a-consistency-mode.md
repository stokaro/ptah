---
title: Choose a consistency mode
description: The decision, its cost, and what each answer leaves you unable to prove.
---

Four answers, and the right one depends on whether you control the writes.

## The decision

**Can you stop writes to the source for the length of the backfill?**

- Yes → `immutable`. Nothing to capture, nothing to catch up, no triggers.
- No → **Do you own every code path that writes to the table?**
  - No → `outbox`.
  - Yes, and trigger overhead is unacceptable → `dual_write`.
  - Otherwise → `outbox`.

`outbox` is the answer unless you have a specific reason against it.

## What each costs

| Mode | Write-time cost | Setup | Cleanup |
| --- | --- | --- | --- |
| `outbox` | Two triggers and one insert per changed row | `prepare` installs them | `retire` removes them |
| `immutable` | None | None | None |
| `dual_write` | Your writer's reporting call | Your application code | Your application code |
| *(none)* | None | None | None — and no cutover |

The outbox cost is a row written into a companion table inside the same
transaction as the change. On a table taking thousands of writes per second that
is measurable; on most tables it is not. Measure it rather than assuming, and
remember it lasts only until the migration ends.

## What each cannot prove

This is the part to read before choosing something other than `outbox`.

**`outbox` proves** that a change which committed has an event. It cannot prove
anything about changes made by a path that bypasses the table's triggers —
`COPY` with triggers disabled, a replication apply, a direct catalog edit.

**`immutable` proves nothing.** It is a declaration. Verification's freshness
layer catches a broken promise after the fact, which is better than nothing and
is not the same as prevention.

**`dual_write` proves what your writer reported.** A change your writer forgot to
report is invisible: the row keeps its old vector, and nothing flags it unless
the source version moved in a way Ptah can independently see. This is the mode
whose failure is quietest.

**No mode** proves nothing and blocks the cutover, which is the correct
consequence.

## Changing your mind

The consistency mode is not part of the generation identity, so switching does
not invalidate vectors already written. What it does need is a fresh `prepare`,
because the boundary and the triggers are installed there.

Switching from `immutable` to `outbox` partway through is the common case — it
happens the first time someone discovers the source was not as paused as
believed. The vectors already computed stay; the boundary is recorded again, and
catch-up covers what happened after.

## A note on `version_strategy`

The consistency mode says *how changes are captured*. `version_strategy` says
*how a stale vector is recognized*, and they are separate choices.

If your source has no timestamp and no counter, `input_hash` makes freshness
answerable without either: a vector is stale when the text that produced it
changed. It costs a hash comparison per row and it is the strategy that needs
least from your schema.
