---
title: Consistency modes
description: How Ptah accounts for source rows that change during a migration, and exactly what each mode can and cannot promise.
type: concept
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How does Ptah model consistency modes?"
goal: "Explain Ptah's model for consistency modes."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

A backfill takes time. Your application writes during it. A consistency mode is
your answer to what happens to those writes.

You choose one in the specification:

```yaml
consistency:
  mode: outbox
```

## The four answers

| Mode | What it means | What it can establish |
| --- | --- | --- |
| `outbox` | Ptah installs a companion table and triggers on the source | A change that committed has an event, because both are one transaction |
| `immutable` | You declare that the source does not change | Nothing has to be accounted for — but only if the declaration is true |
| `dual_write` | Your application reports its own changes | Completeness rests on what your writer reports; Ptah observes the reports and not the writes |
| *(none)* | No mode selected | Nothing establishes that the backfill covers the source as it is now, and the cutover is refused |

## `outbox`, and why it is the default answer

`prepare` creates a companion table and two triggers on your source table. When
a row changes, the trigger writes a row into that table **in the same
transaction as the change itself**.

That is the whole guarantee, and it is worth stating precisely: a change that
committed has an event, because a transaction that committed committed both. A
change that rolled back has no event, for the same reason.

`catchup` reads that table, embeds the rows it names, and records how far it got.

The cost is real: two triggers on a table your application writes to, and a
table that grows until catch-up drains it. Both go away when the migration is
over.

## `immutable`

If nothing writes to the source during the migration, there is nothing to catch
up on. This mode says so.

Ptah does not enforce it. It is a declaration you make, and the run refuses to
declare itself ready if the source is not actually paused —
see [Migrate a paused source](../../guides/migrate-a-paused-source/).

Use it when you genuinely control the writes: a batch-loaded table, a read
replica, a table behind a feature flag you turned off.

## `dual_write`

Your application tells Ptah what it changed. Ptah observes those reports; it does
not observe your writes.

This is the mode with the weakest guarantee, and the one whose weakness is
easiest to miss. If your writer forgets to report a change, Ptah has no way to
know. Nothing detects the gap — the row keeps its old vector, and
verification's freshness layer reports it only if the source version moved in a
way Ptah can see.

Choose it when the outbox triggers are genuinely unacceptable and your writer is
a single, well-understood code path.

## No mode

`plan` says what this means before you run anything:

```text
Consistency mode: none selected
  - nothing will establish that the backfill covers the source as it is now, and
    the cutover will refuse
```

This is a usable state for a source you are about to declare immutable, and a
dead end otherwise.

## Choosing

[Choose a consistency mode](../../strategies/choose-a-consistency-mode/) is the
decision page. The short version: `outbox` unless you have a specific reason,
`immutable` when writes are genuinely stopped, `dual_write` only when you own the
writer and the trigger cost is unacceptable.
