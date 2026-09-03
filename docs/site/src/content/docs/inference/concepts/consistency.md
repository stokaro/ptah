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
  - "integration/inference_outbox_prune_e2e_test.go"
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

`catchup` reads that table, embeds the rows it names, records how far it got,
and removes the events every usable live feeder reading that table has passed.

The cost is real: two triggers on a table your application writes to, and a
table alongside it. The two have different lifetimes. Events go as they are
passed, so the table tracks the backlog rather than the whole history of the
migration. The triggers and the table itself remain until the last non-retired
outbox generation over that source retires.

"Every usable live feeder" is the part worth reading twice. One source table
has one companion table, so two generations over that source share it, and an
event survives until the slower run has processed it. An outbox feeder must be
nonterminal, source-matched, and carry a readable durable resume position. When
that floor is behind the run performing catch-up, the output names each holding
run and its generation.

Those generations may embed different input fields or use different filters.
Ptah keeps their shared update trigger watching the union, so an edit relevant
only to the older generation still produces an event. They must agree on the
ordered source key fields, version strategy, and version field: one shared
event stores one key and one source version. `prepare` refuses an incompatible
generation without replacing the live trigger.

Two actions release a reader for different reasons. `retire` destroys an entire
generation and its vectors. `abandon` permanently ends one run while preserving
the generation, its vectors, and the run's checkpoint for inspection. Ptah
refuses to abandon the last usable live feeder for an active generation or one
inside a maintenance window, because either state still promises that a
positioned, source-matched run will follow source changes. A duplicate
superseded run may be abandoned while another usable live feeder keeps that
promise.

## `immutable`

If nothing writes to the source during the migration, there is nothing to catch
up on. This mode says so.

Ptah does not enforce it. It is a declaration you make, and the run refuses to
declare itself ready if the source is not actually paused —
see [Migrate a paused source](../../guides/migrate-a-paused-source/).

Use it when you genuinely control the writes: a batch-loaded table, a read
replica, a table behind a feature flag you turned off.

## `dual_write`

**This build does not accept `dual_write`.** A specification selecting it is
refused, naming the modes there are. What is missing is the reporting surface:
the assessment that would hold a writer's evidence to a policy is written and
tested, and nothing exists for a writer to report *through*, so a run selecting
the mode could only ever be told that its writer had never reported anything.
Refusing it is what this build can honestly say.

The design below is what the mode means, and it is recorded here because the
mode is coming back rather than going away.

Your application tells Ptah what it changed. Ptah observes those reports; it does
not observe your writes.

This is the mode with the weakest guarantee, and the one whose weakness is
easiest to miss. If your writer forgets to report a change, Ptah has no way to
know. Nothing detects the gap — the row keeps its old vector, and
verification's freshness layer reports it only if the source version moved in a
way Ptah can see.

It is for the case where the outbox triggers are genuinely unacceptable and your
writer is a single, well-understood code path.

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
`immutable` when writes are genuinely stopped; `dual_write` is not selectable in this build, and when it returns it is only for a source you own the
writer and the trigger cost is unacceptable.
