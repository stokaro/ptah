---
title: Migrate a source that is not changing
description: The simpler path when writes are genuinely stopped, and what Ptah does and does not check about that claim.
type: how-to
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I migrate inference state while writes to the source table are paused?"
goal: "Migrate inference state while writes to the source table are paused."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

If nothing writes to the source during the migration, there is nothing to catch
up on. This is the cheapest path and the one with the fewest moving parts — as
long as the claim is true.

## When it applies

- A table loaded in batches, and you are running between loads.
- A read replica or a restored snapshot.
- A table behind a feature flag you turned off.
- A maintenance window in which writes are stopped.

It does **not** apply to a table that is merely quiet. "Nothing has written for
an hour" is not the same as "nothing will write", and the difference is a row
that silently keeps a vector computed from text it no longer has.

## The specification

```yaml
source:
  mutable: false
consistency:
  mode: immutable
```

No triggers are installed, no companion table is created, and no catch-up is
needed.

## Run it

```bash
ptah inference prepare  --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference index    --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference verify   --spec spec.yaml --db-url "$DB" --run-id "$RUN"
```

`catchup` is not in the sequence. Running it against this mode is refused rather
than reported as success:

```console
error: catch-up needs a consistency mode that records changes, and this
specification selects "immutable"
```

That refusal is deliberate. A catch-up that "succeeded" over a mode recording
nothing would be a run reporting itself caught up on a source it never watched.

## What Ptah checks about the claim

`plan` says what the mode means before anything runs:

```text
Consistency mode: immutable
  - this requires writes to be paused for the duration, and the run refuses to
    declare itself ready if they are not
```

Verification's freshness layer is what catches a broken promise: a row whose
source version moved after its vector was computed is reported. If writes
happened during the backfill despite the declaration, that is where it surfaces.

What Ptah cannot do is prevent the writes. `mutable: false` is your statement
about your system, and Ptah takes it and then measures the result.

## If writes turn out to have happened

You have three options, in increasing cost:

1. **Re-run the backfill.** It is resumable and it re-embeds what changed only
   if your `version_strategy` can see the change. With `input_hash` it can.
2. **Switch the specification to `outbox` and start over.** The generation
   identity does not include the consistency mode, so this does not invalidate
   the vectors already written — but the boundary and the triggers have to be
   installed by a fresh `prepare`.
3. **Stop the writes properly and repeat.**

The second is what most people want after discovering the source was not as
paused as they thought.
