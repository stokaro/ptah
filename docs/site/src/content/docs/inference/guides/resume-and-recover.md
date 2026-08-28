---
title: Resume and recover
description: What to do when a run stops - an unreachable provider, a killed process, a lease taken by another worker.
---

A run that stops leaves everything it committed and nothing it did not. There is
no partial state to clean up and no repair command, because the checkpoint and
the vectors it describes are written in one transaction.

The recovery for almost every failure is the same: **run the same command
again**.

## The provider became unreachable

```console
the run stopped after 41 batches, 5120 rows embedded; it resumes from where it is
error: provider: embedding endpoint unreachable: api.example.com: dial tcp:
connect: connection refused
```

The 5120 rows are committed. Fix the endpoint and run `backfill` again — it
continues from its checkpoint rather than starting over.

If the provider is rate-limiting rather than down, lower `--batch-inputs` or add
`--provider-timeout` before retrying. See
[Plan provider capacity](../../strategies/plan-provider-capacity/).

## The process was killed

Same answer. Run the command again. `status` tells you where it got to first:

```bash
ptah inference status --spec spec.yaml --db-url "$DB" --run-id "$RUN"
```

```text
run 2026-08-articles: backfilling, running
  - generation: 31122cc8322d...
  - scanned 5120, embedded 5120, skipped 0, deleted 0
  - 41 batches committed, 0 retries since the last one
  - lease: worker-1, fencing token 1
```

## Another worker holds the lease

```console
error: the state changed underneath this write: run 2026-08-articles is fenced
at token 2 and this write carries 1
```

Somebody — or some other process — took over the run. The lease says who *should*
be working; the fencing token says who may still *commit*, and a worker the run
has moved past is refused before it touches your table.

If the other worker is real, let it finish. If it is a process that died without
releasing the lease, run the command again: starting a verb takes the run, which
moves the token past whatever the dead process held, and the new invocation
becomes the holder.

A lease that has expired does not stop its holder on its own. What stops it is a
later worker taking the run.

## Verification found something

Read which layer:

```text
  - [freshness/blocking] 12 target rows were computed from a source state that
    has since changed
      keys: 4471, 4472, 4480, ...
  - [coverage/blocking] 3 rows have no vector and are not marked skipped or deleted
```

| Layer | Usual cause | Usual fix |
| --- | --- | --- |
| `freshness` | Writes happened after those rows were embedded | Run `catchup` again |
| `coverage` | The backfill did not finish, or rows arrived after it | Run `backfill`, then `catchup` |
| `structural` | The index is missing or invalid | Run `index` |
| `consistency` | Backfill unfinished, or a lease is still held | Finish the phase, or wait for the holder |
| `vector_validity` | The stored vectors are not the declared shape | The dimension in the specification does not match the model |

Only the last one needs the specification changed. The rest are a phase that has
more to do.

## The run does not exist

```console
error: run 2026-08-articles: no rows in result set
```

The run identifier is wrong, or you are pointed at the wrong database. Run
identifiers are yours to choose and are not derived from anything, so a typo
looks exactly like a run that was never prepared.

## Starting over

There is no "reset" command, deliberately. To abandon a generation and start
again:

1. `ptah inference retire --generation <identity> --drop-column ...` destroys it,
   with the same digest-bound approval a cutover needs. It is refused while
   queries still read the generation.
2. Then `prepare` a fresh run.

If the generation was never cut over to, retiring it costs nothing but the
provider spend already made. If it was, put the pointer back first with
`rollback`.

## What is not recoverable

Vectors destroyed by `retire` are gone. Rebuilding them means paying the provider
for the whole corpus again. That is why retirement takes an approval and refuses
while anything reads the generation.
