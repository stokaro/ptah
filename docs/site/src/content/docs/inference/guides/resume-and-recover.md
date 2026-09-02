---
title: Resume and recover
description: What to do when a run stops - an unreachable provider, a killed process, a lease taken by another worker.
type: troubleshooting
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I recover an inference migration after a provider or worker failure?"
goal: "Recover an inference migration after a provider or worker failure."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
  - "integration/inference_outbox_prune_e2e_test.go"
generated: false
searchAliases:
  - "resume inference migration"
  - "abandon inference run"
  - "release outbox floor"
overlaps: []
disposition: keep
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
  - 655360 prompt tokens, 655360 total, as the provider reported them
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
| `consistency` | Backfill unfinished, catch-up behind, or no consistency mode | Finish the phase, or declare a mode |
| `vector_validity` | The stored vectors are not the declared shape | The dimension in the specification does not match the model |

Only the last one needs the specification changed. The rest are a phase that has
more to do.

## The run does not exist

```console
error: not found: run 2026-08-articles
```

The run identifier is wrong, or you are pointed at the wrong database. Run
identifiers are yours to choose and are not derived from anything, so a typo
looks exactly like a run that was never prepared.

## End a superseded run without deleting its vectors

If this attempt is over but you still need its vectors for inspection, abandon
the run:

```bash
ptah inference abandon --db-url "$DB" --run-id "$RUN" \
  --reason "superseded by the multilingual model run"
```

The command needs no specification. The run already records its generation and
source. It becomes terminal, keeps its checkpoint and vectors, fences a worker
that may still be running, and stops holding shared outbox events. Start the
replacement with a new run identifier; an abandoned run cannot resume.

Ptah refuses to abandon the last usable live feeder for a generation that queries
currently read or one inside a maintenance window. For outbox consistency, a
replacement counts only after it has a durable, readable resume position. Move
the active pointer, or keep catching up until the rollback window ends.
Releasing the last feeder would otherwise let the corpus become stale while
Ptah still presented it as current or maintained.

Use `retire` only when the vectors themselves should go. Retirement destroys
the generation under a digest-bound approval; abandonment does not. If the
generation is active, put the pointer back first with `rollback` or cut over to
its replacement.

## What is not recoverable

Vectors destroyed by `retire` are gone. Rebuilding them means paying the
provider for the whole corpus again. That is why retirement takes an approval
and refuses while anything reads the generation. `abandon` leaves those vectors
intact, but the run itself is permanently closed.
