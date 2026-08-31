---
title: Change an embedding model
description: Replace a table's active embedding model with a second generation built and verified beside the first.
type: how-to
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I replace an active embedding generation with one from another model?"
goal: "Replace an active embedding generation with one from another model."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
searchAliases:
  - "change embedding model"
overlaps: []
disposition: keep
---

You have a working generation and you are changing the model. The new vectors go
in a **new column**, beside the old ones, and your queries switch when you say
so.

## Copy the specification and change two things

```yaml
model:
  identifier: text-embedding-3-small   # was: bge-small-en
  reported_dimension: 1536             # was: 384
target:
  column: embedding_v2                 # was: embedding
```

The model change is what you set out to do. The column change is what makes it
possible: two generations over one table need two columns, because a new
generation that overwrote the old one would leave you nothing to go back to.

`prepare` refuses if you forget:

```console
error: column "embedding" on articles holds generation 31122cc8322d, and this
run is generation 9aef540a9038: a generation writes its own column so the
previous one is still there to go back to. Give this one its own target.column
in the specification
```

Keep the previous specification file. You will need it to maintain the previous
generation during the stabilization window.

## Check what changed

```bash
ptah inference plan --spec spec-v2.yaml --db-url "$DB" --current <previous-generation>
```

`--current` tells the plan which generation queries read now, so it can describe
the change rather than a fresh build. `ptah inference status` on the previous run
prints the generation identity to pass.

## Build it

Same sequence as a first generation, with a new run identifier. **New** is the
part that matters: a run records the generation it was prepared for, and every
verb refuses a run prepared for a different one.

```text
run 2026-08-31-v2 is for generation 547ab65200da and this specification
produces b115a08fbd46
```

Reuse the previous generation's run id and you meet that refusal at `prepare`,
before anything is written. It is a refusal rather than a warning because the
alternative was silent: the second `prepare` used to add its columns and
register its generation, then say "leaving it as it is", and the `backfill`
after it resumed the first generation's finished cursor and reported rows it had
not embedded.

```bash
export RUN=$(date +%Y-%m-%d)-v2

ptah inference prepare  --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
ptah inference backfill --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
ptah inference catchup  --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
ptah inference index    --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
```

Both columns now hold vectors. Your application is still reading the first, and
nothing about the second has affected it.

## Compare before switching

This is the step worth not skipping. The new model is not automatically better
for your data.

```bash
ptah inference evaluate --spec spec-v2.yaml --db-url "$DB" \
  --corpus corpus.yaml \
  --baseline <previous-generation> --baseline-spec spec-v1.yaml \
  --max-ndcg-regression 0.02
```

`--baseline-spec` is the previous generation's own specification file, and it is
required with `--baseline`. Scoring a generation embeds every query with **its**
model and searches **its** column; a generation identity carries neither, so the
identity alone names the comparison and the file is what makes it. Ptah refuses
`--baseline` without it, and refuses a file whose identity is not the one named.

With both, the evaluation compares the two generations over the same
questions and refuses when the new one is worse by more than you allowed. The
numbers it reports carry the query parameters they were taken under, because
recall measured at one `ivfflat.probes` setting is not comparable to recall
measured at another.

## Verify and cut over

```bash
ptah inference verify  --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
ptah inference cutover --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
# prints the plan digest, then:
ptah inference cutover --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN" \
  --approve <digest> --approver "your name" --stabilize-for 24h
```

`--stabilize-for` is what makes a rollback possible at all. It is not enough on
its own — see [Rollback and retire](../rollback-and-retire/) for what keeping the
previous generation current actually requires.

## Then change your application

The pointer moved; your SQL did not. Deploy the change that reads
`embedding_v2`, with the operator matching the new metric if it changed.

Order matters here, and both orders are defensible:

- **Cut over, then deploy.** The window between the two is a window in which
  your application reads the old generation, which is still correct.
- **Deploy behind a flag, then cut over, then flip the flag.** More moving
  parts, and no window.

What does not work is deploying the new column first: the vectors are there, but
the pointer still says the old generation is active, so a rollback would move the
pointer back under an application that no longer reads it.
