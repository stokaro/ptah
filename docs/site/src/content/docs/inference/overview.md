---
title: Inference migrations
description: What Ptah does with the vectors an application searches, and whether this feature applies to your system.
type: landing
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "Do inference migrations apply to my persistent model output?"
goal: "Determine whether inference migrations apply to my persistent model output."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
searchAliases:
  - "pgvector"
overlaps:
  - "/inference/quick-start/"
  - "/inference/concepts/lifecycle/"
disposition: keep
---

If your application stores embeddings in PostgreSQL and you need to change the
model that produced them, this area is for you.

Changing an embedding model means every vector in the table has to be computed
again. That is not a schema change. The column type may not move at all, and yet
none of the values in it is usable afterwards, because a vector produced by one
model cannot be compared with a vector produced by another. Ptah manages that
rewrite: it builds the new set of vectors beside the old one, checks the result,
and switches your queries over when you say so.

## Is this for you?

Read on if all of these are true:

- your data lives in **PostgreSQL** with the **pgvector** extension;
- your application searches by vector similarity;
- something about how those vectors are produced is changing — the model, its
  parameters, or the text you feed it.

This feature is not for you if you are looking for a way to serve a model, run
one on a GPU, store models, or route application traffic. Ptah does none of
those. It manages the database state around inference and nothing else.

## Who does what

Four things are involved, and keeping them apart is the fastest way to
understand everything else here.

| Component | Responsibility |
| --- | --- |
| Ptah | Plans, builds, verifies, activates, rolls back, and retires persistent inference state |
| Embedding provider | Converts the configured input into a vector |
| PostgreSQL and pgvector | Store source data, vectors, metadata, and indexes |
| Your application | Creates and updates source rows, produces query vectors, and reads the active generation |

Ptah never produces a vector itself. It reads rows, builds the text to send,
calls the endpoint you configured, and writes what comes back. If your endpoint
is unreachable, Ptah stops and says so; it has nothing of its own to fall back
on.

## What a migration looks like

You describe the change in a file — the source rows, the model, where the
vectors go — and then run a sequence of commands. Each one is a decision you
take separately:

```bash
ptah inference plan      # what would happen, and where each answer came from
ptah inference prepare   # create the target column and the bookkeeping around it
ptah inference backfill  # embed the source into the new generation
ptah inference catchup   # process what changed while the backfill ran
ptah inference index     # build the vector index
ptah inference verify    # the checks a cutover rests on
ptah inference cutover   # make the new vectors the ones queries read
```

Nothing here is implied by anything else. A finished backfill does not mean the
new vectors are correct. A passing verification does not move your queries. A
completed cutover does not throw the old vectors away.

Two commands exist for what comes after: `rollback` puts the previous vectors
back, and `retire` destroys a set of vectors permanently.

## Where to go next

- New to this? [Quick start](../quick-start/) runs one migration end to end
  against a throwaway database.
- Want the ideas first? Start with
  [Embeddings and inference state](../concepts/embeddings-and-inference-state/).
- Changing a model on a table your application is writing to? That is
  [Migrate a live table](../guides/migrate-a-live-table/).
- Something went wrong? [Troubleshooting](../troubleshooting/).

## What is supported today

PostgreSQL with pgvector, and an embedding endpoint that speaks the
OpenAI-compatible `/v1/embeddings` API. No other database engine and no other
provider API. [Support and limitations](../reference/support-and-limitations/)
is the exact list, including what Ptah cannot check for you.
