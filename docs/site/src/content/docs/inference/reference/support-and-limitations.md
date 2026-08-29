---
title: Support and limitations
description: Exactly what is implemented, what is not, and which guarantees depend on you rather than on Ptah.
type: status
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "Which inference migration guarantees are measured, unsupported, or owned by the operator?"
goal: "Distinguish measured, unsupported, and operator-owned inference guarantees."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
lastVerified: "2026-08-30"
evidence:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
overlaps: []
disposition: keep
---

## What is supported

| | |
| --- | --- |
| Database | PostgreSQL with the pgvector extension |
| Provider API | OpenAI-compatible `/v1/embeddings` |
| Vector types | `vector`, `halfvec`, `sparsevec` |
| Metrics | `cosine`, `l2`, `inner_product` |
| Index methods | `hnsw`, `ivfflat` |
| Consistency modes | `outbox`, `immutable`, `dual_write` |

## What is not

**No other database engine.** MySQL, SQLite, ClickHouse, SQL Server, Oracle and
the rest are not supported, and a URL naming one is refused by name:

```console
$ ptah inference plan --spec spec.yaml --db-url "mysql://localhost:3306/db"
error: ptah inference works against PostgreSQL with pgvector, and "mysql://"
names another engine: a generation's run state and its vectors are a PostgreSQL
vertical, so there is nothing here to run against mysql
```

The PostgreSQL family is refused too. CockroachDB, YugabyteDB and Spanner speak
the wire protocol pgvector's driver connects with and have no pgvector.

**No other provider API.** A provider that is not OpenAI-compatible needs a
gateway in front of it.

**One vector per source row.** Chunking a document into several vectors is not
supported. The workable shape is to chunk into rows yourself — a `chunks` table
with one row per chunk — and point Ptah at that table.

**No change-data-capture connectors.** The outbox is triggers on your table.
There is no logical-replication or CDC-tool integration.

**No Kubernetes operator.** Nothing reconciles a generation change from a custom
resource.

**Ptah is not an inference server.** It does not run a model, hold one in memory,
schedule a GPU, store model weights, or answer a search query.

## Guarantees that are Ptah's

- A change that committed to the source has an outbox event, under `outbox`,
  because the event and the change are one transaction.
- A vector and its checkpoint are written in one transaction, so there is no
  state in which the work landed and the record of it did not.
- A write never crosses generations: a row belonging to another generation is
  refused rather than overwritten.
- A stale result does not win. A provider answer computed from a source version
  the row has moved past is discarded rather than written.
- A deleted row stays deleted: a tombstone survives an embedding that was already
  in flight.
- A worker whose lease was taken cannot commit, because the fencing token is
  checked in the write itself rather than read and then trusted.
- A cutover binds to a plan digest, and what is true now is checked again at the
  moment it runs.

## Guarantees that are yours

- **That the source is what you said it is.** `mutable: false` is a declaration
  Ptah takes and measures afterwards; it cannot stop a write.
- **That your writer reports its changes**, under `dual_write`. A change nobody
  reported is invisible.
- **That the model is stable.** If the provider exposes no immutable revision,
  `plan` reports the reproducibility as partial and names the reason. It cannot
  detect a model that changed under a stable name.
- **That your queries read the active generation.** Ptah moves a pointer in its
  own tables. Your application's SQL names a column, and connecting the two is
  yours.
- **That the query vector comes from the same model.** A query embedded by a
  different model is not comparable with the stored vectors, and nothing errors.
- **That the metric and the operator match.** `<=>` for cosine, `<->` for l2,
  `<#>` for inner product. A mismatch returns the wrong rows in a plausible
  order.
- **That the results are good.** Verification measures shape, coverage and
  freshness. Quality is `evaluate`, against a corpus you write.

## What Ptah cannot see

- A change made by a path that bypasses the source table's triggers — `COPY`
  with triggers disabled, a replication apply, a direct catalog edit.
- A provider that logs or retains your inputs.
- Whether the columns you chose for `input_fields` are the right ones.

## Known operational costs

- Two triggers and one companion-table insert per changed row, under `outbox`,
  for the length of the migration.
- Five columns on the target table per generation, and two generations coexist by
  design.
- The provider bill for the whole corpus, per generation.
- A vector index build that is concurrent but not free.

## Version

This page describes the behavior of the current build. Where a page in this area
describes something not yet available, it says so in the sentence rather than
leaving you to find out.
