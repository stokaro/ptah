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
  - "internal/cli/inference"
  - "internal/embedydb"
  - "integration/inference_cli_e2e_test.go"
  - "integration/inference_ydb_cli_e2e_test.go"
generated: false
lastVerified: "2026-08-30"
evidence:
  - "internal/cli/inference"
  - "integration/inference_cli_e2e_test.go"
  - "integration/inference_ydb_cli_e2e_test.go"
overlaps: []
disposition: keep
---

## What is supported

| | PostgreSQL | YDB |
| --- | --- | --- |
| Database | PostgreSQL with the pgvector extension | YDB, through its own vector functions and indexes |
| Provider API | OpenAI-compatible `/v1/embeddings` | OpenAI-compatible `/v1/embeddings` |
| Vector types | `vector`, `halfvec`, `sparsevec` | `float` |
| Metrics | `cosine`, `l2`, `inner_product` | `cosine`, `l2`, `inner_product` |
| Index methods | `hnsw`, `ivfflat` | `vector_kmeans_tree` |
| Consistency modes | `outbox`, `immutable` | `immutable` |
| Target layouts | source columns, own table | source columns, own table |

The `--db-url` scheme decides the engine: a `postgres://` URL, or a
keyword/value DSN with no scheme, is PostgreSQL, and a `ydb://` or `ydbs://`
URL is YDB. Every verb, and the `inference_plan` and `inference_status` tools of
`ptah mcp`, runs the same lifecycle on both.

## What is not

**No other database engine.** MySQL, SQLite, ClickHouse, SQL Server, Oracle and
the rest are not supported, and a URL naming one is refused by name:

```console
$ ptah inference plan --spec spec.yaml --db-url "mysql://localhost:3306/db"
error: ptah inference works against PostgreSQL with pgvector and against YDB,
and "mysql://" names another engine: a generation's run state and its vectors
need one of those two, so there is nothing here to run against mysql
```

The PostgreSQL family is refused too. CockroachDB, YugabyteDB and Spanner speak
the wire protocol pgvector's driver connects with and have no pgvector.

The `inference_plan` and `inference_status` tools of `ptah mcp` give the same
answers for a configured target, before they connect to it.

**No outbox on YDB.** The outbox records each source change with a row trigger
in the transaction that makes it, and YDB has no triggers. `plan` reports it as
a blocker and `prepare` refuses it before anything is created:

```console
$ ptah inference prepare --spec spec.yaml --db-url "ydb://localhost:2136/local" --run-id r1
error: not supported on YDB: YDB 26.2.1.14 has no triggers, and the outbox
consistency mode records each source change with a row trigger in the
transaction that makes it; use consistency mode immutable, which is the mode a
YDB generation supports
```

A YDB source that can change is migrated as a paused source; see [Migrate a
paused source](../../guides/migrate-a-paused-source/).

**On YDB the key is the primary key.** `source.key_fields` has to name the
source table's primary key, in key order. A YDB table keeps its rows in
primary-key order, so the keyset scan is a range read and a write addresses one
row; any other key would make each page a full read. A specification naming
other columns is refused with the key the table has.

**A YDB vector index needs a line that has one.** YDB 25.1 builds a
`vector_kmeans_tree` index only with its `EnableVectorIndex` feature flag on.
`index` refuses on a server whose capabilities have no `vector_indexes`, and
reads the cluster's flags when the URL names its monitoring endpoint.

**On YDB 25.1 and 25.2 an indexed generation takes no more writes.** The vector
index on those lines answers as it stood at its build, while the table moves
on. A write into a generation whose index exists is refused there, naming the
capability, `vector_index_maintained_on_write`, and the line; YDB 25.3 and later
take the write into the index. Build the index after the backfill, as the
lifecycle does, and the refusal is reached only when a source the specification
declares immutable has changed.

**No cascade on a YDB table of the generation's own.** YDB has no foreign keys,
so deleting a source row does not delete its vectors. Verification reports such
a row as outside the generation's source scope.

**No other provider API.** A provider that is not OpenAI-compatible needs a
gateway in front of it.

**One set of chunks per source row, in a table of its own.** A row split into
several vectors needs `layout: own_table`; see [Choose a target
layout](../../strategies/choose-a-target-layout/).

**No change-data-capture connectors.** The outbox is triggers on your
PostgreSQL table. There is no logical-replication, YDB changefeed or CDC-tool
integration.

**No Kubernetes operator.** Nothing reconciles a generation change from a custom
resource. The verbs run as Jobs and a rollout gates on
`status --require-ready`; see [Run in Kubernetes](../../guides/run-in-kubernetes/).

**Ptah is not an inference server.** It does not run a model, hold one in memory,
schedule a GPU, store model weights, or answer a search query.

## Guarantees that are Ptah's

- A change that committed to the source has an outbox event, under `outbox`,
  because the event and the change are one transaction — and catch-up reads
  every one of those events. Its cursor is a transaction identity and a
  sequence together, so a page that fills partway through a transaction resumes
  inside that transaction rather than past it.
- A vector and its checkpoint are written in one transaction, so there is no
  state in which the work landed and the record of it did not. On YDB it is one
  serializable transaction, and a conflict with another transaction aborts it
  and runs it again on what is there now.
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
- On YDB, a lifecycle operation reads the whole run-state registry inside its
  transaction, so two operations on different generations can abort each other
  and run again. A batch checkpoint reads only its own run.
- Five columns on the target table per generation, and two generations coexist by
  design.
- The provider bill for the whole corpus, per generation.
- A vector index build that is concurrent but not free.

## Version

The limits above describe the checked build. Any unavailable inference behavior
is marked at the sentence that names it.
