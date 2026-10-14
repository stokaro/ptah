---
title: Plan vector indexes
description: Which index to build, when to build it, and why the numbers a query returns depend on settings the index does not carry.
type: concept
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "When should I build each type of vector index?"
goal: "Choose a vector index type and when to build it."
sourceOfTruth:
  - "internal/cli/inference"
  - "internal/embedydb"
  - "integration/inference_cli_e2e_test.go"
  - "integration/inference_ydb_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

Without an index, every search is a sequential scan over the whole corpus. With
one, it is approximate — and how approximate depends on settings your query
supplies at run time.

## Declare it in the specification

```yaml
target:
  index_method: hnsw
  index_options:
    m: "16"
    ef_construction: "64"
```

`ptah inference index` builds it. A specification that declares no
`index_method` has no index to build, and the command says so rather than
failing — every query over that generation is then a sequential scan, which is
what its author asked for.

## Which method

| Method | Build | Query | Choose when |
| --- | --- | --- | --- |
| `hnsw` | Slower, more memory | Faster and more accurate at the same recall | The default for most corpora |
| `ivfflat` | Faster, less memory | Needs tuning to reach the same recall | Very large corpora where the HNSW build is prohibitive |

`hnsw` is the answer unless the build time or memory is a real constraint.

## On YDB

YDB has one vector index, `vector_kmeans_tree`, and a generation declares its
shape with two options, both required:

```yaml
target:
  representation: float
  metric: cosine
  index_method: vector_kmeans_tree
  index_options:
    levels: "2"
    clusters: "128"
```

`clusters` is how many clusters each level of the tree splits into, between 2
and 2048, and `levels` is how deep the tree goes, between 1 and 16.
`clusters` to the power of `levels` is at most 2^30. The metric becomes the
index's distance or similarity, and `vector_type` and `vector_dimension` come
from the representation and the provider's dimension. `ptah inference index`
builds it with `ALTER TABLE ... ADD INDEX ... GLOBAL USING vector_kmeans_tree`,
which returns once the index holds every row.

What the index can do depends on the YDB line, and Ptah reads it from the
server's capabilities:

| Line | Builds the index | Takes rows written after the build |
| --- | --- | --- |
| 25.1 | with `EnableVectorIndex` on | no |
| 25.2 | yes | no |
| 25.3 and later | yes | yes |

On a line whose index does not take later writes, a write into a generation
whose index exists is refused rather than left for the index to miss. The
lifecycle builds the index after the backfill, so the refusal appears only when
the source changed after the index was built.

## Build it after the backfill, not before

`ptah inference index` comes after `catchup` in the lifecycle, and the ordering
matters for `ivfflat`: it trains its lists on the data present when it is built,
so one built over an empty column is valid and useless.

On PostgreSQL the build runs concurrently, because the table is one your
application is reading and writing throughout. That is also why the command
reads the index back afterwards — a concurrent build that fails leaves an index
PostgreSQL will not use, and reporting that as done would report a generation
ready while every query over it is a sequential scan.

Running it again is the finished state:

```console
$ ptah inference index --spec spec.yaml --db-url "$DB" --run-id "$RUN"
generation 3b2b6d04c204 already has a valid index
```

An invalid index left by a failed build is dropped and built again, and the
command says which of those it did.

## Recall is a query-time property

This is the part that surprises people, and it is why Ptah records it.

The index settings above affect build cost and the ceiling on quality. What your
users actually get depends on parameters the *query* supplies —
`ivfflat.probes`, `hnsw.ef_search` — which are session settings, not properties
of the index.

Measured on one corpus, recall at a fixed `k` moved from 26.5% to 100% by
changing `ivfflat.probes` alone, over the same index and the same vectors.

So a recall number without the parameters it was taken under means nothing.
`ptah inference evaluate` records the query parameters alongside every number for
exactly this reason:

```text
  - recall 1.000, MRR 1.000, NDCG 1.000 over 3 cases
  - the index agrees with an exhaustive search on 1.000 of results, over 3 cases
  - measured under hnsw.ef_search=40,ivfflat.probes=8
```

Comparing two generations means comparing them at the same settings. The
evaluation's `--baseline` does that for you.

On YDB the setting is `ydb.KMeansTreeSearchTopSize`, the number of clusters a
search descends into. Ptah does not set it, so `evaluate` reports it as
`ydb.KMeansTreeSearchTopSize=(server default)`. The indexed search reads the
generation through `VIEW <index>`, and the exhaustive one reads the table
without it.

## The exact-agreement check

`evaluate` can also compare the index against an exhaustive search over the same
vectors:

```bash
ptah inference evaluate --spec spec.yaml --db-url "$DB" \
  --corpus corpus.yaml --min-exact-agreement 0.9
```

The corpus file is described in
[Evaluation corpus reference](../../reference/evaluation-corpus/).
That measures the index rather than the model: how often the approximate search
returns what a full scan would have. A low number means the index or its query
parameters need attention, not that the embeddings are wrong.

## Index options are not part of the identity

Changing `m` or `ef_construction` produces a different index over the same
corpus, not a different corpus. Ptah excludes them from the generation identity
deliberately, so retuning an index does not force a re-embed.
