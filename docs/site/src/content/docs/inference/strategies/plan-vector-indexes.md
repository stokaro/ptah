---
title: Plan vector indexes
description: Which index to build, when to build it, and why the numbers a query returns depend on settings the index does not carry.
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

## Build it after the backfill, not before

`ptah inference index` comes after `catchup` in the lifecycle, and the ordering
matters for `ivfflat`: it trains its lists on the data present when it is built,
so one built over an empty column is valid and useless.

The build runs concurrently, because the table is one your application is reading
and writing throughout. That is also why the command reads the index back
afterwards — a concurrent build that fails leaves an index PostgreSQL will not
use, and reporting that as done would report a generation ready while every query
over it is a sequential scan.

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
  query parameters: ivfflat.probes=1
  recall@10: 0.80  mrr: 0.90  ndcg: 0.85
```

Comparing two generations means comparing them at the same settings. The
evaluation's `--baseline` does that for you.

## The exact-agreement check

`evaluate` can also compare the index against an exhaustive search over the same
vectors:

```bash
ptah inference evaluate --spec spec.yaml --db-url "$DB" \
  --corpus corpus.yaml --min-exact-agreement 0.9
```

That measures the index rather than the model: how often the approximate search
returns what a full scan would have. A low number means the index or its query
parameters need attention, not that the embeddings are wrong.

## Index options are not part of the identity

Changing `m` or `ef_construction` produces a different index over the same
corpus, not a different corpus. Ptah excludes them from the generation identity
deliberately, so retuning an index does not force a re-embed.
