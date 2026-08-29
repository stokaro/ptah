---
title: Choose a target layout
description: Where the vectors go - a column on the source table, or a table of their own - and what each choice makes easy.
type: concept
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "Which target layout fits my query and lifecycle needs?"
goal: "Choose a vector target layout for the query and lifecycle needs."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

Ptah writes vectors into a column. The choice is which table that column is on.

## A column on the source table

The default and the one every guide here assumes:

```yaml
target:
  schema: public
  table: articles      # the same table as the source
  column: embedding
```

Ptah adds five columns to `articles`: the vector and four bookkeeping columns.

**What it makes easy.** A search query is one table and no join. A row and its
vector are one row, so a delete removes both.

**What it costs.** Five columns on a table your application selects from — and
`SELECT *` now carries a vector of several kilobytes. Two generations means ten
columns.

Use it unless one of the reasons below applies.

## A table of its own

```yaml
source:
  table: articles
  key_fields: [id]
target:
  table: article_vectors
  column: embedding
```

The target table must already have a row per source row, keyed the same way:
Ptah writes with `UPDATE ... WHERE <key>`, so a missing row is a row that gets no
vector rather than a row that gets created.

**What it makes easy.** The source table is untouched. A generation is dropped by
dropping a table. `SELECT *` on the source stays what it was.

**What it costs.** A join on every search, and a row you have to create yourself
when a source row is inserted.

Choose it when the source table is one you would rather not add columns to — a
table owned by another team, or one with a `SELECT *` you cannot audit.

## Two generations, two columns

Either layout needs a new column per generation. `prepare` refuses to write a
generation into a column another one holds, and the reason is that the previous
corpus has to survive for a rollback to mean anything.

Naming: `embedding` then `embedding_v2` is what most people do. Ptah does not
care about the name, only that it differs.

## The representation

```yaml
target:
  representation: vector    # vector | halfvec | sparsevec
```

`vector` stores four bytes per dimension. `halfvec` stores two, at some loss of
precision — worth measuring on a large corpus where the index size matters.
`sparsevec` is for models that produce mostly-zero vectors.

The representation is part of the generation identity: a half-precision copy of
a corpus is not the same corpus, and Ptah treats it as a different one.

## The metric

```yaml
target:
  metric: cosine    # cosine | l2 | inner_product
```

Use what your model's documentation says. Most text embedding models are trained
for cosine similarity.

The metric decides the operator class of the index Ptah builds, and your
application's query operator has to match:

| Metric | Operator |
| --- | --- |
| `cosine` | `<=>` |
| `l2` | `<->` |
| `inner_product` | `<#>` |

A mismatch does not error. It returns the wrong rows in a plausible order, which
is the failure mode worth knowing about before you meet it.
