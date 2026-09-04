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

Ptah writes vectors into a column. The choice is which table that column is on,
and — when it is not the source table — whether the relation is one Ptah creates
or one you maintain.

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

## A table of its own, created by Ptah

```yaml
source:
  table: articles
  key_fields: [id]
target:
  table: article_vectors
  column: embedding
  layout: own_table
```

`prepare` creates `article_vectors`: the key columns, taken from the source
relation so their types are whatever the application declared, a primary key
over them, and a foreign key back to `articles` with `ON DELETE CASCADE`. Rows
arrive as the backfill runs. Deleting a source row takes its vectors with it.

**What it makes easy.** The source table is untouched, and there is nothing to
set up: `SELECT *` on the source stays what it was, and a generation is
destroyed by dropping one relation. `ptah inference retire` does exactly that —
and refuses unless the relation carries the comment Ptah wrote when it created
it, so a target pointed at a table you maintain is never dropped by a
retirement.

**What it costs.** A join on every search.

Choose it when the source table is one you would rather not add columns to — a
table owned by another team, or one with a `SELECT *` you cannot audit.

## A table of its own, maintained by you

```yaml
source:
  table: articles
  key_fields: [id]
target:
  table: article_vectors
  column: embedding
```

Without `layout: own_table` the target table is yours. Ptah adds its five
columns to the relation you name and writes with `UPDATE ... WHERE <key>`, so
the table must already have a row per source row, keyed the same way: a missing
row is a row that gets no vector rather than a row that gets created. Retiring
the generation drops those five columns and leaves the relation.

Choose it when the sidecar table is one your application already writes to, or
when it carries columns of its own beside the vector.

Two tables can disagree about which keys exist, and `ptah inference verify`
reports both directions. A source row with no row in the target table is a
coverage gap, the same finding the single-table layout gives a row with no
vector; the target-row count is the number of rows that relation holds, so the
two numbers differ. A row in the target table carrying this generation's vector
at a key the source does not have is reported as outside the generation's source
scope — the shape to expect after a source row is deleted.

A row in the target table that no generation ever wrote is reported by none of
them. It belongs to no generation, so a generation's verification is not where
it is named. That is the row you get from creating the sidecar row before the
run that fills it, and it does not block a cutover. Under `layout: own_table`
there is no such row, because Ptah is what creates them.

The cascade does not make the out-of-scope finding unreachable, which is worth
saying because it looks as though it should: it removes the rows whose source
row is gone, and leaves the rows whose source row is still there and no longer
passes the specification's `filter`. That second case is what the finding is
for.

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
