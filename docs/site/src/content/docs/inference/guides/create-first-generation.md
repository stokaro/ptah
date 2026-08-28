---
title: Create your first generation
description: Add vectors to a table that has none, on a system where nothing is searching yet.
---

This is the easiest case: a table with no vector column, and no application
reading one. Nothing can break, because nothing depends on the result yet.

If your table already has vectors and you are replacing them, read
[Migrate to another model](../migrate-to-another-model/) instead.

## Decide four things

Before writing the specification, decide:

**Which rows.** All of them, or a subset. `source.filter` narrows the set with a
SQL condition:

```yaml
source:
  table: articles
  filter: "published = true"
```

The row count in `ptah inference plan` is what confirms you got it right.

**Which columns become the text.** `input_fields`, in order, joined by
`preprocessing.separator`. Include what a searcher would search for; leave out
identifiers and timestamps, which add noise.

**How a row's version is known.** `version_strategy` is how Ptah decides whether
a stored vector is still current:

| Strategy | Use when |
| --- | --- |
| `updated_at` | The table has a timestamp column that moves on every write |
| `monotonic` | The table has a counter or sequence that only increases |
| `outbox_sequence` | Neither exists, and the outbox's own ordering is the answer |
| `input_hash` | Nothing orders the rows; a vector is stale when the text changed |

**Where the vectors go.** A column on the same table is the simplest layout and
the one everything here assumes. Alternatives are in
[Choose a target layout](../../strategies/choose-a-target-layout/).

## Write the specification

Start from the one in [Quick start](../../quick-start/) and change the source,
the model, and the target. Then check it against your database without running
anything:

```bash
ptah inference plan --spec spec.yaml --db-url "$DB"
```

The plan refuses if the specification cannot be satisfied — a missing extension,
a source table that is not there, an index method the server does not support —
and reports as `unknown` anything it could not establish.

## Run it

```bash
export RUN=$(date +%Y-%m-%d)-articles

ptah inference prepare  --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference catchup  --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference index    --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference verify   --spec spec.yaml --db-url "$DB" --run-id "$RUN"
```

Give the run an identifier you will recognize later. It is how you resume an
interrupted backfill and how `status` finds the run.

### Sizing the backfill

Two flags matter on a corpus of any size:

```bash
ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id "$RUN" \
  --batch-rows 500 --batch-inputs 64 --provider-timeout 60s
```

- `--batch-rows` is how many source rows are read in one query. It bounds how
  long a cancellation waits.
- `--batch-inputs` is how many texts go to the provider in one request. Your
  provider's limit decides the ceiling.

[Plan provider capacity](../../strategies/plan-provider-capacity/) is the page
for a corpus large enough that this matters.

## Then connect your application

There is no cutover step here, because there is no previous generation to switch
away from. Point your search query at the column and the metric the
specification named:

```sql
SELECT id, title
FROM articles
WHERE embedding IS NOT NULL
ORDER BY embedding <=> $1
LIMIT 10;
```

The operator has to match the metric: `<=>` for `cosine`, `<->` for `l2`, `<#>`
for `inner_product`. A mismatch does not error — it returns the wrong rows in a
plausible order.

Your application produces `$1` by sending the user's query text to the **same
endpoint with the same model**. A query vector from a different model is not
comparable with the stored ones.

## Check the result before trusting it

`verify` says the vectors are the right shape and cover the source. It does not
say the search is good. Write a handful of questions with their expected
answers and measure:

```bash
ptah inference evaluate --spec spec.yaml --db-url "$DB" --corpus corpus.yaml
```

That is the only step that answers "is this actually working".
