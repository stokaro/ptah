---
title: Plan provider capacity
description: Sizing a backfill against what your embedding endpoint can take, and what the numbers in the plan mean for cost.
---

The backfill is one provider call per batch, repeated until the corpus is done.
The provider is the bottleneck and the bill.

## What the plan tells you

```text
What leaves the database:
  - text-embedding-3-small at https://api.openai.com/v1, declared hosted
  - the text of title, body
  - for 482310 rows
```

That row count is the number of texts that will be sent. If it says
`for a number of rows nobody counted`, the plan could not measure the source —
usually because the table is not there yet — and you are sizing blind.

## The two flags

```bash
ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id "$RUN" \
  --batch-rows 500 \
  --batch-inputs 64 \
  --provider-timeout 60s
```

- **`--batch-inputs`** is how many texts go in one request. Your provider's
  documented limit is the ceiling. Larger batches mean fewer round trips and a
  longer wait before anything commits.
- **`--batch-rows`** is how many source rows are read in one query. It bounds how
  long a cancellation waits, because a batch in flight finishes before the run
  stops.
- **`--provider-timeout`** bounds one request. Too short and a large batch fails
  repeatedly; too long and a hung endpoint stalls the run.

A reasonable starting point for a hosted provider: `--batch-inputs 64`,
`--batch-rows 500`, `--provider-timeout 60s`. Then measure.

## Rate limits

Ptah does not implement backoff against a provider's rate limiter. A request
that is refused fails the batch, and the run stops with the provider's own
message. Everything committed stays committed, and running `backfill` again
resumes.

If you are being rate-limited, lower `--batch-inputs` and run the backfill in
sessions rather than expecting one invocation to absorb the limit.

## Cost

The token counts the provider reports are recorded on the run and shown by
`status`:

```text
  - 41 batches committed, 0 retries since the last one
```

`ptah inference status` reports what the provider charged for, which is the
number to compare against your invoice. Ptah does not price anything — it has no
idea what your contract is.

Two things drive the bill more than anything else:

- **The row count.** `source.filter` is the lever. Embedding rows nobody searches
  is the most common avoidable cost.
- **The input length.** Every column in `input_fields` is tokens. A `body`
  column with an entire document in it costs many times what a title does, and
  `preprocessing.max_input_bytes` with `truncate: bytes` is how you cap it.

## Truncation is a decision

```yaml
preprocessing:
  max_input_bytes: 8000
  truncate: refuse     # refuse | bytes
```

`refuse` stops the run on a row that is too long. `bytes` cuts the input and
embeds what is left.

`refuse` is the default for a reason: a silently truncated document produces a
vector for its first half, which searches plausibly and is wrong. Choose `bytes`
deliberately, knowing that is what it means.

## Running it overnight

The backfill is resumable, so the safe shape for a large corpus is a loop that
runs it, and runs it again if it stopped:

```bash
until ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id "$RUN"; do
  echo "stopped; retrying in 60s"
  sleep 60
done
```

Each iteration continues from the checkpoint. Nothing is re-embedded and nothing
is paid for twice.
