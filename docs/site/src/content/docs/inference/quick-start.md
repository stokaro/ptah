---
title: Quick start
description: Run one complete generation change against a throwaway PostgreSQL database, from an empty table to a cutover.
type: tutorial
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I run and verify one complete inference migration?"
goal: "Run and verify one complete inference migration."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
searchAliases:
  - pgvector
  - inference migration tutorial
overlaps:
  - "/inference/guides/create-first-generation/"
  - "/inference/concepts/lifecycle/"
disposition: rewrite
---

This runs a full migration end to end. Use a throwaway database — the last step
moves a pointer and the one before it writes to your table.

You need a PostgreSQL database with pgvector, a built `ptah` binary, and an
embedding endpoint that speaks the OpenAI-compatible `/v1/embeddings` API.

## 1. A table with something in it

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE docs (
  id         BIGINT PRIMARY KEY,
  title      TEXT NOT NULL,
  body       TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO docs (id, title, body) VALUES
  (1, 'Pricing',  'We bill monthly.'),
  (2, 'Support',  'Email support@example.com.'),
  (3, 'Billing',  'Invoices are issued on the first.');
```

Ptah does not install pgvector for you. `CREATE EXTENSION` is a database-wide,
privileged act, and Ptah refuses rather than taking it on your behalf.

## 2. A specification

Save this as `spec.yaml`, with your endpoint and your model's dimension:

```yaml
version: 1
name: docs
source:
  schema: public
  table: docs
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
preprocessing:
  separator: "\n"
  null_policy: empty
  empty_policy: skip
  unicode_normalization: none
  truncate: refuse
model:
  provider: openai-compatible
  endpoint_class: local
  endpoint: http://127.0.0.1:8080/v1
  identifier: bge-small-en
  revision: "1"
  reported_dimension: 384
  normalization: none
target:
  schema: public
  table: docs
  column: embedding
  representation: vector
  metric: cosine
  index_method: hnsw
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
```

Every field is explained in [Specification](../reference/specification/).

## 3. See what would happen

```bash
export DB="postgres://user:password@localhost:5432/mydb?sslmode=disable"
ptah inference plan --spec spec.yaml --db-url "$DB"
```

The plan labels every answer with where it came from, lists the steps, and ends
with what would leave your database. Read the row count before going further —
it is what tells you the scope is what you meant.

## 4. Run the lifecycle

```bash
export RUN=quickstart

ptah inference prepare  --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference catchup  --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference index    --spec spec.yaml --db-url "$DB" --run-id "$RUN"
ptah inference verify   --spec spec.yaml --db-url "$DB" --run-id "$RUN"
```

Each prints what it did:

```text
prepared run quickstart for generation 31122cc8322d
backfill finished: 3 scanned, 3 embedded, 0 skipped
caught up to transaction 901: 3 changed rows, 0 tombstoned
generation 31122cc8322d has a valid index
generation 31122cc8...: 3 source rows, 3 target rows
  - every deterministic layer passed
```

## 5. Look at the table

```sql
SELECT id, embedding_generation, embedding_state FROM docs ORDER BY id;
```

Every row carries the generation its vector belongs to and the state it is in.
`ptah inference status --spec spec.yaml --db-url "$DB" --run-id "$RUN"` reports
the same run from Ptah's side, including the phase it reached.

## 6. Cut over

Run it once to see the plan digest, then approve that exact plan:

```bash
ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id "$RUN"
# refuses, and prints: plan 1df24fc375d7

ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id "$RUN" \
  --approve 1df24fc375d7 --approver "your name"
```

Without `--stabilize-for` this leaves no rollback, and the command says so. That
is the right default for a throwaway database and the wrong one for production —
see [Rollback and retire](../guides/rollback-and-retire/).

## What you have now

One generation, verified, indexed, and active. Your application still reads
whatever column its SQL names; moving the pointer did not change that. The next
real task is
[Migrate to another model](../guides/migrate-to-another-model/), which is where
the second generation and its own column come in.
