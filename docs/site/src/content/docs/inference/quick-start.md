---
title: Quick start
description: Build, verify, and activate an embedding generation with a disposable PostgreSQL and local provider fixture.
type: tutorial
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I run and verify one complete inference migration?"
goal: "Build, verify, and activate one embedding generation without an external model service."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
  - "docs/site/fixtures/inference-quick-start"
generated: false
lastVerified: "2026-08-30"
searchAliases:
  - pgvector
  - inference migration tutorial
  - local embedding model
overlaps:
  - "/inference/guides/create-first-generation/"
  - "/inference/concepts/lifecycle/"
disposition: rewrite
---

Build and activate one embedding generation without sending data to an external
model service. The fixture supplies a disposable PostgreSQL 17 database with
pgvector 0.8.1, three source rows, and a deterministic local embeddings API.

The lifecycle keeps two roles separate: the **candidate generation** receives
new vectors while the application continues to use the **active generation**.
Only cutover changes the active pointer. On this empty fixture there is no old
active generation, so the first candidate becomes the first active one.

## What you need

- Ptah [installed](../../start/install/) or built as `bin/ptah`;
- Docker Compose;
- Bash, `sed`, and `tee` for the approval step;
- ports `55432` and `58080` available on the Docker host.

Run all commands from the repository root. If you installed Ptah elsewhere,
replace `bin/ptah` below with `ptah`.

## 1. Start the disposable services

```bash
docker compose -f docs/site/fixtures/inference-quick-start/compose.yaml up -d --build --wait
```

Compose reports both `postgres` and `embeddings` as healthy. The database init
script installs pgvector and inserts the source rows; the provider maps each
input deterministically to four numbers based on its UTF-8 length.

Set names used by the remaining commands:

```bash
export PTAH_INFERENCE_DB='postgres://ptah:ptah@127.0.0.1:55432/ptah?sslmode=disable'
export PTAH_INFERENCE_SPEC='docs/site/fixtures/inference-quick-start/spec.yaml'
export PTAH_INFERENCE_RUN='quick-start'
```

## 2. Review the plan

```bash
bin/ptah inference plan \
  --spec "$PTAH_INFERENCE_SPEC" \
  --db-url "$PTAH_INFERENCE_DB"
```

Check these stable facts in the output before continuing:

```text
source.estimated_rows = 3 (measured)
target.capability.vector_type = true (measured)
[backfill] embed 3 in-scope source rows
Consistency mode: outbox
```

The plan is read-only. It tells you how many rows are in scope and whether the
target database actually provides the required vector type.

## 3. Prepare and backfill the candidate

```bash
bin/ptah inference prepare \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN"

bin/ptah inference backfill \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN" --batch-rows 10
```

The second command ends with this stable summary:

```text
backfill finished: 3 scanned, 3 embedded, 0 skipped
```

Ptah has now written the candidate vectors, their generation identity, source
version, input hash, and state. Nothing has cut over.

## 4. Catch up, index, and verify

```bash
bin/ptah inference catchup \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN" --batch-rows 10

bin/ptah inference index \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN"

bin/ptah inference verify \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN"
```

Verification reports three source rows and three target rows, followed by
`every deterministic layer passed`. A passing report makes the generation
eligible for cutover; it does not activate it.

## 5. Inspect what Ptah wrote

```bash
bin/ptah inference status \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN"

docker compose -f docs/site/fixtures/inference-quick-start/compose.yaml \
  exec -T postgres psql -U ptah -d ptah -c \
  'SELECT id, embedding_generation, embedding_state FROM docs ORDER BY id;'
```

`status` names the run and its completed phase. The query returns three rows;
each has the same nonempty generation identity and the state `upsert`.

## 6. Approve the exact cutover plan

First run cutover without approval. Refusal is deliberate: it prints the digest
of the plan you are being asked to approve.

```bash
bin/ptah inference cutover \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN" 2>&1 | tee /tmp/ptah-inference-cutover.txt

export PTAH_INFERENCE_PLAN="$(sed -n 's/^plan //p' /tmp/ptah-inference-cutover.txt | head -1)"
```

Confirm that `PTAH_INFERENCE_PLAN` is not empty, then bind the approval to it:

```bash
test -n "$PTAH_INFERENCE_PLAN"

bin/ptah inference cutover \
  --spec "$PTAH_INFERENCE_SPEC" --db-url "$PTAH_INFERENCE_DB" \
  --run-id "$PTAH_INFERENCE_RUN" \
  --approve "$PTAH_INFERENCE_PLAN" --approver 'quick-start operator'
```

The successful command prints `queries now read generation …` with the same
plan digest. Because no previous generation exists, there is nothing to keep as
a rollback target in this first run.

## 7. Verify the active pointer and clean up

```bash
docker compose -f docs/site/fixtures/inference-quick-start/compose.yaml \
  exec -T postgres psql -U ptah -d ptah -c \
  "SELECT target_table, active_generation FROM ptah_embedding_pointer;"
```

The row for `docs` names the generation you inspected in step 5. That pointer,
not the completion of backfill or verification, is what makes a generation
active.

Remove the containers, network, and disposable database volume with one
command:

```bash
rm -f /tmp/ptah-inference-cutover.txt && \
  docker compose -f docs/site/fixtures/inference-quick-start/compose.yaml down -v --rmi local
```

Next, use [Migrate to another model](../guides/migrate-to-another-model/) to
create a second generation and preserve the first through a stabilization
window, or read [Generations](../concepts/generations/) for the complete active,
candidate, previous, and retired state model.
