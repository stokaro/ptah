---
title: Inference migrations
description: Change an embedding model with ptah inference — plan, backfill, catch up, verify, cut over, and go back.
---

Use `ptah inference` when changing an embedding model, its parameters, or the
text it reads. You need a built `ptah` binary, a PostgreSQL database with
pgvector, an embedding endpoint speaking the OpenAI-compatible API, and a
specification file describing the change.

Changing an embedding model rewrites every vector in a corpus. The schema change
is the smallest part of it: the rest is execution over data, with a provider
outside the database in the middle of the loop, and a result that cannot be
derived from the input. A vector is not a column value Ptah can compute — so
these verbs manage a second generation alongside the one queries read, and move
the pointer only when somebody decides to.

![Ptah builds a candidate inference generation from a specification and source rows, calls an external embedding endpoint during backfill and catch-up, verifies the result, switches the active generation at cutover, and retains the previous generation for rollback.](../../../assets/inference-state-migration.png)

## Write the specification

The specification names the source rows, the model, the target column, and the
policy the run is held to. A minimal one:

```yaml
version: 1
name: articles
source:
  schema: public
  table: articles
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
  table: articles
  column: embedding
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
```

Everything in it that can change what a vector comes out as — the fields read,
how they are joined, how text is normalized, the model, its revision, its
parameters — is hashed into a generation identity. Two runs of the same
specification address the same generation; any edit to those fields addresses a
different one, and the vectors do not mix.

`revision` is the field to look at hardest. A provider that serves a model under
a name and changes what it returns has changed the corpus without changing the
identity, and Ptah cannot see that. Where the provider exposes an immutable
revision, put it here; where it does not, `ptah inference plan` says so rather
than implying a reproducibility it cannot offer.

## Run the lifecycle

The verbs follow the lifecycle, and none of them is implied by another. A
backfill finishing does not mean the corpus is right; verification passing does
not mean anything has cut over; and cutting over does not make the old
generation disposable.

```bash
# What would happen, and where each answer came from.
ptah inference plan --spec spec.yaml --db-url "$DB"

# Create the target column, its metadata, the outbox, and the snapshot boundary.
ptah inference prepare --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles

# Embed the source. Resumable: run it again after an interruption.
ptah inference backfill --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles

# Process what changed while the backfill ran.
ptah inference catchup --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles

# Build the vector index, concurrently, and leave it valid.
ptah inference index --spec spec.yaml --db-url "$DB"

# The deterministic checks a cutover rests on.
ptah inference verify --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles

# What the generation actually retrieves, against a corpus you wrote.
ptah inference evaluate --spec spec.yaml --db-url "$DB" --corpus corpus.yaml

# What the run has done and what it is waiting for.
ptah inference status --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles
```

`prepare` creates the vector column and the four bookkeeping columns beside it,
installs the outbox, and records where the source was when the run started. It is
idempotent, so several workers may start at once and an interrupted run is
resumed by running it again.

It does not install pgvector. `CREATE EXTENSION vector` is a database-wide,
privileged act, and Ptah refuses rather than taking it on your behalf — the
refusal names the statement to run.

Two generations over one table need two columns, so a specification whose model
changed also needs its `target.column` changed. A generation writes its own
column; it never overwrites another generation's, which is what makes the
cutover a pointer move rather than a data migration — the previous corpus is
still there to go back to.

`prepare` refuses when the column you named already holds another generation,
and says which. That refusal is deliberately early: the write path refuses the
same thing one row at a time, in the middle of a backfill, after the provider
has already been called for that batch. `backfill` embeds
that snapshot; `catchup` embeds what the outbox collected since. Running
`catchup` until it reports nothing left is what makes a cutover possible —
`cutover` refuses while the backfill is unfinished or the outbox has a backlog.

## Cut over, and keep a way back

A cutover is approved for one plan, by digest. Run it without an approval to see
the digest, then approve that exact plan:

```bash
ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles
# ... refuses, and prints the plan digest

ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles \
  --approve <digest> --approver "your name" --stabilize-for 24h
```

The approval binds to the digest, so a plan that changed between reading it and
approving it is refused rather than applied. The digest covers the plan, not the
clock: what is true now is checked again at the moment of the cutover.

`--stabilize-for` is what makes a rollback possible, and it is not free. The
previous generation stops receiving changes the moment queries stop reading it,
so what keeps it a way back is somebody continuing to run `catchup` against it
during the window:

```bash
ptah inference catchup --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles \
  --maintain-for 1h
```

Without that, the window elapses over a generation that has drifted from the
source, and `rollback` refuses it — which is the honest answer, not a gap. A
cutover run without `--stabilize-for` leaves no rollback at all, and says so.

```bash
ptah inference rollback --spec spec.yaml --db-url "$DB" --to <generation> --window 24h
```

## Retire a generation

Retirement destroys vectors and cannot be undone, so it takes the same
digest-bound approval a cutover does, and is refused while queries still read
the generation:

```bash
ptah inference retire --spec spec.yaml --db-url "$DB" --generation <identity> \
  --approve <digest> --approver "your name" --drop-column
```

## Keep the evidence

`verify` and `cutover` can publish what they measured to an OCI registry, where
the rest of Ptah's evidence already lives:

```bash
ptah inference verify --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles \
  --publish-evidence oci://registry.example.com/articles-evidence:verification
```

The verification record carries the findings whole rather than a verdict, and
also carries what the run did not measure. The cutover record carries the plan
digest the approval bound to, the approver, and the verification it cited — the
answer to "why did this corpus change" six months after the terminal that ran it
is gone.

A verification is attached to the release it is about as an OCI referrer, so it
is found from the release rather than by remembering a tag. Publishing to a
local registry over plain HTTP needs `--plain-http`, the same flag the rest of
the tree uses.

A failure to publish is reported and does not fail the verb. The measurement or
the pointer move already happened, and a registry nobody can reach is not a fact
about the generation.

## What the answers are worth

`ptah inference plan` labels every answer with where it came from: `measured`
from the live database, `configured` from the specification, `inferred` from
something Ptah derived, `unknown` where nothing established it, and
`unsupported` where the target cannot answer at all. A plan that says `unknown`
is telling you a decision rests on something nobody checked.

Retrieval quality is not a schema property, and `ptah inference evaluate` does
not pretend otherwise: it measures what a corpus you wrote actually retrieves,
under query parameters it records alongside the numbers. Recall measured at one
`ivfflat.probes` setting is not comparable to recall measured at another, which
is why the setting is part of the record rather than a footnote.

## Next steps

- [Native commands](../../reference/native-commands/) — every verb and what it does.
- [OCI registry](../oci-registry/) — where the evidence goes, and how to consume it.
