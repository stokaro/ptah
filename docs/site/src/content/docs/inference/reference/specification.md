---
title: Specification reference
description: Every field of an inference specification, its accepted values, and whether it is part of the generation identity.
type: reference
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "Which fields and values does an inference specification accept?"
goal: "Look up the accepted inference specification fields and values."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

The specification is a YAML file. It describes the source rows, how they become
text, which model turns that text into vectors, where the vectors go, how source
changes are accounted for, and what a cutover requires.

Fields marked **identity** are hashed into the generation identity: changing one
addresses a different corpus.

Fields marked **required** are refused when they are absent, before any verb
does work -- `describe` included, which opens no database. A field not marked
required is not thereby harmless to leave out: `source.table` is accepted by
`describe` and produces a specification nothing else can run. What the mark says
is narrower and worth having exactly: without these, every verb that reads the
specification exits 2 and names the field. `version` is required in the same
way, at the top of the file.

## Top level

| Field | Meaning |
| --- | --- |
| `version` | The specification format version. `1`. |
| `name` | A human-readable name for the migration. Not the generation identity. |
| `description` | Free text. |
| `source` | Which rows are read. |
| `preprocessing` | How a row becomes text. |
| `model` | What turns text into vectors. |
| `target` | Where the vectors go. |
| `consistency` | How source changes during the run are accounted for. |
| `policy` | What a cutover requires. |

## `source`

| Field | Identity | Required | Meaning |
| --- | --- | --- | --- |
| `schema` | yes | | The schema the source table is in. |
| `table` | yes | | The source table. |
| `filter` | yes | | A SQL condition narrowing the rows in scope. |
| `key_fields` | yes | | The columns identifying a row, in order. |
| `input_fields` | yes | | The columns whose text is sent, in order. |
| `version_strategy` | yes | | How a stale vector is recognized. |
| `version_field` | yes | | The column the strategy reads, where it needs one. |
| `mutable` | no | **yes** | Whether the source changes during the run. |

`filter` narrows both halves of the run, and the second half is worth stating.
The backfill scans only rows the condition matches, and catch-up rereads a
changed row through the same condition — so a row that stops matching is
tombstoned on the next catch-up rather than kept, and a row that never matched
is never sent to the provider even when a write to it produced an outbox event.

There is a case the condition does not reach. A row leaves scope through a
column the outbox does not watch — `UPDATE articles SET published = false`, where
`published` is neither a key, an input field, nor the version — and that write
produces no event at all, so the vector the backfill gave it stays until
something else about the row changes. Put the filter's columns among the input
fields, or expect to re-run the backfill, if a row leaving scope has to lose its
vector promptly.

`version_strategy` accepts:

| Value | Means |
| --- | --- |
| `updated_at` | A timestamp column that moves on every write. Needs `version_field`. |
| `monotonic` | A counter or sequence that only increases. Needs `version_field`. |
| `outbox_sequence` | The outbox's own ordering is the version. |
| `input_hash` | A vector is stale when the text that produced it changed. |

The strategy also decides how two versions of one row are put in order, which is
what stops a late answer overwriting a newer one. `updated_at` compares
instants, `monotonic` and `outbox_sequence` compare numbers, and `input_hash`
records no version and orders nothing — under it a repeated answer is
recognized by its input hash rather than by its age.

**`version_field` has to hold a value the strategy can read.** A `monotonic`
column holding a timestamp, or an `updated_at` column holding a counter, gives
versions that order nothing: a late retry then replaces a newer vector, because
nothing establishes which is newer. Ptah does not guess an order for a value it
cannot read — guessing is what made a shorter rendering of a later instant look
stale — so this is a configuration error with a silent cost, and the pairing is
worth checking when you write the specification.

## `preprocessing`

Every field here is **identity**: each one changes the text that is sent. Four
of the eight are also required, including `truncate` even where
`max_input_bytes` names no cap for it to act at.

| Field | Required | Meaning |
| --- | --- | --- |
| `separator` | | What joins the input fields. |
| `prefix` | | Text prepended to every input. Some models expect one. |
| `null_policy` | **yes** | What a NULL field becomes: `empty`, `skip`, or `refuse`. |
| `empty_policy` | **yes** | What an empty input means: `skip` or `refuse`. |
| `unicode_normalization` | **yes** | `none`, `nfc`, `nfd`, `nfkc`, or `nfkd`. |
| `collapse_whitespace` | | Whether runs of whitespace become one space. |
| `max_input_bytes` | | The cap on one input's size. |
| `truncate` | **yes** | What happens at the cap: `refuse` or `bytes`. |

`null_policy: skip` leaves the field out of the joined text. `refuse` stops the
run on that row. `empty_policy: skip` records the row as a deliberate skip, which
coverage verification counts as accounted for.

## `model`

| Field | Identity | Required | Meaning |
| --- | --- | --- | --- |
| `provider` | yes | | The API shape. `openai-compatible` is what is implemented. |
| `endpoint_class` | yes | **yes** | `local`, `hosted`, or `gateway`. Your declaration, not a measurement. |
| `endpoint` | no | | The base URL. A credential in its userinfo is refused. |
| `identifier` | yes | | The model name sent to the provider. |
| `revision` | yes | | The provider's immutable revision, where it has one. |
| `requested_dimension` | yes | | The dimension asked for, where the provider supports asking. |
| `reported_dimension` | yes | **yes** | The dimension the model produces. |
| `normalization` | yes | **yes** | `none` or `l2`. |
| `pooling` | yes | | The pooling strategy, where the provider exposes one. |
| `credential` | no | | Where the credential is, never what it is. |

`endpoint` is excluded from the identity because moving the same model behind a
different address does not change the vectors. `endpoint_class` is included
because a change of trust boundary is a change worth being a different corpus.

`credential` is a reference: `env:PTAH_EMBED_TOKEN` reads that environment
variable at run time. The value is never written to the run state or to published
evidence.

`endpoint` is held to the same rule, and it is refused rather than accepted with
a warning:

```text
spec.yaml: model.endpoint carries a credential in its userinfo, before the
"api.example.com" host; a key must not appear in project configuration, so put
it in model.credential as env:NAME or file:/path
```

A URL written `https://user:secret@api.example.com/v1` becomes an
`Authorization: Basic` header on every provider request, so it is a credential
that reached the wire through the field with no check on it. It is refused at
the document, which is before any verb reads a row or opens a connection, and
the message names the host rather than the URL so that reporting the problem is
not another copy of it.

## `target`

| Field | Identity | Required | Meaning |
| --- | --- | --- | --- |
| `schema` | yes | | The schema the target table is in. |
| `table` | yes | **yes** | The table the vectors go on. |
| `column` | yes | **yes** | The vector column. Two generations need two columns. |
| `representation` | yes | **yes** | `vector`, `halfvec`, or `sparsevec`. |
| `metric` | yes | **yes** | `cosine`, `l2`, or `inner_product`. |
| `index_method` | yes | | `hnsw` or `ivfflat`. Omit for no index. |
| `index_options` | **no** | | Build options such as `m` and `ef_construction`. |

`index_options` is excluded deliberately: retuning an index trades build cost
against recall over the *same* vectors, so it does not make a different corpus.

Option values must be whole numbers, and option names lower-case identifiers.
PostgreSQL takes no parameter in a `WITH` clause, so anything else is refused by
name rather than escaped.

## `consistency`

| Field | Meaning |
| --- | --- |
| `mode` | `outbox`, `immutable`, or omitted. |
| `paused` | Whether writes are declared stopped. |

Not part of the identity: how changes were captured does not change what the
vectors are. See [Consistency modes](../../concepts/consistency/).

## `policy`

| Field | Meaning |
| --- | --- |
| `require_exact_approval` | A cutover needs an approval bound to the plan digest. |
| `require_signed_approval` | That approval has to be a verified signature rather than a name typed beside the digest. Needs `require_exact_approval`, because a signature is given over one exact plan. |
| `require_consistency_mode` | A cutover is refused when no mode is selected. |
| `allow_accepted_findings` | Whether `cutover --accept-finding` may name a blocking finding to proceed over. |
| `max_plan_age` | How old a plan may be when it is approved. |

## A complete example

```yaml
version: 1
name: articles
source:
  schema: public
  table: articles
  filter: "published = true"
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
preprocessing:
  separator: "\n"
  null_policy: empty
  empty_policy: skip
  unicode_normalization: nfc
  collapse_whitespace: true
  max_input_bytes: 8000
  truncate: refuse
model:
  provider: openai-compatible
  endpoint_class: hosted
  endpoint: https://api.example.com/v1
  identifier: text-embedding-3-small
  revision: "2024-02"
  reported_dimension: 1536
  normalization: none
  credential: env:PTAH_EMBED_TOKEN
target:
  schema: public
  table: articles
  column: embedding_v2
  representation: vector
  metric: cosine
  index_method: hnsw
  index_options:
    m: "16"
    ef_construction: "64"
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
```

## Checking one

`ptah inference plan` resolves the specification against a live database and
reports what it could not establish. It writes nothing, so it is the safe way to
find out whether a specification says what you meant.
