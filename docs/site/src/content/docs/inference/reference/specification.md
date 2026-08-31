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

| Field | Identity | Meaning |
| --- | --- | --- |
| `schema` | yes | The schema the source table is in. |
| `table` | yes | The source table. |
| `filter` | yes | A SQL condition narrowing the rows in scope. |
| `key_fields` | yes | The columns identifying a row, in order. |
| `input_fields` | yes | The columns whose text is sent, in order. |
| `version_strategy` | yes | How a stale vector is recognized. |
| `version_field` | yes | The column the strategy reads, where it needs one. |
| `mutable` | no | Whether the source changes during the run. |

`version_strategy` accepts:

| Value | Means |
| --- | --- |
| `updated_at` | A timestamp column that moves on every write. Needs `version_field`. |
| `monotonic` | A counter or sequence that only increases. Needs `version_field`. |
| `outbox_sequence` | The outbox's own ordering is the version. |
| `input_hash` | A vector is stale when the text that produced it changed. |

## `preprocessing`

Every field here is **identity**: each one changes the text that is sent.

| Field | Meaning |
| --- | --- |
| `separator` | What joins the input fields. |
| `prefix` | Text prepended to every input. Some models expect one. |
| `null_policy` | What a NULL field becomes: `empty`, `skip`, or `refuse`. |
| `empty_policy` | What an empty input means: `skip` or `refuse`. |
| `unicode_normalization` | `none`, `nfc`, `nfd`, `nfkc`, or `nfkd`. |
| `collapse_whitespace` | Whether runs of whitespace become one space. |
| `max_input_bytes` | The cap on one input's size. |
| `truncate` | What happens at the cap: `refuse` or `bytes`. |

`null_policy: skip` leaves the field out of the joined text. `refuse` stops the
run on that row. `empty_policy: skip` records the row as a deliberate skip, which
coverage verification counts as accounted for.

## `model`

| Field | Identity | Meaning |
| --- | --- | --- |
| `provider` | yes | The API shape. `openai-compatible` is what is implemented. |
| `endpoint_class` | yes | `local`, `hosted`, or `gateway`. Your declaration, not a measurement. |
| `endpoint` | no | The base URL. A credential in its userinfo is refused. |
| `identifier` | yes | The model name sent to the provider. |
| `revision` | yes | The provider's immutable revision, where it has one. |
| `requested_dimension` | yes | The dimension asked for, where the provider supports asking. |
| `reported_dimension` | yes | The dimension the model produces. |
| `normalization` | yes | `none` or `l2`. |
| `pooling` | yes | The pooling strategy, where the provider exposes one. |
| `credential` | no | Where the credential is, never what it is. |

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

| Field | Identity | Meaning |
| --- | --- | --- |
| `schema` | yes | The schema the target table is in. |
| `table` | yes | The table the vectors go on. |
| `column` | yes | The vector column. Two generations need two columns. |
| `representation` | yes | `vector`, `halfvec`, or `sparsevec`. |
| `metric` | yes | `cosine`, `l2`, or `inner_product`. |
| `index_method` | yes | `hnsw` or `ivfflat`. Omit for no index. |
| `index_options` | **no** | Build options such as `m` and `ef_construction`. |

`index_options` is excluded deliberately: retuning an index trades build cost
against recall over the *same* vectors, so it does not make a different corpus.

Option values must be whole numbers, and option names lower-case identifiers.
PostgreSQL takes no parameter in a `WITH` clause, so anything else is refused by
name rather than escaped.

## `consistency`

| Field | Meaning |
| --- | --- |
| `mode` | `outbox`, `immutable`, `dual_write`, or omitted. |
| `paused` | Whether writes are declared stopped. |

Not part of the identity: how changes were captured does not change what the
vectors are. See [Consistency modes](../../concepts/consistency/).

## `policy`

| Field | Meaning |
| --- | --- |
| `require_exact_approval` | A cutover needs an approval bound to the plan digest. |
| `require_signed_approval` | That approval has to be a verified signature rather than a name typed beside the digest. Needs `require_exact_approval`, because a signature is given over one exact plan. |
| `require_consistency_mode` | A cutover is refused when no mode is selected. |
| `allow_accepted_findings` | Findings that may be present and not block. |
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
