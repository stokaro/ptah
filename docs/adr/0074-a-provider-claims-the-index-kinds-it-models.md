# ADR 0074: A provider claims the index kinds it models

Status: Proposed

Contract: 25.24

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0036](0036-a-provider-claims-yaml-entry-keys-and-hcl-blocks.md),
[ADR 0037](0037-a-provider-encodes-the-source-it-decodes.md),
[ADR 0057](0057-a-provider-spells-a-declared-index-kind-as-it-reports-a-held-one.md),
[ADR 0068](0068-an-inspected-index-carries-its-own-facets.md),
[ADR 0072](0072-a-provider-claims-the-platform-group-of-a-table.md)

## Context

A Go `ptah:schema:index` directive, a YAML index entry and an HCL `index`
block name an index's kind with `type`, and YDB has kinds the common index
model has no place for:

```go
//ptah:schema:index name="docs_emb" fields="gen,emb" include="body" type="vector_kmeans_tree" distance="cosine" vector_type="float" vector_dimension="3" levels="1" clusters="2"
```

Ptah's built-in YDB engine read such a declaration as a common index with an
owner facet for its settings. Measured with the host at 74de0b65b on YDB
25.1.4.7 and 26.2.1.14:

- A vector index from Go, YAML or HCL creates `INDEX docs_emb GLOBAL USING
  vector_kmeans_tree ON (gen, emb) COVER (body) WITH (distance=cosine, ...)`,
  and the next run is synced. A changed setting plans `DROP INDEX` and
  `ADD INDEX`; an index the source stops declaring plans `DROP INDEX`.
- On 26.2.1.14, a full-text index (`type="fulltext_relevance"
  tokenizer="standard"`) and a column table's local index (`type="bloom_filter"
  false_positive_probability="0.01"`) from Go and YAML are created and the next
  run is synced. A renamed local index plans `RENAME INDEX`, a changed setting
  `DROP INDEX` and `ADD INDEX`. 25.1.4.7 refuses both by capability.
- `ptah-compat schema inspect` writes a vector index as `type = "GLOBAL USING
  vector_kmeans_tree"` with its settings, and the document applied back is
  synced. It writes a full-text or local index without its options, and the
  full-text document applied back is refused: `a YDB full-text index requires
  analyzer settings in WITH`.

The YDB provider models these kinds outside the common index: its read puts
vector and full-text indexes in the table's native index set and local indexes
in the column table's storage, and its planner creates, changes and drops them
there.

Through the provider the same sources fail. A Go or YAML index with
options is refused at decode; a vector index without options, from Go, and an
HCL block with `type = "vector_kmeans_tree"` are refused at table creation:
`it is a vector_kmeans_tree index, and this provider creates a table with its
global indexes`; a local index is refused with `does not support
DeclaredTable.indexes`. The shared frontends build a common index from every
declaration, `type` included, and a provider claim
([ADR 0036](0036-a-provider-claims-yaml-entry-keys-and-hcl-blocks.md)) can
only add facets that the index owns. It cannot take the declaration out of the
common model, nor put it in a facet of the table, where the provider's read and
planner keep the index. The built-in engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so the
provider has to read and write these declarations.

## Decision

### An engine names the index types it owns

`source.index-kinds` is a two-way feature. The provider and each engine that
claims a type list it, beside `source.decode`. Contract 25.24 adds:

```proto
message Engine {
  // ...
  repeated SourceIndexKind index_kinds = 25;
}
message SourceIndexKind {
  repeated string types = 1;
  string selector = 2;
  repeated string models = 3;
}
message SourceFragment {
  // ...
  SourceIndexKind index_kind = 13;
}
```

A claim lists types in the form a declaration's type is compared in: ASCII
lower case, trimmed, with each run of white space read as one space
(`provider.NormalizeIndexType`). A type is claimed once in an engine. The
models are desired models of the engine, and a fragment's values are facets of
the index's table. The YDB provider claims `vector_kmeans_tree`,
`fulltext_plain` and `fulltext_relevance` with its native index set, and
`bloom_filter`, `bloom_ngram_filter` and `min_max` with its column storage,
each also in its `global using` or `local using` spelling.

### The frontend sends the declaration whole

For a declaration whose type a claim names, the Go, YAML and HCL frontends
build no common index. The declaration reaches the provider as one fragment
that carries the claim and no other claim. Its binding is the index's table,
and its subject the index: the table's schema, the table as the only path
component, and the index's name. The frontend's
own part of the declaration is the fragment's inherited attributes, each
present only when the declaration writes it, in this order: `type`, as written;
`columns` and `include`, sequences of column names; `unique`, true; `comment`
and `ops`, text. The engine's own attributes travel in the content, as a
mapping: those the engine claims on the format's index declaration, its
INDEX-scope Go claim on `ptah:schema:index`, its INDEX-scope YAML keys or its
`INDEX_ATTRIBUTES` HCL claim, in each format's value shape.

A declaration of another type keeps the claims it has.

The frontend refuses, by name, an attribute of its own the fragment cannot
carry, such as `where`, `nulls_distinct`, an `on` block or a platform group:
the provider would never receive it. A model of the claim that the document's
format does not take is refused at the declaration, since the provider could
not decode it. Without the feature negotiated nothing changes: the frontends
build the common index.

### What a document states

The models keep the rules of their formats. A Go or YAML document that claims
a model completely states it for every table it declares, so a table that
writes no index of a claimed type states that it has none. An HCL document
claims a provider's model only through its header
([stokaro/ptah#5329](https://github.com/stokaro/ptah/pull/5329)), and a model
that lists no HCL is described only for the tables that write an index of the
claim ([ADR 0072](0072-a-provider-claims-the-platform-group-of-a-table.md)).

### Export writes the declaration back

An encoded fragment, or a part of a group, may carry an index-kind claim. Its
shape is the decoded fragment's, without positions, and its values are facets
of its table. The host writes it as the frontend writes a common index: a Go
`ptah:schema:index` directive in the table's struct, after the table's own
indexes, and an HCL `index` block among the table's indexes by name, with the
engine's attributes where the frontend writes an owner's attributes of an
index. A facet such parts write in part, such as a column table's storage of
which HCL holds only the local indexes, may also be reported as a loss of an
HCL reply. The host writes no YAML through a provider, so YAML export is not
part of the decision.

### What the SDK enforces

The SDK refuses a claim on an engine that does not list the feature, a
feature without a claim, a type that is empty, not normalized or claimed
twice, and a model that is not a desired model of the engine. Both transport
boundaries refuse an index-kind fragment of a claim the engine does not hold,
with another selector, in a SQL document, beside another claim, with models
outside the document's scope, without a table binding and an index of that
table as its subject, with an inherited attribute that is unknown, out of
order, or of the wrong value shape, without a claimed `type` or `columns`, and
with a content key the engine does not claim on the format's index
declaration. A decoded value of such a fragment must be a facet of the index's
table. Without the feature negotiated, the SDK removes the claims from the
description the host receives.

## Alternatives considered

**The provider models the kinds as common indexes.** The provider would report
a vector, full-text or local index as an `ObservedIndex` and keep its settings
in an index facet ([ADR 0068](0068-an-inspected-index-carries-its-own-facets.md)),
as the built-in engine did. The host's common index comparison and planner
would then decide these indexes: a rename in place
([ADR 0060](0060-a-table-owner-renames-an-index-in-place.md)) that YDB answers
by building a vector index again, a column table's local index the common
model places on the table, kinds the comparison spells by the provider's
method ([ADR 0057](0057-a-provider-spells-a-declared-index-kind-as-it-reports-a-held-one.md))
and then rebuilds. Each YDB rule would have to reach the host's common index
code as a policy. The provider's read, its YQL source and its planner already
keep the indexes outside the common model. Rejected.

**An index-type list on each format's index claim.** `GoSourceDirective`,
`YAMLSourceKey` and `HCLSourceBlock` would each carry the types. A YAML claim
is one key of an entry and an entry has no claim of its own, so a whole entry
would have no claim to carry, and one decision would have three spellings.
Rejected.

**The format's own syntax as the content.** The fragment would carry the
directive's attributes, the entry's mapping or the block as written. The
provider would read three grammars of the frontend's index, comma-separated
fields in Go, sequences in YAML, references and `on` blocks in HCL, and an
export would have to write that syntax, which the encoding contract leaves to
the host ([ADR 0037](0037-a-provider-encodes-the-source-it-decodes.md)).
Rejected: the frontend reads its own part, and the provider reads one shape.

**A provider facet on the common index, moved by the provider's planner.** The
host would build the common index, and the provider would read it from the
declared table and plan it natively. The common index would still be compared
with the read, which reports no such index, and planned as a common change on
every run. Rejected.

## Consequences

- Contract 25.24 adds one Engine field, one message, one SourceFragment field
  and a two-way feature, and lets a loss name a value an index-kind part
  writes in part. It is the 25.24 batch.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- With the feature negotiated, a provider reads the index declarations of its
  claimed types from Go, YAML and HCL, and writes them back in Go and HCL.
  Without it, nothing changes.
- A declaration of a claimed type that writes an attribute of the frontend's
  the fragment cannot carry is refused, where the built-in engine read some of
  them.
- The provider decides, for a Go or YAML source that writes none of a table's
  indexes of its types, whether the table holds none; the YDB provider drops
  them, as the built-in engine did.

## Acceptance evidence

- SDK tests accept an index-kind fragment and its decoded facet, an empty
  content and a Go document's attribute claim; refuse each malformed claim,
  fragment, value and encoded part above; accept a partial loss of a value an
  index-kind part writes; and remove the claims when the host does not list
  the feature.
- Through the SDK fixture provider's `index-kinds` program, a tree index from
  a Go directive, a YAML entry and a top-level entry, and an HCL block reaches
  the provider as one fragment, builds no common index, and becomes the
  table's tree index set; an attribute the fragment cannot carry is refused in
  each format; `goschematogo` and `ptah schema export --to hcl` write the set
  back as the same directives and blocks, which read back to the same set; and
  a partial loss is a warning at the table.
- Through the YDB provider on 25.1.4.7 and 26.2.1.14, the declarations above
  create the same indexes as the built-in engine, the next run is synced, a
  changed setting, a dropped index and a renamed local index plan as the
  built-in engine planned them, and `ptah-compat schema inspect` writes a
  vector index the document applied back finds synced
  ([stokaro/ptah-ydb-plugin#206](https://github.com/stokaro/ptah-ydb-plugin/issues/206)).
