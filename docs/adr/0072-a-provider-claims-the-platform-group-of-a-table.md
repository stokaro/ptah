# ADR 0072: A provider claims the platform group of a table

Status: Proposed

Contract: 25.22

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0036](0036-a-provider-claims-yaml-entry-keys-and-hcl-blocks.md),
[ADR 0037](0037-a-provider-encodes-the-source-it-decodes.md)

## Context

An HCL table carries the settings of one target in a `platform` block named
for that target:

```hcl
table "items" {
  platform "ydb" {
    override "row_deletion_column" { value = "ts" }
    override "row_deletion_interval" { value = "P30D" }
  }
}
```

The HCL frontend reads every `platform` block into `Table.Overrides`, and the
built-in engines' owners read their keys from there. The built-in YDB engine
reads a table's TTL from these keys and writes it back as the same block.
Measured on YDB 26.2.1.14 and 25.1.4.7 with the host's `ptah schema apply` and
`ptah schema inspect --format hcl`:

- The document above creates `CREATE TABLE ... WITH (TTL = Interval("P30D") ON
  ts)`, and a changed interval or unit plans `ALTER TABLE ... SET (TTL = ...)`,
  on a row table and on a column table alike.
- `schema inspect` writes the block for a row table, with
  `row_deletion_unit` for an integer column, and for a column table whose TTL
  only deletes. A setting HCL has no spelling for, such as a partitioning
  count or a column table's storage, is a warning at the table, and the TTL is
  still written.
- The document applied back plans nothing, and so does the same document
  without the block: a table that writes no `platform "ydb"` block leaves its
  TTL as the database holds it.

On the provider path the same document is refused: `unsupported feature: the
experimental provider contract does not support Table.Overrides`. An HCL claim
([ADR 0036](0036-a-provider-claims-yaml-entry-keys-and-hcl-blocks.md)) stands
at the top level, in a table, or in the attributes of the frontend's `table`,
`index`, routine and `materialized` blocks. None of them names a labeled
`platform` block, so the YDB provider cannot claim its group, and the host
cannot send it. HCL export through the provider writes no TTL and warns that
the provider's table facet is not represented in HCL. YAML has the
counterpart already: a `TABLE` key `platform.ydb` claims the `ydb` group of a
table's `platform` key. The built-in engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so the
provider has to read and write the HCL group too.

## Decision

### A claim site for a table's platform group

`source.hcl-platform` is a two-way feature. The provider and each engine that
claims a platform group list it, beside `source.hcl`. Contract 25.22 adds one
site:

```proto
message HCLSourceBlock {
  enum Site {
    // ...
    TABLE_PLATFORM = 7;
  }
}
```

A `TABLE_PLATFORM` claim reads the `platform` block of a `table` whose label is
the claim's `type`. The `type` is the engine's ID, so an engine claims its own
group and no other engine's. Each `override` of the group is an attribute of
the claim: its label is the attribute's name and its `value` the attribute's
value. Every attribute is a `STRING`, because an override's value is a string.
The claim takes no naming, children, declared type, grant kind or inherited
attributes.

### What the host sends

The frontend claims a table that writes the group: a block whose label is the
claim's type or one of the engine's aliases. Two such blocks in one table are
one group, and a key written twice is refused, as the frontend refuses it in
the groups it keeps. An `override` the claim does not list is refused at its
block, naming the keys the claim lists, and one that differs only in case
names the listed key. A claimed group leaves `Table.Overrides`; every other
group stays there for its readers. The fragment is bound to the table, has no
subject, and carries the overrides as a mapping in the order the claim lists
them.

### What a document states

A claim's models are desired models of the engine, and need not list HCL among
their source formats. An HCL document's scope is the engine's desired models
that list HCL and the models of its `TABLE_PLATFORM` claims. A model that does
not list HCL is one HCL does not describe for every table: a table that writes
no group of the engine states nothing about what the group carries. The host
reads such a model's kind-wide knowledge as uninspected, as it reads a model
the format cannot describe for every registered engine, and keeps the
provider's subject knowledge for each table whose group it decoded. A YDB
table without the block therefore keeps the TTL it holds, as on the built-in
engine.

The models stay as they are when the host does not list the feature: only the
claim goes.

### What the provider writes

The provider writes the part of a facet a group spells as a `TABLE_PLATFORM`
fragment bound to the facet's table, with the overrides as a mapping. The host
writes it as the table's `platform` block of the claim's type, among the
table's other `platform` blocks and in the same layout: keys sorted, each value
quoted. An override the table's other groups already hold is refused.

A platform group spells part of a facet: a YDB table's TTL, and not the
partitioning or the column storage beside it. Under the feature, a loss of an
HCL reply may name a value a `TABLE_PLATFORM` part writes. The value is written
in part, and the loss says what the document does not hold of it. The host
reports it where it writes the table, as it reports a value it does not write.

A document never claims more of a model that lists no HCL than the tables it
writes, so no header narrows it: a request that knows such a model less, as a
read of selected directories does, needs no `ptah:not-described` limit. A
model that lists HCL keeps the rule `source.limits` sets for it.

### What the SDK enforces

The SDK refuses a `TABLE_PLATFORM` claim on an engine that does not list the
feature, a claim whose type is not the engine's ID, an attribute that is not a
`STRING`, and a naming, a child, a declared type, a grant kind or an inherited
attribute on it. Both transport boundaries refuse a fragment of the site that
is not bound to a table, that has a subject, or whose content is not a mapping
of the claim's attributes. A loss that names a written value is refused unless
the format is HCL, the feature is negotiated and a `TABLE_PLATFORM` part writes
the value. Under `source.limits`, an encoding request whose knowledge is
narrowed needs a limit only for a model that lists the request's format.
Without the feature negotiated, the SDK removes the engine's `TABLE_PLATFORM`
claims from the description the host receives.

## Alternatives considered

**The provider reads `Table.Overrides`.** The common table wire would carry a
table's overrides, and the provider would read the keys it knows. The keys
would reach the provider outside the source contract: no claim says which keys
it reads, a key nobody reads stays silent, the overrides of every target
travel, and export needs a second path. Rejected.

**Owner attributes on the `table` block.** A `TABLE_ATTRIBUTES` claim reads
`row_deletion_column = "ts"` already. The built-in engine neither reads nor
writes that spelling, so every document it wrote would be refused through the
provider, and a target's settings would leave the block HCL keeps them in.
Rejected.

**The model lists HCL, and negotiation removes the format.** Without the
feature, the host would receive the model without HCL, so an older host keeps
its behavior. Value models travel in source requests and coverage records, and
each side compares a model it receives with the one it declares. A model that
negotiation changes differs from the provider's own copy, and the provider
refuses the host's requests. A model that lists HCL also states complete
knowledge for every table, so a table without the block would be read as
having no TTL. Rejected: the claim brings its models into scope.

**A loss is for a whole value.** A facet would be written whole or lost. A YDB
column table's facet always holds its storage, which HCL cannot spell, so its
TTL would never be written; the built-in engine writes it and warns about the
storage. Rejected.

**Claim sites for column and index groups too.** The frontend reads `platform`
blocks in columns and indexes as well. No provider needs them: the YDB
provider reads a table's TTL only. Deferred until one does.

## Consequences

- Contract 25.22 adds an enum value and a two-way feature, and lets a loss of
  an HCL reply name a value a platform part writes.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- With the feature negotiated, a provider reads and writes a table's platform
  group of its engine, and a table without the group keeps what the group would
  set. Without it, nothing changes: a document with the group is refused.
- A provider that claims a group sees an `override` it does not list refused
  by the frontend, in the frontend's words.

## Acceptance evidence

- SDK tests accept a negotiated `TABLE_PLATFORM` claim and refuse each
  malformed claim, fragment and loss above, and the description without the
  feature carries no such claim and the same models.
- Through the SDK fixture provider, an HCL document's group reaches the
  provider as a fragment bound to its table, the table without a group has no
  value and uninspected kind knowledge, an unlisted override is refused, and
  `ptah schema export --to hcl` writes the provider's part as the group, with
  the partial loss as a warning.
- Through the YDB provider on 26.2.1.14 and 25.1.4.7, the document above
  creates the TTL, `ptah schema inspect --format hcl` writes the block for a
  row table, an epoch TTL and a column table, and the document applied back
  plans nothing, with and without the block.
