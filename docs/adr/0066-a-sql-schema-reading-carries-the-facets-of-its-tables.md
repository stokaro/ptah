# ADR 0066: A SQL schema reading carries the facets of its tables

Status: Proposed

Contract: 25.21

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md),
[ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md)

## Context

An engine whose dialect the shared SQL grammar cannot read, such as YDB's YQL,
declares the `DOCUMENT` mode: the provider reads every SQL schema document of
the load whole and answers `ReadSQLSchema` with a `SQLSchema` reply. The reply
has a home for the common objects and for the provider's own objects
(`native_objects`, each owned by its own kind). It has no home for a facet: a
setting or a set of children that belongs to a table, a column or an index.

A YAML or Go source of the same engine carries such a value as a facet, owned
by its table. The YDB provider carries a table's changefeeds that way, with
their topics' consumers, and so it does a row table's TTL, partitioning,
column families and bloom filter, a column's family, and a table's native
index settings. A YQL document declares the same things in its own statements:
`ALTER TABLE ... ADD CHANGEFEED`, `ALTER TOPIC <table>/<changefeed> ADD
CONSUMER`, `CREATE TABLE ... WITH (TTL = ...)`, `FAMILY`, `INDEX ... WITH`. The
YDB provider's YQL reader parses all of them and has to refuse each, because
the reply cannot say it. The host refuses `DeclaredTable.features` in a reply
for the same reason.

Measured with Ptah master ba83f5966 and `stokaro/ptah-ydb-plugin` master
07068a0 on local YDB 26.2.1.14 and 25.1.4.7, with the same results on both
lines. Each step is a YQL file applied with `ptah schema apply --schema-file
schema.sql --schemas <dir>`, then planned again. The table is `CREATE TABLE
events (id Int64 NOT NULL, PRIMARY KEY (id))` and the changefeed is `ALTER TABLE
events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON',
retention_period=Interval('PT12H'))`.

| Step | The provider path | The built-in YDB engine |
| --- | --- | --- |
| the table, the changefeed, and consumer `worker` on its topic | refused while reading: ``changefeed `updates` of table `dir/events` has no home in a SQL schema reading: the host refuses the owned objects of a table in a SQL schema reading at position 65 (line 1, column 66)`` | `CREATE TABLE`, `ADD CHANGEFEED`, `ADD CONSUMER worker`; then synced |
| consumer `audit` (important) instead of `worker` | refused the same way | `DROP CONSUMER worker`, `ADD CONSUMER audit WITH (important = TRUE)`; then synced |
| the changefeed without consumers | refused the same way | `DROP CONSUMER audit`; then synced |
| the table alone | not reached | `DROP CHANGEFEED updates`, with a comment that unread records are lost; then synced |

The built-in YDB engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so the
provider becomes the only reader of a YQL schema.

## Decision

### The reply carries facets

`schema.sql-facets` is a two-way feature. An engine that declares the
`DOCUMENT` mode lists it beside `schema.read-sql`, the provider lists it, and
the host lists it in its host features. Contract 25.21 adds one field and one
trace collection:

```proto
message SQLSchema {
  // ...
  repeated Envelope facets = 14;
}
message SQLObjectTrace {
  enum Collection {
    // ...
    FACETS = 9;
  }
}
```

A facet is the value a YAML source of the engine would carry for the same
declaration: a DESIRED value of a provider-owned model whose source formats
include `SQL`. Its owner is a table of `tables`, named by kind `table`, its
schema and its name, or a column or an index of one, named by kind `column` or
`index` with the table as the only path component. These are the three owners
a YAML facet may have. An owner holds at most one facet of a kind, and each
facet has one trace that points at a statement that declares it.

The reply's `coverage` covers each facet's kind, as it covers the kinds of
`native_objects`. A kind the load claims completely and a table it declares
without the facet together say that the table has none: a YQL table without
`ADD CHANGEFEED` is a table with no changefeeds, as a YAML table without
`changefeeds:` is.

### What the SDK enforces

Both transport boundaries refuse facets that the request did not negotiate
(the engine offers the feature and the request's target features list it), a
facet of a model the engine does not offer in SQL or the reply does not cover,
an owner that is not a declared table, column or index of the engine under
the request's identifier rules, two facets of one kind on one owner, and a
facet without its trace. An incomplete reply carries no facets.

### What the host does

The host normalizes each facet with its owner, as it normalizes the reply's
native objects, and places it on its table, column or index with the engine as
its target, where a YAML source places it. A comparison, a plan and an export
then read a YQL table as they read a YAML one.

### What the YDB provider sends

The provider lists the feature for its engine and sends one facet of
changefeeds for each table the load gives a changefeed, with the consumers
`ALTER TOPIC ... ADD CONSUMER` adds to their topics. Its claim covers the
changefeed model, and the model lists `SQL` among its source formats. A host
that does not list the feature gets the refusal it gets today, which names the
feature.

### What stays open

The field carries a row table's settings, a column's family and an index's
settings too, so none of them needs another contract change. The YDB
provider's YQL reader still refuses them by name: the TTL, partitioning,
column families, column storage, and an index's partitioning and vector
settings. Each needs the reader to build the facet the YAML source builds, and
its own measurement against the built-in engine.

## Alternatives considered

**`DeclaredTable.features` in the reply.** The field exists, and a common view
already carries its facets this way. But it holds normalized comparison
records with their own coverage, a planning shape, beside the reply's single
claim, so the two could disagree. It also cannot hold a column's or an index's
facet unless a column record names its table by another rule. Every released
host refuses it in a reply. Rejected.

**A table's facet in `native_objects`.** A native object is owned by its own
kind, and the SDK and the host tell an object from a facet by that rule.
Accepting a table owner there would give an existing field a second meaning.
Rejected.

**A native kind for the changefeed.** The provider could report a changefeed
as an object of its own kind in a SQL reading only. Its comparison and planning
take changefeeds as a facet of their table in every other format, so one
declaration would have two representations. Rejected.

**The host parses YQL.** The `DOCUMENT` mode exists because the shared grammar
cannot read the dialect. Rejected.

## Consequences

- Contract 25.21 adds a field, a trace collection and a two-way feature. The
  same minor carries the additions of ADR 0065, ADR 0067 and ADR 0068.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- Without the feature nothing changes: a provider refuses the construct as it
  does today.
- With it, a YQL schema declares a table's changefeeds and their consumers as a
  YAML schema does, and the provider plans and applies them.

## Acceptance evidence

- SDK tests accept facets of a table, a column and an index, and refuse facets
  that an engine, a provider or a request did not negotiate, and each
  malformed facet above, at both transport boundaries.
- Through the SDK fixture provider, the host places a facet a SQL document
  declares where a YAML source places the same facet, for a table and a
  column.
- Through the YDB provider on 25.1.4.7 and 26.2.1.14, the four steps of the
  table above plan what the built-in engine plans, apply, and plan nothing
  afterwards.
