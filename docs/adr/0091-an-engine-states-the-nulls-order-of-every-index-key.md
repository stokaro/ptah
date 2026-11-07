# ADR 0091: An engine states the NULLS order of every index key

Status: Proposed

Contract: 25.28

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0052](0052-inspected-indexes-reach-the-host-and-index-changes-reach-the-table-owner.md),
[ADR 0057](0057-a-provider-spells-a-declared-index-kind-as-it-reports-a-held-one.md)

## Context

Each key part of an index sorts NULLs first or last. A part that says nothing
takes the engine's default for its direction, and engines disagree:
PostgreSQL sorts NULLs last in an ascending key and first in a descending
one, SQLite the other way round.

Under `schema.observed-indexes` the host compares a declared index with the
held one by its uniqueness, key columns and covered columns, and with
`schema.index-methods` by its kind. It does not compare the NULLS order: a
part's `nulls` may be empty on either side, and the host cannot tell an empty
order from the default without the engine's rule.

Ptah's built-in Spanner engine at d59a3b63d compares it. Its comparison rule
resolves each part's order with Spanner's default and plans a changed order as
a drop and a create, measured on the PGAdapter emulator v0.56.2. Spanner also
reports explicit orders the declaration never wrote: the index it builds for
a foreign key reads back as `("author_id" NULLS FIRST)`. Through a provider,
a declaration that changes only a key's NULLS order plans nothing, and the
index keeps the old order.

## Decision

### Both sides state the order

The two-way feature `schema.index-nulls-order` has an engine state the NULLS
order of every key part on both sides. A read states it on each part of each
held index, in `CapturedIndexPart.nulls`, and lists every key part in
`parts`. Table preparation states it for each key part of each declared
index, in the new field `PreparedIndex.nulls`, filling a part that states no
order with the engine's default for its direction. Both spell it `FIRST` or
`LAST`, so the host compares the two exactly. The provider and the engine list
it, and the host lists it in its host features. The engine needs
`schema.observed-indexes` and `schema.prepare-tables`.

The prepared orders serve the comparison alone: the declaration keeps the
order its source wrote.

### Preparation states each declared index once

`PreparedTable.indexes` holds one `PreparedIndex` for each declared index, in
the declared order, under `schema.index-methods` or this feature. Without
index methods an entry's `method` stays empty.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's,
without observed indexes or without table preparation. Preparation validation
refuses NULLS orders without negotiation and, with it, an index whose orders
do not number its key parts, a spelling other than `FIRST` or `LAST`, and a
method without index methods. Inspection validation refuses, under the
feature, a held index that lists no part and a part without `FIRST` or
`LAST`. A read without the feature is checked as before it.

## Alternatives considered

**Compare the order where both sides state it.** The host needs no contract
for that, and it misses the change that matters: a declaration that omits the
order against a held index that states the other one. Only the engine knows
which order the omitted one is.

**Describe the engine's default in its description.** An engine could state
that NULLs sort high or low, and the host could resolve both sides. That
holds a rule in the host that the engine owns, and it says nothing of an
engine whose default depends on more than the direction. The preparation
already states the declared index's kind the same way.

**Let the owner of the table compare the whole index.** The host could hand
every index of an owned table to its owner. The comparison of uniqueness, keys
and covered columns is common and already works; only the order needs the
engine's knowledge.

## Consequences

- A key's NULLS order is compared on the provider path as the built-in engine
  compares it, and a change plans a drop and a create of the index.
- A provider that lists the feature states an order for every key part it
  reads and every one it prepares, the default included.

## Acceptance evidence

- SDK unit tests: `TestIndexNullsOrder_HappyPath`,
  `TestPreparedIndexNullsOrder_FailurePath`,
  `TestPreparedIndexMethodWithoutTheFeature_FailurePath`,
  `TestInspectedIndexNullsOrder_FailurePath` and
  `TestIndexNullsOrderDescription_FailurePath` in `provider-sdk/provider`.
- The host adoption compares the prepared order of each declared key with the
  order a read reports.
