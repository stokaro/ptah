# ADR 0068: An inspected index carries its own facets

Status: Proposed

Contract: 25.21

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0052](0052-inspected-indexes-reach-the-host-and-index-changes-reach-the-table-owner.md),
[ADR 0057](0057-a-provider-spells-a-declared-index-kind-as-it-reports-a-held-one.md),
[ADR 0060](0060-a-table-owner-renames-an-index-in-place.md)

## Context

A Go index directive, a YAML index entry and an HCL index block can state a
global YDB index's automatic partitioning and read replicas:

```go
//ptah:schema:index name="by_body" fields="body" auto_partitioning_by_load="ENABLED" auto_partitioning_min_partitions_count="2"
```

Ptah's built-in YDB engine reads these attributes into a facet of the index,
compares it with the facet its read gives the index, and writes `ALTER TABLE
... ALTER INDEX ... SET (...)` for a difference, after `CREATE TABLE` or `ADD
INDEX` for a new index. Its `ptah introspect` writes the same attributes on
`ptah:schema:index`, and its HCL inspect writes them in the `index` block, so
an export applies back with nothing planned. Measured on YDB 26.2.1.14 and
25.1.4.7 with the host at ba83f5966.

The YDB provider claims the attributes, since the frontend would otherwise
drop them, and refuses every one at decode. A source's claimed index
attributes reach the provider as a fragment whose subject is the index, and
the host attaches the decoded value to that index: the facet must be owned by
the index. The host already compares index facets and carries them to a
table's owner in planning and creation. What it cannot do on the provider path
is read the other side: an inspected `Table` carries facets owned by the table
and by its columns, and `ObservedIndex` has no facets. A declared index facet
would be compared against nothing, and a changed setting of an index the table
holds would plan nothing.

The provider keeps an index's settings in its table's native index facet.
That keeps the index common and the rename of
[ADR 0060](0060-a-table-owner-renames-an-index-in-place.md) possible, but the
native facet is owned by the table, so a source's index facet has no operand
to compare with.

## Decision

### An inspected index carries the facets its owner observed

`schema.observed-index-facets` is a two-way feature. The provider and each
engine that reports index facets list it, and the engine also lists
`schema.observed-indexes`, which carries the index. Contract 25.21 adds one
field:

```proto
message ObservedIndex {
  // ...
  repeated Envelope facets = 21;
}
```

Each facet is owned by its index: kind `index`, the table's engine, database
and schema, the table as the only path component, and the index's name, which
`provider.IndexReference` builds from the table's identity. That is the subject
a source's index fragment names, so the declared and the observed facet have
one owner. The SDK places the facets of a table's indexes after its columns'
facets in the values the host normalizes, in the order the indexes are listed.

The host reads them into the index of its catalog, beside the facets of tables
and columns. Planning, creation and source encoding already carry index facets,
so the comparison compares one owner's facet on both sides, the planner
receives a changed index facet as a change of its table, and an export writes
an observed index facet back through the provider's index claims.

### A rename keeps an index's facets

With the feature negotiated, the host treats every facet of a provider's index
as one a rename in place keeps. The rename is the owner's own step
([ADR 0060](0060-a-table-owner-renames-an-index-in-place.md)), and YDB's
`RENAME INDEX` keeps the index's settings. The comparison pairs an index that
carries facets as it pairs one that carries none, and compares the facets of a
renamed index under the name the declaration gives it: the observed facet
reaches the comparison and the planner with the new name as its subject, while
the value keeps the owner the read gave it. The owner reads the facet as the
renamed index's.

Without the feature an index that carries a facet is not paired, as before,
because the host cannot say the rename keeps it.

### What the SDK enforces

Both transport boundaries refuse an inspected index facet unless the feature
is negotiated: the provider and the engine list it, and the snapshot's
`target_features` list it. They refuse a facet owned by another index, by the
table, by a column or by another table's index, and an engine that lists the
feature without the provider's or without observed indexes.

## Alternatives considered

**The provider compares the declared index facet with its table's native
facet.** The native index facet holds every common index's description,
settings included, and the comparison request carries it. The two operands
would be facets of different owners and different models, which the host's
coverage, rename pairing, reverse planning and drift reports all treat as two
unrelated facets: the declared one would read as a facet the database lacks,
and the native one as a facet the declaration leaves out. A Go export would
have to write an index directive's attributes from a value owned by the table,
which the encoding contract does not allow. Rejected.

**`ObservedIndex.storage_parameters` and `DeclaredIndex.storage_parameters`.**
Both are string maps the common model already compares, and a provider could
report its settings there. The host's bundled decoder fills them only for the
built-in engines' owners, the comparison would plan a changed setting as a
rebuild of the index, which YDB changes in place, and a provider's own models
would bypass its normalization and its rules. Rejected.

**An index facet carried on the table, owned by the index.** `Table.facets`
could take values owned by its indexes. The table's list would then hold
values of two kinds of owner, which every reader of it would have to split, and
a provider without observed indexes could send a facet for an index no read
reports. Rejected.

## Consequences

- Contract 25.21 adds one field and a two-way feature. The same minor carries
  the additions of ADR 0065, ADR 0066 and ADR 0067.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- With the feature negotiated, a provider compares, creates, alters and
  exports an index's own settings as a table's, and a renamed index keeps them.
  Without it, nothing changes: an inspected index carries no facet.
- A provider that reports index facets owes the host its rename rule: a facet
  its rename does not keep must not be reported as an index facet.

## Acceptance evidence

- SDK tests accept an index facet owned by its index and keep it through the
  wire, refuse one that either side did not negotiate and one owned by another
  index, by the table, by a column or by another table's index, refuse a
  repeated facet, and refuse an engine that lists the feature without the
  provider's or without observed indexes. A host that does not list the feature
  receives a description without it.
