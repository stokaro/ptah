# ADR 0060: A table owner renames an index in place

Status: Proposed

Contract: 25.17

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md),
[ADR 0052](0052-inspected-indexes-reach-the-host-and-index-changes-reach-the-table-owner.md)

## Context

The host's comparison pairs an index a table holds with one the declaration
states under another name, on the same table and with the same definition,
and records the pair in `SchemaDiff.IndexesRenamed`. It pairs only on a target
with the capability `index_rename`. The built-in YDB engine has it: `ALTER
TABLE ... RENAME INDEX` keeps the index, its kind, its covered columns and its
partitioning, measured on YDB 25.1.4.7 and 26.2.1.14, where a drop and a
create builds the index again from every row of the table. YDB refuses the
rename while the feature flag `EnableMoveIndex` is off (`Move index is not
supported yet`), so the engine reports the capability only with the flag on.

On the provider path an index change of a table the provider owns is a common
step the owner claims
([ADR 0052](0052-inspected-indexes-reach-the-host-and-index-changes-reach-the-table-owner.md)).
The step reads the table and creates or drops one index, and carries no
operand. There is no step for a rename, so the host's provider planner refuses
`SchemaDiff.IndexesRenamed`. The YDB provider therefore must not report
`index_rename`, and every rename it plans is a drop and a create: measured
through the host with the YDB provider on 26.2.1.14, a renamed index plans two
`row-table-alter-operation` steps and the index is built again. The built-in
engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so the
provider has to rename in place too.

## Decision

### A rename is a common step with its own operand

`planning.index-renames` is a two-way feature. The provider and each engine
that renames list it, and the engine also lists `schema.observed-indexes`,
through which the host compares the indexes it pairs. Contract 25.17 adds one
field and one message:

```proto
message CommonPlanningStep {
  // ...
  CommonIndexRename renamed_index = 6;
}
message CommonIndexRename {
  string from = 1;
  string to = 2;
}
```

For each pair on a table the provider owns and the plan keeps, the host sends
one common step. Its parent is the table, its operand names the index as the
table holds it (`from`) and as the declaration names it (`to`), and its
effects read the table, drop the index subject `from` and create the index
subject `to`. The owner claims the step by rewriting it to its own operation,
as it claims an index addition or removal; an unclaimed step refuses the plan.
The operation is the owner's: the YDB provider writes `RENAME INDEX`, which
keeps every setting the index holds.

The effects are the footprint of a drop of one name and a create of another.
A step on the same table that reads or changes either index orders against it
through the plan graph, without a rule of its own.

### What the SDK enforces

Both transport boundaries refuse a step with `renamed_index` unless the
feature is negotiated: the engine lists it in the negotiated description, and
the request lists it in `target_features`. They also refuse the step when it
has no parent or another operand beside the rename, when it names one index
twice or an empty name, when the request does not alter its parent, when the
parent does not hold `from` and declare `to`, and when its effects are not the
read, the drop and the create, in that order, on index subjects of the parent.

### When the host pairs

The comparison pairs only under `index_rename`, which a provider's session
reports. A provider reports it only to a host that lists the feature. A host
that does not list the feature has no step to send a pair with, so the
capability from such a provider would make every renamed index refuse the
plan. Without the capability the comparison pairs nothing, and the rename is
a drop and a create.

An index that carries a facet is not paired: the comparison cannot say a
rename keeps the facet. The YDB provider keeps an index's partitioning in its
table's native facet, so its indexes carry none, and it compares the
partitioning of a renamed index under the new name itself.

### Comments move with the index

The comparison records the comment of a renamed index in
`IndexCommentsChanged`, with the held name in `From`. With the common-object
wire, the host sends a comment step that clears the comment under the held
name and one that sets it under the new name. An engine that keeps a comment
apart from the index, as YDB keeps it in a table attribute, would otherwise
leave the comment under a name no index has.

## Alternatives considered

**The provider pairs a drop and a create itself.** The host would keep sending
a removal and an addition, and the owner would recognize them as one index.
The pairing rules would then live twice, in the host's comparison and in each
provider, and the two could disagree: the host would clear the comment of the
removed index and set the comment of the added one, while the provider renamed
it. Rejected.

**A step without an operand, recognized by its three effects.** The effects
already name both indexes. An owner that recognizes steps by footprint would
have to tell this step from a malformed one, and the SDK could not check what
the host meant. The operand states the rename, and the SDK checks the effects
against it. Rejected.

**A host feature alone, without an engine listing.** A provider can serve
engines that rename and engines that do not, and the host plans per engine.
The engine lists the feature, as it lists observed indexes. Rejected.

## Consequences

- Contract 25.17 adds a field, a message and a two-way feature.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- With the feature negotiated and `index_rename` reported, a provider that
  owns indexes keeps a renamed index instead of building it again. Without
  either, nothing changes: the rename is a drop and a create.
- Moving a renamed index's comment needs the host's comment steps for the
  indexes of a table the provider owns.

## Acceptance evidence

- SDK tests accept a negotiated rename step and refuse an engine that lists the
  feature without the provider's or without observed indexes, a host that does
  not list it, and each malformed step above.
- Through the SDK fixture provider, the host plans a rename as one claimed
  step followed by the comment moved to the new name, refuses the rename for a
  provider without the feature, and refuses a session that reports
  `index_rename` from an engine that does not negotiate it.
- Through the YDB provider on 26.2.1.14 and 25.1.4.7, with `EnableMoveIndex`
  on, a renamed index is one `row-table-alter-operation` followed by the two
  comment steps. The index keeps an `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`
  of 3 that no declaration states, and its comment moves to the new name. The
  next run is synced, and a read through the new name finds the table's row.
  With the flag off, or with a host that does not list the feature, the
  rename is a drop and an add.
