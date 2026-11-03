# ADR 0057: A provider spells a declared index's kind as it reports a held one

Status: Proposed

Contract: 25.14

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0052](0052-inspected-indexes-reach-the-host-and-index-changes-reach-the-table-owner.md).

## Context

Under `schema.observed-indexes` a provider reports the common indexes a table
holds, and `ObservedIndex.method` carries an index's kind in the provider's
spelling. The host compares a declared index with the held one by its
uniqueness, its key columns and its covered columns. It does not compare the
kind. The declared kind is whatever the source wrote in the index's type, and
the host has no rule to read a provider's spelling of either side.

The YDB provider reports `async` for an asynchronous index and no method for a
synchronous one. Ptah's built-in YDB engine reads several spellings as one
kind: an empty type, `BTREE`, `GLOBAL`, `SYNC` and `GLOBAL SYNC` are
synchronous, and `ASYNC` and `GLOBAL ASYNC` are asynchronous. YDB cannot
change an index's kind in place, so the built-in engine plans a kind change as
a drop and a create. With the provider against YDB 26.2.1.14, a schema that
changes only an index's type from synchronous to `async` plans no changes, and
the index keeps its kind.

Comparing the two strings in the host would be worse. A source that writes
`GLOBAL SYNC` or `ASYNC` would rebuild its index on every run, because the
provider reports neither spelling.

## Decision

### The provider states the kind of each declared index

The provider already prepares the comparison inputs of its tables offline:
`PrepareTables` receives each declared table, with its common indexes, and the
table the database holds. Under the two-way feature `schema.index-methods`,
`PreparedTable.indexes` carries one `PreparedIndex` for each index the table
declares, in the order `DeclaredTable.indexes` lists them. Its `method` is the
kind the provider reads the declared index as, spelled exactly as the
provider's `ObservedIndex.method` spells the kind of a held index. An empty
method is a kind like any other: a held index of that kind reports no method.

The provider reads the kind from the declaration alone, never from the held
index, so the prepared kind cannot echo what the database holds. A provider
that cannot read a declared kind refuses the preparation with its reason, as
it refuses to create such an index.

### The SDK refuses an incomplete list

The feature requires `schema.observed-indexes` and `schema.prepare-tables` on
the same engine. A provider lists it, an engine lists it only when its
provider does, and a reply carries prepared kinds only when the provider, the
engine and the request's target features list it. Without negotiation, a
reply that states a kind is refused.

With negotiation, both transport boundaries refuse a reply that does not state
one kind for each declared index in the declared order: a missing entry, an
extra one, an index the table does not declare, one stated twice, and another
order. A missing kind would leave the host comparing the authored spelling
with the reported one, which is the rebuild on every run that this decision
removes. A later minor could not add the check, because a validator that
refuses what an earlier peer sends is an incompatible change.

### The host compares the two spellings exactly

When the feature is negotiated, the host keeps the prepared kinds beside the
prepared table, and its comparison gives a provider's index rules the declared
index with its prepared kind in place of the authored type. The rules compare
it with the held index's method exactly. A kind change is then a definition
change, which the shared comparison plans as a drop and a create under the
index's name, and ADR 0052 hands both steps to the table's owner.

The authored type stays in the declaration. The plan, the provider's planner
and a created table see the type the source wrote. The prepared kind serves
the comparison alone. The host prepares the tables before it pairs a dropped
and an added index as a rename, so the pairing reads the same kinds.

Without the feature, the host compares no kind, and a change of kind alone
plans nothing. The provider documentation states this.

## Alternatives considered

**The host compares the declared type with the reported method.** Every
spelling the provider reports differently from the source rebuilds an index on
every run. Rejected.

**A spelling table in the description.** An engine could list each kind with
the declared spellings that mean it, and the host could match a type against
the table without a call. The table has to encode the provider's grammar: the
YDB provider ignores case and treats an underscore as a space. It can read only
the type, while a provider may need more of the declaration, such as its
uniqueness. And the host could not give the provider's reason for a type no
entry matches. Rejected.

**The provider's planner compares the kinds.** The owner receives an altered
table's declared and held indexes, but only when the comparison reports a
change on the table. A kind change the comparison does not see never reaches
the owner, and `schema diff` and drift reports would miss it too. Rejected.

**Preparation rewrites the declared type.** Column key membership is prepared
that way, in place. The planner of the table's owner reads the declared index
from the prepared table, and a created table takes its indexes from the plan,
so the provider's spelling would replace the author's in what is planned.
Rejected.

**A new call that reads index kinds.** It would add a round trip to every
comparison, while `PrepareTables` already carries every declared table with its
indexes and runs before the index comparison. Rejected.

## Consequences

- Contract 25.14 adds a field, a message and a two-way feature.
- A provider that negotiates the feature states a kind for every declared
  index, also of a table the database does not hold.
- A declared kind the provider cannot read refuses the preparation, and with
  it the comparison, not only a plan that creates or replaces the index.
- A provider without the feature compares no kind, so a change of kind alone
  plans nothing.
- The SDK release comes first. The host and the YDB provider adopt it in their
  own changes, against the published tag.

## Acceptance evidence

- SDK tests accept a complete list and a table that declares no index. They
  refuse prepared kinds the provider, the engine or the request did not
  negotiate, while the same reply without them passes, and refuse a missing,
  extra, undeclared, repeated or reordered entry. Both transport ends refuse a
  reply the host did not negotiate.
- The host and the YDB provider each carry a test in which a change of kind
  alone drops and creates the index, and the next comparison is synced.
