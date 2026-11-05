# ADR 0078: A provider plans the change of a primary key

Status: Proposed

Contract: 25.26

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0051](0051-inspected-primary-keys-retain-their-common-order.md)

## Context

A composite primary key has an order of its own. Table columns `(a, b)` can
carry the key `(b, a)`, and on YDB the key order is the order the rows are
stored and split in. ADR 0051 gave inspection a way to report it:
`schema.observed-primary-keys` carries each table's key, in key order, in
`Table.primary_key`. No host listed the feature, so a provider's table read
reported only which columns are in the key.

Ptah's built-in YDB engine read the key as a constraint and kept its order.
Measured with the host at 74de0b65b on YDB 25.1.4.7 and 26.2.1.14, for a
table declared with `primary_key="b,a"` over fields `a` and `b`:

- `ptah introspect` writes `primary_key="b,a"` on the table directive;
- a declaration that changes the key to `a,b` is refused by `schema apply`
  and `migrations generate` with `adding constraint pk_items_pkey, the
  primary key, which requires target capability primary_key_alterable,
  unavailable on this ydb target; YDB makes it by rebuilding the table, which
  Ptah plans when asked with --allow-table-rebuild`;
- with `--allow-table-rebuild`, `schema apply` rebuilds the table with the
  declared key.

Through the YDB provider the same commands write `primary="true"` on each
field, which reads back as the key `(a, b)`, and plan nothing for the key
change, with or without `--allow-table-rebuild`. The table keeps a key the
declaration no longer states, and the run reports it synced.

A host that lists `schema.observed-primary-keys` sees the change: the shared
comparison pairs the declared key with the held one and reports a
`PRIMARY KEY` constraint to drop and one to add. It cannot plan them. A
provider that owns its tables' lifecycle leaves the host no common table
operation to write, and nothing in the contract says the provider plans a
key change it is handed.

## Decision

### An engine states that it plans key changes

The two-way feature `planning.primary-key-changes` states that an engine
plans the change of the primary key of a table whose lifecycle it owns. The
provider and the engine list it. The engine needs
`schema.observed-primary-keys`, through which the host compares keys, and a
planning service with parent kinds, through which it owns its tables, and it
does not list `schema.common-table-identity.v1`, under which the host plans a
table's operations itself.

The feature adds no field. The planning request already carries what the
engine compares: the declared key in `DeclaredTable.primary_key` and the
held key as the `PRIMARY KEY` entry of `ObservedTable.constraints`, its
columns in key order.

### The host hands the change as an alteration of the table

A host that lists the feature reads each inspected key into the common
catalog as a constraint, without sorting its columns or inventing a name.
When the comparison reports a changed key of a table the plan keeps, the
host plans the table as an `ALTER`, as it does a table whose indexes alone
change, and adds no common step for the key. The engine compares the keys,
plans the change, for a YDB table a rebuild when the operator asked for one,
and answers the table's parent receipts as for any alteration. A refusal is
the engine's, in its words.

A host with observed primary keys negotiated and this feature not
negotiated refuses a plan with a key change of a provider table before it
plans, naming the table, rather than plan the table as if its key matched.
Without observed primary keys negotiated the host has no key to compare and
plans as before.

### What the SDK enforces

`ValidateDescription` refuses the feature on an engine without the
provider's feature, without observed primary keys, without a planning
service with parent kinds, or with common table operations. A host that does
not list the feature receives an engine without it.

## Alternatives considered

**A common step that stands for the key change.** The host could add a
placeholder step for each changed key, as it does for an index addition, and
refuse a plan in which the provider leaves it unclaimed. The step would
carry nothing the parent's captures do not, and every rebuild would have to
claim one more host step. A key change on YDB is always a rebuild or a
refusal, and an owner that receives a whole altered table already accounts
for a modified column the same way, without a step.

**A field that marks the table's key as changed.** `PlanningTable` could
carry a flag. The engine compares the keys itself, as it does columns and
indexes, and a flag the host sets and the engine checks is a second
statement of the same comparison.

**List `schema.observed-primary-keys` alone.** The host could hand key
changes to every engine that reports ordered keys. That feature promises a
read, not a plan, and an engine that reports keys without planning their
change would let a key change pass as planned.

## Consequences

- A provider table keeps its key order through an export, and a key change
  is planned or refused instead of reported synced.
- An engine that lists the feature owns every key change of its tables,
  including a change of members.

## Acceptance evidence

- SDK unit tests: `TestPrimaryKeyChangesFeature_HappyPath`,
  `TestPrimaryKeyChangesWithoutTheHost_HappyPath` and
  `TestPrimaryKeyChangesFeatureOfAnEngine_FailurePath` in
  `provider-sdk/provider`.
- The host adoption reads an inspected key into the catalog and hands its
  change to the engine's planning.
- The YDB provider exports, refuses and rebuilds a key order change through
  the host on YDB 25.1.4.7 and 26.2.1.14 as the 74de0b65b binary does.
