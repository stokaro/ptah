# ADR 0089: A provider plans the constraint changes of a table it owns

Status: Proposed

Contract: 25.28

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0078](0078-a-provider-plans-the-change-of-a-primary-key.md),
[ADR 0088](0088-a-read-reports-the-foreign-keys-and-checks-a-table-holds.md)

## Context

With [ADR 0088](0088-a-read-reports-the-foreign-keys-and-checks-a-table-holds.md)
a host compares the foreign keys and CHECKs a provider table holds with the
declared ones, and the shared comparison reports a constraint to add, to drop
or both. A provider that owns its tables' lifecycle leaves the host no common
table operation to write, and the host's provider planner accepts a
constraint change of the primary key alone: any other is refused as
`does not support SchemaDiff.ConstraintsAdded`.

Ptah's built-in Spanner engine at d59a3b63d plans both on a kept table.
Measured on the PGAdapter emulator v0.56.2, the second `schema apply` of a
schema whose CHECK the server rewrote plans

```sql
ALTER TABLE "posts" DROP CONSTRAINT "posts_score_check";
ALTER TABLE "posts" ADD CONSTRAINT "posts_score_check" CHECK (score >= 0);
```

and `schema render` writes a foreign key as an `ALTER TABLE ... ADD
CONSTRAINT ... FOREIGN KEY` of its own, after the tables it joins. Spanner
adds a foreign key to an existing table in place, and builds the index that
enforces it itself.

## Decision

### An engine states that it plans constraint changes

The two-way feature `planning.constraint-changes` states that an engine plans
the addition and the removal of a FOREIGN KEY or CHECK constraint on a table
whose lifecycle it owns and the plan keeps. The provider and the engine list
it. The engine needs `schema.observed-constraints`, through which the host
compares the constraints, and a planning service with parent kinds, through
which it owns its tables, and it does not list
`schema.common-table-identity.v1`, under which the host plans a table's
operations itself.

The feature adds no field. The planning request already carries what the
engine compares: the declared constraints in `DeclaredTable.constraints`,
`DeclaredTable.checks` and each column's `check` and `foreign_reference`, and
the held ones in `ObservedTable.constraints`.

### The host hands the change as an alteration of the table

A host that lists the feature plans a kept table whose constraints change as
an `ALTER`, as it does a table whose primary key or indexes alone change, and
adds no common step for the constraint. The engine compares the constraints,
plans the change, for Spanner an `ALTER TABLE ... ADD` or `DROP CONSTRAINT`,
and answers the table's parent receipts as for any alteration. A refusal is
the engine's, in its words. A constraint of a table the plan creates travels
in the creation's declaration, and one of a table the plan removes leaves
with the table.

A host with observed constraints negotiated and this feature not negotiated
refuses a plan with such a change of a provider table before it plans, naming
the table, rather than plan the table as if its constraints matched.

### What the SDK enforces

`ValidateDescription` refuses the feature on an engine without the provider's
feature, without observed constraints, without a planning service with parent
kinds, or with common table operations. A host that does not list the feature
receives an engine without it.

## Alternatives considered

**A common step for each constraint.** The host could add a placeholder step
for each added or dropped constraint, as it does for an index addition, and
refuse a plan in which the provider leaves it unclaimed. The step would carry
nothing the parent's captures do not, and an engine that rebuilds a table to
change a constraint would claim one more host step for each. An owner that
receives a whole altered table accounts for a changed column and a changed
key the same way, without a step.

**Fold the feature into `planning.primary-key-changes`.** One feature for
every constraint would be simpler to list. It would also say that every engine
that plans key changes plans foreign keys and checks, and the YDB provider,
which plans key changes, has neither.

**Leave constraint changes to the host's common planner.** The host could
render `ALTER TABLE ... ADD CONSTRAINT` itself. It renders nothing for a table
it does not own, and the spelling, the order against the engine's own steps
and whether the engine can do it in place at all are the engine's.

## Consequences

- A provider that owns its tables plans or refuses each declared constraint
  change instead of the host refusing all of them.
- An engine that lists the feature owns every foreign key and CHECK change of
  its tables, a change of a definition included, which the comparison reports
  as a removal and an addition of one name.

## Acceptance evidence

- SDK unit tests: `TestConstraintChangesFeature_HappyPath` and
  `TestConstraintChangesFeature_FailurePath` in `provider-sdk/provider`.
- The host adoption hands a constraint change of a kept table to the engine's
  planning, and refuses it by table without the feature.
