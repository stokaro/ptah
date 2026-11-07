# ADR 0100: A read reports the UNIQUE constraints an engine keeps apart from its indexes

Status: Proposed

Contract: 25.31

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0088](0088-a-read-reports-the-foreign-keys-and-checks-a-table-holds.md),
[ADR 0089](0089-a-provider-plans-the-constraint-changes-of-a-table-it-owns.md)

## Context

A provider's read reports a table's foreign keys and CHECKs in
`Table.constraints` under `schema.observed-constraints`
([ADR 0088](0088-a-read-reports-the-foreign-keys-and-checks-a-table-holds.md)),
and its indexes in `Table.indexes`. ADR 0088 left UNIQUE constraints out,
because on most engines a UNIQUE constraint is an index and is already read
as one, and named a later minor for an engine that keeps them apart.

DuckDB keeps them apart. Measured on DuckDB 1.4.1, after

```sql
CREATE TABLE u (a INT, b INT, c INT UNIQUE, CONSTRAINT u_ab UNIQUE (a, b));
```

`duckdb_constraints()` and `information_schema.table_constraints` list the
UNIQUE constraints `u_c_key` over `c` and `u_a_b_key` over `a, b`, and
`duckdb_indexes()` lists nothing. DuckDB also does not keep the declared name
`u_ab`. A single-column UNIQUE fits the column's own `unique` fact, which
names no constraint; one over several columns has no field at all, so the
DuckDB provider refuses such a table by name.

The host's catalog keeps both shapes for the built-in engines. The PostgreSQL
reader lists every UNIQUE constraint and marks a column that one over it alone
covers as unique, and the comparison pairs declared and held UNIQUE
constraints by name, leaving to a column the key it declares itself.

## Decision

### The list carries the UNIQUE constraints too

Under the two-way feature `schema.observed-unique-constraints`,
`Table.constraints` carries every UNIQUE constraint the table holds, over one
column or several, as `ObservedConstraint` with the type `UNIQUE`, beside the
foreign keys and CHECKs. A UNIQUE constraint the list does not name is absent.
A column that a UNIQUE over it alone covers states `ObservedColumn.unique`, so
the catalog holds what it holds for a built-in engine. An index the engine
builds to enforce one stays out of `Table.indexes`, as for a foreign key.

The provider and the engine list the feature, and the engine needs
`schema.observed-constraints`. A read whose session did not negotiate it
carries no UNIQUE constraint, as before.

### A provider that plans constraint changes plans these too

An engine that also lists `planning.constraint-changes` plans the addition
and the removal of a UNIQUE constraint on a table the plan keeps, as it plans
those of a foreign key or a CHECK: the host hands the table as an `ALTER`, the
declared constraints travel in the desired declaration and the held ones in
the current observation. Without it, the host refuses such a change by
constraint and table before it plans.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's or
without observed constraints. Inspection validation refuses a UNIQUE
constraint without the feature, and with it, one without columns, with a
foreign key's attributes or a CHECK clause, with `include_columns`,
`using_method` or `hash_shard_buckets`, with a timing and no deferral, and one
over a single column that does not state that it is unique. `nulls_distinct`
may state NULLS [NOT] DISTINCT, which is the constraint's own.

## Alternatives considered

**Report the constraint as a unique index.** The read could list it under
`schema.observed-indexes`. DuckDB's catalog has no such index, a plan would
drop and create an index for a declared constraint, and an export would write
an index the source did not declare.

**Report only UNIQUE constraints over several columns.** A single-column one
would stay the column's `unique` fact. That fact names nothing, so a source
that names its single-column UNIQUE pairs with nothing, and the plan adds the
constraint on every run. The host would also look in two places for one kind
of object.

**A separate planning feature for UNIQUE changes.** An engine could report
UNIQUE constraints and plan foreign keys and CHECKs without planning UNIQUE
changes. No engine needs that split, and an engine that lists both features
would then have to list a third to keep its tables plannable.

## Consequences

- A DuckDB table with a UNIQUE over several columns reads whole instead of
  being refused.
- A provider reports the name the engine holds. On an engine that renames a
  constraint, as DuckDB does, the provider owns the names it plans under, as
  it owns the names of the foreign keys and CHECKs it creates.
- A rollback plan does not predict a UNIQUE constraint, as it does not predict
  a primary key; a down file that needs one is refused.

## Acceptance evidence

- SDK unit tests: `TestObservedUniqueConstraints_HappyPath`,
  `TestObservedUniqueConstraints_FailurePath` and
  `TestObservedUniqueConstraintsDescription_FailurePath` in
  `provider-sdk/provider`.
- The conformance suite's memory provider reports a UNIQUE constraint, and
  `TestCheck_FailurePath` reports one sent to a host that did not list the
  feature.
- The host adoption reads each UNIQUE constraint into the catalog and hands
  its change to the table's owner.
