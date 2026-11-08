# ADR 0121: A read reports the objects outside the model that block a table's removal

Status: Proposed

Contract: 25.39

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register S47)

## Context

An engine can hold objects that its provider does not model and that still
decide whether a table or a column can be dropped. A Spanner change stream is
one. Spanner refuses to drop a table that a stream names in its `FOR` clause,
and a column that a stream names in its column list:

```text
Cannot drop table t. Table t is tracked by 1 Change Stream: cs1. Tables
explicitly tracked by a Change Stream cannot be dropped.
```

A stream created `FOR ALL` names nothing, and a stream `FOR t` names no
column of `t`, so neither blocks a drop. The Spanner provider leaves change
streams out of its reads, so a plan does not know about them. Measured on the
emulator behind PGAdapter 0.56.2: a plan that creates a table and drops a table
a stream names applied the creation and then stopped at the drop.

The provider can refuse such a drop when the statements run: it reads the
catalog before the first one (stokaro/ptah-spanner-plugin#31). It cannot refuse
it at plan time. `PlanFeatures` reads no database, and a planning request
describes a held table only through `PlanningTable.current`, whose
`ObservedTable` holds columns, indexes, constraints and captured facets. So
`schema apply --dry-run` prints the drop, and `migrations generate` writes it
into a file that then fails when it runs.

## Decision

`schema.removal-blockers` adds `RemovalBlocker` and two fields that carry it:
`Table.removal_blockers` in inspection and `ObservedTable.removal_blockers` in
planning.

A removal blocker is an object outside the engine's model that names a table.
It has a kind, which is a lowercase token such as `change_stream`, a name, and
the table's columns it names. Every blocker blocks the removal of its table. A
blocker that names columns also blocks the removal of each of those columns,
and one that names no column blocks the removal of no column.

A read reports the blockers of each table it describes. With the feature
negotiated, a table whose list is empty has no blocker. The host carries each
list unchanged from the read's `Table` into the `ObservedTable` of the
planning request that holds the table. It never compares the list, never
exports it, and never sends it outside a planning request. The planning
service that owns the table reads it and refuses a plan that drops a blocked
table or column, before a statement runs. `--dry-run` and `migrations
generate` refuse it too.

The feature is two-way. The provider lists it, and so does each engine whose
reads report blockers. The engine needs `schema.observed-columns`, because the
SDK checks a blocker's columns against the table's columns. The SDK refuses
these replies and requests:

- a list on either side when the feature was not negotiated;
- a kind that is not a token, or a blank name, a name with a control
  character, or a name that is not UTF-8;
- the same kind and name listed twice for one table;
- a column the table does not have, or a column named twice;
- a list in a CHECK comparison's held table, since only a planning request
  carries one.

## Alternatives considered

**Model change streams as native objects and plan through
`planning.object-context`.** That carries the stream itself: a model with
desired and observed codecs, a comparison, a conversion and an export
encoding for every source format, which is the work of register gap S16. A
plan needs much less than that. It needs to know what blocks a drop, not what
the stream is, and an engine has other blockers that no source declares.

**A table facet that records what the streams name.** A facet needs a desired
codec and a comparison. Every conversion answers one value for each value it
receives, so a read's facet would reach every export as a desired value that
no source can declare. It would be a setting that nobody can set.

**Refuse at execution only.** The provider does this already. It is still the
answer for a migration that someone writes by hand, and it cannot refuse a
plan before the plan is written.

**Let the host refuse the drop.** The host does not know which objects an
engine refuses a drop for, or how a drop is spelled in the plan. The
planning service plans the drop, so it also refuses it, in its own words.

## Consequences

- The Spanner provider can refuse a plan that drops a table or a column a
  change stream names, in `schema apply`, `--dry-run` and `migrations
  generate`, naming the stream. Its check before execution stays for
  migrations that someone writes by hand.
- A provider that does not list the feature reads and plans as before.
- Another engine can report its own blockers under its own kind with no new
  minor.

## Acceptance evidence

- SDK: `TestRemovalBlockers_HappyPath`,
  `TestRemovalBlockersDescription_FailurePath`,
  `TestRemovalBlockersInspection_FailurePath`,
  `TestRemovalBlockersPlanning_FailurePath` and
  `TestRemovalBlockersInACheckComparison_FailurePath` in
  `provider-sdk/provider`.
- Host adoption: the host lists the feature and carries the list from the read
  into planning, and a fixture provider whose read reports a blocker refuses a
  drop at plan time.
- Spanner adoption: a read reports the change streams that name each table, and
  a dropped table or column a stream names is refused at plan time.
