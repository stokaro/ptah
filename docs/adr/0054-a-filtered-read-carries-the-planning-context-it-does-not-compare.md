# ADR 0054: A filtered read carries the planning context it does not compare

Status: Proposed

Contract: 25.11

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5169](https://github.com/stokaro/ptah/issues/5169)

Relates to: [ADR 0049](0049-native-object-context-is-assessed-without-inventing-schema-differences.md).

## Context

ADR 0049 lets a planning service assess the native objects of its context
kinds, which a change can affect without changing them. The YDB provider uses
it for streaming queries: a query must stop before an input it reads is
dropped or replaced. Whether an unknown query reads the input is a question
about the whole database, so the provider refuses a write of a held input
unless the read covered every streaming query in the database.

A filtered inspection covers only the selected directories. The SDK refuses a
filtered snapshot that claims knowledge outside them, which is right for
compared state: a filtered read must not prove an object absent that it did
not read, or a plan would create or drop outside its scope. It leaves no place
for context a plan reads but never compares.

Measured with the YDB provider against YDB 26.2: a filtered `schema apply`
that drops a table, or changes a held topic, external object, view or resource
pool, is refused with "input changes require complete streaming query
inventory for the selected database", although the database holds no
streaming query. The built-in YDB engine applies the same change.

## Decision

The two-way feature `planning.context-inventory` adds
`Schema.planning_context`: the observed values of the engine's planning
context kinds that a filtered read leaves out of its scope, and their
coverage. It requires `planning.object-context`.

The inventory is context only. Its records name objects outside the
requested scope, each kind is a context kind of a planning service of the
engine, and its coverage may span the session's database. An unfiltered read
carries none, because its own coverage already spans the database. The host
never compares the inventory and never plans a change for it.

`PlanFeaturesRequest.context_inventory` carries the inventory, as an observed
comparison state, to the planning service that assesses those kinds, beside
`current_objects`. Its subjects are disjoint from the captured desired and
current objects, and the service returns no receipt for them: they are not
part of the assessment inventory of ADR 0049. A service reads them to prove
what lies outside the scope. The YDB provider refuses a write of an input
that a streaming query outside the scope reads, naming the query, rather than
stopping and starting an object the command does not select.

## Consequences

A filtered plan can write a held input when the database proves that no
streaming query outside the scope reads it. A query outside the scope that
reads the input still refuses the plan, now by name.

The host carries the inventory from the read to planning without comparing
it: the catalog keeps it beside the feature objects, and the schema diff keeps
the current side's inventory.

The SDK release comes first. The host and provider adapters adopt it in their
own changes, against the published tag.
