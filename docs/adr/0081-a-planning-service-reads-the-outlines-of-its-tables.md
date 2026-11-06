# ADR 0081: A planning service reads the outlines of its tables

Status: Proposed

Contract: 25.27

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0079](0079-a-described-model-travels-by-reference.md),
[ADR 0083](0083-a-list-of-coverage-kinds-travels-once-per-message.md)

## Context

A host plans a migration by calling each planning service of a provider's
engine that has a change to plan, a parent kind to assess or a context kind
to assess. Every call carries `PlanFeaturesRequest.tables`: every table of the
plan with its declared and observed state as captured, columns, keys,
indexes and the captured features of each state, with their coverage.

The YDB provider declares three services that a plan of new tables calls:

- the planner of row tables, which owns the tables and assesses their
  settings (parent kinds);
- the planners of streaming queries and of async replications, which assess
  the objects of their context kinds.

A migration plans both directions, so `migrations generate` of 500 new tables
makes six planning calls. Measured with YDB 25.1.4.7, host #5464 and #5471,
SDK #5472 and the provider of stokaro/ptah-ydb-plugin#291 and #293, each call
carried 2,273,991 to 2,564,761 bytes on the wire: 500 whole tables, which the
host and the provider built, copied, validated and decoded, about 0.3 to 0.5
seconds of each call and as much of the host's time before it.

The streaming and replication planners plan nothing for a table creation.
They read the tables of a request only to learn which tables the plan keeps,
creates, changes and drops: the subject, the action, and whether the table
is declared and whether it is held.

The SDK refuses a request whose parent assignment the host narrowed, and
nothing in the contract lets a host send a table without its capture, so the
host had no way to send these services less.

## Decision

### A service says what it reads of each table

`ModelPlanning.tables` (`PlanningTables`) states what a request carries of
each table for the service. `PLANNING_TABLES_CAPTURED`, the zero value, is
each table as captured, as before. `PLANNING_TABLES_OUTLINED` is the outline
of each table: its `subject`, its `action`, and each state the plan holds by
its name alone, `DeclaredTable{name}` and `ObservedTable{name}`, with no
columns, keys, indexes or captured features. `provider.TableOutline` builds
it.

A service with parent kinds plans the features an outline leaves out, so it
is never outlined. A service with change or context kinds and no parent
kinds can be: the YDB provider outlines every service but the planner of row
tables, which owns the tables, and the planner of external data sources,
which reads the TTL a column table keeps.

### The feature is negotiated

`planning.table-outlines` is a two-way feature. The provider and the engine
list it, and the host lists it in its host features. A host that lists it
sends an outlined service the outlines; a host that does not sends every
table whole, and `provider.NegotiatedDescription` then shows every planning
service as `PLANNING_TABLES_CAPTURED`, so the SDK validates the whole tables
such a host sends. A service that reads outlines reads the same subjects,
actions and states from a whole table, so a provider serves both hosts with
one service.

### What the SDK enforces

`ValidateDescription` refuses an outlined service on an engine that does not
list the feature, the engine's feature without the provider's, an outlined
service with parent kinds, and a value of `PlanningTables` the SDK does not
know.

`ValidatePlanningRequest` refuses, for an outlined service, a table that
carries more than its outline, a state without its name, an action its
states disagree with, two tables of one subject, and a table that is not a
table. For a service that reads whole tables it refuses an outline, since an
outline carries no captured features. The check that an index rename's
parent holds the index under its old name and declares it under its new one
reads the parent's indexes; for an outlined service only the parent is
checked, since its outline holds no index and the owner of the table, which
receives the same rename step with the whole table, checks the rest.

## Alternatives considered

**A service that reads no tables.** The host would send no table at all. The
streaming planner reads the declared tables a query reads and writes, and the
replication planner refuses a replica at a path a declared or held table
occupies, so both need the outline. A common step that names a parent needs
the parent in the request too. An outline of 500 tables is about 35 KB.

**Let the host narrow the tables to the ones a service's changes name.** A
streaming query names its tables in its text, which only the provider
parses, and a service that assesses context kinds has no change to narrow
by. The host cannot know which tables the provider reads.

**Skip a service that has no change and no object of its kinds.** The host
could leave out the call when nothing is assigned. The service still owns the
assessment of an empty inventory, and the first object of its kind brings
back a request of every whole table; outlines make the call cheap whenever it
happens.

**Leave the tables whole and make each one cheaper.** ADR 0079 and ADR 0083
shrink a table's coverage, but a whole table still carries its columns,
indexes and features, and each side still builds, validates and decodes them
for a service that reads none.

## Consequences

- The services of a migration that assess named objects receive outlines:
  four of the six planning calls of a migration of 500 new YDB tables, each
  of about 115 KB where it was 2.3 MB, most of it the common steps every
  service receives, and each of 30 to 100 ms where it was 300 to 500. The
  owner of the tables still receives them whole.
- An outlined service cannot read a table's columns, indexes or features. A
  service that needs them reads whole tables, and a provider that starts
  reading them stops outlining that service.
- A host of an earlier contract sends whole tables, as before.

## Acceptance evidence

- SDK unit tests: `TestTableOutlines_HappyPath`,
  `TestTableOutlinesWithoutTheHost_HappyPath`,
  `TestTableOutlinesDescription_FailurePath` and
  `TestOutlinedPlanningRequest_FailurePath` in `provider-sdk/provider`.
- The host lists the feature and sends an outlined service the outlines.
- The YDB provider outlines its services that plan no table's settings, and
  a migration of 500 new tables makes four outlined planning calls.
