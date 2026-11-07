# ADR 0108: A provider decides whether two view queries are one

Status: Proposed

Contract: 25.33

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G204)

Relates to: [ADR 0090](0090-a-provider-decides-whether-two-checks-are-one.md)

## Context

A comparison decides whether a declared view and the view the database holds
under its name have one query. For an engine without rules of its own the
host folds both texts with its shared reading
(`migration/schemadiff/internal/compare`): it trims them, collapses spaces,
drops the default aggregate aliases and lower-cases every unquoted word. Two
queries whose folded texts match are one view, and the view stays.

Lower-casing is right for an engine whose unquoted names fold, and wrong for
one whose names keep case. YDB is such an engine: `Id` and `id` name two
columns. The built-in YDB engine at 74de0b65b registered view rules of its
own (`schemacomparison.Views.QueryEqual`) and compared queries keeping case.
Through the YDB provider no rules exist, the host's fold runs, and a change of
a view's query that only changes the case of a name is never reported: the
schema says one query, the database keeps another, and every plan reports the
schema in sync (register G204).

The contract had no way to carry a view-query rule. The host's own rules are
Go functions inside the host process, which a provider in its own process
cannot register, and a shared fold cannot be made right for every engine: the
case of a name, the quoting a server adds and the parts of a query it
rewrites are each engine's own.

## Decision

### The engine's owner answers

`CompareViewQueries` asks the owner of an engine whether the query a declared
view states and the query of the view the database holds under its name are
one query. The request carries the engine, its identifier facts and
capabilities, and the pairs: each the held view, a view or a materialized view
as a read reported it, without its facets, and the declared query, without
`CREATE VIEW`, the view's name, its options or a materialized view's data
clause. The reply answers each pair, in the request's order, with the held
view's identity and `equivalent`. The owner decides from the request alone; it
opens no session and runs nothing.

A host that asks keeps a view whose query the owner calls equivalent, plans
the replacement of one it does not, and compares no view query of that engine
by its text. The pairs are the ones the comparison matches by name, so a
declared view the database does not hold is created from its declaration and
asks nothing.

### The feature is the provider's alone

`schema.view-query-equivalence` enables the call. The provider and each
engine that compares view queries list it. The call is new and adds nothing to
an existing message, so a host that does not know it never makes it, and the
feature is not two-way, as `schema.check-equivalence` is not. A held view
travels as a common object, so a pair needs the engine to carry views on the
common-object wire, which is negotiated under `schema.common-objects`.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's.
`ValidateCompareViewQueriesRequest` refuses a request for an engine that does
not compare view queries, invalid identifier facts, a pair whose held view is
not a view or a materialized view of the engine, names a path or an overload,
is of a kind the negotiated engine does not carry, has no query or carries
facets, a blank declared query, and two pairs of one view, a view and a
materialized view sharing their names under the engine's table name rules.
`ValidateComparedViewQueries` refuses an incomplete reply and one that leaves
out, reorders or renames a pair.

## Alternatives considered

**Describe the engine's fold in its description.** An engine could state
whether unquoted names keep case, and the host would fold accordingly. It
covers G204 and nothing beyond it: the quoting a server adds, the parts it
rewrites and the keywords it upper-cases are each engine's own, and every new
one would be another description field the host has to interpret. One bit of
answer per pair leaves nothing for the host to interpret.

**Ask the provider to spell the declaration as its server keeps it.** This is
the host's PostgreSQL view probe moved to the provider. It still leaves the
comparison of two texts to the host's fold, which is the defect here, and a
spelling cannot say that two texts which differ are one query.

**Decide on a live session.** The owner could create the declared view in a
rolled-back transaction and read its stored text, as the host does on
PostgreSQL. Some engines create a view outside any transaction, and an offline
call also serves a file-to-file comparison that has no session at hand.

## Consequences

- A view query on a provider's engine is compared by the engine's own rules: a
  change the engine sees is reported, and a rewrite it does not see is not.
- A provider that lists the feature owns every view query comparison of its
  engine, and a wrong answer plans a replacement or hides a change.
- A host of an earlier contract, and a provider that does not list the
  feature, fold the texts as before.

## Acceptance evidence

- SDK unit tests: `TestViewQueryEquivalence_HappyPath`,
  `TestCompareViewQueriesRequest_FailurePath`,
  `TestComparedViewQueries_FailurePath`,
  `TestViewQueryEquivalenceMissingMessages_FailurePath` and
  `TestViewQueryEquivalenceDescription_FailurePath` in `provider-sdk/provider`,
  and the CompareViewQueries runtime tests in `provider-sdk/runtime`; the
  conformance suite probes the service and validates the configured calls.
- The host adoption asks a provider that compares view queries about every
  view the comparison matches on the provider's engine
  (`TestProviderComparesViewQueriesThroughItsOwner_HappyPath` in
  `integration/providercli`).
