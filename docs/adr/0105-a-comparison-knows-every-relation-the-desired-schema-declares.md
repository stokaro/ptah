# ADR 0105: A comparison knows every relation the desired schema declares

Status: Proposed

Contract: 25.32

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G211); the open contract gaps of
[ADR 0102](0102-the-provider-contract-freezes-with-ptah-1-0-and-keeps-major-25.md)

## Context

`CompareModelsRequest.declared_relations` is a list of comparison identities.
No comment says what it holds. The host fills it with the views and
materialized views the desired schema declares
(`migration/schemadiff/features.go`), and the engine runtime refuses any
other kind (`engine/comparison.go`). The one built-in consumer, the
TimescaleDB continuous aggregate comparison, reads the declared tables from
the comparison parents instead.

The YDB provider compares transfers, which read a topic and write a table.
Under the built-in YDB engine, a transfer into an undeclared table was
refused; through the provider it is not, because its comparison sees no
declared table (register G211). A table is a relation the host declares, not
a model of the provider's, so the provider has nowhere else to learn of it.

## Decision

### The list holds every declared relation under a two-way feature

`models.declared-relations` gives the list its complete meaning: every table,
view and materialized view the desired schema declares, each identity's kind
naming which (`table`, `view` or `materialized view`). The provider lists the
two-way feature, and so does each engine whose comparisons read the list; the
engine needs a model comparison.

Without the feature the list holds the declared views and materialized views
alone, as a host of contract 25.31 sends it, and that meaning is now written
down too. A relation of the provider's own models, such as a topic, is not
listed in either case: the comparison's state carries it.

### What the SDK enforces

`ValidateComparisonRequest` refuses a declared relation of kind `table` when
the engine or the request's target features do not list the feature, and a
relation of any other kind always. `ValidateDescription` refuses the engine's
feature without the provider's or without a model comparison.

## Alternatives considered

**A separate list of declared tables.** A new field `declared_tables` would
leave the existing list untouched. It splits one question, "is this name a
relation the schema declares", across two lists, and the next relation kind
would need a third.

**Send the declared tables as comparison parents.** Parents carry tables that
own facet values, with their lifecycles, and object comparisons receive
them too. Their meaning is ownership, not declaration, and a comparison would
then infer declaration from a list kept for another purpose.

**Let each provider read the declared tables from its own models.** A
provider sees only what it is sent. A table is the host's common object, and
no provider model carries it.

## Consequences

- The YDB provider can refuse a transfer into an undeclared table once both
  sides list the feature, as the built-in engine did.
- The host adds its declared tables to the list for an engine that lists the
  feature, and keeps sending views alone to one that does not.

## Acceptance evidence

- SDK unit tests: `TestDeclaredRelations_HappyPath`,
  `TestDeclaredRelations_FailurePath` and
  `TestDeclaredRelationsNegotiation_FailurePath` in `provider-sdk/provider`.
- The conformance suite's narrowed host profiles include the feature for every
  provider that offers it.
