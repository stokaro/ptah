# ADR 0109: A comparison knows the declared objects of another comparison's models

Status: Proposed

Contract: 25.33

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G211, the topic half)

Relates to: [ADR 0105](0105-a-comparison-knows-every-relation-the-desired-schema-declares.md)

## Context

The YDB provider compares transfers. A transfer reads a topic and writes a
table. Ptah's built-in YDB engine at 74de0b65b refused a declared transfer
whose topic the schema did not declare, as a topic or as a changefeed:

```text
transfer t: it reads topic s, which the schema declares neither as a topic nor
as a changefeed, so the plan leaves no such topic, and YDB accepts such a
transfer and then stops it (`Discovery error`); declare the topic or the
changefeed
```

YDB checks the topic only after the fact. Measured on 26.2.1.14, `CREATE
TRANSFER` from a missing topic is accepted, and the transfer then stops with
`Discovery error: no_topic: SCHEME_ERROR`. A plan that leaves the topic out
drops it, so the refusal is the only warning the operator gets.

ADR 0105 lets the YDB provider refuse a transfer into an undeclared table,
because a table is a relation and `declared_relations` lists it. It said a
topic needs no list, because "the comparison's state carries it". That holds
for a topic only when the comparison that receives the transfer also owns the
topic. It does not: the engine runtime batches a comparison request by the
models each comparison owns (`engine/comparison.go`, `comparisonBatches`), and
the YDB provider owns topics and transfers in separate comparisons
(`ydb-topics`, `ydb-transfers`). The transfer comparison sees no desired topic
and cannot tell a declared topic from an undeclared one.

## Decision

### A comparison names the models whose declared objects it reads

`models.declared-objects` adds `ModelComparison.declared_kinds`: object model
kinds of the same engine, each owned by another object comparison of that
engine, whose declared objects the comparison reads. For such a comparison a
host fills `CompareModelsRequest.declared_objects` with the identity of every
desired object of those kinds, each once, in identity order, the identity's
kind naming its model. The provider lists the two-way feature, and so does
each engine with a comparison that names declared kinds.

The list holds what the desired schema the comparison request is built from
holds: the run's scope applies first. A topic a `--include` or `--schemas`
scope leaves out is not declared to the comparison, as it is not declared to
the topic comparison either; a provider that refuses a transfer of an
undeclared topic refuses it under such a scope too, which is what the
built-in engine did with the same desired schema.

A relation stays in `declared_relations`, and a provider's own model never
goes there: a topic is not a relation. The two lists answer one question for
two kinds of object, and the proto comment of `declared_relations` now says
so.

A host that does not list the feature sees no declared kinds and sends no
declared object, so a provider compares as it does for a host of contract
25.32.

### What the SDK enforces

`NegotiatedDescription` removes the declared kinds unless the host lists the
feature. `ValidateDescription` refuses the engine's feature without the
provider's or without a model comparison, and declared kinds on an engine that
does not list the feature, on a facet comparison, which receives no object
context, a kind named twice or blank, the comparison's own kind, whose
declared objects its desired schema already holds, and a kind no object
comparison of the engine owns. `ValidateComparisonRequest` refuses declared
objects that the engine or the request's target did not negotiate, an
identity named twice, an invalid identity and an identity of a kind the
comparison does not name.

## Alternatives considered

**List topics in `declared_relations`.** The list would answer "is this name
declared" for every kind at once. ADR 0105 rejected it: a topic is not a
relation, a table can take a name a topic holds, and the list's validator
would have to accept every model kind of every provider. A comparison that
reads relations would then have to skip kinds it does not know.

**Send every desired object of the engine to every comparison.** It needs no
description field, and the YDB provider would see topics. It also sends each
comparison the identities of every object it never reads, on every call, and
the SDK could not refuse a list a comparison did not ask for. A declared
interest keeps the request to what the owner reads, and is checked on both
sides.

**Let the transfer's desired value require the topic.** A normalized value
already lists `required_objects`, and the host keeps a required object from
being dropped. A requirement keeps a held object; it does not say whether the
object is declared, and the host does not refuse a requirement nothing
declares, since a required object may be one the database holds and the
schema leaves alone. Turning requirements into a declaration check would
change their meaning for every provider.

**Assign topics and transfers to one comparison.** The YDB provider could
compare both in one service, and the state would then carry both. It merges
two owners with separate change kinds, actions and refusals only to share a
list, and the next provider with a cross-model reference would have to do the
same.

## Consequences

- The YDB provider can refuse a transfer of an undeclared topic once both
  sides list the feature, as the built-in engine did. A topic held as a
  changefeed of a declared table is a facet of the table, not an object
  model, so the provider still reads it from `declared_relations` and the
  table's own facets.
- A comparison's description states which other models it reads, and the
  host computes the list before the runtime batches the request per
  comparison.
- A host of contract 25.32 sends nothing new, and a provider compares as
  before for it.

## Acceptance evidence

- SDK unit tests: `TestDeclaredObjects_HappyPath`,
  `TestDeclaredObjects_FailurePath`, `TestDeclaredObjectsNegotiation_HappyPath`,
  `TestDeclaredObjectsNegotiation_FailurePath` and
  `TestDeclaredObjectsDescription_FailurePath` in `provider-sdk/provider`.
- The conformance suite's narrowed host profiles include the feature for every
  provider that offers it.
- The host adoption sends the declared objects through a provider process
  (`integration/providercli`).
