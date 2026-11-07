# ADR 0119: A comparison reads the declared facets of another comparison's models

Status: Proposed

Contract: 25.37

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G228)

Relates to: [ADR 0109](0109-a-comparison-knows-the-declared-objects-of-another-comparisons-models.md)

## Context

The YDB provider refuses a declared transfer whose topic the schema does not
declare, as a topic or as a changefeed of a declared table, because YDB accepts
a transfer of a missing topic and then stops it (`Discovery error`). Ptah's
built-in YDB engine at 74de0b65b refused it in these words:

```text
transfer t: it reads topic c2533/orders/other, which the schema declares
neither as a topic nor as a changefeed, so the plan leaves no such topic, and
YDB accepts such a transfer and then stops it (`Discovery error`); declare the
topic or the changefeed
```

ADR 0109 gave the transfer comparison the declared topics: a comparison names
object models of the engine in `ModelComparison.declared_kinds`, and the host
sends their identities in `CompareModelsRequest.declared_objects`. A
changefeed is not an object. In the YDB provider the changefeeds of a table are
one facet of the table, owned by the facet comparison of row tables, and
ADR 0109 refuses a facet kind among the declared kinds. So the transfer
comparison sees the declared table and not its changefeeds, takes any stream of
a declared table for one of its changefeeds, and plans a transfer of an
undeclared changefeed. Measured on YDB 26.2.1.14 with plugin master: the
provider plans `CREATE TRANSFER` from `orders/other` where the declaration
declares the table `orders` with another changefeed, and the built-in engine
refused it.

## Decision

`models.declared-facets` adds `ModelComparison.declared_facet_kinds`: facet
model kinds of the same engine, each owned by a facet comparison of that
engine, whose declared facets an object comparison reads. For such a
comparison a host fills `CompareModelsRequest.declared_facets` with every
desired facet of those kinds, as a `ComparisonRecord`: the owner's identity,
the kind, the targets and the value, once and in kind and owner order. A facet
the declaration excludes on purpose has no value and is not listed. As for
declared objects, the list holds what the desired schema the request is built
from holds, after the run's scope.

A two-way feature: the provider lists it, and so does each engine with a
comparison that names declared facet kinds. A host that does not list it sees
no declared facet kinds and sends no declared facet. The SDK refuses declared
facet kinds on an engine without the feature or on a facet comparison, a kind
named twice or blank, and a kind no facet comparison of the engine owns; and,
in a request, declared facets the engine or the target did not negotiate, a
facet of a kind the comparison does not name, an owner of a kind the facet's
comparison does not own, a facet without a value of its kind, a facet listed
twice, and declared facets sent to a facet comparison.

## Alternatives considered

**Let `declared_kinds` name a facet kind and send the facet's identity.** A
facet's identity is its owner and its kind. The changefeed names a transfer
needs are inside the facet's value, so the identity alone answers nothing.

**Give each facet value the identities of the objects it holds.** The provider
could list each changefeed's stream as an identity, and the host would send
those in `declared_objects`. That is a new concept every facet owner would
have to state, for a comparison of the same engine that can decode the facet
value itself.

**Refuse the transfer in the facet comparison of row tables.** That comparison
sees the changefeeds and not the transfers, which are objects of another
comparison.

## Consequences

- The YDB transfer comparison can refuse a transfer of a changefeed the
  declaration does not declare, as the built-in engine did.
- A comparison that names a facet kind receives each such facet's whole value.
  For changefeeds that is small; a comparison names only the kinds it reads.
- The built-in engine also accepted a transfer of a topic the read recorded
  and could not describe. The transfer comparison receives no current coverage
  of topics, and this feature does not add it; register G228 keeps that half
  open.

## Acceptance evidence

- SDK: `TestDeclaredFacets_HappyPath`, `TestDeclaredFacets_FailurePath`,
  `TestDeclaredFacetsNegotiation_HappyPath`,
  `TestDeclaredFacetsNegotiation_FailurePath` and
  `TestDeclaredFacetsDescription_FailurePath` in `provider-sdk/provider`.
- Host adoption: the host lists the feature and sends the declared facets of
  the kinds a comparison names, through a provider process.
- YDB adoption: the transfer of an undeclared changefeed refused on 26.2.1.14,
  as built-in 74de0b65b refused it.
