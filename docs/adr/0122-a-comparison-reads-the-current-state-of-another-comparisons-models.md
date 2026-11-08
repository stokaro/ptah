# ADR 0122: A comparison reads the current state of another comparison's models

Status: Proposed

Contract: 25.40

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G239)

Relates to: [ADR 0109](0109-a-comparison-knows-the-declared-objects-of-another-comparisons-models.md),
[ADR 0119](0119-a-comparison-reads-the-declared-facets-of-another-comparisons-models.md)

## Context

The YDB provider refuses a declared transfer whose topic the schema declares
neither as a topic nor as a changefeed of a declared table, because YDB accepts
a transfer of a missing topic and then stops it (`Discovery error`). ADR 0109
and ADR 0119 give the transfer comparison what it needs for that refusal: the
identities of the declared topics and the declared changefeeds of the declared
tables.

Ptah's built-in YDB engine at 74de0b65b refused the same transfer, with one
exception. A topic or a changefeed the read recorded without describing it is
one the plan neither drops nor changes, so a transfer that reads it stays
valid. The built-in engine accepted such a source: a topic the read recorded as
uninspected or unrepresentable, such as a persistent queue group, and a
changefeed of a declared table the read recorded as unrepresentable, such as
one whose consumer names a codec Ptah does not declare.

The transfer comparison cannot see what the read holds. The host gives each
comparison the current state of its own kinds only, and topics and changefeeds
belong to other comparisons. So the provider refuses such a transfer. Measured
on YDB 26.2.1.14 with host 2e824eb99 and YDB plugin f5426f30, for a changefeed
whose topic has a consumer with codec 10042 and a schema that declares the
table without the changefeed:

```text
transfer t/ingest: it reads topic t/orders/feed, which the schema declares
neither as a topic nor as a changefeed, so the plan leaves no such topic, and
YDB accepts such a transfer and then stops it (`Discovery error`); declare the
topic or the changefeed
```

The built-in engine planned nothing for a transfer the database holds, and
`CREATE TRANSFER` for a new one, each with a warning that the changefeed could
not be compared. On 25.1.4.7 both refuse a transfer by the capability.

## Decision

`models.current-context` adds `CompareModelsRequest.current_context`, a
`ComparisonState` in the observed representation. For a comparison that names
declared kinds or declared facet kinds, a host fills it with every current
object of those kinds and every current facet of those facet kinds, each with
its value, and the current coverage of those kinds, as the comparisons that own
them receive them. It holds what the current state the request is built from
holds, after the run's scope. A comparison that names no such kind receives
none.

A two-way feature: the provider lists it, and so does each engine whose
comparisons read it. A host that does not list it sends no current context, and
such a comparison decides as it does today. The SDK refuses the feature on an
engine without the provider's or without a model comparison; and, in a
request, a current context the engine or the target did not negotiate, one sent
to a comparison that names no declared kind or declared facet kind or to a facet
comparison, a record of a kind the comparison does not name, a facet whose owner
is of a kind the facet's comparison does not own, and a state that fails the
checks of a comparison's own current state.

## Alternatives considered

**Refuse in the transfer planner.** A planning service can take the topics as
a context kind (`planning.object-context`) and every table whole
(`PLANNING_TABLES_CAPTURED`), so it sees the declared and the current topics
and changefeeds without a contract change. But the host plans only after a
comparison found a change. A run with no change would report a transfer of a
missing topic as synced, as the built-in engine did (measured on 26.2.1.14:
`Schema is synced` over a running transfer whose topic was dropped by hand),
where the comparison refuses it today; that hides a transfer YDB has stopped.
A command that compares without planning would lose the refusal too. The
planner would also take every table whole on every plan in place of its
outline, which contract 25.27 exists to avoid, and answer a receipt for every
topic.

**Send only the identities of the subjects a read recorded.** The YDB read
holds a changefeed it cannot describe as a value of the table's changefeed
facet, which the provider fails to convert, and not as a coverage record. A
facet's coverage record names its owner, not one changefeed, so an identity
alone cannot say which stream of the table the read holds.

**Mark the read transfer when the read could not describe its source.** That
covers a transfer the database already holds. A new transfer of such a
changefeed has no read value to carry the mark.

**Let the transfer comparison own topics and changefeeds too.** A model has
one owner, and those models already belong to the topic comparison and to the
facet comparison of row tables.

## Consequences

- The YDB transfer comparison can accept a transfer of a topic or a changefeed
  the read recorded without describing it, as the built-in engine did, and
  still refuse one of a topic the plan leaves absent.
- A comparison that names declared kinds receives the current values of those
  kinds. For the YDB transfer comparison that is the changefeed facet of every
  row table, beside the declared ones it already receives.
- A run with no change still refuses a transfer of a topic neither the
  declaration nor the read holds. The built-in engine refused in its planner,
  which such a run never reaches, so it reported the run as synced. The
  provider is stricter here, deliberately.

## Acceptance evidence

- SDK: `TestCurrentContext_HappyPath`, `TestCurrentContext_FailurePath`,
  `TestCurrentContextNegotiation_HappyPath`,
  `TestCurrentContextNegotiation_FailurePath` and
  `TestCurrentContextDescription_FailurePath` in `provider-sdk/provider`.
- Host adoption: the host lists the feature and sends the current state of the
  kinds a comparison names, through a provider process.
- YDB adoption: a transfer of a changefeed the read cannot describe is planned
  on 26.2.1.14, as built-in 74de0b65b planned it.
