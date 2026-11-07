# ADR 0116: An engine states where its connection reads capabilities

Status: Proposed

Contract: 25.35

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5770](https://github.com/stokaro/ptah/issues/5770)

Relates to: [ADR 0115](0115-a-provider-engine-states-the-lexical-rules-of-its-text.md)

## Context

`ptah db capabilities` lists the capabilities a server answers differently
from its release line, under the line that says where the connection read
them, so an operator knows what to change to change them. The host writes
that line from what it knows of a built-in dialect: an installed extension on
PostgreSQL, or a generic sentence for a provider's connection.

The YDB provider reads refined capabilities from the cluster's feature flags
at its monitoring endpoint. The host named that source by comparing the
dialect with `"ydb"` (#5751). It was the only comparison of a provider
engine's name left in non-test host code, where shared code reads a
descriptor field rather than an engine's name (#4204).

## Decision

Contract 25.35 adds `Engine.connection_capability_source`, one line of text,
under the two-way feature `engine.connection-capability-source`. The provider
and each engine that states the line list the feature, and so does a host
that prints it.

The SDK refuses:

- an engine that lists the feature without the provider's;
- an engine that lists it and states no line, or states one without listing
  it;
- a line that is not one line of text, or is longer than 512 bytes.

A host that does not list the feature receives no line and prints its own
sentence for a provider's connection, as a host of contract 25.34 does.

While the provider is selected, the host registers the line for the engine's
ID and aliases, and `ptah db capabilities` prints it beneath a refined
capability of that engine. The host compares no engine name to word it.

## Consequences

- The YDB wording moves to the YDB provider, which states its monitoring
  sentence. Until a provider release states it, a host prints the generic
  sentence for YDB. Pre-v1, no compatibility is owed.
- Any provider can name its own source the same way, without a host change.
