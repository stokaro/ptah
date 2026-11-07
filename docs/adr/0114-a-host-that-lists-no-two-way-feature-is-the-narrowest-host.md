# ADR 0114: A host that lists no two-way feature is the narrowest host

Status: Proposed

Contract: 25.33 (no change of its own)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Amends: [ADR 0092](0092-a-provider-is-checked-by-a-suite-that-acts-as-a-strict-host.md)

## Context

The conformance suite describes a provider to a host that lists every
two-way feature, to a host that lists none, and to each host that lists every
feature but one. Under a host that leaves out one feature it compares the reply
with the full description narrowed by `NegotiatedDescription`. Where the SDK's
`ValidateDescription` refuses that narrowed description, because a feature the
host still lists needs the one it left out, the provider has to refuse the
host, and the SDK's server does.

The host that lists no feature was held to another rule. Its `Describe` had to
succeed, and a refusal failed `description.valid`. A contract 25.0 host, which
announces no version and lists no feature, failed
`handshake.contract-25.0-host` the same way.

The DuckDB provider found the difference with contract 25.32. Its one planning
service owns the engine's tables under `planning.table-ownership` and plans
nothing else. Without the feature that service plans nothing, the SDK refuses
the description (`missing or duplicate planning service`), and its server
refuses `Describe`. The suite accepted that refusal from a host that leaves out
`planning.table-ownership` and failed it from a host that lists nothing.

## Decision

A host that lists no two-way feature is a narrowed host like the others. Where
the full description without every two-way feature is one the SDK refuses, the
provider refuses that host, and the suite counts the refusal as the answer
negotiation asks for: `description.valid` checks the descriptions the provider
gives, and `handshake.contract-25.0-host` passes on the refusal. A provider
that describes itself to such a host anyway fails, since the SDK refuses the
description it sent.

The other checks that use the connection of that host, the offline calls, the
reads and the probes of unknown sessions, skip it when the provider refused it,
as they skip a narrowed host that the provider refused.

## Alternatives considered

**Make `planning.table-ownership` a required feature for a provider that
declares it.** A provider would list it among the features a host must list,
and a host that does not list it would be refused before negotiation. It fixes
one feature and leaves the rule open for the next: any two-way feature a
provider's only service depends on meets the same check. Required features also
belong to the host's side of the contract, where a host says what it cannot
work without.

**Keep the rule and require providers to stay valid without every two-way
feature.** A provider whose only planning service owns its tables would have to
declare a second service that never plans anything, which is the model contract
25.32 removed.

## Consequences

- A provider that cannot work with a host that lists no two-way feature
  conforms, and so does one whose only planning service owns its tables.
- A provider that answers a host the SDK would refuse still fails.
- The suite has one rule for narrowed hosts, which the SDK README already
  stated.

## Acceptance evidence

- `TestCheck_HostWithoutTwoWayFeatures_HappyPath` in `provider-sdk/conformance`
  checks the memory provider with one planning service that owns its tables,
  and every check passes. Without the decision `description.valid` fails;
  without the matching rule for a contract 25.0 host
  `handshake.contract-25.0-host` fails.
