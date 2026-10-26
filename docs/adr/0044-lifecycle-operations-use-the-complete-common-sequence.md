# ADR 0044: Lifecycle operations use the complete common sequence

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0024](0024-the-provider-owns-table-changes-through-one-lifecycle-service.md),
[ADR 0041](0041-a-provider-plans-authored-declarations-without-inventing-observed-absence.md).

Contract: 25.5.

## Context

An owned streaming query may need to stop before a table changes and restart
after the table exists in its new shape. Table creation and removal also need
the complete common sequence to place their resource effects correctly.

The `DEPENDENT` phase has point placement in the host graph. Expanding that
phase to span the whole sequence would change the meaning older providers rely
on and could reorder their operations. A new phase needs explicit host graph
support, not only a new enum name.

## Decision

`PlanningOperation.LIFECYCLE` is a distinct phase. The two-way feature
`planning.lifecycle` authorizes it only when both peers support the placement
semantics. An engine lists it beside a planning or declaration service, and the
host lists it only after implementing that graph window.

The phase starts with a placement window across the complete common sequence.
Resource effects, explicit dependencies and rewrite constraints narrow that
window. It does not override them or allow the provider to schedule a private
sequence. The host validates and orders the complete graph before exposing a
rendered or executable result.

`DEPENDENT` keeps its existing point placement. The SDK refuses `LIFECYCLE`
without negotiation and refuses unknown phase values. Removing a declaration
service during negotiation also removes lifecycle support from an engine with
no remaining planner.

## Alternatives considered

**Broaden `DEPENDENT`.** This gives an existing name a different guarantee and
changes older providers' ordering. Rejected.

**Use only explicit dependencies.** Provider code cannot enumerate host steps
introduced by other owners, and resource lifetimes still need graph placement.
Rejected.

**Require only contract minor 25.5.** Knowing the enum does not establish that a
host implements its scheduler. Rejected.

## Consequences

The graph can place a lifecycle around common work without weakening an older
phase. Tests must keep a `DEPENDENT` control and prove both early and late
lifecycle placement, explicit and resource constraints, cycle refusal, and
whole-plan validation. Providers must prove stop, mutation and restart order
through the host graph before using this phase for live work.
