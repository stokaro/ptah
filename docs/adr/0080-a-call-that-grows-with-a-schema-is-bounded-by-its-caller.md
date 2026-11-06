# ADR 0080: A call that grows with a schema is bounded by its caller

Status: Proposed

Contract: 25.26 (no change)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0079](0079-a-described-model-travels-by-reference.md)

## Context

The SDK client bounded every provider call by `runtime.CallTimeout`, 30
seconds, unless the caller set an earlier deadline. An `Execute` that set a
statement timeout got 30 seconds plus that timeout for each unit; one that set
none got 30 seconds for the whole batch.

The work of some calls grows with the schema: inspection reads every object,
the offline services validate, prepare, compare, plan and render every table,
and an `Execute` of a plan carries every statement. Measured on YDB 25.1.4.7
and 26.2.1.14 through the YDB provider, after ADR 0079 lets a request of 500
tables fit one message: an apply that creates 500 tables sends them as one
`Execute`, which took 27 seconds on a quiet runner and ran past 30 on a loaded
one, ending as an unknown outcome after 212 tables; the read and the planning
calls of a 500-table plan took 4 to 7 seconds each, and grew past 30 under
load. Ptah's built-in YDB engine at 74de0b65b had no such bound, and applied
the same 500 tables in 19 to 25 seconds.

A fixed bound protects a host from a provider that never answers. It also
fails a read or a plan the database would answer in full, and the larger the
database the more certainly. No fixed number serves both.

## Decision

A call whose work grows with a schema runs until its caller's context ends:
`Inspect`, every offline service (`callOffline`), `AnalyzeSQL`, and an
`Execute` that sets no statement timeout. The caller's own deadline applies
whenever it sets one.

An `Execute` that sets a statement timeout keeps its bound of 30 seconds plus
the timeout for each unit, so the provider's stop at the timeout still reaches
the caller as a confirmed outcome. Every other call keeps `CallTimeout`: opening
and closing a session, a lock, a transaction, a history or seed call, a
statement, and the embedding store's calls, whose bounds the embedding store
section of the SDK README states.

Nothing is negotiated. The bound is the client's own, a provider of any
contract minor serves the same request either way, and the contract keeps its
minor.

## Consequences

- A read, a plan or an apply of a large schema no longer fails because it is
  large. It ends when it completes, when the database or the provider fails
  it, or when the caller's context ends: an interrupt, or a deadline the
  caller sets.
- A provider that never answers such a call holds the host until the caller's
  context ends, as the built-in engine held it on a database that never
  answered. A host that needs a bound sets a deadline of its own.
- A provider sees no deadline on such a call unless the host set one. A
  provider that bounds its own work does so with its own limits, as before.
