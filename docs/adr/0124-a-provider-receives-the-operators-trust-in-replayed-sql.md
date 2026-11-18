# ADR 0124: A provider receives the operator's trust in replayed SQL

Status: Proposed

Contract: 25.43

Deciders: Ptah maintainers

Issue: [stokaro/ptah#6091](https://github.com/stokaro/ptah/issues/6091)

## Context

`--trust-replay-sql` and `PTAH_TRUST_REPLAY_SQL=1` state that the operator
trusts the SQL a run executes on its dev database, with the privileges of the
dev connection. The replay guards of the built-in engines read the answer as
`replay.Scope.TrustedSQL`: they accept a statement they refuse only because
they cannot confine its effects to the dev database, and keep every refusal
that protects the run.

An executable provider has no way to receive the answer. `ReplayScope` carries
the database, the ownership of the server and the session, and nothing else,
so a provider's guard judges every replay with the strict default. A provider
can also check statements again when a dev session executes them: the YDB
provider confines every statement of a session opened in a dev realm. A guard
that learned of trust would accept a statement that the same provider then
refuses at execution.

Trust has to stay apart from ownership. Ownership decides how much of the
server the run may change and what its cleanup removes; trust decides how much
the operator accepts the guard cannot confine. Reading trust as ownership
would select the guard of a server the run owns and widen the cleanup.

## Decision

`database.dev-replay-trust` adds two fields, each sent only when both sides
list the feature:

- `ReplayScope.trusted_sql`, the operator's answer for the statements of a
  `GuardReplay` request;
- `OpenRequest.trusted_sql`, the same answer for a session the host opens on a
  dev database whose run trusts its SQL, so the provider's checks at execution
  agree with its guard.

The provider decides what trust lifts, because it reads the dialect. It lifts
only refusals whose reason is that the effects cannot be confined to the
scope, and keeps every refusal that protects the run: the replay session's own
state, the identity it runs as, server configuration, an object the cleanup
cannot remove, and a statement it cannot read. Trust never sets
`ReplayScope.server`, never selects another guard, and never changes what
`CleanDevDatabase` removes.

The provider and each engine that honors trust list the feature beside
`database.dev-replay-guard`. When the operator asks for trust and the
negotiated engine does not list it, the host refuses to open the dev session
and names the provider, the engine and the two ways forward: a provider release
that supports the feature, or a run without trust. The SDK refuses either field
set without the feature, on the client and on the server, and a description
that lists the feature without the replay guard.

## Consequences

- An older provider keeps working with the strict default: the host sends no
  trust it did not negotiate.
- An explicit opt-in is never dropped in silence. It reaches a provider that
  can honor it, or the run stops before the replay.
- A provider that confines execution reads trust from the session, so a
  statement its guard accepts under trust also runs.

## Alternatives considered

- **Select the owned-server guard under trust.** It would lift refusals that
  protect the cleanup and claim the whole server for a run that owns none of
  it.
- **Carry trust only in `ReplayScope`.** A provider that confines execution
  would refuse at execution what its guard accepted, and the replay would fail
  part way.
- **Carry trust on every `Execute` and `RunStatement`.** Trust belongs to the
  run on one dev database, not to one call, and every call path would have to
  repeat it. The session already binds a provider to one dev database.
- **Ignore trust for a provider that does not negotiate it.** The operator
  would get the strict policy they opted out of without being told.
