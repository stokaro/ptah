# ADR 0097: A provider starts in the host's working directory

Status: Proposed

Contract: 25.30

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md)

## Context

An engine that runs inside the provider's process, such as DuckDB, opens a
database file the address names. A user writes a relative address,
`duckdb://./app.db`, and means the file next to where they ran `ptah`, as the
host's built-in SQLite engine reads `sqlite://./app.db`.

`OpenRequest` carries the engine and the address and nothing else. The DuckDB
provider resolves a relative path against its own working directory, which is
right only because the SDK's bootstrap starts the provider without setting
one, so the process inherits the host's. Nothing states this, nothing tests
it, and a non-Go bootstrap, or a later change to the SDK's, could start the
provider somewhere else and open another file without an error.

## Decision

### The working directory is the host's

The contract states that a provider starts in the host's working directory,
the one the host resolves its own relative paths against, and that a relative
file path in `OpenRequest.address` resolves against it. `runtime.Start`, and
the bootstrap the conformance suite shares with it, set the directory
explicitly rather than rely on inheritance. A provider must not change its
working directory while it serves. The process test
`TestProviderStartsInTheHostsWorkingDirectory` starts a provider after the
host changes directory and reads the provider's directory back.

The rule needs no field and no feature: every SDK release starts a provider
this way, and so it holds for each host and provider of the contract.

## Alternatives considered

**Carry the directory in `OpenRequest`.** A new field would be a two-way
feature, and every provider would have to resolve a path against it, where
today the operating system does that for them. A provider of an earlier
contract would ignore it and still resolve against its own directory, which
is the same directory. A field also invites a second answer: a host could
send a directory other than the one it resolves its own paths against.

**Resolve relative paths on the host.** The host does not parse a provider's
address and cannot tell a file path from any other part of it.

**Refuse a relative address.** It is a common spelling for a file database,
and the community binary accepts it for SQLite.

## Consequences

- A relative file address names the same file for the host and the provider.
- A host that embeds the SDK in a long-running process resolves a relative
  address against that process's directory, as it resolves its own paths.
- A non-Go bootstrap must start the provider in the host's working directory.

## Acceptance evidence

- `TestProviderStartsInTheHostsWorkingDirectory` in `provider-sdk/integration`.
