# ADR 0070: A provider states the capabilities its engine is planned with offline

Status: Proposed

Contract: 25.22

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0069](0069-a-provider-scans-the-sql-text-of-its-engine.md)

## Context

A command that plans without a connection still plans against capabilities.
`ptah schema render`, `schema validate`, the generator, a checkpoint, an export
and the offline lint all read them from `capability.ForDialect`, which answers
the default preset of the dialect's descriptor. A connected command reads the
capabilities the provider's session states instead.

The built-in YDB descriptor named the preset `YDB262`. While it was registered,
an offline command for YDB planned with that preset, even when the YDB provider
served the command. Deleting the built-in engine (stokaro/ptah#5298) deletes the
preset. `capability.ForDialect("ydb")` then answers the empty set, and every
key reads as unknown.

The YDB provider refuses to plan against unknown facts, which is the right
default. Measured with `ptah schema render --dialect ydb` over one Go entity
with an `Int64` key and a `Utf8` column, with stokaro/ptah-ydb-plugin 07068a0:

| Host | Result |
| --- | --- |
| master 51812210f, built-in engine | `CREATE TABLE` rendered |
| master, the provider through `--provider-config` | the same statement |
| #5298, the provider | `error rendering ydb schema: YDB table creation requires established nontransactional DDL` |

`--server-version` is refused the same way. The built-in descriptor resolved
a YDB version string to the preset of its release line. Without it, `ptah
schema render --server-version 26.2 --dialect ydb` is refused with
`--server-version selects a capability preset of a built-in dialect, and the
ydb engine is served by a provider, which declares none`, and `ptah sql lint`
loses the per-line facts its findings name, such as `vector_indexes` false on
25.1.

The contract has no place for the facts. `QueryDialect.default_capabilities`
holds the keys the query builder reads and nothing else. `Engine.release_lines`
names lines but carries no capabilities. The provider knows the values: its
session states them for each server it opens.

## Decision

### The engine states its offline capabilities

Contract 25.22 adds one field to `Engine`, one to `ReleaseLine` and the
two-way feature `engine.default-capabilities`:

```proto
message Engine {
  // ...
  // The capability facts the host plans the engine with when no session
  // states the server's: those of the release line a command assumes without
  // a server. A present false is an established answer; an absent key is
  // unknown. Sent only to a host that lists engine.default-capabilities.
  map<string, bool> default_capabilities = 24;
}
message ReleaseLine {
  // ...
  // The capability facts of a server of the line with its default settings,
  // which the host plans with for a --server-version that names the line.
  map<string, bool> capabilities = 7;
}
```

The provider and each engine that states the field list the feature. The
host lists it too. Negotiation removes the field for a host that does not, so
an older host reads the description it reads today.

The facts are the provider's answer for the line it would assume: the newest
line it supports, with the server's default settings. Each release line
carries the same answer for that line. Neither is a claim about any particular
server. A command that connects plans with the session's facts, as it does
today.

A `--server-version` names the line whose `line` equals it, or the longest
line it starts with followed by a dot: `26.2.1.14` names `26.2`. This is the
whole rule the host applies to a version string. A version that names no line
is refused with the lines the provider declares, never planned with the
default.

### What the SDK enforces

Both transport boundaries refuse:

- the field on an engine that does not list the feature;
- the feature on an engine with no facts, or with a release line that has
  none;
- facts on a release line of an engine that does not list the feature;
- a capability name the session's names would refuse;
- the engine's feature without the provider's.

### What the host does

While a provider is selected, the host registers each engine's facts under the
engine's ID and its aliases. `capability.ForDialect` answers them for a name no
descriptor describes, so every offline command plans with them. A connected
command reads the session's facts where it plans with the session, and
`capability.ForDialect` everywhere else, so the registered facts reach a
connected plan too. The host also
registers the facts of each release line, and resolves a `--server-version`
of the engine by the rule above for `schema render`, `schema validate`, `sql
lint` and `migrations lint`. A registration lasts for the life of the
process, as the query dialect's does. A name registered again with other facts
is refused, because two answers for one engine cannot both be its default.

### What the YDB provider sends

The provider lists the feature for `ydb`, states the facts its session states
for a 26.2 server with default feature flags, and states each line's facts the
same way. To them it adds every other key of the built-in engine's presets,
with the presets' values: the host reads keys no session states, such as
`index_covering_columns`, which decides whether a changed list of covered
columns is a change of the index. Without it every index with covered columns
compares as changed, and a second apply of an unchanged schema drops the index
and creates it again.

## Alternatives considered

**Use `QueryDialect.default_capabilities`.** The field exists and the host
reads it. Its contract is the query builder's: the keys a builder clause
reads. Making it the engine's planning facts would give one field two meanings,
and a provider that left out a key the planner reads would plan differently
from a provider that did not know about the second meaning. Rejected.

**Treat an unknown key as the provider's default.** The provider would plan
an unknown fact as if it were established. That reverses the meaning of an
absent key everywhere else in the contract, and a refusal that names a missing
fact would become a silent guess. Rejected.

**Resolve a version through the provider.** An RPC could answer the facts of
any version string by the engine's own reading, including YDB's
`stable-25-4-1`. Every command that takes `--server-version` would then start
the provider to read a flag, and the answer would depend on a call where a
declared line does not. The dotted-prefix rule covers every line a provider
declares. A version spelled another way is refused with the lines to choose
from. Rejected.

**Keep the YDB presets in the host.** The presets are engine knowledge that
moves with the provider (ADR 0023). They would lag every provider release.
Rejected.

## Consequences

- Contract 25.22 adds two fields and a two-way feature, beside the RPC that
  ADR 0069 adds to the same contract.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- An offline command for an engine whose provider states the facts plans as
  it did with the built-in engine, with or without a `--server-version`
  naming a declared line. Without them it plans against unknown facts, as
  today, and the provider decides what to refuse.
- `ptah db capabilities` can show the facts as the line the session refines.

## Acceptance evidence

- SDK tests accept an engine that states its facts and lists the feature, and
  refuse each case above. Negotiation removes the field for a host that does
  not list the feature.
- Through the SDK fixture provider, `ptah schema render` plans with the
  engine's stated facts, and a provider without them renders as today.
- Through the YDB provider with the host of stokaro/ptah#5298, the render
  measurement above returns the statement master returns, and the plugin's
  host-driven render and lint tests that pass `--server-version` pass again.
