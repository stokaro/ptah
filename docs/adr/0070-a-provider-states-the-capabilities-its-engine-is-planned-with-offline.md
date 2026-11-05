# ADR 0070: A provider states the capabilities its engine is planned with offline

Status: Proposed

Contract: 25.27 (provisional: renumbered if an earlier claim merges first)

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

The contract has no place for the facts. `QueryDialect.default_capabilities`
holds the keys the query builder reads and nothing else. `Engine.release_lines`
names lines but carries no capabilities. The provider knows the values: its
session states them for each server it opens.

## Decision

### The engine states its offline capabilities

Contract 25.27 adds one field to `Engine` and the two-way feature
`engine.default-capabilities`:

```proto
message Engine {
  // ...
  // The capability facts the host plans the engine with when no session
  // states the server's: those of the release line a command assumes without
  // a server. A present false is an established answer; an absent key is
  // unknown. Sent only to a host that lists engine.default-capabilities.
  map<string, bool> default_capabilities = 22;
}
```

The provider and each engine that states the field list the feature. The
host lists it too. Negotiation removes the field for a host that does not, so
an older host reads the description it reads today.

The facts are the provider's answer for the line it would assume: the newest
line it supports, with the server's default settings. The answer is not a
claim about any particular server. A command that connects plans with the
session's facts, as it does today.

### What the SDK enforces

Both transport boundaries refuse:

- the field on an engine that does not list the feature;
- the feature on an engine with no facts;
- a capability name the session's names would refuse;
- the engine's feature without the provider's.

### What the host does

While a provider is selected, the host registers each engine's facts under the
engine's ID and its aliases. `capability.ForDialect` answers them for a name no
descriptor describes, so every offline command plans with them. A registration
lasts for the life of the process, as the query dialect's does. A name
registered again with other facts is refused, because two answers for one
engine cannot both be its default.

`--server-version` is unchanged: Ptah holds no release lines for an engine
only a provider serves. Render refuses the flag, and lint refuses it with the
reason. Per-line facts are a separate decision: they would need the provider's
reading of a version string, which differs by engine.

### What the YDB provider sends

The provider lists the feature for `ydb` and states the facts its session
states for a 26.2 server with default feature flags. These are the facts of
the built-in engine's `YDB262` preset, restricted to the keys the provider
reads.

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

**Facts on each release line.** `--server-version` would resolve offline
again. The host would need to map a version string to a line, which the
provider's engine defines: YDB writes `stable-25-4-1` and `26.2.1.14`. The
offline default needs no such mapping. Deferred to its own record.

**Keep the YDB presets in the host.** The presets are engine knowledge that
moves with the provider (ADR 0023). They would lag every provider release.
Rejected.

## Consequences

- Contract 25.27 adds a field and a two-way feature. It is numbered after
  contract 25.26, which ADR 0069 proposes.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- An offline command for an engine whose provider states the facts plans as
  it did with the built-in engine. Without them it plans against unknown
  facts, as today, and the provider decides what to refuse.
- `ptah db capabilities` can show the facts as the line the session refines.

## Acceptance evidence

- SDK tests accept an engine that states its facts and lists the feature, and
  refuse each case above. Negotiation removes the field for a host that does
  not list the feature.
- Through the SDK fixture provider, `ptah schema render` plans with the
  engine's stated facts, and a provider without them renders as today.
- Through the YDB provider with the host of stokaro/ptah#5298, the render
  measurement above returns the statement master returns.
