# ADR 0084: A captured claim names a provider's model by the identity the provider owns

Status: Proposed

Contract: 25.27

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0037](0037-a-provider-encodes-the-source-it-decodes.md),
[ADR 0075](0075-a-provider-names-the-identities-its-models-had-before.md)

## Context

An HCL export through a provider records what it knows of the provider's
models in a `ptah:feature-coverage` header, and a reader refuses a file whose
header names a model by an identity it does not hold (ADR 0075). The identity
has an owner, a kind, a representation, a schema version and a definition
digest.

The host names a provider's model there by the identity of its own codec for
the model. That codec's definition is the host's resolved-model JSON
(`ptah.provider.resolved-model.v3`): the provider's id, release version,
semantic identity, protocol revision and negotiated features, and for each
engine the model, the engine's features, identifier rules, YAML keys, Go
directives, comparisons and planning services. The digest in the header is
the SHA-256 of that JSON, and the schema version is always 1.

Measured through the host at 2a2ca2c45 with the YDB provider
(stokaro/ptah-ydb-plugin#299), on YDB 25.1.4.7 and 26.2.1.14, the header digest
of the coordination-node model, whose own `definition_sha256` is `ed553660...`
in every one of these builds:

| Provider build | built as dev | built as 0.4.0 | built as 0.4.1 |
| --- | --- | --- | --- |
| 62dd5121 | `20c8cd0f...` | | |
| d8db5375 | `66e0d75f...` | `14278ae0...` | `c15bc0e8...` |
| fc280a33 | `006a2693...` | `d93f517a...` | `3bd696b1...` |

The HCL export of one build labeled 0.4.0 is refused by the same code labeled
0.4.1: `incompatible feature codec: coverage model
"github.com/stokaro/ptah-ydb-plugin/coordination-node"/desired`. Between
d8db5375 and fc280a33 no definition digest moved, yet every header digest
did, because the provider's features and planning services changed. So every
HCL export a provider release writes is refused by its next release, whatever
the models mean.

The provider cannot repair this with a predecessor. It does not know the
digest, which the host computes over what the two sides negotiated. And the
SDK refuses a predecessor whose former owner is the provider and whose former
kind is a model the engine declares, so a provider cannot name an earlier
definition of its own model even when it knows it.

No Ptah release writes a captured header for a provider's model: v0.16.0
through v0.21.0 write none. The built-in YDB engine's header, which names its
own model, is the only one a released export carries.

## Decision

### The claim names the identity the provider owns

A host writes a provider's model into a captured claim under the identity
the provider describes: the provider's id as the owner, the model's kind and
representation, its `schema_version`, and `sha256:` followed by its
`definition_sha256`. Reading a captured claim, it resolves each kind record
against the models the provider describes and the predecessors it declares,
and holds the claim under its own codec's identity from then on, as it holds
every other value of the model.

The header is the only place that changes. A saved plan's envelopes and its
fingerprint keep the host's resolved identity, because a saved plan binds to
the provider release that planned it: another release must not apply it.

A model the provider's engines describe with different schema versions or
digests has no one identity a claim can name. The host refuses to read or
write a header that claims such a model.

### A provider names an earlier definition of its own model

`models.own-predecessors` is a two-way feature of contract 25.27, beside
`models.predecessors`. With it, a `ModelPredecessor` may name the provider as
`former_owner` and the model's own kind as `former_kind`, with a
`schema_version` or a `definition_sha256` the model does not have now. A
provider whose definition text changes without changing what a claim means
names the identity it replaced, and a file exported before the change reads
as the same claim.

The provider lists the feature, and so does each engine that declares such a
predecessor. A host that lists `models.predecessors` and not this feature
receives the other predecessors and none of these; an engine left with none
does not list `models.predecessors` for that host. A host that lists neither
receives no predecessor, as before.

### What the SDK enforces

`ValidateDescription` refuses an engine's `models.own-predecessors` without
the provider's, a predecessor that names the provider and the model's own
kind without the engine's feature, one that is the model's current identity,
and one that names another model the provider declares. A predecessor that
names the provider and a kind the engine no longer declares is a former
identity like any other owner's, as before.

## Alternatives considered

**Keep the resolved digest and let the provider declare it.** The provider
would have to compute a host-internal JSON over features it learns only when
a host describes it, and declare, for every model, the digest of every
earlier release with every feature set a host might have negotiated. The
list grows with each release, and a host that changes its JSON breaks all of
it.

**Leave the release out of the resolved definition.** The version and the
features are not the only parts that move. A new planning service or YAML key
moves the definition of every model the engine declares, and fc280a33 moved
all of them that way while no model's meaning changed.

**Relax the SDK check without a feature.** A host built with provider-sdk
v0.22.0 or v0.23.0 that lists `models.predecessors` would refuse the whole
description of a provider that names an earlier definition of its own model,
so that provider would not start with it. Negotiation lets the provider serve
such a host without those predecessors.

**Write no header for a provider's models.** A file would read with every
release, but a model it leaves out would no longer be claimed, so a node
removed from an exported file would not be dropped (ADR 0075).

## Consequences

- An HCL export one provider release writes reads with its next release when
  no model definition changed, and with a changed definition when the
  provider names the identity it replaced.
- A provider that changes a definition decides whether an earlier file still
  means the same thing. When it names no predecessor, the file is refused,
  which is the outcome a change of meaning needs.
- The header no longer says which release wrote it, and a saved plan still
  does.
- An HCL export written through the provider by a host before this change
  names the host's resolved digest and is refused. No release wrote one, and
  Ptah owes no compatibility with its own earlier behavior before v1.

## Acceptance evidence

- SDK unit tests: `TestModelPredecessorsDescription_HappyPath`,
  `TestModelPredecessorsDescription_FailurePath`,
  `TestModelPredecessorsNegotiation` and `TestOwnPredecessorsNegotiation` in
  `provider-sdk/provider`.
- The host writes and reads a provider's header under the identity the
  provider owns, and reads an export of one fixture release with another, in
  `integration/providercli`.
- The YDB provider reads, on YDB 25.1.4.7 and 26.2.1.14, an HCL export of one
  build labeled 0.4.0 with the same code labeled 0.4.1, and an export written
  before a definition text changed once it names the replaced identity.
