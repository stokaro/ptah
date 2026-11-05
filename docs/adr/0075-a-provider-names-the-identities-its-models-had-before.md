# ADR 0075: A provider names the identities its models had before

Status: Proposed

Contract: 25.25

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0036](0036-a-provider-claims-yaml-entry-keys-and-hcl-blocks.md),
[ADR 0037](0037-a-provider-encodes-the-source-it-decodes.md),
[ADR 0074](0074-a-provider-claims-the-index-kinds-it-models.md)

## Context

An HCL export records what it knows of an owner's models in a
`ptah:feature-coverage` header. The header names each model by its exact
identity: owner, kind, representation, schema version and definition digest.
A reader with another definition refuses the document rather than give the
claim a meaning it was not written with.

Ptah's built-in YDB engine wrote such a header into every HCL export. Its
coordination nodes are the one model it recorded there:

```hcl
// ptah:feature-coverage {"format":2,"representation":"desired","kinds":[{"model":{"owner":"ptah.run/ydb","kind":"ptah.run/ydb/coordination-node","representation":"desired","version":1,"definition":"sha256:4c044f15565424fd0212b50fa50c1f760e53c228028c0f0e9ac8399ad3d2183a"},"knowledge":[{"outside":{"state":"complete"}}]}]}
```

The claim says the document describes every coordination node, so a node the
file leaves out is planned for removal. Measured with the host at 74de0b65b on
YDB 25.1.4.7 and 26.2.1.14: `ptah schema inspect --format hcl`, with and
without `--schemas`, its `--out-dir` files and `ptah-compat schema inspect`
all write this header, byte for byte the same on both lines. The codec
definition and the digest computation are unchanged from v0.14.0 to v0.21.0,
so every release that wrote the header wrote this identity. Applied back with
that binary, the export is synced, and with a node block removed it plans
`DROP COORDINATION NODE`.

The YDB provider owns the same model under its own identity,
`github.com/stokaro/ptah-ydb-plugin/coordination-node`, with its own
definition. The host reads a captured header with the provider's codecs, so
through the provider the same file is refused before anything is compared:
`unknown feature codec: "ptah.run/ydb/coordination-node"/desired`. A user
upgrading from a release with the built-in engine cannot apply the schema
files that release exported.

## Decision

### An engine names its models' former identities

`Engine.model_predecessors` lists `ModelPredecessor` entries under the two-way
feature `models.predecessors`. An entry names a desired or observed model of
the engine by kind and representation, and one former identity of it: the
former owner, the former kind, the schema version and the definition digest,
in lowercase hexadecimal, as a captured claim names it after `sha256:`. The
YDB provider names the built-in engine's coordination-node identity as a
predecessor of its own coordination-node model.

The provider makes the statement, because only the owner of a model can say
that an earlier definition means the same thing as its own. The host holds no
list of former engines or of their digests.

### The host reads a captured claim under the current identity

A host that lists the feature registers each predecessor with the provider's
codec of that model. Reading a coverage document, it replaces a kind record's
predecessor identity with the model's current identity, and the kind of each
subject record of the former kind with the model's kind, before it validates
the document. The claim keeps its knowledge exactly: complete stays complete,
a region stays a region, a subject that is not described stays not described.

A document that names a model under both identities is refused as a duplicate
claim. A predecessor never makes a claim the document did not make, and a
document that names an identity neither current nor declared is refused as
before.

A host writes a claim it restates, such as the header it repeats in every file
of a split document, under the current identity. The former identity is only
read.

### What the SDK enforces

`ValidateDescription` refuses predecessors on an engine that does not list the
feature, the feature on an engine with none or without the provider's feature,
a predecessor of a model the engine does not declare or of an operation or
change model, an empty owner or kind, a kind or owner with white space or a
control character, a zero schema version, a digest that is not 64 lowercase
hexadecimal digits, a former identity that is a model the provider declares,
and a former identity named twice in an engine. A host that does not list the
feature receives no predecessor.

## Alternatives considered

**A table of the built-in engine's identities in the host.** The host could
map `ptah.run/ydb/...` kinds to the provider that serves the `ydb` engine. It
would put engine knowledge back into the host, which shared code must read
from a descriptor, and it would let the host decide that two definitions mean
the same thing, which only their owner knows.

**Hand the captured header to the provider's `DecodeSource`.** The provider
could translate the claim itself. `DecodeSource` carries no header today, so
this is a contract addition too, and every reader of a captured header (the
HCL frontend, the splitter that restates a header, a coverage round trip)
would need a provider call where a codec registration serves them all.

**Read a header of an unknown model as no claim.** The file would read, but a
node removed from it would not be dropped, where the release that wrote it
drops it. The claim is the reason the header exists.

**Rewrite the files with a tool.** It asks every user to find and convert
their exports before upgrading, and an export left unconverted still fails.

## Consequences

- A file exported by a release with the built-in YDB engine reads through the
  provider with the meaning it was written with, and a restated or split file
  carries the provider's identity.
- A provider that changes a model's definition can name its own earlier
  identity as a predecessor, so a file it exported keeps reading.
- A predecessor states equivalence of meaning, which no check proves. A
  provider that names one owns that statement.

## Acceptance evidence

- SDK unit tests: `TestModelPredecessorsDescription_HappyPath`,
  `TestModelPredecessorsDescription_FailurePath` and
  `TestModelPredecessorsNegotiation` in `provider-sdk/provider`.
- The host adoption reads a captured header under a registered predecessor
  in `core/schemaext` and the provider host.
- The YDB provider reads exports recorded from the 74de0b65b binary through
  the host on YDB 25.1.4.7 and 26.2.1.14.
