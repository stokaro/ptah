# ADR 0077: A provider says where its Go directives stand

Status: Proposed

Contract: 25.26

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0037](0037-a-provider-encodes-the-source-it-decodes.md),
[ADR 0074](0074-a-provider-claims-the-index-kinds-it-models.md)

## Context

A Go export hangs the standalone objects of a schema on one struct, and the
writer puts their directives in its comment in a fixed order of groups: the
schemas, then the types and sequences, the views, the roles and grants, and
the default privileges. A directive an owner adds names its group:
`AfterSchemas`, `AfterViews` or `AfterPrivileges` in `core/annotation`.

Ptah's built-in YDB engine used two of them. Its writer at 74de0b65b
(`internal/ydbsource/annotations_encode.go`) put topics, async replications,
transfers, secrets, external data sources and tables, and streaming queries
after the views, and resource pools, their classifiers and coordination
nodes after the default privileges, below the grants. The YDB provider
writes the same directives through `EncodeSource`, and the host places every
directive of a provider's document after the views
(`internal/providerhost/go_encode.go`), because a reply has no way to say
otherwise.

Measured on YDB 25.1.4.7 and 26.2.1.14 with a topic, a resource pool and its
classifier in one directory and a coordination node in another, `ptah
introspect --single-file` with the host at 74de0b65b writes the topic on the
first line of the comment and the pools, the classifier and the node after
the 12 grants. Through the provider the pools, the classifier, the topic and
the node all stand before the grants. The file reads back the same, but an
export the built-in engine wrote and one the provider writes differ in every
such line, so a project that keeps its export under review sees the objects
move.

## Decision

### An encoded Go directive of the document says where it stands

`source.go-placements` adds `GO_AFTER_SCHEMAS`, `GO_AFTER_VIEWS` and
`GO_AFTER_PRIVILEGES` to `EncodedFragment.Placement`, the enumeration with
which an HCL block of the owner's already says where it stands. An encoded
part of a Go `DOCUMENT` directive may carry one, and the host writes the
directive in that group. A part that carries none stands after the views, as
it does today. Within one group the writer keeps the order of the owner's
parts.

The YDB provider places resource pools, classifiers and coordination nodes
after the default privileges, and returns its parts in the built-in engine's
order of groups.

### What the SDK enforces

`ValidateDescription` refuses the feature on an engine without the provider's
feature or without `source.encode`. `ValidateEncodedSource` refuses a Go
placement on a part that is not a Go `DOCUMENT` directive, an HCL placement on
a Go directive, and a Go placement in a reply to a host that did not list the
feature.

## Alternatives considered

**A placement on the claim.** `GoSourceDirective` could name its group once
in the description. Every fragment carries a copy of its claim, which both
peers compare with the negotiated claim, so the field would ride on every
decoded fragment that never needs it, and an encode reply already says where
an HCL block stands part by part.

**A table of directive names in the host.** The host could know which YDB
directives belong after the default privileges. It would put engine knowledge
back into shared code.

**Sort the provider's directives after the grants.** It moves the topics and
the other objects that read or move data, which the built-in engine wrote
after the views.

## Consequences

- A Go export through the provider writes the YDB objects in the lines the
  built-in engine wrote them in.
- A provider that adds a directive of the document decides its group. A
  directive that should stand among the types, such as a type a column is
  declared over, can say so.

## Acceptance evidence

- SDK unit tests: `TestGoPlacements_HappyPath`, `TestGoPlacements_FailurePath`,
  `TestGoPlacementsOfAnHCLBlock`, `TestGoPlacementsDescription_FailurePath` and
  `TestGoPlacementsNegotiation` in `provider-sdk/provider`.
- The host adoption writes a placed directive in its group in
  `internal/providerhost`.
- The YDB provider's Go export places its directives as the built-in engine's
  export does on YDB 25.1.4.7 and 26.2.1.14.
