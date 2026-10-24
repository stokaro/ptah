# ADR 0025: A provider declares the source syntax it owns

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

## Context

A table described with Go annotations or YAML does not reach a provider-selected
run today. The measurements that show it:

- `providerhost.Host.Provider()` fills the provider's ID, its value codecs and
  its YAML source keys. It sets none of `Annotations`, `HCL` and `Selections` on
  `engine.Provider`. A provider-selected run therefore knows no engine
  directive, no table attribute and no column attribute written in Go.
- The protocol declares `YAMLSourceKey` and nothing for Go or HCL. The SDK
  validators already accept GO and HCL source documents
  (`provider-sdk/provider/source.go`), so the decode side exists and the claim
  side does not.
- The YDB attributes (`ttl=`, partitioning, `columnfamily`, `changefeed`, index
  options) are decoded only by the bundled `internal/ydbsource`. It emits
  `ptah.run/ydb/*` facets. The plugin has no codec for those kinds, so
  `operandFacets` drops them on the way to the provider without an error.
- The plugin claims one YAML key, `coordination_nodes`, at document scope. It
  claims no table-scope key and no Go directive.
- Directive names belong to exactly one provider. Selecting the plugin for
  `ydb` while the bundled source stays registered for the same target is a
  collision, or a silent loss of the bundled facets.

The provider cannot become the default for an engine until a table written the
way users write tables reaches it.

## Decision

**A provider declares the Go syntax it owns, the way it declares its YAML keys.**
The provider description lists directives and attributes with their scope:
document, table, column or index. The host does the lexing, which is the same
for every engine: it splits a comment into directive names and key/value pairs
and checks that each name is claimed by exactly one provider. It sends the
provider the parsed attributes for the object, not the comment text. The
provider returns facets through `DecodeSource` for a GO document, and the host
attaches them to the table, column or index the way `DecodeAttributes` does for
the bundled sources. Meaning stays with the provider; syntax mechanics stay with
the host.

**A provider-selected engine replaces the bundled source for that target.** When
a provider is selected for an engine, the bundled source registered for the same
target is not used. The host does not project bundled facets into the provider's
native models. A projection would make the provider depend on the host's facet
kinds, which is the coupling ADR 0023 removes.

**The provider accepts the syntax the bundled source accepts.** A user who
switches an engine to its provider keeps every annotation and YAML key that
worked. Until the bundled code is deleted, a corpus of annotated fixtures is
decoded through both paths and the resulting desired schema is compared. A
difference is a defect of the provider.

**Coverage is declared per source.** A provider reports for each decoded source
which kinds it covered completely. The host treats a kind the provider did not
cover as unknown, not as absent.

## Alternatives

**Project the bundled facets into the provider's models.** The host keeps
`ydbsource`, converts `ptah.run/ydb/*` facets into the plugin's native kinds and
sends those. It needs no protocol change. It keeps the engine's syntax in the
host, ties the plugin to host types, and leaves a second decoder to maintain
after the flip. Rejected.

**Send the provider the raw comment text.** The provider lexes directives
itself. Every provider would carry a copy of the lexer, error positions would
differ per provider, and the host could not check name ownership before the
call. Rejected.

**Keep both sources and merge their output.** The bundled source and the
provider both decode, and the host merges. Two owners for one directive name is
the case the ownership rule exists to prevent, and a merge has no answer when
they disagree. Rejected.

## Consequences

- The protocol grows a Go source claim and a decode request for GO documents
  before the v1 freeze. The SDK validators and the conformance suite follow.
- The two decoders coexist until the default flips, and the fixture corpus is
  the cost of proving they agree.
- A provider-selected run no longer has the bundled source for its engine. A
  user of a provider who relied on a bundled attribute the provider does not
  accept gets an error naming the attribute instead of a silent drop.
- The host holds the generic part of the work and no engine knowledge: lexing,
  ownership of names, attaching facets. Each engine's attribute list lives with
  its provider.

## Amendments

- [ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md)
  limits "the host does the lexing" to dialects that share the host's grammar. A
  dialect with its own grammar declares `DOCUMENT` mode and owns the whole SQL
  document.
- HCL claims remain open; a later record applies this ADR's claim pattern to them.
