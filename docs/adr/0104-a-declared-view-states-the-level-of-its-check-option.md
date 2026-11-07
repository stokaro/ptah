# ADR 0104: A declared view states the level of its check option

Status: Proposed

Contract: 25.32

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5647](https://github.com/stokaro/ptah/issues/5647);
the open contract gaps of [ADR 0102](0102-the-provider-contract-freezes-with-ptah-1-0-and-keeps-major-25.md)

Relates to: [ADR 0047](0047-common-view-facets-retain-authored-and-observed-state.md)

## Context

A view's check option has two levels on PostgreSQL, MySQL and MariaDB.
`LOCAL` checks a row written through the view against the view's own
condition. `CASCADED`, what `WITH CHECK OPTION` means when it names no level,
checks it against the conditions of the views it reads as well.

The contract's desired view, `SQLView`, holds a bool, `with_check`. It travels
in a host's view creation, in the declared common objects a host sends for
preparation and planning, and in the views a provider reads from SQL. The
observed view already reports the level as text, `ObservedView.check_option`.
So a provider can read `LOCAL` and cannot be asked to create it, and a
provider that parses `WITH LOCAL CHECK OPTION` from SQL has no field to
return it in. Issue #5647 measures the same loss in the host's own model.

## Decision

### A level beside the bool under a two-way feature

`schema.view-check-options` adds `SQLView.check_option`, an enum of
`CHECK_OPTION_NONE`, `CASCADED` and `LOCAL`. The provider lists the two-way
feature, and so does each engine whose views carry the level. Under it every
`SQLView` states the level, in every message that carries one, and
`with_check` is true exactly when the level is `CASCADED` or `LOCAL`.

A peer that does not list the feature sends `CHECK_OPTION_NONE`, and
`with_check` alone says whether a view has a check option, as it does for a
peer of contract 25.31. The bool stays: replacing it would change what a
peer of 25.31 reads.

### What the SDK enforces

Every validator that reads a declared view refuses a level sent to a target
that does not list the feature, a level that disagrees with `with_check`, and
a level the SDK does not know. `PrepareCommonObjects` keeps the level each
declared view has. `ValidateDescription` refuses the engine's feature without
the provider's.

## Alternatives considered

**Replace the bool with the enum.** It leaves one field and no rule tying two
together. A peer of contract 25.31 reads the bool, so removing it is an
incompatible change. The freeze may still decide it, since it allows
incompatible changes until Ptah 1.0; this record does not.

**A `local_check` bool beside `with_check`.** Issue #5647 proposes it for the
host's model, where a zero value must keep today's meaning. On the wire an
enum with an explicit `CHECK_OPTION_NONE` says the same and leaves room for
an engine's own level without a third bool.

**Refuse `LOCAL` for providers.** It is the cheapest honest answer, and the
host's model cannot express the level yet. It keeps a provider that reads
`LOCAL` unable to return it from SQL, and every provider would need the field
anyway once the host's model carries it.

## Consequences

- A provider can create a `LOCAL` view, and return one it reads from SQL, once
  both sides list the feature.
- The host fills the level from its own model. Until that model carries the
  level (#5647), a host sends `CASCADED` for every view with a check option.
- The observed text and the desired enum name the same levels.

## Acceptance evidence

- SDK unit tests: `TestViewCheckOptions_HappyPath`,
  `TestViewCheckOptions_FailurePath` and
  `TestViewCheckOptionNegotiation_FailurePath` in `provider-sdk/provider`.
- The conformance suite's narrowed host profiles include the feature for every
  provider that offers it.
