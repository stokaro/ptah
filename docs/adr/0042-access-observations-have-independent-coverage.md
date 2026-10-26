# ADR 0042: Access observations have independent coverage

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Amends: [ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md).

Contract: 25.5.

## Context

YDB inspection records the owner of a schema object and group memberships
independently of the principals selected for description. Its catalog can name
a membership whose member is out of scope, and a server may report
self-membership. Security analysis must retain those observations even when
Ptah refuses to author an equivalent declaration.

The common wire carries memberships only inside described roles and has no
object-owner record. A list of out-of-scope role names cannot carry these facts.
A login flag also needs to distinguish a principal known not to log in from a
principal whose login state was not inspected.

## Decision

The two-way feature `schema.common-access`, negotiated beside
`schema.common-objects`, adds `OBJECT_OWNER` and `ROLE_MEMBERSHIP` to an engine's
common-object kinds. Each has its own `CommonObjectCoverage` record. Complete
principal inspection does not imply complete ownership or membership inspection.

`ObservedCommonObjects.object_owners` contains `ObjectOwner` records. Each has a
structured `Reference`, a nonempty owner principal and optional
`owner_can_login`. An absent flag means unknown, a present false means known not
to log in, and true means known to log in. The host preserves this distinction
in its catalog and refuses decisions that need a login fact it does not know.

The reference belongs to the selected engine. Its object identity follows that
engine's identifier rules. Repeated or conflicting owners for one normalized
identity are invalid. Database-root references may have an empty name, as
database grant targets do; this does not invent a database name.

`ObservedCommonObjects.memberships` contains independent `RoleMembership`
records. A pair needs nonempty group and member names and must be unique. It
does not require either principal to appear in the described roles. Observed
self-membership is valid. Authoring and planning policies may refuse a requested
self-membership without deleting a server fact from inspection.

When an engine advertises `ROLE_MEMBERSHIP`, every observed role's `member_of`
must be empty. The independent list is the only membership representation for
that engine. This prevents conflicting or duplicated facts across two fields.

An owner or membership record appears only when its kind's coverage is complete.
Uninspected coverage carries a reason and cannot be interpreted as an empty
collection. The host keeps these coverage kinds separate when scoping,
comparing and analyzing security.

Negotiation removes the new kinds when either required feature is unavailable.
It removes an empty common-object declaration rather than advertising a service
with no supported kind. Existing declared roles, memberships and grants retain
their validation and rendering contracts.

## Alternatives considered

**Attach all memberships to described roles.** This forces inspection to expand
its principal scope and still cannot represent a membership when neither role
is described. Rejected.

**Use complete role coverage for all access facts.** Principal visibility does
not prove membership or ownership visibility. Rejected.

**Default an unread login flag to false.** This converts missing evidence into a
security assertion. Rejected.

**Carry access facts as provider payloads.** These are common catalog and
security facts. Opaque models would duplicate their host interpretation.
Rejected.

## Consequences

The wire preserves observed access facts without broadening authored schema
policy. Hosts need distinct coverage kinds and an explicit unknown login state.
Tests cover out-of-scope and self memberships, duplicate representations,
partial coverage, normalized owner collisions and all login states. A provider
must establish the same facts in live inspection before advertising completeness.
