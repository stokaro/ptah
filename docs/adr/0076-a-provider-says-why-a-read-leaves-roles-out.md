# ADR 0076: A provider says why a read leaves roles out

Status: Proposed

Contract: 25.26

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0075](0075-a-provider-names-the-identities-its-models-had-before.md)

## Context

A read may leave out of its description roles the server holds. The host
counts them in a note, so an operator who sees no role block can tell a server
without roles from a description that declined to mention them:

```text
note: 9 roles Ptah manages on this server are not described, because <cause>; comparison still treats them as present, so none of them is planned as a CREATE ROLE.
```

`internal/rolescope` words the cause from the dialect's descriptor. A
PostgreSQL read leaves out the roles nothing in the inspected schemas refers
to. Ptah's built-in YDB engine declared another rule, `RoleScopeUserMade`: its
read left out the users and groups the cluster made for itself, the
database's owner, and every user and group of a dev realm. Measured with the
host at 74de0b65b on YDB 25.1.4.7 and 26.2.1.14, `ptah db read` and
`ptah-compat schema inspect` print, for the 9 roles a fresh `local-ydb`
holds:

```text
because the cluster made them or they own the database, or the read was of a dev realm, which shares its database's users and groups
```

The YDB provider leaves out the same 9 roles for the same reasons and names
them in `ObservedCommonObjects.roles_out_of_scope`. The host keeps no
descriptor of a provider's dialect, so the note falls back to the default
rule, on both lines:

```text
because nothing in the inspected schemas refers to them
```

The sentence is false for YDB. The roles are left out because the cluster
made them, not because nothing refers to them, and an operator who declares
a grant to one of them is told the wrong reason it was not described.

## Decision

### A read says why it leaves roles out

`ObservedCommonObjects.roles_out_of_scope_reason`, under the two-way feature
`schema.role-scope-reasons`, is the owner's own clause for why this read left
`roles_out_of_scope` out of the description. The host prints it after
"because" in its note. The reason belongs to the read, not to the engine, so
an owner can word a read of a dev realm differently from a read of a
database; the YDB provider gives the built-in engine's clause for both, as
the built-in engine did.

A read without a reason keeps the note the host words from the dialect's
descriptor, so a built-in dialect and a provider that does not list the
feature print what they print today.

### What the SDK enforces

`ValidateDescription` refuses the feature on an engine without the provider's
feature or without the `ROLE` common kind. A read's reason is refused without
the feature negotiated, without a role out of scope, and when it is not one
trimmed line of at most 300 bytes or ends with a period, since the host
continues the sentence after it. The note never names a role; the reason
must not either, which no check can prove. A host that does not list
`schema.common-objects` receives no common objects and no reason.

## Alternatives considered

**An engine declares a role scope the host words.** The provider could name
`RoleScopeUserMade` in its description. The host would keep the YDB sentence,
engine knowledge that shared code must read from a declaration rather than
hold, and a provider whose read leaves roles out for a third reason could
not say so.

**A reason on the engine.** One sentence in the description would be enough
for the built-in engine's wording, but a reader that leaves roles out for
different reasons on different reads, as a dev realm read does, could not
say which applied.

**Keep the default rule.** It tells the operator something false about the
server, which is the one thing the note exists to prevent.

## Consequences

- An inspect of a YDB database through the provider names the cause the
  built-in engine named.
- The YDB descriptor value `RoleScopeUserMade` has no dialect left in Ptah
  that declares it.
- The note's wording for a provider is the provider's; a reason that reads
  badly is the provider's to fix.

## Acceptance evidence

- SDK unit tests: `TestRoleScopeReasonsDescription_HappyPath`,
  `TestRoleScopeReasonsDescription_FailurePath`,
  `TestRoleScopeReasonsNegotiation`, `TestRolesOutOfScopeReason_HappyPath` and
  `TestRolesOutOfScopeReason_FailurePath` in `provider-sdk/provider`.
- The host adoption prints a provider's reason in `internal/rolescope` and
  carries it from the provider host.
- The YDB provider's note equals the built-in engine's on YDB 25.1.4.7 and
  26.2.1.14.
