# ADR 0111: A planning service receives the grants of each held table

Status: Proposed

Contract: 25.33

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G222, the TEXT form)

Relates to: [ADR 0106](0106-a-planning-service-owns-tables-without-a-table-setting.md)

## Context

Some changes cannot be made to a table in place. A provider that owns a
table's lifecycle then plans a rebuild: it creates a new table, copies the
rows, drops the old table and gives the new one the old name. On YDB a
table's grants live in the table's own access list, so the drop takes them
with it, and the new table starts with its directory's permissions alone.

The built-in YDB engine at 74de0b65b planned a GRANT for every permission the
rebuilt table held, except one the same plan revoked or granted anyway
(`internal/planner/dialects/ydb/access.go`, `regrants`). It read the held
grants from the comparison, so the GRANTs were part of the plan in every form:
an apply, a saved plan and the SQL of a migration file.

The YDB provider gives the grants back only when it runs the rebuild itself:
its copy step reads the old table's access list from the server and grants it
on the new table before the rows. A rebuild written as the SQL text of a
migration file runs later, from the file, and nothing in the file reads the
server, so the grants are lost and the next run grants them again (G222). The
provider cannot write the GRANTs into the text, because a planning request
carries no grant: `PlanningTable.current` is an `ObservedTable`, which has no
access list, and the host never fills `PlanFeaturesRequest.current_common`.

## Decision

### Each held table carries its grants under a two-way feature

`planning.table-grants` adds `PlanningTable.current_grants`, a
`PlanningTableGrants` message. The provider lists the feature, and so does
each engine whose planning reads the grants; such an engine carries `GRANT`
common objects, since the grants are what its read reports in
`ObservedCommonObjects.grants`.

Under the feature, every table of a planning request whose `current` is
present carries `current_grants`, for a service that reads whole tables:

- `complete` with every grant the read found whose target is the table, a
  grant on some of its columns included. Each target names the table as the
  read named it: kind `table`, the held table's schema and name, no path.
- not `complete` and empty when the read did not describe grants. The owner
  then knows nothing about them, which is not the same as knowing there are
  none.

The list is always present under the feature, so its absence cannot be read
as "no grants". A grant the same plan revokes or grants is in the list too;
the request's common steps carry the revoke or the grant, and the owner leaves
it out of what it gives back, as the built-in engine did. An outlined table
carries no grants, since an outline carries nothing but the names of its
states.

A host that does not list the feature sends no grants, as a host of contract
25.32 does. When the host's features leave the engine no `GRANT` common
objects, the negotiation removes the engine's feature: such a host reads no
grant to send.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's and
on an engine without `GRANT` common objects. `ValidatePlanningRequest` refuses
grants on a table without a current state or under a negotiation that leaves
the feature out, a held table without its grants when the feature is
negotiated, grants on a table whose read did not describe them, a grant
without a role or a privilege, a grant on another object, a repeated column
and a repeated grant.

## Alternatives considered

**Fill `PlanFeaturesRequest.current_common`.** The field exists and carries a
read's common objects as context. The host keeps no copy of what a provider's
read returned: it converts the read into its own model, so it would have to
rebuild a whole `ObservedCommonObjects` from that model for every planning
call, with the coverage of each kind, the roles, the owners, the memberships
and the namespaces. That conversion loses what the model does not keep, and
every message it builds meets the SDK's strict validation of a read, a new way
for planning to fail on every provider engine. The owner needs the grants of
the tables it replaces, not the whole read.

**Let the host plan the GRANTs itself.** The host sees the rebuild only as a
provider's operation, and cannot tell which rebuilds lose a table's grants.
The YDB provider's own copy step already gives them back when it runs the
rebuild, and an engine that renames a table in place keeps them, so a GRANT
the host added would duplicate one or fix nothing.

**Add the grants to `ObservedTable`.** The message is the table as the read
holds it and travels in other requests too, such as a CHECK comparison. Grants
there would reach requests that have no use for them and would need a rule for
each.

## Consequences

- A provider can write the GRANTs of a rebuilt table into a migration file's
  SQL, as the built-in YDB engine did, once both sides list the feature.
- Every planning request for a service that reads whole tables grows by the
  grants of the tables it holds.
- A provider that lists the feature reads `complete` before it trusts an empty
  list.

## Acceptance evidence

- SDK unit tests: `TestTableGrants_HappyPath`,
  `TestTableGrantsNegotiation_HappyPath`, `TestTableGrantsContent_FailurePath`,
  `TestTableGrantsPresence_FailurePath` and
  `TestTableGrantsDescription_FailurePath` in `provider-sdk/provider`.
- The conformance suite's narrowed host profiles include the feature for every
  provider that offers it.
- The host adoption sends each held table's grants to a planning service that
  negotiated the feature, and a provider process test writes them back after a
  rebuild in the SQL of a migration file.
