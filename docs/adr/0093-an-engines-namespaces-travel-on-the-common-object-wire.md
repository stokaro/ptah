# ADR 0093: An engine's namespaces travel on the common-object wire

Status: Proposed

Contract: 25.29

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md)

## Context

A provider's read reports tables and the common objects of the kinds its
engine declares: views, sequences, roles, grants, comments, owners and
memberships. Nothing reports a namespace, and no change creates or removes
one. The host's provider planner refuses `SchemaDiff.SchemasAdded` and
`SchemaDiff.SchemasRemoved`.

Three engines that leave or join the host have namespaces. DuckDB has
schemas inside each catalog, `main` by default, created with `CREATE SCHEMA`.
Spanner's PostgreSQL interface has schemas, `public` by default. YDB has
directories, which a table at a path creates and which a provider removes
when the last object in them goes.

Ptah's built-in Spanner engine at d59a3b63d plans `CREATE SCHEMA IF NOT EXISTS
"app"` for a SQL, YAML or HCL declaration of `app.t`, creates an empty
declared schema, and drops in a down file a schema the up file created,
measured on the PGAdapter emulator v0.56.2. Through the provider, a SQL or HCL
document that declares `app` is refused with `does not support
SchemaDiff.SchemasAdded`, also when `app` exists, because the read reports no
namespace and the comparison takes every declared one for an addition. The
provider creates the schema of a table it creates in a schema of its own
(stokaro/ptah-spanner-plugin#12), but it cannot see whether the schema
exists, and nothing can create an empty one.

## Decision

### SCHEMA is a common-object kind

The common-object wire gains the kind `SCHEMA`, under the two-way feature
`schema.common-namespaces`. The provider and each engine that declares the
kind list the feature, and the engine needs `schema.common-objects`. A host
that does not list the feature receives a description without the kind.

A read that covers the kind lists, in `ObservedCommonObjects.namespaces`,
every namespace of its scope, the session's default one included when the
scope holds it, each by a `Reference` of kind `schema` with its name and the
session's database. A namespace of the scope the list does not name is
absent. A read that does not cover the kind says so in its coverage, as for
every common kind, and lists none.

A change of kind `schema` creates a namespace, `create_schema` with a
`NamespaceCreation` (its name and, on an engine that carries comments, its
comment), or removes one, `drop_schema`. The engine renders both.

### The host compares and orders, as it does for PostgreSQL

With the kind negotiated, the comparison treats the provider's engine as it
treats PostgreSQL: it adds a namespace the desired schema declares and the
read does not hold, and it never removes one the read holds and the
declaration does not name. A plan creates a namespace before the tables,
views and sequences in it, and creates the namespace of a created table the
read does not hold even when no document declares it. A namespace leaves
only in a plan that reverses a creation, such as a down file, after every
object of the plan, and the removal never cascades: what the namespace holds
by then is something the plan does not know, and the server refusing the
removal is the right answer.

### What the SDK enforces

`ValidateDescription` refuses the engine's feature without the provider's or
without the engine's common objects, and the kind without the feature or the
feature without the kind. Inspection refuses a namespace of a kind the read
did not cover, one that is not a `schema` reference with a name and no
schema, catalog or path, and a namespace listed twice under the engine's
rules for table names. A render request refuses a creation without a name,
a comment on an engine without comments, and a removal of anything but a
namespace.

## Alternatives considered

**A capability of its own.** A `schema.namespaces` feature with a field of
`Schema` and two operations of `Change` would carry the same facts. Every
engine above already carries views or grants on the common-object wire, and
that wire has what a namespace needs: per-kind coverage that tells an empty
list from an unread one, typed creation and removal operands, and host-side
ordering. A second mechanism would repeat each of them.

**Let the provider create the namespace inside its table creation.** That is
what the Spanner provider does without the contract. It cannot create an
empty declared namespace, it cannot tell an existing namespace from a missing
one, so it guards every creation, and a down file cannot remove what the up
file created. Each provider would write the same rule again.

**Drop a namespace the declaration no longer names.** The community binary
drops it with `CASCADE`. The host's PostgreSQL comparison keeps it, and so
did the built-in Spanner engine; dropping it would take objects the plan
never compared.

**Make YDB report directories.** A YDB directory exists because a table is in
it, and the YDB provider removes one with the table. The kind is optional: an
engine whose namespaces follow its objects need not declare it, and the
comparison then adds nothing.

## Consequences

- A SQL or HCL declaration of a schema, and a table in it, plan on a provider
  that declares the kind, and an empty declared schema is created.
- A down file removes a schema its up file created.
- A provider that declares the kind owns the completeness of the namespace
  list: a namespace it leaves out is planned again.
- A provider of an earlier contract, and one that does not declare the kind,
  is planned as before: a declared schema is refused by name.

## Acceptance evidence

- SDK unit tests: `TestCommonNamespaces_HappyPath`,
  `TestCommonNamespacesDescription_FailurePath`,
  `TestObservedNamespaces_FailurePath`,
  `TestObservedNamespacesWithoutTheFeature_FailurePath` and
  `TestRenderNamespaces_FailurePath` in `provider-sdk/provider`.
- The conformance suite's memory provider reads its namespace through the
  kind, and `TestCheck_FailurePath` reports `live.inspect` for one that lists
  namespaces to a host that did not list the feature.
- The host adoption compares and plans namespaces through a provider that
  declares the kind.
