# ADR 0039: A provider declares how a scope names its objects and which grants name them

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0025](0025-a-provider-declares-the-source-syntax-it-owns.md),
[ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md)

Contract: 25.4, with ADR 0038.

## Context

The `--include`, `--exclude` and `--schema` selectors of the schema commands
name an owner's objects through `core/selectext`. An owner registers an
`Extension` with its provider (`engine.Provider.Selections`), and
`internal/atlasfilter` applies the selectors with no import of the owner. What
the embedded YDB owner registers, in `internal/ydbsource/selections.go`:

- **Selections.** Ten kinds, each named by a selector type of its own:
  `coordination_node`, `streaming_query`, `secret`, `topic`,
  `external_data_source`, `external_table`, `async_replication` and
  `transfer` in the directory that holds them (`selectext.InSchema`), and
  `resource_pool` and `resource_pool_classifier` by a database-wide name in
  which a dot is a literal character (`selectext.DatabaseWide`).
- **A service that keeps what a kept object needs.** A scope that keeps a
  classifier or a streaming query keeps the resource pool it routes to, when
  the side holds the pool and no exclusion names it. The service reads the
  pool from the object's value.
- **Grant objects.** A grant can name a topic, a secret or any of nine kinds,
  and it rides that object: a scope that excludes the object excludes the
  grant (`selectext.GrantService`, from `internal/ydbacl`). The object types a
  grant spells, `TOPIC` to `RESOURCE POOL`, and the YAML and annotation
  attributes that declare such a grant, `on_topic` to `on_resource_pool`, are a
  list in the host, `core/schemamodel.GrantObjectKinds`.

A provider has none of this. `internal/providerhost/schema.go` builds the
provider's `engine.Provider` with no `Selections`, and the contract has no field
to fill it from. So on a provider's engine a selector cannot name an owner's
object by type, a `--schema` exclusion does not reach one, and a grant on one
is not excluded with it. A grant
target of a kind outside the host's nine is refused when the host reads a
provider's SQL schema (`sqlGrant` in `internal/providerhost/sql_schema_model.go`).

The wire already carries two of the facts this needs. Inspection and the common
objects of ADR 0033 report a grant's target as a structured `Reference` whose
kind is the owner's, and a normalized value lists the objects it needs in
`NormalizedValue.required_objects`, which the host reads as
`schemaext` prerequisites. What is missing is the declaration that ties a kind
to a selector, a naming rule and a grant object type.

## Decision

### The engine declares its scope selections

The two-way feature `scope.selections` adds a field to `Engine`. The provider
lists it in `Description.features` and in each engine that declares selections,
and the host lists it in `DescribeRequest.host_features`. A provider sends the
field only to a host that lists the feature.

```proto
// Engine gains: repeated ScopeSelection scope_selections = 18;
message ScopeSelection {
  // A model kind of the engine with a DESIRED and an OBSERVED model.
  string kind = 1;
  // The selector type that names an object of the kind, such as "topic" in
  // `*[type=topic]`.
  string selector_type = 2;
  enum Naming {
    NAMING_UNSPECIFIED = 0;
    // Named by its schema and its name, as a table is.
    IN_SCHEMA = 1;
    // Named by its name alone, which is unique in the database; a dot is part
    // of the name.
    DATABASE_WIDE = 2;
    // In a schema, or on the database itself with no schema.
    SCHEMA_OR_DATABASE = 3;
  }
  Naming naming = 3;
  // The object's name is a relation's name: excluding the object excludes a
  // table of its name too. IN_SCHEMA only.
  bool relation = 4;
  // The object type a grant spells for an object of the kind, such as TOPIC.
  // Empty when no grant names one.
  string grant_object_type = 5;
  // Kinds of this engine's selections whose objects a kept object keeps when
  // it requires them.
  repeated string keeps_required = 6;
}
```

Each field has the meaning its `selectext.Selection` field has. The host builds
one `selectext.Extension` for the provider from the list, owned by the provider,
and registers it with the provider's targets, so `engine` checks it as it checks
a built-in owner's: the provider owns the desired codec of each kind, and two
owners cannot select one kind or define one selector type two ways.

### Naming is the host's, as for every owner

The host already reads a selector's name with the naming an owner declares; the
owner never parses a selector. A provider's objects are named the same way, so
their identities fit the naming: an `IN_SCHEMA` object's reference carries a
schema and a name and no database, catalog or path, a `DATABASE_WIDE` one a name
alone, and a `SCHEMA_OR_DATABASE` one a name and an optional schema. The host
refuses a scope over an object whose identity does not fit, naming the kind,
rather than keep or drop it on a guess.

### What a kept object keeps

`keeps_required` replaces the owner's selection service for the case YDB has.
When a scope keeps an object of the kind, it keeps each object the value
requires, as `NormalizedValue.required_objects` lists them, whose kind is in
`keeps_required`, when the side holds it and no exclusion names it. An exclusion
stays authoritative, as it is for the embedded owner. A requirement of another
kind keeps nothing, so the declaration says exactly which dependencies a scope
follows. The provider computes `required_objects` already; nothing new is
called.

### Grants ride the object they name

A grant whose object type is a selection's `grant_object_type`, compared without
regard to case or spacing, names an object of that selection's kind. The host
reads the grant's object name with the selection's naming, so an excluded or
unkept object takes its grants with it. The YAML and annotation attribute of
the type is `on_` followed by the type in lower case with spaces as
underscores: `TOPIC` is `on_topic`, `COORDINATION NODE` is
`on_coordination_node`, the spellings the host reads today. A grant target the
provider reports with a declared type is mapped to the host's grant model as a
built-in one is.

### What stays out

`selectext` has more than this record carries: fields an exclusion clears
(`*[type=synonym].comment`), schema-wide subjects, a baseline service and
additive change projections. No provider engine needs them yet, and each needs a
call to the owner. A later minor adds them with the owner that needs them.

### Validation

Both transport boundaries refuse, for each engine:

- `scope_selections` the negotiated description does not allow, or a
  non-empty list from an engine that does not list `scope.selections`;
- a selection whose kind is not a DESIRED and OBSERVED model of the engine, a
  kind selected twice, or an unspecified naming;
- a selector type that is not lower-case letters, digits and underscores
  starting with a letter, and a selector type two selections define with
  different naming or relation;
- `relation` with a naming other than `IN_SCHEMA`;
- a grant object type that is not upper-case words separated by single spaces,
  one two selections declare, or one of the common targets `TABLE`, `VIEW`,
  `MATERIALIZED VIEW`, `SEQUENCE`, `SCHEMA`, `DATABASE`, `FUNCTION`,
  `PROCEDURE` and `ROUTINE`;
- a `keeps_required` kind that is not another selection's kind, or one listed
  twice.

### Versioning

`scope.selections` is a compatible addition, part of contract 25.4. A provider
that declares selections does not raise its minimum contract: an older host
receives none and reads the provider's objects as it does today.

## Alternatives considered

**A selection RPC.** The provider would answer, for each side, which objects a
selector names and what the kept ones require, as the embedded owner's
`SelectObjects` does. The naming of a selector is already the host's for every
owner, and the one service result YDB needs is a dependency the wire carries.
An RPC would put a call into a filter that is pure data today. Rejected; a later
minor adds one if an owner needs fields cleared.

**Grant object types as a list of their own.** The engine would list grant
object types beside its selections. Every type names a kind that a scope also
names, and a grant must ride an object a scope can keep or exclude, so a type
without a selection would name an object no scope can reach. Rejected; the type
is a property of the selection.

**Keep every required object.** A kept object would keep everything
`required_objects` lists. YDB's scope keeps the pool a classifier routes to and
nothing else: an external table does not keep its data source. Following every
dependency would change what a scope keeps on YDB. Rejected.

**The provider resolves grant names.** The host would send a grant's object name
as written, as `RequestedChange.written` does for a secret path (ADR 0035). A
secret path has a spelling only YDB defines. A grant names its object the way a
selector names it, and the host already owns that reading for every owner.
Rejected.

## Consequences

- `Engine` grows one field, behind one feature. No RPC is added.
- The host builds a provider's selection extension from the declaration, and
  `internal/ydbsource.Selections` and `internal/ydbacl`'s grant objects move into
  the plugin's declaration.
- `core/schemamodel.GrantObjectKinds`, today a list of YDB's nine kinds, is read
  from the selected owners: the built-in ones and the providers' declarations.
- A provider reports its objects with identities that fit their naming, or the
  scope is refused.

## Acceptance criteria

These are for the implementation. None holds today.

- The SDK carries `Engine.scope_selections` as contract 25.4, and both
  boundaries refuse each case under "Validation". In-process tests negotiate
  the feature in both directions, and each validator is mutation-checked.
- The fixture provider declares two kinds, one `IN_SCHEMA` with a grant object
  type and one `DATABASE_WIDE`, and one that keeps the other when it requires
  it.
- The host excludes the fixture's object by selector type, by its schema and
  with its grant, and keeps a required object a scope did not name, through
  `ptah-compat schema inspect --exclude` and `ptah schema diff --exclude`.
- The YDB provider's CI excludes each of its ten kinds by type and a topic with
  its grant, and keeps the pool of a kept classifier, on each YDB line it
  supports.
