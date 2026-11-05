# ADR 0065: A filtered read reports the objects of its database

Status: Proposed

Contract: 25.22

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0054](0054-a-filtered-read-carries-the-planning-context-it-does-not-compare.md),
[ADR 0063](0063-a-read-names-the-tables-it-leaves-out.md)

## Context

Some objects belong to a database rather than to a namespace of it. A YDB
resource pool and a resource pool classifier are two: no directory holds them,
YDB names them by name alone, and the provider reports them at the database
root, with an empty schema. A read filtered to directories (`--schemas app`)
asks for the objects of those directories, and the contract binds every subject
of a filtered read to the namespaces its selectors name. A pool lies in none of
them, so the SDK refuses a filtered read that reports one ("inspected subject
is outside the requested scope"), and the YDB provider leaves both kinds
uninspected on such a read. A run must select the root as well (`--schemas
app,/`) to manage them, and selecting the root claims the root's tables known.

The built-in YDB engine reads the pools on every read, filtered or not.
Measured with Ptah master 51812210f and `stokaro/ptah-ydb-plugin` master
07068a0 on local YDB 25.1.4.7 and 26.2.1.14 with `EnableResourcePools` on, the
same on both lines. The database holds a directory with one table and the
default pool; the schema declares the table, and in the second row a pool.

| `--schemas` the directory | The provider path | The built-in YDB engine |
| --- | --- | --- |
| `ptah introspect` | `//ptah:schema:notdescribed kind="resource_pool" reason="not-inspected" provenance="observed"`, and the same for `resource_pool_classifier` | `//ptah:schema:resourcepool name="default"` |
| `schema plan` of a declared pool | `Warning: .../resource-pool ...: The current resource pool or its absence was not established.` and `No changes planned, but 1 comparison limit remains unresolved.` | `CREATE RESOURCE POOL ... WITH (CONCURRENT_QUERY_LIMIT = 2);`, and after the apply `Schema is synced` |
| `schema plan` of a schema without pools | `Schema is synced` | `Schema is synced` |

The built-in engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so a user
who scopes a run to a directory loses the pools of the database.

The scope rule is right for everything a namespace holds. It keeps an empty
selection from claiming global absence and a partial read from widening its
own request. Two ways of reporting the pools inside the rule were ruled out:

- Binding a directory selector to the root namespace as well would let the
  read report the pools, and would claim every root table and root object
  known, which the read did not establish.
- The planning context of a filtered read (ADR 0054) carries objects outside
  the scope, but for planning only: the comparison never reads it, so a
  declared pool would still be compared against nothing.

## Decision

### An engine names the kinds whose objects belong to its database

`schema.database-kinds` is a two-way feature: the provider and the engine list
it, and the host lists it in its host features. Contract 25.22 adds one field:

```proto
message Engine {
  // ...
  repeated string database_kinds = 22;
}
```

Each entry is the kind of one of the engine's OBSERVED models whose objects
belong to the session's database rather than to a namespace of it. The list is
non-empty exactly when the engine lists the feature, holds no kind twice, and
holds no kind a planning service of the engine assesses as context, since an
object of such a kind would then have two homes on a filtered read. A host
that does not list the feature receives no list.

### A filtered read reports them beside its scope

An object of a database kind is named at the database: its reference has the
database and no catalog, schema or path. On a filtered read the scope rule
gives way for exactly these records, and for no other:

- a coverage subject of a database kind whose subject and normalized subject
  are such a reference, of the subject's own kind, so a facet of a root table
  never qualifies;
- a coverage region of a database kind whose scope is the database namespace:
  the database, an empty catalog and an empty schema, and no path prefix.

The native objects of those subjects follow, since a native object needs
complete subject coverage. Everything else a filtered read names still lies
inside its bound namespaces, a database kind still cannot claim global
completeness, and the regions of every other kind keep the rule. A region of a
database kind says what the read knows of that kind alone, so it claims
nothing about the tables or the other objects at the root.

Both transport boundaries apply this, and the host binds each such subject and
region to the session's database as it binds any other.

### What the host does

The host lists the feature and reads the objects and their coverage as it
reads those of an unfiltered read: the comparison compares them, the plan
creates, changes or keeps them, and an export writes them. A run scoped to a
directory then manages the database's pools as the built-in engine does.

### What the YDB provider sends

The provider lists the kinds of its observed models of resource pools and
resource pool classifiers as database kinds, and reads both on every read of a
database, filtered or not. A dev realm still reads none: the pools it would
see are its database's.

## Alternatives considered

**Treat the root as part of every filtered scope.** The pools would fit inside
the rule, and the read would claim the root's tables and objects known.
Rejected.

**A field on each value model.** `ValueModel.database_wide` would say the same
on the model. A model is a representation and is shared between the observed
and desired sides and across engines; where an object lives is the engine's
fact. The engine list also lets negotiation remove the whole addition in one
place. Rejected.

**Carry the pools in the planning context.** The comparison never reads the
planning context, so a declared pool would not be compared. Rejected.

**Leave it to the operator to select the root.** That is the provider's
answer today, and it takes the root's tables into the run. Rejected.

## Consequences

- Contract 25.22 adds a field and a two-way feature. It is numbered after
  contracts 25.20 and 25.21, which ADR 0063 and ADR 0064 propose.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- Without the feature nothing changes: a filtered read leaves the database's
  pools uninspected, and a run must select the root to manage them.
- With it, `ptah introspect --schemas app` writes the pools, and `schema
  apply --schemas app` plans a declared pool, as through the built-in engine.

## Acceptance evidence

- SDK tests accept database-kind subjects and regions outside the scope of a
  negotiated filtered read, and refuse them when the feature is not
  negotiated, for a kind that is not a database kind, for a reference with a
  schema, catalog or path, for a facet of a root table, and for a region below
  the database namespace. The description refuses a list without the feature,
  the feature without a list, a repeated kind, a kind without an observed model
  and a planning context kind.
- Through the SDK fixture provider, a filtered read that holds no table reports
  a database object at the root, and the host keeps it with its coverage.
- Through the YDB provider on 25.1.4.7 and 26.2.1.14, `ptah introspect
  --schemas` the directory writes the default pool, and `schema apply
  --schemas` the directory creates a declared pool and then reads it as
  synced.
