---
title: SQL Server
description: SQL Server in Ptah - connection URLs, the supported T-SQL subset, collation-aware identifier comparison, and filtered indexes.
---

Ptah supports SQL Server and Azure SQL as a deliberately conservative portable
subset under the canonical dialect name `sqlserver` (the aliases `mssql`,
`sql-server`, `sql_server`, and `tsql` normalize to it). This page covers the
connection URL, what the subset includes, and the two behaviors that most
often surprise SQL Server users: collation-aware identifier comparison and
filtered-index predicate spelling.

## Connecting

Pass a canonical `sqlserver://` URL; `mssql://` input is accepted and
normalized:

```bash
ptah db read --db-url "sqlserver://sa:$SA_PASSWORD@localhost:1433?database=app&encrypt=disable"
```

The Ptah-only `schema` query parameter selects the default schema for
introspection, migration metadata, and write helpers; it is removed before the
URL reaches the driver, and it defaults to `dbo`:

```bash
ptah db read --db-url "sqlserver://sa:$SA_PASSWORD@localhost:1433?database=app&schema=audit&encrypt=disable"
```

`--migrations-schema` still takes precedence for migration metadata placement.

## Supported surface

- T-SQL rendering with bracket-quoted identifiers and schema-qualified names
  such as `dbo.users`.
- `IDENTITY(start,increment)` for auto-increment columns and
  `NVARCHAR`/`NVARCHAR(MAX)` string mapping.
- Core table DDL: primary keys, unique constraints, foreign keys, `CHECK`
  constraints, and indexes with ordered ascending or descending key columns.
- Filtered indexes (see below).
- Views and triggers rendered from raw SQL definitions.
- Synonyms: declared, rendered, introspected from `sys.synonyms`, and diffed,
  including targets in another database or behind a linked server (see below).
- Extended properties at database, schema, table and column scope: declared,
  rendered, introspected from `sys.extended_properties`, and diffed (see
  below).
- Live introspection from `sys.tables`, `sys.columns`, `sys.indexes`, and
  related catalog views.
- Transactional migration apply for DDL SQL Server supports in transactions,
  and migration-run serialization through a session application lock.
- Transactional dev-database cleanup across supported user schemas, with a
  preflight that rejects replication-enabled databases and replicated tables.
- One-row upsert rendering to `MERGE` through the Go DML AST (an embedder
  surface, not a CLI command).

SQL Server has no native enum object, so enum annotations render as
`NVARCHAR(255)` columns with a generated `CHECK` constraint:

```sql
[status] NVARCHAR(255) NOT NULL CHECK ([status] IN ('active', 'blocked'))
```

## Synonyms

A synonym is an alias for another object, and it is the one schema object whose
target may live somewhere Ptah does not manage:

```go
//ptah:schema:synonym name="current_orders" schema="app" target="sales.orders"
type CurrentOrdersSynonym struct{}
```

```sql
CREATE SYNONYM [app].[current_orders] FOR [sales].[orders];
```

The target is written with as many parts as it needs. Two parts name an object
in this database; three name another database; four name a linked server. Ptah
records what it was given and never rewrites it, because the part count is what
tells SQL Server where to resolve the name, and adding a qualifier would turn a
remote reference into a local one.

**Ptah manages the alias and never the target.** A synonym pointing outside this
database is a supported declaration rather than an error — SQL Server does not
require the target to exist either — and dependency ordering treats it as having
no local dependency, so nothing tries to create or drop the object it points at.
A local target does participate in ordering: the alias is emitted after the
table or view it names.

A changed target is planned as a drop and a create in that order, because T-SQL
has no `ALTER SYNONYM` and `CREATE SYNONYM` refuses a name that already exists.

Targets are compared with the server's own bracket quoting normalized away, so a
declared `dbo.orders` and a stored `[dbo].[orders]` are the same target rather
than a difference reported on every run.

Every other target names a declared synonym as skipped rather than rendering
nothing: the object exists in the schema model for one engine, and a dialect
that dropped it silently would lose a declaration without saying so.

## Extended properties

An extended property is a named value attached to a schema, a table, or a
column of one:

```go
//ptah:schema:extendedproperty name="retention" schema="app" table="orders" value="90d"
type OrdersRetentionProperty struct{}

//ptah:schema:extendedproperty name="classification" schema="app" table="orders" column="email" value="pii"
type OrdersEmailClassification struct{}
```

```sql
EXEC sp_addextendedproperty @name = N'retention', @value = N'90d', @level0type = N'SCHEMA', @level0name = N'app', @level1type = N'TABLE', @level1name = N'orders';
EXEC sp_addextendedproperty @name = N'classification', @value = N'pii', @level0type = N'SCHEMA', @level0name = N'app', @level1type = N'TABLE', @level1name = N'orders', @level2type = N'COLUMN', @level2name = N'email';
```

**The address is the identity.** SQL Server stores a property under a class and
up to two ids, so the same name on the database, on a schema, on a table of it,
and on a column of that table is four different properties. Which levels a
declaration sets decides its scope: **no level at all** is the database's own
property, `schema` alone is schema scope, adding `table` addresses the table,
and adding `column` addresses a column of it. A level without the one above it
is refused, because SQL Server has no level N without a level N-1.

```go
//ptah:schema:extendedproperty name="deployment_tier" value="production"
type DeploymentTierProperty struct{}
```

```sql
EXEC sp_addextendedproperty @name = N'deployment_tier', @value = N'production';
```

A database-scoped property passes no level, and that is not an omission: an
empty `@level0name` would be a property on a schema called `""`, which the
procedure also accepts and which belongs to nothing. It is in no schema either,
so `--schema` neither selects nor removes it — the same rule an extension
follows, where placement is not ownership.

A changed value plans `sp_updateextendedproperty` rather than a drop and an
add, so the property is never absent partway through a script. Properties are
emitted after every object one can hang off, and dropped before the objects
they belong to.

`MS_Description` is refused by name. Ptah already models it as the object's
comment — the reader turns it into one and the renderer writes it as one — and
accepting it here as well would give one live value two owners, each planning
against it without seeing the other.

**A value Ptah cannot write back is reported and left alone.**
`sp_addextendedproperty` takes a `sql_variant`, so a property may hold an `int`
or a `date` as well as a string, and Ptah writes values as `N''` literals.
Re-emitting a non-string value through one would change its stored type, and
`CONVERT(NVARCHAR, value)` on a date answers a locale-dependent rendering
rather than the value. Such a property appears in a read with its base type and
no value, and a comparison plans nothing for it in either direction.

One kind of property is outside this scope and is not read at all: one on an
object other than a table — a view, a procedure, an index — which takes a
different `@level1type` than the one Ptah writes.

Realm cleanup still refuses a database whose database- or schema-scoped
properties it would leave behind. That guard predates this support and is
unchanged: adding cleanup for them is a destructive change with its own
measurement.

Every other target names a declared extended property as skipped rather than
rendering nothing, for the same reason a synonym is: the object exists in the
schema model for one engine, and a dialect that dropped it silently would lose
a declaration without saying so.

Extended properties travel through the schema model, the renderer, the reader
and the comparison. They are not part of the HCL surface `schema inspect`
writes, which is the same boundary synonyms sit on.

**A document that cannot name them does not drop them.** HCL has no block for
either object and YAML has no key, so a file written in those formats could not
have declared one — and reading its silence as intent made the round trip
through Ptah's own output destructive:

```bash
ptah schema inspect --db-url "$SQLSERVER_URL" > out.hcl
ptah schema apply --db-url "$SQLSERVER_URL" --to file://out.hcl
```

planned `sp_dropextendedproperty` for every property on the server and
`DROP SYNONYM` for every synonym. Each loader now records what its format cannot
express, and the comparison withholds those removals.

A **Go schema can** declare both — `//ptah:schema:synonym` and
`//ptah:schema:extendedproperty` — so its silence about one is still a request
to remove it, and nothing about that changed.

## Identifier collation

SQL Server compares object identifiers using catalog collation rules — case,
accent, locale, kana, and width sensitivity all depend on the target catalog.
Ptah does not reproduce those rules locally; the live catalog is the source of
truth. When comparing against a live database, Ptah asks SQL Server to resolve
candidate identifier equivalence under `COLLATE CATALOG_DEFAULT` and carries
that resolved snapshot through comparison, planning, checkpoint generation,
and shadow verification (shadow execution is rejected when the shadow catalog
resolves names differently).

An offline, dialect-only comparison cannot know the target collation, so it
confirms only exact-spelling identity and treats distinct unresolved names in
one namespace as potential conflicts: planning rejects the ambiguity instead
of guessing. This makes offline SQL Server planning intentionally stricter
than live planning. Embedders choose between the live-aware and offline
comparison APIs — see [Public Go API](../../extend/public-api/).

## Filtered indexes

Declare a filtered index with the index annotation's `condition` (or `where`)
attribute:

```go
//ptah:schema:index name="idx_active_users" fields="status" condition="status = 1"
```

The predicate renders verbatim as `CREATE INDEX ... WHERE status = 1`. SQL
Server cannot alter a predicate in place, so a changed predicate plans as
`DROP INDEX` plus `CREATE INDEX ... WHERE` with the new predicate.

SQL Server stores predicates in a canonical spelling — `status = 1` comes back
as `([status]=(1))` — and Ptah normalizes bracket quoting, parenthesized
numeric literals, case, and whitespace before deciding whether an index
changed. Rewrites beyond that spelling (such as the `N'...'` prefix on Unicode
string literals) are not reconstructed, so those predicates compare as changed
on every run. If a filtered index keeps reporting drift after it was applied,
read the stored spelling with `ptah db read` and use it in the annotation; the
rendered SQL always preserves your annotation text verbatim.

## Limitations

- No PostgreSQL-style extensions, row-level security, roles and grants, or
  materialized views.
- Column drift planning emits direct `ALTER COLUMN` only for type and
  nullability changes; default, generated-expression, unique, and `CHECK`
  changes need a manual migration.
- Automatic column removal is rejected, because dependent constraints,
  defaults, and indexes must be dropped in the correct order first.
- Standalone sequence objects are outside the subset.
- A synonym's target is recorded and resolved by the server, not validated by
  Ptah: a synonym naming an object that does not exist is created successfully
  and fails when something uses it, which is SQL Server's own behavior.
- View and trigger introspection records the persisted definition text without
  normalizing it into drift-safe definitions.
- Index planning preserves key order, direction, and filtered predicates, but
  not included columns.
- Engine-specific options such as `WITH (ONLINE = ON)` are not planned.
- Dev-database cleanup rejects database replication, replicated tables, and
  unsupported database-scoped artifacts before its first DDL statement.

## Next steps

- Comparing engine depth before committing: [Database support matrix](../support-matrix/).
- Declaring indexes and constraints: [Go annotation reference](../../reference/go-annotations/).
- Gating destructive changes on any engine: [Integrity and safety](../../versioned/integrity-and-safety/).
