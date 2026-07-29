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
- Live introspection from `sys.tables`, `sys.columns`, `sys.indexes`, and
  related catalog views.
- Transactional migration apply for DDL SQL Server supports in transactions,
  and migration-run serialization through a session application lock.
- One-row upsert rendering to `MERGE` through the Go DML AST (an embedder
  surface, not a CLI command).

SQL Server has no native enum object, so enum annotations render as
`NVARCHAR(255)` columns with a generated `CHECK` constraint:

```sql
[status] NVARCHAR(255) NOT NULL CHECK ([status] IN ('active', 'blocked'))
```

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
- View and trigger introspection records the persisted definition text without
  normalizing it into drift-safe definitions.
- Index planning preserves key order, direction, and filtered predicates, but
  not included columns.
- Engine-specific options such as `WITH (ONLINE = ON)` are not planned.

## Next steps

- Comparing engine depth before committing: [Database support matrix](../support-matrix/).
- Declaring indexes and constraints: [Go annotation reference](../../reference/go-annotations/).
- Gating destructive changes on any engine: [Integrity and safety](../../versioned/integrity-and-safety/).
