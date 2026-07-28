---
title: Dialect Notes
description: Practical dialect-specific behavior for Ptah users.
---

Ptah uses capability metadata instead of assuming every database with a similar
dialect name behaves the same way. This page summarizes the operational
differences readers most often need after reading [Capabilities](../capabilities/).

## PostgreSQL

PostgreSQL is Ptah's primary first-party target. It has the broadest coverage
for schema objects such as schemas, extensions, enum types, functions, views,
materialized views, triggers, roles, grants, row-level security, advisory locks,
and concurrent index creation.

PostgreSQL concurrent index creation requires transaction-aware planning:

```sql
CREATE INDEX CONCURRENTLY idx_users_email ON users (email);
```

The statement cannot run inside a transaction block, so migration files that use
it need no-transaction handling.

## SQLite

SQLite is supported for local workflows, examples, and lightweight test
databases. It has intentionally different migration semantics:

- many schema changes require table-rebuild planning;
- foreign key enforcement depends on runtime settings;
- schema namespaces, roles, grants, and RLS are not SQLite concepts;
- in-memory URLs are useful for examples and CI smoke checks.

Example:

```bash
ptah atlas schema inspect --url "sqlite://dev?mode=memory" --format sql
```

## MySQL And MariaDB

MySQL and MariaDB share much of the renderer and planner surface, but
capabilities still differ by dialect variant and version. Common differences include
online DDL behavior, enum handling, index options, generated columns, and
constraint support.

Prefer explicit `--dialect mysql` or `--dialect mariadb` in examples and CI
jobs. Avoid assuming that a plan generated for one dialect variant is reviewed for the
other.

## SQL Server

SQL Server support is a subset. Use capabilities and generated SQL review to
decide whether a workflow is covered. SQL Server differs from PostgreSQL/MySQL
in identity syntax, schema ownership, quoting, transactional DDL behavior, and
object metadata.

Use `schemadiff.CompareWithDatabase` for live comparisons. It asks SQL Server
to resolve the finite candidate identifier set under `CATALOG_DEFAULT` and
carries that immutable result through comparison and planning. Offline
dialect-only comparisons confirm only exact identity. Distinct unresolved
names in one catalog namespace remain potentially equivalent, so planning
rejects the ambiguity instead of approximating SQL Server collation behavior.

Filtered indexes are supported: an index annotation `condition` renders as
`CREATE INDEX ... WHERE ...`, and a changed predicate is planned as
`DROP INDEX` plus `CREATE INDEX ... WHERE`. Predicate comparison normalizes
the canonical `sys.indexes.filter_definition` spelling (bracket quoting and
parenthesized numeric literals); predicates SQL Server rewrites further keep
comparing as changed, so prefer the catalog's stored spelling for those.

## PostgreSQL-Compatible Targets

CockroachDB, YugabyteDB, Spanner PostgreSQL interface, and similar targets can
accept PostgreSQL-like syntax while missing important PostgreSQL capabilities.
Ptah models these through capabilities rather than treating the server as a
drop-in PostgreSQL server.

Examples of differences to verify before rollout:

- advisory lock support;
- row-level security support;
- enum and identity behavior;
- transactional DDL behavior;
- generated column support;
- online index semantics.

## Rule Of Thumb

Use the dialect name to pick parser and renderer families. Use capabilities to
decide whether an individual operation is valid for a concrete target.
