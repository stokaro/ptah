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
