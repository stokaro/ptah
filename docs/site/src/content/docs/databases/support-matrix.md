---
title: Database support matrix
description: The database engines Ptah supports, at what depth, and the operational differences to know before rollout.
---

This page lists every database engine Ptah supports, the dialect name and URL
schemes each one uses, and the engine-specific behavior to check before
rollout. How dialect names and capability gates interact is explained in
[Dialects and capabilities](../../concepts/dialects-and-capabilities/); the
per-capability tables are in [Capabilities](../../reference/capabilities/).

## Engines at a glance

| Engine | Dialect (aliases) | URL schemes | Support |
| --- | --- | --- | --- |
| [PostgreSQL](../postgresql/) | `postgres` (`postgresql`) | `postgres://`, `postgresql://` | Primary first-party target with the broadest schema-object coverage. |
| [SQLite](../sqlite/) | `sqlite` (`sqlite3`) | `sqlite://` | Supported for local workflows, examples, and lightweight test databases. |
| MySQL | `mysql` | `mysql://` | Supported, with dialect-specific limitations. |
| MariaDB | `mariadb` | `mariadb://` | Supported, with dialect-specific limitations. |
| [SQL Server](../sqlserver/) | `sqlserver` (`mssql`, `tsql`) | `sqlserver://`, `mssql://` | Deliberately conservative portable subset. |
| CockroachDB | `cockroachdb` (`cockroach`, `crdb`) | `cockroachdb://`, `crdb://` | PostgreSQL-compatible path with capability differences. |
| YugabyteDB | `yugabytedb` (`yugabyte`, `ysql`) | `yugabytedb://`, `ysql://` | PostgreSQL-compatible path with capability differences. |
| ClickHouse | `clickhouse` (`ch`) | `clickhouse://`, `ch://` | Capability-limited support. |
| Spanner (PostgreSQL interface) | `spanner` (`cloudspanner`) | `spanner://` | Most conservative capability-limited support. |

Accepted URL formats, and the difference between target, dev, shadow, and
throwaway databases, are on
[Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/).

## PostgreSQL

PostgreSQL has the broadest coverage of any engine: schemas, extensions, enum
types, functions, views, materialized views, triggers, standalone sequences,
user-defined types, roles, grants, and row-level security all participate in
the generate / compare / migrate / rollback lifecycle.
[PostgreSQL](../postgresql/) covers each area and its version-dependent
behavior.

## MySQL and MariaDB

MySQL and MariaDB share one planner and renderer family, but they are separate
dialects with different capability sets. Pass an explicit `--dialect mysql` or
`--dialect mariadb` in examples and CI jobs, and treat a plan reviewed for one
variant as unreviewed for the other. Differences that show up in generated
SQL:

- Enums are inline `ENUM` column types, not standalone type objects.
- MariaDB guards constraint and index drops with `IF EXISTS`; MySQL rejects
  that guard, so the `mysql` renderer strips it.
- The `DROP CHECK` spelling exists only on MySQL 8.0.16+; MariaDB uses the
  generic `DROP CONSTRAINT` clause.
- DDL commits implicitly on both engines, so a failed migration cannot be
  rolled back by the surrounding transaction.

For large tables, `ptah migrations up` and `down` can route `ALTER TABLE`
statements through gh-ost or pt-online-schema-change, either per migration
with a `-- +ptah online_ddl_tool=ghost` directive or automatically above a
configured row-count threshold:

```yaml
online_ddl:
  tool: ghost
  threshold_rows: 1000000
```

A tool-routed migration runs on the tool's own connections and is not atomic:
keep online-DDL migrations minimal, ideally one `ALTER` per file. The
`online_ddl` keys, including `fallback` and `args`, are listed in
[Configuration](../../reference/configuration/).

## SQLite

SQLite is the engine the documentation examples default to: it needs no
daemon, and Ptah's declarative tests run each case against a fresh ephemeral
SQLite database unless told otherwise. Its migration semantics differ deliberately — many
schema changes require a table rebuild, and PostgreSQL-only objects are
rejected. [SQLite](../sqlite/) covers URL forms, the supported DDL surface,
and the rebuild rules.

## SQL Server

SQL Server and Azure SQL are supported as a deliberately conservative portable
subset: core table DDL, `IDENTITY` columns, filtered indexes, and live
introspection, with collation-aware identifier comparison driven by the target
catalog. [SQL Server](../sqlserver/) covers connection URLs, the supported
surface, and its limitations.

## PostgreSQL-compatible distributed targets

CockroachDB, YugabyteDB, and the Spanner PostgreSQL interface accept
PostgreSQL-like syntax while missing PostgreSQL capabilities, so Ptah routes
each one as a distinct dialect through the PostgreSQL implementation family
with its own capability preset instead of treating the server as a drop-in
PostgreSQL server. A live connection reads the server banner and selects the
matching preset automatically.

- **CockroachDB**: the preset excludes concurrent index creation, standalone
  sequences, `XML` columns, advisory locks, role management, and row-level
  security.
- **YugabyteDB**: the preset excludes concurrent index creation because
  regular `CREATE INDEX` is already asynchronous in YSQL; role management,
  standalone sequences, and `XML` columns are included.
- **Spanner**: the most conservative preset — enums, foreign keys, standalone
  sequences, row-level security, `XML` columns, advisory locks, and concurrent
  indexes are all excluded.

CockroachDB and YugabyteDB run in integration coverage against live
open-source containers; Spanner coverage is offline (capability, planning,
rendering, URL, and detection), so review generated SQL before relying on it.

## ClickHouse

ClickHouse support is capability-limited. The preset models enums as inline
`Enum8`/`Enum16` column types; foreign keys and enforced `CHECK` constraints
are outside the preset. Review generated SQL and the
[capability gates](../../reference/capabilities/) before adopting a workflow
on ClickHouse. Dev-database replay cleanup requires ClickHouse 24.11 or newer
so Ptah can prove complete catalog visibility with `CHECK GRANT`.

## Choosing behavior by capability, not by name

The dialect name picks the parser and renderer family; whether an individual
operation is valid on a concrete target is decided by its capability set —
see [Dialects and capabilities](../../concepts/dialects-and-capabilities/).
