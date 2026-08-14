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
| [PostgreSQL](../postgresql/) | `postgres` (`postgresql`, `pgx`) | `postgres://`, `postgresql://` | Primary first-party target with the broadest schema-object coverage. |
| [SQLite](../sqlite/) | `sqlite` (`sqlite3`) | `sqlite://` | Supported for local workflows, examples, and lightweight test databases. |
| MySQL | `mysql` | `mysql://` | Supported, with dialect-specific limitations. |
| MariaDB | `mariadb` | `mariadb://` | Supported, with dialect-specific limitations. |
| [SQL Server](../sqlserver/) | `sqlserver` (`mssql`, `tsql`, `sql-server`, `sql_server`) | `sqlserver://`, `mssql://` | Deliberately conservative portable subset. |
| CockroachDB | `cockroachdb` (`cockroach`, `crdb`) | `cockroachdb://`, `crdb://` | PostgreSQL-compatible path with capability differences. |
| YugabyteDB | `yugabytedb` (`yugabyte`, `ysql`) | `yugabytedb://`, `ysql://` | PostgreSQL-compatible path with capability differences. |
| ClickHouse | `clickhouse` (`ch`) | `clickhouse://`, `ch://` | Capability-limited support. |
| Spanner (PostgreSQL interface) | `spanner` (`cloudspanner`, `google-spanner`, `google_spanner`) | `spanner://` | Most conservative capability-limited support. |

Accepted URL formats, and the difference between target, dev, shadow, and
throwaway databases, are on
[Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/).

## Supported release lines

Every engine ships several versions at once, and Ptah models each release line
with its own capability preset. This matrix is the supported set: the line, the
preset it claims, and whether continuous integration measures that claim
against a live server on every pull request.

The table is generated from the single declaration the CI matrix also reads, so
the supported set cannot say one thing here and another in a workflow file.

<!-- BEGIN GENERATED VERSION MATRIX -->
| Dialect | Release line | Capability preset | Refinement | Probed |
| --- | --- | --- | --- | --- |
| `postgres` | 18 | `Postgres17` | version-ladder | yes |
| `postgres` | 17 | `Postgres17` | version-ladder | yes |
| `postgres` | 16 | `Postgres16` | version-ladder | yes |
| `postgres` | 15 | `Postgres16` | version-ladder | yes |
| `postgres` | 14 | `Postgres16` | version-ladder | yes |
| `postgres` | 13 | `Postgres13` | version-ladder | yes |
| `mysql` | 26.7 | `MySQL84` | version-ladder | yes |
| `mysql` | 9.7 | `MySQL84` | version-ladder | yes |
| `mysql` | 8.4 | `MySQL84` | version-ladder | yes |
| `mariadb` | 12.3 | `MariaDB1011` | version-ladder | yes |
| `mariadb` | 11.8 | `MariaDB1011` | version-ladder | yes |
| `mariadb` | 11.4 | `MariaDB1011` | version-ladder | yes |
| `mariadb` | 10.11 | `MariaDB1011` | version-ladder | yes |
| `cockroachdb` | 26.2 | `CockroachDB26` | version-ladder | yes |
| `cockroachdb` | 25.4 | `CockroachDB25` | version-ladder | yes |
| `yugabytedb` | 2026.1 | `YugabyteDB25` | measured-release-line | yes |
| `yugabytedb` | 2025.2 | `YugabyteDB25` | measured-release-line | yes |
| `clickhouse` | 26.7 | `ClickHouse24` | dialect-default | no |
| `clickhouse` | 26.3 | `ClickHouse24` | dialect-default | no |
| `clickhouse` | 25.8 | `ClickHouse24` | dialect-default | no |
| `clickhouse` | 24.10 | `ClickHouse24` | dialect-default | no |
| `sqlserver` | 17.0 (SQL Server 2025) | `SQLServer2022` | dialect-default | no |
| `sqlserver` | 16.0 (SQL Server 2022) | `SQLServer2022` | dialect-default | no |
| `sqlserver` | 15.0 (SQL Server 2019) | `SQLServer2022` | dialect-default | no |
| `sqlite` | 3 | `SQLite3` | dialect-default | no |
| `spanner` | 0 | `SpannerPostgres` | banner-substring | no |

Declared release lines: 26. Probed on every pull request: 17.

Lines that are declared and not probed, and why:

- `clickhouse` 26.7 — the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing.
- `clickhouse` 26.3 — the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing.
- `clickhouse` 25.8 — the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing.
- `clickhouse` 24.10 — the capability probe has no statement table for the clickhouse dialect, so a server on this line would be asked nothing.
- `sqlserver` 17.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlserver` 16.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlserver` 15.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlite` 3 — no container image is declared for this line; the capability probe has no statement table for the sqlite dialect, so a server on this line would be asked nothing.
- `spanner` 0 — no container image is declared for this line.

Lines whose container tag does not name the line, so which patch it resolves to has to be read off the tag:

- `sqlserver` 17.0, pinned as `mcr.microsoft.com/mssql/server:2025-latest`.
- `sqlserver` 16.0, pinned as `mcr.microsoft.com/mssql/server:2022-latest`.
- `sqlserver` 15.0, pinned as `mcr.microsoft.com/mssql/server:2019-latest`.
<!-- END GENERATED VERSION MATRIX -->

`Refinement` says how a server reaches its preset. `version-ladder` selects an
arm by parsed version, so an observation belongs to that line alone;
`measured-release-line` reaches the preset through an engine banner but has
been measured directly; `banner-substring` and `dialect-default` hand every
release of the engine the same set, so an observation on one release cannot be
credited to one line rather than its siblings.

A future line with no preset resolves onto the newest preset Ptah has, which is
a stand-in rather than a match. The pipeline reports that condition as a
failure, and [issue 916](https://github.com/stokaro/ptah/issues/916) tracks the
remaining version-specific refinement work.

Which versions a vendor supports is recorded, with its source, next to each
block of cells in `internal/capabilityprobe/cells.go`. PostgreSQL does not
label releases LTS, so the reading used here is the newest patch of each
still-supported major line. The container that reproduces each line is
recorded there too. CockroachDB's `latest-v<line>` aliases follow the newest
patch. YugabyteDB publishes no equivalent aliases, so the matrix driver
resolves the highest numeric Docker Hub tag under each declared line before it
starts the container.

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
- Portable foreign keys require InnoDB tables and compatible column types,
  signedness, character sets, and collations. MariaDB generated FK columns and
  MySQL virtual generated FK columns fail before rendering. MySQL stored
  generated columns reject referential actions the engine cannot apply. When
  an FK-participating table has no declared engine, Ptah emits
  `ENGINE=InnoDB` explicitly instead of trusting the session default.
- `SET NULL` requires nullable local columns. Explicit foreign-key names are
  limited to 64 characters; generated names are shortened deterministically.
- A nonunique referenced key must be a complete leftmost BTREE prefix.
  FULLTEXT, SPATIAL, HASH, parser-backed, expression, and prefix indexes do not
  qualify.
- DDL commits implicitly on both engines, so a failed migration cannot be
  rolled back by the surrounding transaction.

Database-realm cleanup requires global `SELECT`, `DROP`, `ALTER`,
`ALTER ROUTINE`, `EVENT`, `LOCK TABLES`, and `PROCESS`. MySQL also requires
global `TRIGGER` and, on MySQL 8.0.20 and newer, `SHOW_ROUTINE`; MariaDB
requires global `SHOW VIEW`. Ptah verifies this privilege set before destructive
DDL. Cleanup fails closed when another user database contains a routine, event,
or trigger because its body can reference the cleanup realm without a catalog
dependency. Grant these privileges only to credentials used with a dedicated
disposable dev database.

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

SQLite checks foreign-key candidate keys from primary and inline unique
declarations. Ptah does not accept a standalone unique index as a referenced
key when its per-column collation is absent from the schema IR; this avoids SQL
that creates successfully but fails later with `foreign key mismatch`.

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

- **CockroachDB**: the preset excludes concurrent index creation and drops,
  `XML` columns, and advisory locks. Live CockroachDB v26.2.5 accepts role
  management, row-level security, standalone sequences, and `SERIAL` columns.
- **YugabyteDB**: the preset includes concurrent index creation, role
  management, row-level security, standalone sequences, `XML` columns, and
  advisory locks on the measured 2026.1 line. `DROP INDEX CONCURRENTLY`
  remains excluded because that server line rejects it. A generated concurrent
  create therefore rolls back with ordinary `DROP INDEX`; only the forward
  migration requires no-transaction execution.
- **Spanner**: foreign keys are included, including composite and circular
  relationships rendered in two phases. Spanner manages the referenced-key
  backing index, so Ptah does not require an input unique/index declaration.
  Participating columns must have compatible key-capable types; JSON and array
  columns fail before rendering.
  The preset excludes enums, standalone sequences, row-level security, `XML`
  columns, advisory locks, and concurrent indexes. Foreign key actions are
  limited to `ON DELETE NO ACTION` or `CASCADE`; `ON UPDATE` fails before
  rendering.

CockroachDB and YugabyteDB run in integration coverage against live
open-source containers. Their reader coverage seeds a table, index, view,
materialized view, sequence, and row-level security policy, then verifies both
`ptah db read` and `ptah-compat schema inspect`. Spanner coverage is offline
(capability, planning, rendering, URL, and detection), so review generated SQL
before relying on it.
PostgreSQL and YugabyteDB reject unsupported database-scoped publications,
subscriptions, logical replication slots, event triggers, and non-extension
foreign-data objects before dev-database cleanup. PostgreSQL additionally
removes database large objects inside the cleanup transaction; YugabyteDB does
not support that catalog write path.

## ClickHouse

ClickHouse support is capability-limited. The preset models enums as inline
`Enum8`/`Enum16` column types; foreign keys and enforced `CHECK` constraints
are outside the preset. Review generated SQL and the
[capability gates](../../reference/capabilities/) before adopting a workflow
on ClickHouse. Dev-database replay cleanup requires ClickHouse 24.11 or newer
and global `SHOW DATABASES` plus `SHOW TABLES`. Because ordinary views have no
complete catalog dependency metadata, cleanup also requires that other user
databases contain no view-like or dictionary objects and no `Buffer`,
`Distributed`, or `Merge` tables.

The narrower reset used by shadow verification, by the `schema apply` dev
rehearsal, and by `schema clean` removes tables, views, and materialized views.
A materialized view goes as one object, with `DROP VIEW`, so its inner storage
table leaves with it rather than being dropped out from under it. Live views and
window views are left alone, matching what the ClickHouse reader reports; use
the database-realm cleanup above when a database has to be emptied completely.

Plain views participate in the complete render, plan, and introspection cycle.
Ptah emits `CREATE VIEW`, `CREATE OR REPLACE VIEW`, and `DROP VIEW`, preserving
qualified names and query bodies, and reads ordinary views from
`system.tables`. An empty query body, `WITH CHECK OPTION`, or
`DROP VIEW ... CASCADE` fails instead of being ignored.

Materialized views do too. Ptah emits
`CREATE MATERIALIZED VIEW <name> ENGINE = MergeTree ORDER BY tuple() AS <query>`,
reads the object back from `system.tables`, and plans a changed query as a drop
followed by a create, because ClickHouse has no statement that edits the query
of an existing materialized view. Three ClickHouse-specific points are worth
knowing before adopting them:

- The storage clause is written explicitly rather than left to the server.
  ClickHouse 25.x and later accept a materialized view with no storage clause
  and supply `MergeTree ORDER BY tuple()` themselves; 24.x rejects it with
  `ORDER BY or PRIMARY KEY clause is missing`.
- The drop is spelled `DROP VIEW`. `DROP MATERIALIZED VIEW` is a syntax error on
  ClickHouse, and `DROP VIEW` removes the view together with the inner table
  that stores its result.
- `POPULATE` is never emitted, so a materialized view starts empty and fills
  from inserts into its source rather than from the rows already there.
  `POPULATE` is a one-shot argument that leaves no trace in the catalog, so
  nothing Ptah reads back could diff it. Backfill existing rows yourself if you
  need them.
- A query written without a database qualifier is read back carrying one.
  ClickHouse resolves the query when the view is created and records what it
  resolved, so `SELECT count() AS c FROM users` comes back from
  `system.tables.as_select` as `SELECT count() AS c FROM <database>.users`.
  Comparison removes the qualifier the object's own database added, the same way
  it does for an ordinary view, so an unchanged declaration is not reported as
  drift and is never planned as a drop and a create. A qualifier naming some
  other database is a real difference and is still reported.

The `TO <target table>` form and refreshable materialized views are not emitted:
the shared schema model carries a name and a query, so it cannot name a separate
target table, and `REFRESH MATERIALIZED VIEW` remains a named diagnostic because
ClickHouse has no such statement.

### Atlas revision metadata on ClickHouse

Ptah creates the Atlas revision table with `partial_hashes` declared as text.
ClickHouse reads a trailing `NULL` on a column definition as `Nullable(T)`, so
declaring that column `JSON` asks for `Nullable(JSON)`, and no ClickHouse server
handles that the way the rest of the Atlas revision layout needs:

- Servers that reject `Nullable(JSON)` refuse the `CREATE` during type analysis
  (`code: 43, Nested type JSON cannot be inside Nullable type`). The rejection
  happens before the `IF NOT EXISTS` existence check, so an already-provisioned
  database does not escape it, and Atlas-format apply fails before any user
  migration SQL runs.
- Servers that accept `Nullable(JSON)` coerce the JSON null Ptah writes into the
  JSON type and store `{}` instead. The value the author wrote is replaced, and
  the column can no longer be scanned into a string by an Atlas-compatible
  consumer.

Declaring the column text resolves to `Nullable(String)` on both, which stores
the same JSON null every other dialect stores. This is stated by behavior rather
than by a version cut-off on purpose: the release that changed `Nullable(JSON)`
from rejected to accepted was not measured, and the column type is correct on
either side of it.

`CREATE TABLE IF NOT EXISTS` does not alter a table that already exists, so a
revision table created by an earlier Ptah against a server that accepted
`Nullable(JSON)` keeps that column type and keeps storing `{}`. Ptah ignores a
legacy `{}` on clean rows, but reads `partial_hashes` on dirty Atlas-format rows
with committed progress. Malformed metadata, including `{}` where a digest
array is required, fails closed during status or retry. Ptah does not rewrite
the column automatically; alter it to `Nullable(String)` or recreate the table
before recovering such a dirty row.

Setting a revision directly (`SetAtlasRevision`) is still unsupported on
ClickHouse, because revision history cannot be updated atomically there.

## Choosing behavior by capability, not by name

The dialect name picks the parser and renderer family; whether an individual
operation is valid on a concrete target is decided by its capability set —
see [Dialects and capabilities](../../concepts/dialects-and-capabilities/).
