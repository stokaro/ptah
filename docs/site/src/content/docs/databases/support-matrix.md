---
title: Database support matrix
description: The database engines Ptah supports, at what depth, and the operational differences to know before rollout.
owns:
  - cli-ptah-db-capabilities
---

This page lists every database engine Ptah supports, the dialect name and URL
schemes each one uses, the release lines Ptah declares for it, and where its
engine guide is. How dialect names and capability gates interact is explained in
[Dialects and capabilities](../../concepts/dialects-and-capabilities/); the
per-capability tables are in [Capabilities](../../reference/capabilities/).

## Engines at a glance

| Engine | Dialect (aliases) | URL schemes | Coverage |
| --- | --- | --- | --- |
| [PostgreSQL](../postgresql/) | `postgres` (`postgresql`, `pgx`) | `postgres://`, `postgresql://` | Primary first-party target with the broadest schema-object coverage. |
| [SQLite](../sqlite/) | `sqlite` (`sqlite3`) | `sqlite://` | Supported for local workflows, examples, and lightweight test databases. |
| [MySQL](../mysql/) | `mysql` | `mysql://` | Supported, with dialect-specific limitations. |
| [MariaDB](../mysql/) | `mariadb` | `mariadb://` | Supported, with dialect-specific limitations. |
| [SQL Server](../sqlserver/) | `sqlserver` (`mssql`, `tsql`, `sql-server`, `sql_server`) | `sqlserver://`, `mssql://` | Deliberately conservative portable subset. |
| [CockroachDB](../distributed/) | `cockroachdb` (`cockroach`, `crdb`) | `cockroachdb://`, `crdb://` | PostgreSQL-compatible path with capability differences. |
| [YugabyteDB](../distributed/) | `yugabytedb` (`yugabyte`, `ysql`) | `yugabytedb://`, `ysql://` | PostgreSQL-compatible path with capability differences. |
| [ClickHouse](../clickhouse/) | `clickhouse` (`ch`) | `clickhouse://`, `ch://` | Capability-limited support. |
| [Spanner (PostgreSQL interface)](../distributed/) | `spanner` (`cloudspanner`, `google-spanner`, `google_spanner`) | `spanner://` | Most conservative capability-limited support. |
| [Oracle](../oracle/) | `oracle` | `oracle://` | Renders, plans, and reads a live catalog. |

Accepted URL formats, and the difference between target, dev, shadow, and
throwaway databases, are on
[Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/).

## Engines Ptah does not support

Snowflake, Amazon Redshift and Databricks have no dialect entry. Naming one is
an error rather than a fallback:

```text
ptah schema render --dialect snowflake
error: error rendering snowflake schema: unsupported database dialect: snowflake
```

**The blocker is not the price.** Every one of the three has a free route to
*something*. What none of them has is a free route to **the engine itself,
startable per pull request without a human-held credential**.

### Snowflake

Free: a 30-day trial with credits and no credit card, renewable by signing up
again. Several open-source emulators also exist, and one of them, `fakesnow`, is
genuinely faithful — it accepts `NUMBER(38,0)`, `TIMESTAMP_NTZ` and `VARIANT`,
and refuses `CREATE INDEX` exactly as the real engine does.

Why it does not qualify: the emulators reimplement Snowflake on DuckDB, so a
capability preset measured against one describes the emulator rather than
Snowflake. The trial is a real endpoint, but it is one person's account, not
something a pull request can start.

One trap for anyone who tries anyway: the Snowflake SQL API answers **HTTP 200
on a failed statement**, with the failure only in the body. A probe that keys on
the status code scores every capability as supported.

### Amazon Redshift

Free: credits on Redshift Serverless for a first-time account, and container
images that advertise Redshift.

Why it does not qualify: those containers are PostgreSQL wearing the name.
Measured against both of the ones commonly cited, every Redshift catalog view
answers the same way, while a control query against `pg_class` returns rows
normally:

```text
pg_table_def, svv_columns, svv_table_info, stl_query  ->  relation does not exist
Redshift DDL                                          ->  rejected at "distkey"
```

Redshift *removes* indexes, sequences and enforced constraints, and that removal
is invisible from the PostgreSQL wire protocol it shares — so wire compatibility
is the wrong thing to reason from, and routing Redshift onto the PostgreSQL
dialect would render statements the engine rejects.

### Databricks

Free: Databricks Free Edition, a genuine perpetual free tier with no credit card
and no expiry, providing a SQL warehouse a driver can reach with a workspace
token.

Why it does not qualify: one workspace per account, with no account-level API to
provision another, so continuous integration would share a single
human-created token — and a pull request from a fork would get none. Its terms
also bar commercial use. There is no Databricks emulator, official or otherwise.

Why this is stricter than it may look: a capability preset in this repository is
a transcription of what a live server actually answered, the capability probe
starts a container per cell on every pull request, and the integration suite
starts a server. Support inferred from a vendor's documentation is refused
everywhere else in this tree, and an emulator's behavior recorded under the
engine's name would be the same thing with better manners.

Any of three developments would reopen this: an emulator faithful enough that
measuring it is a statement about the engine, a free tier that a forked pull
request can provision without human-held credentials, or an explicit decision to
relax the measurement standard for one engine — taken deliberately and written
down, rather than by drift. The findings behind the current answer, including
what was executed rather than read, are in
[#1879](https://github.com/stokaro/ptah/issues/1879).

## Declared release lines

Every engine ships several versions at once, and Ptah models each release line
with its own capability preset. The table below is the declared set: the line,
the preset it claims, how much testing stands behind that claim, and whether the
capability probe measures it against a live server on every pull request.

Declared is not the same as usable, and `Probed` is not the same as `Support`. A
release line absent from the table connects, resolves capabilities, and performs
the operations those capabilities allow. A certified line can be unprobed for
either of two reasons: the integration suite starts a server the capability
probe has no statement table for, or the line needs no server at all — SQLite is
compiled into the binary, so every `go test ./...` run exercises it.
[Support levels](#support-levels) defines what each level promises.

The table is generated from the single declaration the CI matrix also reads, so
the declared set cannot say one thing here and another in a workflow file.

<!-- BEGIN GENERATED VERSION MATRIX -->
| Dialect | Release line | Support | Capability preset | Probed |
| --- | --- | --- | --- | --- |
| `postgres` | 18 | certified | `Postgres18` | yes |
| `postgres` | 17 | certified | `Postgres17` | yes |
| `postgres` | 16 | certified | `Postgres16` | yes |
| `postgres` | 15 | certified | `Postgres16` | yes |
| `postgres` | 14 | certified | `Postgres16` | yes |
| `postgres` | 13 | legacy-tested | `Postgres13` | yes |
| `mysql` | 26.7 | certified | `MySQL84` | yes |
| `mysql` | 9.7 | certified | `MySQL84` | yes |
| `mysql` | 8.4 | certified | `MySQL84` | yes |
| `mariadb` | 12.3 | certified | `MariaDB1011` | yes |
| `mariadb` | 11.8 | certified | `MariaDB1011` | yes |
| `mariadb` | 11.4 | certified | `MariaDB1011` | yes |
| `mariadb` | 10.11 | certified | `MariaDB1011` | yes |
| `clickhouse` | 26.7 | certified | `ClickHouse2411` | yes |
| `clickhouse` | 26.3 | certified | `ClickHouse2411` | yes |
| `clickhouse` | 25.8 | certified | `ClickHouse2411` | yes |
| `clickhouse` | 24.10 | legacy-tested | `ClickHouse24` | yes |
| `oracle` | 23 | certified | `Oracle23` | yes |
| `oracle` | 21 | certified | `Oracle21` | yes |
| `cockroachdb` | 26.3 | certified | `CockroachDB263` | yes |
| `cockroachdb` | 26.2 | certified | `CockroachDB26` | yes |
| `cockroachdb` | 25.4 | certified | `CockroachDB25` | yes |
| `yugabytedb` | 2026.1 | certified | `YugabyteDB25` | yes |
| `yugabytedb` | 2025.2 | certified | `YugabyteDB25` | yes |
| `yugabytedb` | 2024.2 | certified | `YugabyteDB24` | yes |
| `spanner` | 0 | best-effort | `SpannerPostgres` | yes |
| `sqlserver` | 17.0 (SQL Server 2025) | certified | `SQLServer2022` | no |
| `sqlserver` | 16.0 (SQL Server 2022) | best-effort | `SQLServer2022` | no |
| `sqlserver` | 15.0 (SQL Server 2019) | best-effort | `SQLServer2022` | no |
| `sqlite` | 3 | certified | `SQLite3` | no |

Declared release lines: 30. Probed on every pull request: 26.

Support levels across the 30 declared lines: 25 certified, 2 legacy-tested, 3 best-effort.

Lines that are declared and not probed, and why:

- `sqlserver` 17.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlserver` 16.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlserver` 15.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlite` 3 — no container image is declared for this line; the capability probe has no statement table for the sqlite dialect, so a server on this line would be asked nothing.

Lines whose container tag does not name the line, so which patch it resolves to has to be read off the tag:

- `oracle` 23, pinned as `gvenzl/oracle-free:slim`.
- `oracle` 21, pinned as `gvenzl/oracle-xe:21-slim`.
- `spanner` 0, pinned as `gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator:v0.55.2`.
- `sqlserver` 17.0, pinned as `mcr.microsoft.com/mssql/server:2025-latest`.
- `sqlserver` 16.0, pinned as `mcr.microsoft.com/mssql/server:2022-latest`.
- `sqlserver` 15.0, pinned as `mcr.microsoft.com/mssql/server:2019-latest`.
<!-- END GENERATED VERSION MATRIX -->

Whether an observation can be credited to one line rather than its siblings is a
separate axis, recorded per line in the declaration and left out of the table
above: it describes the resolver rather than the promise, and the two together
render wider than this page's reading column. A line refined by parsed version
is attributable on its own; one reached through an engine banner is attributable
only where it has been measured directly; and where every release of an engine
receives the same set, an observation on one release says nothing about which of
them produced it. That is why several ClickHouse and SQL Server lines share one
preset.

`ptah db capabilities` reports a related but narrower fact per server, as
`Preset source`: which rule chose that server's preset, not whether the line is
attributable.

A future line with no preset resolves onto the newest preset Ptah has, which is
a stand-in rather than a match. The pipeline reports that condition as a
failure, and [issue 916](https://github.com/stokaro/ptah/issues/916) tracks the
remaining version-specific refinement work.

Which versions a vendor supports is recorded, with its source, next to each
block of cells in `internal/capabilityprobe/cells.go`. PostgreSQL does not
label releases LTS, so the reading used here is the newest patch of each
still-supported major line. The container that reproduces a line is recorded
there too, or the reason there is none: SQLite is compiled into the binary.
Spanner's container is the vendor's emulator behind PGAdapter, which is the only
Spanner endpoint a container can provide. CockroachDB's `latest-v<line>` aliases follow the newest
patch. YugabyteDB publishes no equivalent aliases, so the matrix driver
resolves the highest numeric Docker Hub tag under each declared line before it
starts the container.

## Support levels

The `Support` column records how much testing stands behind a release line. It
is a claim about Ptah's continuous integration, not about the server: what an
operation may do against a live target is decided by the capability set resolved
for that target.

| Status | Meaning |
| --- | --- |
| `certified` | Ptah exercises the line in continuous integration and commits to the tested feature surface. |
| `legacy-tested` | Upstream end-of-life, kept on purpose as a regression sentinel. Runtime behavior is the same as a certified line; the promise is weaker. |
| `best-effort` | Not regularly tested and not rejected. Capabilities are resolved as for any other server, and the operations they allow are performed. |
| `known-incompatible` | A concrete technical incompatibility is known and named. A vendor end-of-life date is not one, and no release line carries this level today. |

Upstream end-of-life does not make a release unsupported by Ptah at runtime. It
lowers the testing guarantee; it is not a refusal. No code in Ptah reads a
support level to decide whether an operation may proceed. PostgreSQL 13 moved to
`legacy-tested` when its final release shipped on 2025-11-13: measured against a
live 13.23 server on 2026-08-16, the line reports `legacy-tested`, its
capabilities resolve, and the operations they allow run.

### A line Ptah does not declare

A server whose version falls on no line in the table resolves to `best-effort`.
Its capabilities come from the preset its dialect resolves for that server — a
version ladder where the dialect has one, the dialect default or a banner match
otherwise — and the connection is not refused. The capability-probe pipeline is stricter than the
runtime here on purpose: it reports an unmatched line as a failure, because a
measurement it cannot attribute to a declared line is not evidence.

Ask the server in front of you what Ptah resolved for it:

```bash
ptah db capabilities --db-url "$DATABASE_URL"
```

Expected output includes the level and the line, here against a live PostgreSQL
18.4:

```text
Support level:      certified
Release line:       18
```

The report also names the dialect, the server version and banner, the preset and
the ladder that produced it, the non-boolean behavior values, and every
capability key as supported or unsupported; `--format json` emits everything the
text form shows, plus each capability key's documentation string, as a stable
sorted document.

Against a MySQL 8.0.46 server, a line this matrix does not declare, the same
command reports `best-effort` and says what it fell back to:

```text
Note: mysql 8.0.46 is not a measured release line; capabilities fall back to the preset its ladder assigns (newest measured line: 26.7)
```

That server connects, resolves its capabilities, and works.

### How a level is assigned

The rubric has two inputs: does anything in Ptah's continuous integration
exercise the line, and does the vendor still support it. Both yes is
`certified`. Exercised but upstream end-of-life is `legacy-tested`. Not exercised
is `best-effort`, whatever the vendor says, because certification is a claim
about Ptah's testing and an untested line has none to make.

Five declared lines are `best-effort`: ClickHouse 26.3, ClickHouse 25.8, SQL
Server 2022 (16.0), SQL Server 2019 (15.0), and Spanner. Four of the five sit
inside their vendor's support window. What four of them lack is a run in this
repository: the capability probe has no statement table for the `clickhouse` or
`sqlserver` dialects, and the only ClickHouse and SQL Server versions the
integration suite starts are ClickHouse 26.7, ClickHouse 24.10, and SQL Server
2025.

**Spanner is the exception, and it is deliberate.** Its capability rows are
measured on every pull request, against the Cloud Spanner emulator behind
PGAdapter — the only Spanner endpoint a container can provide. That run is worth
having: it catches a preset drifting from the interface. It is not evidence
about the managed service, and the two already differ measurably — the reference
says a serial column needs the database option `default_sequence_kind` set
first, and the emulator accepts one without it. So the line is exercised and
stays `best-effort`, which is the one place those two answers come apart
([issue 942](https://github.com/stokaro/ptah/issues/942)).

### What Spanner models, and what it does not

A table's **row deletion policy** — `TTL INTERVAL '30 days' ON created_at` — is
read, rendered, planned and compared. The interval is compared as a value rather
than as text, because the server rewrites it: `30 days` is stored as
`4 WEEKS 2 DAYS` and `60 days` as `2 MONTHS`, all of it reducing to a whole
number of days at thirty days to a month. It travels on the SQL surface, which
is what `ptah db read` writes and what `--schema-file` reads; the HCL and Go
annotation surfaces do not carry it yet.

A **change stream** is not modeled. It is a database object with its own
lifecycle rather than a table property, and a Spanner description says so rather
than staying silent about it: the read records the kind as not described, so the
absence of change streams from a document is never read as their absence from
the database ([issue 2236](https://github.com/stokaro/ptah/issues/2236)).

Presence in the table is not certification; the `Support` column is the place to
read.

### Keeping the levels current

A level is declared with its release line in `internal/capabilityprobe/cells.go`,
next to the preset the line claims and, where one exists, the container that
reproduces it, so a line and its promise cannot drift apart.

The two halves of the rubric are refreshed differently. The
continuous-integration half is machine-checked: a census test reads the CI matrix
and the integration workflow and fails any cell claiming `certified` or
`legacy-tested` that nothing exercises, so a line cannot earn certification by
being written down. The upstream half is a scheduled reading of the vendor pages
named beside each block of cells. Several vendors publish a duration rather than
a date — the three latest stable releases, a year after release — so a line's
status can change with no date passing.

That reading never removes a line. An upstream end-of-life date moves a cell from
`certified` to `legacy-tested` and does nothing else.

## PostgreSQL

PostgreSQL has the broadest coverage of any engine: schemas, extensions, enum
types, functions, views, materialized views, triggers, standalone sequences,
user-defined types, roles, grants, and row-level security all participate in
the generate / compare / migrate / rollback lifecycle.
[PostgreSQL](../postgresql/) covers each area, its version-dependent
behavior, and the materialized-view refresh boundary.

## MySQL and MariaDB

MySQL and MariaDB share one planner and renderer family and are separate
dialects with different capability sets, so a plan reviewed for one variant is
unreviewed for the other. [MySQL and MariaDB](../mysql/) covers the differences
that reach generated SQL, the foreign-key and routine rules that fail before a
plan is written, the privileges dev-database cleanup needs, and online-DDL
routing for large tables.

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

## Oracle

Oracle renders, plans, connects and reads a live catalog, against two measured
release lines, 23 and 21. [Oracle](../oracle/) covers the bare-identifier rule
the engine forces, the two type mappings Oracle has no direct equivalent for,
what the 23 and 21 lines do differently, object types, and PL/SQL
routines.

## PostgreSQL-compatible distributed targets

CockroachDB, YugabyteDB, and the Spanner PostgreSQL interface accept
PostgreSQL-like syntax while missing PostgreSQL capabilities, so Ptah routes
each one as a distinct dialect through the PostgreSQL implementation family
with its own capability preset instead of treating the server as a drop-in
PostgreSQL server. [CockroachDB, YugabyteDB, and Spanner](../distributed/)
covers what each preset excludes, the coverage behind each one, and CockroachDB
row-level TTL.

## ClickHouse

ClickHouse support is capability-limited: enums are inline `Enum8`/`Enum16`
column types, and foreign keys and enforced `CHECK` constraints are outside the
preset. [ClickHouse](../clickhouse/) covers the MergeTree round trip, views and
materialized views, roles and grants, the revision table's storage engine, and
Atlas revision metadata.

## Choosing behavior by capability, not by name

The dialect name picks the parser and renderer family; whether an individual
operation is valid on a concrete target is decided by its capability set —
see [Dialects and capabilities](../../concepts/dialects-and-capabilities/).
