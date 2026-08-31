---
title: Database support matrix
description: The database engines Ptah supports, at what depth, and the operational differences to know before rollout.
type: status
audience:
  - "database-engineer"
readerQuestion: "Which database release lines does Ptah exercise, and what does each support level mean?"
goal: "Identify the exercised release lines and interpret each support level."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema"
generated: false
lastVerified: "2026-08-30"
evidence:
  - "internal/capabilityprobe/cells.go"
  - ".github/workflows/capability-matrix.yml"
  - ".github/workflows/go-integration-tests.yml"
searchAliases:
  - "MySQL supported versions"
overlaps:
  - "/reference/capabilities/"
  - "/concepts/dialects-and-capabilities/"
disposition: keep
owns:
  - cli-ptah-db-capabilities
---

Use this page to answer two lookup questions: whether Ptah has a dialect for an
engine, and how much continuous testing stands behind a declared release line.
The labels are testing guarantees, not runtime gates. Read
[Support policy](../support-policy/) before using a line outside the declared
set, and [Support evidence](../support-evidence/) to see how the claims are
measured.

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

Engines with no dialect entry are listed, with the reason and dated evidence,
on [Support evidence](../support-evidence/#engines-with-no-ptah-dialect).

## Declared release lines

Every engine ships several versions at once, and Ptah models each release line
with its own capability preset. The table below is the declared set: the line,
the preset it claims, how much testing stands behind that claim, and whether the
capability probe measures it against a live server on every pull request.

Declared is not the same as usable, and `Probed` is not the same as `Support`.
A certified line can be unprobed when another CI path exercises it, or when the
line needs no server at all — SQLite is compiled into the binary, so every
`go test ./...` run exercises it. [Support policy](../support-policy/) defines
the levels and explains what happens to a release absent from this table.

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
| `clickhouse` | 26.8 | certified | `ClickHouse2411` | yes |
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

Declared release lines: 31. Probed on every pull request: 27.

Support levels across the 31 declared lines: 26 certified, 2 legacy-tested, 3 best-effort.

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

## Interpret the result

The `Support` column answers how Ptah tests a release line. The capability
profile answers what a particular server can do. Ptah does not read a support
level to allow or refuse an operation.

For a server you operate, ask Ptah which release line and capability preset it
resolved:

```bash
ptah db capabilities --db-url "$DATABASE_URL"
```

Read [Support policy](../support-policy/) for the level definitions and the
behavior of undeclared versions. Read [Support evidence](../support-evidence/)
for probe attribution, emulator limits, and the checks that keep this table tied
to CI. Engine-specific capabilities and limitations live on the linked engine
pages instead of being repeated here.
