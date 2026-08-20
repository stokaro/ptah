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

| Engine | Dialect (aliases) | URL schemes | Coverage |
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
| `postgres` | 18 | certified | `Postgres17` | yes |
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

Declared release lines: 27. Probed on every pull request: 23.

Support levels across the 27 declared lines: 22 certified, 2 legacy-tested, 3 best-effort.

Lines that are declared and not probed, and why:

- `sqlserver` 17.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlserver` 16.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlserver` 15.0 — the capability probe has no statement table for the sqlserver dialect, so a server on this line would be asked nothing.
- `sqlite` 3 — no container image is declared for this line; the capability probe has no statement table for the sqlite dialect, so a server on this line would be asked nothing.

Lines whose container tag does not name the line, so which patch it resolves to has to be read off the tag:

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
[PostgreSQL](../postgresql/) covers each area and its version-dependent
behavior.

### Materialized view refresh strategies

`refresh_strategy` accepts `manual`, and that is the whole answer rather than a
feature waiting to be written. Any other value is refused before rendering or
comparison, with the dialect, the view, the value, and the reason.

| Strategy | Status |
| --- | --- |
| `manual` | Accepted. Ptah emits no separate refresh, because it never needs one — see below. |
| `concurrently` | Refused. It is a data operation with no point in a schema apply to attach to. |
| `every <interval>` | Refused. None of the supported engines schedules a plain materialized view. |

`manual` needs no refresh because a Ptah apply never leaves a view stale.
Measured on PostgreSQL 18: `CREATE MATERIALIZED VIEW` populates, and a body
change is planned as `DROP` plus `CREATE`, which populates again. ClickHouse has
no refresh statement at all — its materialized views are maintained by inserts
into the source.

`concurrently` is refused for the same reason. `REFRESH MATERIALIZED VIEW
CONCURRENTLY` is real on the PostgreSQL family, and its precondition is real
too — without a unique index the server answers `cannot refresh materialized
view "public.mv" concurrently` — but the only moment it would matter is on a
view the current run did **not** change. Refreshing there is a data operation on
an unchanged schema, which apply performs for no other object.

To refresh on your own schedule, issue `REFRESH MATERIALIZED VIEW` yourself; it
is not a property of the schema Ptah manages.

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
- A modified `SQL SECURITY DEFINER` routine is refused before migration SQL is
  planned when its catalog `DEFINER` differs from the connected
  `CURRENT_USER()`. Connect as that definer, change the desired routine to
  `SQL SECURITY INVOKER`, or leave the foreign routine unchanged. Missing
  ownership facts fail closed too.
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
  It is also the one target that ADDS to PostgreSQL's surface rather than
  subtracting from it: see [CockroachDB row-level TTL](#cockroachdb-row-level-ttl).
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
`ptah db read` and `ptah-compat schema inspect`.

Spanner sits between the two:

- Its capability rows are measured on every pull request, against the Cloud
  Spanner emulator behind PGAdapter.
- It has no integration target exercising render, plan, apply and read end to
  end, the way the engines above do.
- An emulator is evidence about the PostgreSQL interface, not about the managed
  service.

Review generated SQL before relying on it.
PostgreSQL and YugabyteDB reject unsupported database-scoped publications,
subscriptions, logical replication slots, event triggers, and non-extension
foreign-data objects before dev-database cleanup. PostgreSQL additionally
removes database large objects inside the cleanup transaction; YugabyteDB does
not support that catalog write path.

### CockroachDB row-level TTL

CockroachDB expires rows on a schedule the server runs, declared as table
storage parameters. Ptah manages that policy through the render, plan, apply,
introspect, and diff cycle: a declared TTL is applied, read back from
`pg_class.reloptions`, and compared to zero difference on the next run.

Declare it as attributes on the table, named exactly for the storage parameters
they become:

```go
//ptah:schema:table name="sessions" ttl_expiration_expression="expires_at" ttl_job_cron="@daily"
type Sessions struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="expires_at" type="TIMESTAMPTZ"
	ExpiresAt time.Time
}
```

`ptah schema render --dialect cockroachdb` emits that as:

```sql
CREATE TABLE "sessions" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "expires_at" TIMESTAMPTZ
) WITH (ttl_expiration_expression = 'expires_at', ttl_job_cron = '@daily');
```

Changing the policy emits `ALTER TABLE ... SET (...)`, and removing it emits
`ALTER TABLE ... RESET (ttl)`, which drops the whole configuration in one
statement and leaves the table alone.

#### What Ptah manages

Ten parameters. One of the two enablers is required; the rest are refused
without one. Nine read back from the catalog exactly as written on both declared
lines, and `ttl_expire_after` is compared by the interval it denotes rather than
by its text, because the server rewrites the value it stores:

| Attribute | What it sets |
| --- | --- |
| `ttl_expiration_expression` | The SQL expression whose value is when a row expires. |
| `ttl_expire_after` | The interval after a row is written at which it expires, such as `3 days`. |
| `ttl_job_cron` | The schedule the deletion job runs on. |
| `ttl_select_batch_size` | Rows selected per batch; at least 1. |
| `ttl_delete_batch_size` | Rows deleted per batch; at least 1. |
| `ttl_select_rate_limit` | Rows selected per second; at least 1. |
| `ttl_delete_rate_limit` | Rows deleted per second; at least 1. |
| `ttl_pause` | Pauses the deletion job without removing the policy. |
| `ttl_label_metrics` | Labels the job's metrics with the table name. |
| `ttl_disable_changefeed_replication` | Omits the job's deletes from changefeeds. |

#### What Ptah refuses, and why

- **`ttl_row_stats_poll_interval` is not supported**: the
  server canonicalizes the duration (`'600s'` becomes `'10m0s'`) and stores
  nothing at all for a value below one second.
- **An interval Ptah cannot read is refused.** `ttl_expire_after` accepts a
  sequence of quantity-and-unit pairs (`3 days`, `2 years 3 months`,
  `1 day 2 hours`), an optional trailing `HH:MM:SS`, and the ISO-8601 form
  (`P1Y2M3D`, `PT1H30M`). A spelling outside that surface is refused rather than
  sent, because the server would normalize it into a form Ptah could not predict
  and the plan would re-issue the change forever. Ambiguous abbreviations such as
  a bare `m` are refused for the same reason: minutes and months are two
  different retention policies.
- **`ttl` cannot be declared.** It is derived from the other parameters, and
  the server refuses it when it arrives alone.
- **A knob without `ttl_expiration_expression` is refused**, because the server
  refuses it too: every other `ttl_` parameter needs an expiry configured.
- **Zero and negative knob values are refused.** The server rejects a negative
  value and accepts zero while storing the parameter nowhere at all, so neither
  can ever read back as declared. Omit the attribute to keep the engine default.
- **A `false` boolean normalizes to "not declared"**, because on the server
  those are the same state: `ttl_pause = false` is stored nowhere, and setting
  it erases an existing `true` exactly as a reset does.

#### Two things the server does that Ptah works around

**The interval is rewritten on the way in.** Measured on both declared lines,
`ttl_expire_after = '72 hours'` is stored as `'72:00:00'`, `'5 minutes'` as
`'00:05:00'`, `'1 week'` as `'7 days'`, and `'P1Y2M3D'` as
`'1 year 2 mons 3 days'`. Ptah sends what you wrote and compares what the
interval *denotes*, so a declaration converges whichever spelling it uses. The
three fields of a PostgreSQL interval stay apart in that comparison: a month is
not thirty days and a day is not twenty-four hours, and the server keeps them
apart too.

**Hidden columns are left out of the description.** `ttl_expire_after` adds a
`crdb_internal_expiration` column that CockroachDB marks hidden, and a table
declaring no primary key gets a hidden `rowid` the same way. Neither is a column
anybody declared, and describing them made a read unreplayable — applying it
back asked for a column the engine owns. Both are now excluded from a
CockroachDB read. PostgreSQL and YugabyteDB have no such notion and their reads
are unchanged.

#### On other engines

Row-level TTL is refused on every target without the capability — PostgreSQL,
YugabyteDB, Spanner, MySQL, MariaDB, SQLite, SQL Server and ClickHouse — before
anything is applied. PostgreSQL answers `unrecognized parameter
"ttl_expiration_expression"` on its own, but YugabyteDB first answers `WARNING:
storage parameter ttl_expiration_expression is unsupported, ignoring`. An engine
that ignores a retention policy is worse than one that refuses it, so Ptah does
not leave that decision to the server.

A CockroachDB dev database is required for dev-database workflows on a
CockroachDB target; a mismatched `--dev-url` is refused with
`--dev-url dialect "postgres" does not match --url dialect "cockroachdb"`.

## ClickHouse

ClickHouse support is capability-limited. The preset models enums as inline
`Enum8`/`Enum16` column types; foreign keys and enforced `CHECK` constraints
are outside the preset. Roles and grants are managed declaratively, within the
boundaries [ClickHouse roles and grants](#clickhouse-roles-and-grants) states.
Review generated SQL and the
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

A MergeTree table's sorting key is carried through the read, so a table Ptah
creates reads back as itself: the primary-key flag comes from
`system.columns.is_in_primary_key`, and the renderer derives the `ORDER BY` a
MergeTree engine requires from it. Where a table declares both a narrower
`PRIMARY KEY` and a wider `ORDER BY`, the sorting key is carried separately, so
a description does not silently sort by fewer columns than the table it
describes. Before this the read dropped the key entirely: a declaration carrying
`primary="true"` differed from its own table on every comparison, so
`ALTER TABLE ... MODIFY COLUMN` was re-planned forever, and `ptah db read`
exited 2 against any ClickHouse database Ptah could create because the
description it produced could not be rendered (stokaro/ptah#1603).

Plain views participate in the complete render, plan, and introspection cycle.
Ptah emits `CREATE VIEW`, `CREATE OR REPLACE VIEW`, and `DROP VIEW`, preserving
qualified names and query bodies, and reads ordinary views from
`system.tables`. An empty query body, `WITH CHECK OPTION`, or
`DROP VIEW ... CASCADE` fails instead of being ignored.

Materialized views do too. Ptah emits
`CREATE MATERIALIZED VIEW <name> ENGINE = MergeTree ORDER BY tuple() AS <query>`,
reads the object back from `system.tables`, and plans a changed query as a drop
followed by a create. Several ClickHouse-specific points are worth knowing
before adopting them:

- `refresh_strategy` accepts only `manual`, which means Ptah emits no separate
  refresh operation, and does not change ClickHouse's insert-driven materialized
  view maintenance. See
  [materialized view refresh strategies](#materialized-view-refresh-strategies)
  for why the other values are refused.

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

- A changed query is applied destructively. The drop takes the inner storage
  table and every row the view had accumulated, and the replacement omits
  `POPULATE`, so the view starts empty again and refills only from new inserts.
  ClickHouse does have `ALTER TABLE <view> MODIFY QUERY`, which keeps the stored
  rows, but it refuses any query whose output columns differ from the ones the
  storage table already has, and a Ptah declaration carries a query body with no
  column list to compare, so the planner cannot tell the two cases apart before
  the statement runs. Treat a materialized-view body change as a change that
  empties the view.

The `TO <target table>` form and refreshable materialized views are not emitted:
the shared schema model carries a name and a query, so it cannot name a separate
target table, and `REFRESH MATERIALIZED VIEW` remains a named diagnostic because
ClickHouse has no such statement.

A materialized view created elsewhere with `TO <target table>` is still read, and
it is read as though it owned its storage: `system.tables` reports the same
engine and the same `as_select` for both forms, and the target appears only in
`create_table_query`, which this reader does not consult. Such a view therefore
compares as synchronized against a declaration of the same query, and a later
body change is planned as a drop and a create that recreates it in the
inner-storage form, so inserts stop reaching the original target table. Do not
manage a `TO` materialized view with Ptah.

### ClickHouse roles and grants

Roles and grants complete the render, plan, apply, introspect, and diff cycle.
A declared role and a database- or table-scoped grant are applied, read back
from `system.roles` and `system.grants`, and compared to zero difference on the
next run. Declaring them is the same pair of annotations every other engine
uses, with the grant scope written as `database.table`:

```go
//ptah:schema:role name="ptah_reader"
type PtahReaderRole struct{}

//ptah:schema:grant role="ptah_reader" privileges="SELECT" on_table="ptah_test.orders"
type PtahReaderOrdersGrant struct{}
```

`ptah schema render --dialect clickhouse` emits those as:

```sql
CREATE ROLE IF NOT EXISTS `ptah_reader`;
GRANT SELECT ON `ptah_test`.`orders` TO `ptah_reader`;
```

Roles are always planned before grants, because ClickHouse refuses a grant to a
role it does not know. `GRANT ... WITH GRANT OPTION` is emitted when the
declaration asks for it, and taking that option away again is the single
statement `REVOKE GRANT OPTION FOR ...`, which leaves the privilege itself in
place. Removing a grant emits `REVOKE <privileges> ON <scope> FROM <role>`.
`DROP ROLE IF EXISTS` has a rendering for a schema source that spells one out,
but comparison never plans it — see "Roles are never dropped" below.

What Ptah manages here is roles and grants, and nothing else in ClickHouse's
access control:

| Not managed | What that means for you |
| --- | --- |
| Users | No user is created, altered, or read, so no credential enters a description, a plan, or a log. Provision users outside Ptah and grant them a managed role. |
| Role membership | `GRANT <role> TO <role>` is not modeled. Ptah reads and writes privilege grants only. |
| Quotas, row policies, settings profiles | Outside the schema model entirely; a declaration cannot express them and a read does not report them. |
| Column-scoped grants | `GRANT SELECT(id) ON db.t` is refused when declared and excluded when read. Grants are managed at database and table scope. |
| Wildcard and global scopes | `*.*` and a wildcard database are refused. Such a grant reaches objects no declared schema describes. |
| Privilege names the server rewrites | `ALL`, `CREATE`, `DROP`, `SYSTEM`, `SYSTEM FLUSH`, `ACCESS MANAGEMENT`, `SHOW ACCESS`, `SHOW FILESYSTEM CACHES`, and — at table scope only — `SHOW` and `ALTER`. See below. |

Five refusals arrive before anything on the server changes, and the reason for
each is worth knowing in advance:

- **A ClickHouse role carries no attributes.** `system.roles` is
  `(name, id, storage)`, so a declared `password`, `login`, `superuser`,
  `createdb`, `createrole`, or `replication` is refused rather than dropped.
  Dropping a password would leave you believing a credential was set on an
  object that cannot hold one. `ALTER ROLE` is refused for the same reason:
  there is nothing to alter.
- **The server absorbs a narrower grant into a broader one.** Granting `SELECT`
  on `db.*` and on `db.t` leaves one row for `db.*`, in either order, and the
  table-level grant is recorded nowhere. Declaring both is refused, because the
  pair could never converge: the absorbed grant would read as missing on every
  inspection and the plan would re-issue it forever. Declare the broader scope.
- **A grant scope must be qualified as `database.table`.** An unqualified table
  name is refused. Rendering is offline and has no current database to resolve
  it against, and resolving an access-control decision against whichever
  database a session happens to have selected is not a formatting mistake. A
  trailing dot such as `shop.` is refused too, rather than read as the whole
  database: a typo must not widen one table's privilege to every table.
- **A grant must name a role the same schema declares.** ClickHouse resolves a
  grantee by name across users AND roles, with no syntax to say which is meant.
  If nothing of that name exists the `GRANT` fails partway through a migration
  with `UNKNOWN_ROLE`; if a *user* of that name exists it succeeds and lands on
  the user, where the reader never sees it again — the plan re-issues it forever
  and a real account quietly holds a privilege nobody declared for it. Declaring
  the role removes both outcomes, because Ptah creates it in the same plan.
  One case this cannot cover: if a live ClickHouse user shares a name with a
  declared role, resolution still prefers the user. Do not name a role after an
  account.
- **Some privilege names are groups the server rewrites.** `GRANT CREATE ON db.*`
  records four rows — `CREATE DATABASE`, `CREATE TABLE`, `CREATE VIEW`,
  `CREATE DICTIONARY` — and never reads back as `CREATE`, so a schema declaring
  it can never converge. `GRANT ALL` records 45 individual rows on 26.7 and 39 on
  24.10. `GRANT SHOW ACCESS` is stored as `SHOW ROW POLICIES`. `GRANT SHOW
  FILESYSTEM CACHES` is accepted and records *nothing at all*, which would tell
  you a grant applied while the role held no privilege. Declare the individual
  privileges instead. Group names that do read back as written stay
  declarable — `ALTER TABLE`, `ALTER VIEW`, `ALTER COLUMN`, `ALTER INDEX`,
  `ALTER STATISTICS`, `ALTER PROJECTION`, `ALTER CONSTRAINT`, `SYSTEM SENDS`,
  and `SHOW` and `ALTER` on a database scope. A name the server itself refuses,
  such as `INTROSPECTION` or `SYSTEM RELOAD`, needs no gate here: ClickHouse
  answers `Code: 509 ... cannot be granted on the database level`, which names
  the problem more precisely than Ptah could.

Three more boundaries shape what a run does:

- **Roles are never dropped.** A role that exists on the server and not in the
  schema is named in the plan and left alone. A ClickHouse role is server-wide
  and may carry grants outside the managed schema, so removing it is not Ptah's
  decision to make. A read reports only the roles the described grants name, and
  leaves out roles defined in the server's configuration files, whose `storage`
  is `users_xml`, because SQL does not own those.
- **A partial revoke fails the comparison.** `GRANT SELECT ON db.* TO r`
  followed by `REVOKE SELECT ON db.t FROM r` leaves two rows in `system.grants`,
  and the role's effective privileges are the first minus the second. A
  declaration cannot express an exception, so on a managed role Ptah refuses
  instead of comparing equal and reporting convergence. Remove the partial
  revoke, or stop declaring the role.
- **An account that may not read the access catalog still gets a read.**
  Reading `system.roles` and `system.grants` needs a privilege reading a table
  does not, and an account holding only `SELECT`, `SHOW TABLES` and
  `SHOW COLUMNS` is answered `Code: 497 ... (ACCESS_DENIED)` by both. Rather
  than failing the whole read — which would break reading a schema that
  declares no role at all — Ptah describes everything else and records that it
  did not look. Comparison then withholds every declared role instead of
  planning a `CREATE ROLE` it could not verify, and nothing destructive can
  follow, because removal is decided from live rows and there are none.
  `ptah-compat schema inspect` says so on stderr.

Two operational notes. ClickHouse RBAC statements carry no `ON CLUSTER` clause,
so on a cluster they affect the connected replica only; Ptah does not model
cluster propagation, and a multi-replica deployment needs its own arrangement
for that. And a ClickHouse role is server-wide rather than database-scoped, so a
role created against a dev or throwaway database outlives the database that
workflow drops.

The connected account needs the privileges these statements require —
`ROLE ADMIN` plus `GRANT OPTION` on what it grants. The `docker-compose.yaml`
in this repository configures `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`, which
is what gives its `ptah_user` that authority.

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
