---
title: YDB
description: YDB in Ptah - connecting with ydb:// URLs, what renders and plans for row tables, type mappings, keys, defaults and indexes, what each release line can do, and what is not supported yet.
type: reference
audience:
  - "database-engineer"
readerQuestion: "Which YDB tables, changes and release lines does Ptah support?"
goal: "Look up what Ptah renders, plans, reads and applies on YDB."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema/ydb"
  - "internal/ydbtype"
generated: false
searchAliases:
  - "YDB support"
  - "YQL"
overlaps: []
disposition: keep
owns:
  - dialect-ydb
---

Ptah renders YQL for YDB row tables, plans a migration between two schemas,
connects to a live database, reads its tables back, applies DDL to it and runs
versioned migrations against it. The
dialect name is `ydb`. YDB is its own dialect rather than a PostgreSQL-family
one: Ptah writes YQL and talks to the server through the YDB Go SDK. A schema
Ptah applies reads back as itself, which the integration suite checks against a
live YDB 26.2 in CI. The nightly capability matrix runs the same suite on each
YDB line it probes.

Data changes, lint, dev databases, `ptah-compat` and the YDB object families
such as TTL, column families, changefeeds, views and vector indexes are not
supported yet. See [What is not supported yet](#what-is-not-supported-yet).

## Connecting

`ydb://` connects over plaintext gRPC, on port 2136 by default. `ydbs://`
connects over TLS, on port 2135 by default. The database comes from the URL
path or from the `database` parameter:

```text
ydb://localhost:2136/local
ydbs://ydb.example.com:2135/?database=/ru-central1/b1g/etn
```

A URL naming the database both ways is refused when the two disagree, and so is
a URL without a host, an empty parameter or a repeated one.

Credentials come from exactly one source:

- the URL's user and password, for a static user;
- the `token` parameter, for an access token;
- `use_env_credentials=true`, which reads the `YDB_*` variables the YDB SDKs
  share, service-account keys and instance metadata included. Ptah refuses the
  environments in which the SDK would connect anonymously without saying so.

Besides `database`, `token` and `use_env_credentials`, a URL may carry the SDK
parameters `go_balancer`, `go_default_idempotent` and
`prefetch_query_result_parts`, and `monitoring`, which names the cluster's
monitoring endpoint (see [Feature flags](#feature-flags)). Any other parameter
is refused with the list of the accepted ones.

A server in a container on another host advertises its node through discovery
as `localhost:2136`, which the client cannot reach. Connect to such a server
with `go_balancer=disable`:

```text
ydb://203.0.113.10:2136/local?go_balancer=disable
```

## Paths, schemas and names

A YDB database has no schemas. A table is a path in a directory tree, so a Ptah
schema is a directory relative to the database root, and the root by default.
A table `orders` in the schema `shop` is the path `shop/orders`, written as one
backticked name. Creating the table creates its directories. Ptah never reads
or writes a directory whose name starts with a dot, such as `.sys` or
`.metadata`.

Names are case-sensitive: `Users` and `users` are two tables. Ptah always
quotes a name with backticks and escapes a backtick or a backslash inside it.

## Tables and types

This schema:

```yaml
tables:
  users:
    columns:
      id: {type: BIGSERIAL, primary: true}
      email: {type: VARCHAR(255), not_null: true}
      display_name: {type: TEXT}
      score: {type: INTEGER, default: "0"}
      created_at: {type: TIMESTAMP, not_null: true}
    indexes:
      users_email_uq:
        fields: [email]
        unique: true
      users_score_ix:
        fields: [score]
```

renders on YDB 26.2 as:

```sql
CREATE TABLE `users` (
    `id` BigSerial NOT NULL,
    `email` Utf8 NOT NULL,
    `display_name` Utf8,
    `score` Int32 DEFAULT 0,
    `created_at` Timestamp64 NOT NULL,
    PRIMARY KEY (`id`),
    INDEX `users_email_uq` GLOBAL UNIQUE SYNC ON (`email`),
    INDEX `users_score_ix` GLOBAL SYNC ON (`score`)
);
```

The type map:

| Declared type | YDB type |
| --- | --- |
| `VARCHAR(n)`, `CHAR(n)`, `TEXT` | `Utf8` |
| `BYTEA`, `BLOB`, `BINARY`, `VARBINARY` | `String` |
| `BOOLEAN` | `Bool` |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `Int8`, `Int16`, `Int32`, `Int64` |
| unsigned integer types | `Uint8` to `Uint64` |
| `SMALLSERIAL`, `SERIAL`, `BIGSERIAL` | `SmallSerial`, `Serial`, `BigSerial` |
| `REAL`, `DOUBLE PRECISION` | `Float`, `Double` |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `Decimal(p,s)`, with p up to 35 |
| `TIMESTAMP`, `TIMESTAMPTZ` | `Timestamp64`, or `Timestamp` on 25.1 |
| `DATE`, `INTERVAL` | `Date32` and `Interval64`, or `Date` and `Interval` on 25.1 |
| `JSON`, `JSONB` | `Json`, `JsonDocument` |
| `UUID` | `Uuid` |

A YDB type name such as `Utf8`, `Uint64` or `DyNumber`, or a
`platform.ydb.type` override, passes through as written.

YDB has no length-limited string, so `VARCHAR(255)` becomes `Utf8` and the
length is not enforced. A comparison does not report the dropped length as a
change. `ptah schema validate --dialect ydb --no-skipped` names it:

```text
ydb: column "users.email": type modifier=VARCHAR(255): length 255 would be skipped
1 problem
```

A type YDB cannot store is refused with the reason: `TIME`, arrays, enums,
`XML`, ranges, and a bare `DECIMAL` with no precision. Ptah never writes
`Varchar`: YDB accepts the word and makes the column bytes rather than text.

## Keys, defaults and indexes

Every table needs a primary key, and a table without one is refused before
anything is rendered. The key is written as a table-level `PRIMARY KEY`, and
every key column is `NOT NULL` unless the model declares it nullable.

A default is a literal, written as the typed YQL literal YDB reads back: `0`,
`'x'u` for text, `Timestamp('2026-01-01T00:00:00Z')`. An expression default
such as a function call is refused, because YDB takes literals only.

Indexes are global. A plain index is `GLOBAL SYNC`, a unique one
`GLOBAL UNIQUE SYNC`, an asynchronous one `GLOBAL ASYNC`, and `INCLUDE` columns
become `COVER (...)`. A unique index treats NULLs as distinct, as PostgreSQL
does by default.

The indexes of a new table are written inside its `CREATE TABLE`. YDB keeps
adding a unique index to a table that already exists behind a feature flag that
is off by default, so Ptah refuses that change and declares a unique index with
the table instead. A cluster that turns the flag on takes the change when the
URL names its monitoring endpoint (see [Feature flags](#feature-flags)). Other indexes added to an existing table get one
`ALTER TABLE ... ADD INDEX` each, because YDB adds one index per statement.

A foreign key, a `CHECK` constraint and a `UNIQUE` constraint are refused. A
`UNIQUE` refusal points to a unique index, which enforces the same rule.

## Planning changes

YDB changes a table in place less than the SQL engines do, and runs a schema
statement outside any transaction. A plan therefore refuses what the server
cannot do before it emits anything, and orders what it emits so that no
statement needs one that has not run yet:

1. Create the added tables, with their indexes.
2. Drop the indexes the plan removes, before any column they name. YDB refuses
   to drop an indexed or a covered column.
3. Per table: add columns, then change columns in place, then drop columns.
4. Add the new indexes of existing tables.
5. Drop the removed tables.

Each statement runs as its own query. A query of several schema statements is
not atomic on YDB, and each of its statements compiles against the schema as it
stood before the query.

A plan refuses a primary key change, a column type change and `SET NOT NULL`,
none of which YDB has. Where a release line cannot make a change in place, the
refusal names the capability that line lacks.

## What each release line does

Ptah measures six YDB release lines. Each has a capability preset, and a
server's `SELECT Version()` selects it, whether the server reports `26.2.1.14`
or `stable-25-4-1`:

| Preset | Lines | Compared with the line above, lacks |
| --- | --- | --- |
| `YDB262` | 26.2 | — |
| `YDB261` | 26.1 | `SET DEFAULT` and `DROP DEFAULT` on an existing column |
| `YDB253` | 25.3, 25.4 | a column added with a default |
| `YDB252` | 25.2 | a `JsonDocument` or `DyNumber` default |
| `YDB251` | 25.1 | the 64-bit date and time types, `Decimal` precision other than 22,9, an `Int16` or `Uint16` default |

`ptah schema render --dialect ydb --server-version 25.1.4.7` renders for a line
without a server. The capability probe measures 26.2, the current release, and
25.1, the one line with a published support date, against a server of its own on
each run of the capability matrix. The other lines keep the presets measured on
them and are best-effort.

### Feature flags

A preset describes a release line running with its default feature flags. A
cluster that turns a flag on or off can do more or less than that. Name the
cluster's monitoring endpoint in the URL, and Ptah reads the flags from
`/viewer/json/feature_flags` there when it connects:

```text
ydb://localhost:2136/local?monitoring=http://localhost:8765
```

The flags decide these capabilities:

| Feature flag | Capability |
| --- | --- |
| `EnableAddUniqueIndex` | `unique_index_on_existing_table` |
| `EnableAddColumsWithDefaults` | `add_column_with_default` |
| `EnableSetDropDefaultValue` | `alter_column_default` |
| `EnableTableDatetime64` | `wide_date_time_types` |
| `EnableParameterizedDecimal` | `parameterized_decimal` |

A flag the cluster does not list leaves the capability as the release line's
preset has it. Ptah sends no credentials to the monitoring endpoint and follows
no redirect from it. A failed read fails the connection rather than planning
without the flags. `ptah db capabilities` lists the keys
the flags changed under `Set by this server rather than by its release line`.

Without the parameter the line's preset stands. A statement the cluster
refuses because a flag is off then fails with an error that names the
capability and the flag. Which lines are declared, and at what support level, is on
the [support matrix](../support-matrix/).

## Reading a live database

`ptah db read --db-url ydb://...` and the commands that compare against a
database read every row table under the database root, its columns, defaults,
`Serial` columns, primary key and global indexes.

What Ptah does not model yet is recorded rather than dropped: views, topics,
column-oriented tables, sequences other than a `Serial` column's, and the
settings of a table such as TTL, column families, partitioning options and
changefeeds. A command reports them, and a plan neither drops nor changes them.

An index kind Ptah cannot read, such as a vector or a full-text index, is
refused by name rather than read as a plain index.

## Versioned migrations

`ptah migrations up`, `down`, `status`, `baseline`, `set`, `repair`, `tag`,
`log` and `test` run against YDB, with the revision table in either format.
YDB runs a schema statement only outside a transaction and never in the same
query as a data statement, so a migration file runs as a sequence of queries:
each schema statement on its own, and each run of data statements as one
transaction together with the revision checkpoint that records it. A
definition such as `$name = ...` or a `PRAGMA` reaches every query after it.
`--tx-mode file` therefore runs a file the way `none` does, and `all` is
refused. [Migrations on YDB](../../versioned/apply/#migrations-on-ydb) has the
details, including the files refused before a run writes anything.

The migration lock is a semaphore on the coordination node `ptah_locks` at the
database root, which Ptah creates on first use and keeps. A run that loses it
stops before its next statement; see
[locking](../../versioned/apply/#locking-and---migration-lock-timeout).
`--lock-timeout` and `--statement-timeout` are refused: YDB has no lock wait to
bound, and a client timeout cannot promise a schema statement did not commit.

## What is not supported yet

These are refused with a message that names what is missing:

- the query builder and data changes: seeds, data plans and declared rows;
- `ptah sql lint` and `ptah migrations lint` over YQL;
- a YDB database as a dev or shadow database;
- comments, views, users, groups and permissions;
- table settings: TTL, partitioning, column families and changefeeds;
- vector, full-text, JSON and column-table indexes;
- every `ptah-compat` command with a YDB URL, from any source.

The work is planned in [#4015](https://github.com/stokaro/ptah/issues/4015).

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
- URL forms for every engine: [Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/).
