---
title: YDB
description: YDB in Ptah - connecting with ydb:// URLs, what renders and plans for row tables, type mappings, keys, defaults and indexes, what each release line can do, linting YQL, seeds, declared rows and the query builder, and what is not supported yet.
type: reference
audience:
  - "database-engineer"
readerQuestion: "Which YDB tables, changes and release lines does Ptah support?"
goal: "Look up what Ptah renders, plans, reads and applies on YDB."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema/ydb"
  - "internal/ydbgap"
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
connects to a live database, reads its tables back, applies DDL to it, runs
versioned migrations against it, lints YQL for it, and writes data to it:
seeds, declared rows and the statements the query builder renders. The
dialect name is `ydb`. YDB is its own dialect rather than a PostgreSQL-family
one: Ptah writes YQL and talks to the server through the YDB Go SDK. A schema
Ptah applies reads back as itself, which the integration suite checks in CI
against live YDB 26.2 and 25.1 servers. The nightly capability matrix runs the
same suite on each YDB line it probes.

`ptah-compat` takes a YDB URL on every verb; see [ptah-compat](#ptah-compat).
Dev databases, inference and the YDB object families such as TTL, column
families, changefeeds, views and vector indexes are not supported yet. See
[What is not supported yet](#what-is-not-supported-yet).

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
`GLOBAL UNIQUE SYNC`, an asynchronous one (`type="async"`) `GLOBAL ASYNC`, and
`include` columns become `COVER (...)`. A unique index treats NULLs as
distinct, as PostgreSQL does by default.

YDB has no `CREATE INDEX` statement, which the `create_index_statement` key
records, so the indexes of a new table are written inside its `CREATE TABLE`. YDB keeps
adding a unique index to a table that already exists behind a feature flag that
is off by default, so Ptah refuses that change and declares a unique index with
the table instead. A cluster that turns the flag on takes the change when the
URL names its monitoring endpoint (see [Feature flags](#feature-flags)). Other
indexes added to an existing table get one `ALTER TABLE ... ADD INDEX` each,
because YDB adds one index per statement.

A foreign key and a `CHECK` constraint are refused. YDB has no `UNIQUE`
constraint, so a declared one becomes the global unique index that holds the
same rows: a second row with the same value is refused, and rows whose value
is NULL are not. The index takes the constraint's name. A column's own
`UNIQUE` and an unnamed `UNIQUE` take `<table>_<columns>_key`, as PostgreSQL
names them. A `UNIQUE` over the primary key columns needs no index, because the
key holds those rows unique already. The comparison reads the declared
constraint as that index, so a database holding the index plans nothing, and a
`UNIQUE` added to a table that exists needs the same flag as any unique index.
A deferrable, `NOT ENFORCED`, partial or commented `UNIQUE` is refused.

### Index partitioning

Each global index is a table of its own, which YDB splits into partitions as it
grows. An index declares how, with attributes named after the YDB settings,
in lower case:

| Attribute | Value |
| --- | --- |
| `auto_partitioning_by_size` | `ENABLED` or `DISABLED` |
| `auto_partitioning_partition_size_mb` | megabytes, at least 1 |
| `auto_partitioning_by_load` | `ENABLED` or `DISABLED` |
| `auto_partitioning_min_partitions_count` | at least 1 |
| `auto_partitioning_max_partitions_count` | at least 1 |
| `read_replicas_settings` | `PER_AZ:<n>` or `ANY_AZ:<n>` |

The same keys work on an index in a YAML schema. A setting an index leaves out
is the value YDB gives a new index: split by size at 2048 MB, not by load, at
least one partition, no maximum and no read replicas. An index does not take
its table's settings.

This index:

```go
//ptah:schema:index name="orders_customer_ix" fields="customer" include="status,total" type="async" auto_partitioning_by_load="ENABLED" auto_partitioning_min_partitions_count="4" auto_partitioning_max_partitions_count="16"
```

renders as:

```sql
INDEX `orders_customer_ix` GLOBAL ASYNC ON (`customer`) COVER (`status`, `total`)
```

inside its table's `CREATE TABLE`, followed by:

```sql
ALTER TABLE `orders` ALTER INDEX `orders_customer_ix` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 16);
```

No statement that creates an index takes the settings, so they are an
`ALTER INDEX` of their own, which runs as its own query. That statement names
every setting, because setting one can reset another: setting
`AUTO_PARTITIONING_BY_LOAD` resets the minimum partition count to 1, and
setting `AUTO_PARTITIONING_BY_SIZE` resets the size and the minimum.

A change of the settings is made in place with the same statement, and read
back from the index's implementation table. YDB cannot remove a maximum
partition count, so an index that drops its maximum is rebuilt: dropped,
added again and given the rest of its settings. A partition size on an index
that does not split by size is refused, as YDB refuses it. Other dialects
refuse an index that declares its partitioning.

### Renaming an index

An index the declaration renames, on the same table with the same columns,
kind, cover and uniqueness, is renamed in place rather than dropped and built
again:

```sql
ALTER TABLE `orders` RENAME INDEX `orders_customer_ix` TO `orders_by_customer`;
```

The index keeps its rows, its kind, its cover and its partitioning. A rename
that also changes the partitioning is a rename followed by an `ALTER INDEX`. A
pair of indexes that swap names, or an index renamed onto a name the table
still holds, is dropped and built again. The rollback of a planned migration
renames the index back.

A cluster that turns `EnableMoveIndex` off refuses the rename with
`Move index is not supported yet`. When the URL names the cluster's monitoring
endpoint, Ptah reads the flag and plans the renamed index as dropped and added
again, under the rules for adding an index to an existing table (see
[Feature flags](#feature-flags)).

## Planning changes

YDB changes a table in place less than the SQL engines do, and runs a schema
statement outside any transaction. A plan therefore refuses what the server
cannot do before it emits anything, and orders what it emits so that no
statement needs one that has not run yet:

1. Create the added tables, with their indexes.
2. Drop the indexes the plan removes, before any column they name. YDB refuses
   to drop an indexed or a covered column.
3. Rename the indexes the declaration renames, then change the partitioning of
   the indexes that keep their definition.
4. Per table: add columns, then change columns in place, then drop columns.
5. Add the new indexes of existing tables.
6. Drop the removed tables.

Each statement runs as its own query. A query of several schema statements is
not atomic on YDB, and each of its statements compiles against the schema as it
stood before the query.

YDB has no primary key change, no column type change and no `SET NOT NULL`. A
plan refuses each of them, naming the capability the line lacks, unless the
command was given `--allow-table-rebuild`, or `ptah-compat` ran with
`PTAH_ALLOW_TABLE_REBUILD=1`. Where a release line cannot make
another change in place, the refusal names the capability that line lacks.

### Table rebuilds

With `--allow-table-rebuild`, `ptah schema apply`, `schema plan`, `schema diff`,
`schema compare`, `migrations plan` and `migrations generate` plan those three
changes as a rebuild of the table. `ptah-compat` takes no flag the Atlas
community CLI lacks, so its `schema apply`, `schema diff` and `schema plan new`
ask with the variable `PTAH_ALLOW_TABLE_REBUILD=1` instead (see
[ptah-compat](#ptah-compat)). The rebuild is the same:

1. `CREATE TABLE` a scratch table, `__ptah_rebuild_<table>`, from the
   declaration, with its indexes inside it.
2. `INSERT INTO` the scratch table `SELECT` the old rows, converting each
   changed column.
3. `ALTER TABLE` the old table `RENAME TO __ptah_replaced_<table>`.
4. `ALTER TABLE` the scratch table `RENAME TO` the table's name.
5. `DROP TABLE` the renamed old table.

YDB has no transactional DDL, and YQL has no statement that swaps two tables
at once, so the steps are not atomic. **Rows written to the table between the
copy and the swap are lost, and YDB has no lock to stop them**: stop writing to
the table until the last step has run. The plan says so in a comment above the
steps. The old rows stay until step 5, and the table's name is free only between
steps 3 and 4.

The copy is one data query, which commits whole or not at all:

- A value the new type cannot hold fails it with a message that names the
  column, because the conversion unwraps the `CAST` rather than writing NULL in
  silence. A NULL row of a column that becomes NOT NULL fails it the same way.
  Fix the rows and rerun; `migrations up --allow-dirty` resumes at the copy.
- YDB refuses a copy that carries more than a limit. Measured with rows of about
  80 bytes, 400000 rows copy and 600000 rows are refused, on 26.2 with `Out of
  buffer memory. Used 74395928 bytes of 67108864 bytes` and on 25.1 with
  `Datashard program size limit exceeded (56281409 > 50331648)`. Nothing is then
  copied, and the old table keeps serving. A larger table has to be rebuilt by
  hand.

In a versioned migration each step is a query of its own, and progress is
recorded after each. A run interrupted between two steps resumes at the next
one with `migrations up --allow-dirty`.

Even with the flag, a rebuild is refused when it would damage the table:

- a table with a Serial column. The new table's sequence would start at 1 while
  the copied rows keep their values, so the next insert would collide, and YDB's
  `ALTER SEQUENCE ... RESTART WITH` takes only a literal;
- a table carrying a setting Ptah does not model yet: a TTL, changefeeds,
  column families, or partitioning, read replica and key bloom filter options.
  Recreating the table would drop them.

## What each release line does

Ptah measures six YDB release lines. Each has a capability preset, and a
server's `SELECT Version()` selects it, whether the server reports `26.2.1.14`
or `stable-25-4-1`:

| Preset | Lines | Compared with the line above, lacks |
| --- | --- | --- |
| `YDB262` | 26.2 | — |
| `YDB261` | 26.1 | `SET DEFAULT` and `DROP DEFAULT` on an existing column |
| `YDB253` | 25.3, 25.4 | a column added with a default |
| `YDB252` | 25.2 | a `JsonDocument` or `DyNumber` default, `UPDATE ... RETURNING` on a table with a unique index |
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
| `EnableMoveIndex` | `index_rename` |

`EnableAsyncIndexes` decides no capability: a cluster with the flag off still
builds a `GLOBAL ASYNC` index, so `async_indexes` keeps the preset's answer.

A flag the cluster does not list leaves the capability as the release line's
preset has it. A failed read fails the connection rather than planning without
the flags. `ptah db capabilities` lists the keys the flags changed under
`Set by this server rather than by its release line`.

Ptah reads the page as the user the URL connects as. It sends the token the
connection presents to the server in the `Authorization` header, which is how
the monitoring endpoint of a cluster that enforces authentication accepts it,
and an anonymous connection sends none. YDB 26.2 serves the page only to a
user with `DESCRIBE SCHEMA` on the database, and answers anyone else with
`400 Bad Request: Failed to resolve database`; YDB 25.1 serves it to any user it
knows. A `ydbs://` connection's token is not sent to a plain `http://`
endpoint: name the endpoint with `https://`. The monitoring parameter carries
no credential of its own, and Ptah follows no redirect from the endpoint.

Without the parameter the line's preset stands. A statement the cluster
refuses because a flag is off then fails with an error that names the
capability and the flag. Which lines are declared, and at what support level, is on
the [support matrix](../support-matrix/).

## Reading a live database

`ptah db read --db-url ydb://...` and the commands that compare against a
database read every row table under the database root, its columns, defaults,
`Serial` columns, primary key and global indexes, with each index's
partitioning and read replicas.

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
`--statement-timeout` puts a deadline on each query of a migration: a data
query it stops applies nothing, and an index build or a column backfill it stops
is canceled; any other schema statement YDB keeps running, and the run records
its outcome as unknown. `--lock-timeout` is refused, because no statement on
YDB waits for a lock. See
[statement timeouts on YDB](../../versioned/apply/#statement-timeouts-on-ydb).

## Linting

`ptah migrations lint --dialect ydb` and `ptah sql lint --dialect ydb` read
YQL by the rules the migrator splits it with: a double-quoted `"x"` is a
string, a backslash escapes a quote, and a block or an action body is one
statement. `--server-version` names the release line a capability is read
from, and the newest line is used without it.

Migration lint reports the statements YDB refuses, or runs with an effect the
statement does not state, under the `YD` family: a unique index added to an
existing table, a block that mixes schema and data statements, an `ADD COLUMN`
the line refuses, a dropped column an index or the TTL uses, a partitioning
change that resets the minimum partition count, and a dropped table a view
reads. [Lint rules](../../reference/lint-rules/#ydb) lists each rule with its
meaning.

`YD104` and `YD106` read the indexes, TTL and views the directory's own
earlier migrations declare, because a YDB database cannot be a dev database
yet; a table the directory never created is unknown to them. The rules for
every dialect run too, and the
[lint rules](../../reference/lint-rules/#what-the-rules-for-every-dialect-do-on-ydb)
say what each does on YDB.

`ptah sql lint` reads YQL without the SQL parser, which has no YQL grammar. It
reports a `CREATE TABLE` without a primary key, which YDB refuses, as `DDL001`,
and a capability an `ALTER TABLE` needs that the line lacks as `CAP001`.

`ptah migrations up` blocks on the `DS` family on YDB as on every engine. A
`.ptah-lint.yaml` with `gate: { families: [YD] }` refuses a pending migration
that carries a `YD` error before any migration runs.

## Data

YQL types every literal and converts few of them, so Ptah writes each value in
the type of the column it lands in: `5` for an `Int32` column and `5l` for an
`Int64` one, `'x'u` for `Utf8`, `Timestamp('2026-01-02T03:04:05Z')` for a
`Timestamp`, and an escaped string for the bytes of a `String`. A value its
column cannot hold is refused with the column and the type rather than
wrapped: an integer outside the type's range, a negative one for an unsigned
type, a moment more precise than the type or before 1970 in a narrow one.

### Declared rows

Rows declared with `//ptah:schema:data` reach YDB through `ptah schema plan`,
`ptah schema apply`, `ptah migrations data` and the drift report. Each column's
type comes from the live table, because YDB cannot change a column's type, or,
for a table the same plan creates, from the type its declaration lands on for
the server's release line. A declared row is compared with the stored one in
the column's type, so `12.50` matches the `12.5` a `Decimal` stores and an
upper-case UUID matches the lower-case one YDB keeps: a converged table plans
nothing.

### Seeds

`ptah seed` creates the tracker `schema_seeds` at the database root, with the
seed path as its key. A seed file runs as the queries YQL reads it into, so a
`$name = ...` definition reaches the statements after it, and all of them run
in one serializable transaction with the row that records the seed. When YDB
aborts that transaction for a conflicting one (`Transaction locks
invalidated`), nothing of it is applied, and Ptah runs it again, up to five
times. A file that holds a schema statement, or a `BATCH UPDATE` or `BATCH
DELETE`, is refused before it runs, because YDB runs those only outside a
transaction; put them in a migration.

YDB has no savepoint, and a key conflict ends the transaction it happens in. So
`--idempotent` rolls the whole file back and records the seed in a transaction
of its own: as on the other engines, nothing of the file is applied and the seed
reads as applied. `--protected-table` is checked against the tables in the
connection's directory.

### Query builder

`core/query` renders SELECT, INSERT, UPDATE, DELETE and YDB's `UPSERT INTO`,
which `query.UpsertInto` builds: it writes each row over the row with the same
primary key, keeps the columns it does not name, and inserts the row where there
is none. Parameters are `$p1`, `$p2` and so on, and each argument is a
`sql.NamedArg` of that name, so a YDB connection binds it by name. A Go
`int64` binds as `Int64`, so a narrower column takes a value of its own Go type.
LIMIT and OFFSET bind as `Uint64`.

YQL has no `ON CONFLICT`, no `WITH` clause, no correlated subquery, no JOIN
condition other than equalities between the joined tables' columns, and no
OFFSET without a LIMIT. The builder refuses each before it renders anything,
with the capability key the target lacks, and `RETURNING` is refused on 25.1
and 25.2; see the [query builder](../../extend/query-builder/#dialect-coverage).

YDB 26.2 fails an `UPSERT` on a table with a unique index when the statement
names no column that a synchronous global index is keyed on. The server answers
`INTERNAL_ERROR` with
`verification=!hasUniqIndex || !usedIndexes.empty();fline=kqp_opt_phy_upsert_index.cpp:359`
and writes nothing. This is a defect in the server: 25.1 runs the same
statement. The column list is the caller's, and the builder does not know the
table's indexes, so it cannot refuse the statement. Name every column of the
table in such an `UPSERT`, or use `UPDATE` or `INSERT`, which 26.2 runs. Ptah
writes no such statement itself: declared rows are written with `INSERT`,
`UPDATE` and `DELETE`, and the tables Ptah keeps for migrations and seeds have no
secondary index.

## ptah-compat

No Atlas edition has a YDB driver, so YDB on `ptah-compat` is a Ptah
extension. The default profile takes a `ydb://` or `ydbs://` URL on every verb,
from a flag, its `PTAH_*` variable or `atlas.hcl`, with no variable to turn it
on. Each verb runs the native capability behind it: the planner and the writer
for `schema apply`, the reader for `schema inspect`, the migrator for the
`migrate` verbs, and the data layer for declared rows and `script`.

`migrate apply`, `status`, `set` and `down` keep the Atlas revision table
`atlas_schema_revisions`, at the database root or in the directory
`--revisions-schema` names. `schema apply` and `migrate apply` take their lock
as a semaphore on the coordination node `ptah_locks`, and `--lock-timeout`
bounds how long they wait for it.

`schema inspect` writes YDB's own type names in its HCL, which no Atlas binary
reads; Ptah reads it back. A type is written bare, `Decimal(22,9)` included, a
`Serial` column as its integer type with `auto_increment = true`, and an index's
kind as its `type`, such as `"GLOBAL SYNC"` or `"GLOBAL ASYNC"`. A directory is a
`schema` block, and a table at the database root, which has no name, carries no
`schema` attribute:

```hcl
schema "shop" {
}

table "users" {
  column "id" {
    type = Int64
    auto_increment = true
  }
  primary_key {
    columns = [column.id]
  }
}

table "orders" {
  schema = schema.shop
  column "total" {
    type = Decimal(22,9)
  }
}
```

The YDB driver reports a row count it did not measure, so a `script exec` or
`script loop` step reports its count as not reported, and `expect_rows` is
refused rather than judged against the number. A script spells parameters the
way YQL reads them, `$p1`, `$p2` and so on; `?` is not YQL.

A primary key change, a column type change and `SET NOT NULL` are refused
unless `PTAH_ALLOW_TABLE_REBUILD=1` asks `schema apply`, `schema diff` or
`schema plan new` for a [table rebuild](#table-rebuilds), and the refusal names
the variable. `migrate diff` reads it too, and plans with it once YDB can be its
dev database. A malformed value fails the run before it does anything, and
strict mode refuses the variable.

Verbs that need a dev database are refused until YDB can be one: `migrate
diff`, `migrate lint`, `migrate checkpoint`, `migrate validate --dev-url`,
`schema plan validate` and `schema apply --plan`. Every local-ydb server serves
the database `/local`, so two of them are refused as one database before that.

`PTAH_ATLAS_STRICT_COMPAT=1` reproduces the community binary, which answers
`sql/sqlclient: unknown driver "ydb". See: https://atlasgo.io/url` on every verb,
with `ydbs` for a `ydbs://` URL. Strict mode refuses a YDB URL in those words
wherever it comes from, a data source in `atlas.hcl` included; a `PTAH_*`
variable is refused as a variable. See
[Compatibility differences](../../atlas/retained-divergences/#a-ydb-database-url).

## Other commands

### Go models from a database

`ptah introspect --db-url ydb://...` writes the row tables as annotated Go
structs. A field keeps the column's YDB type name as its declared type, such as
`type="Uint64"` or `type="Timestamp64"`, which renders as the same type again.
A `Serial` column is written as its integer type with `auto_increment`, which
renders as the `Serial` type again. The models, planned against the database
they came from, plan nothing.

The Go type of a field is the type the YDB Go SDK's `database/sql` driver scans
the column into:

| YDB type | Go type |
| --- | --- |
| `Int8` to `Int64`, `Uint8` to `Uint64` | `int8` to `int64`, `uint8` to `uint64` |
| `Float`, `Double` | `float32`, `float64` |
| `Bool` | `bool` |
| `Utf8`, `Json`, `JsonDocument`, `Uuid`, `DyNumber` | `string` |
| `String`, `Yson` | `[]byte` |
| `Date`, `Datetime`, `Timestamp` and their 64-bit types | `time.Time` |
| `Interval`, `Interval64` | `time.Duration` |
| `Decimal(p,s)` | `types.Decimal` from `github.com/ydb-platform/ydb-go-sdk/v3/table/types` |

A nullable column is a pointer field, except a `[]byte` one. The SDK scans a
`Decimal` into its own `types.Decimal` only, and refuses a `string` or a
`float64`, so a package with a `Decimal` field imports the SDK.

YDB accepts a nullable key column, and no Ptah declaration can ask for one,
because Ptah writes `NOT NULL` on every YDB key column. `ptah introspect`
refuses such a column by name and writes nothing.

### Schema analysis

`ptah schema stats` counts the tables, columns and indexes the reader
describes, labeled with the dialect and the `--schemas` directories. Like
every other dialect, it counts objects and reads no row counts or sizes.

`ptah schema lineage --db-url ydb://...` traces views. YDB has no routines, so a
directory without views has nothing to trace. The reader does not read a view
yet, so a directory holding one is refused rather than reported as empty.

`ptah schema security --db-url ydb://...` is refused: the analysis reads the
access model, and Ptah does not read YDB users, groups and permissions yet.

`ptah viz --dialect ydb` draws a declared schema with its YDB types. With
`--security`, the rules run under the capabilities of the line
`--server-version` names, and a rule YDB gives nothing to check, such as the
row-level security rule, is listed as not checked.

### Agents, editors and CI

The tools of `ptah mcp` take `ydb` as a dialect and a YDB database as a
configured target. `render_schema` writes YQL, `validate_schema` reports what
YDB cannot store, and `read_database` reads a target's row tables. The two
inference tools refuse a YDB target before they connect.

The language server reads YDB models as it reads any other: YDB type names, a
`platform.ydb.<key>` override and `dialects="ydb"` draw no diagnostic.

The Ptah GitHub Action takes a YDB URL in `db-url` and `ydb` in `dialect`. The
plan, the safety report, the generated migration files and the lint of them run
as they do for any other database.

## What is not supported yet

These are refused with a message that names what is missing:

<!-- BEGIN GENERATED YDB GAPS -->
- a YQL file as the desired schema (Go structs and YAML schemas work);
- a scratch database for each case of `ptah migrations test` and `ptah schema test`, since YQL cannot create a database;
- a YDB database as a dev or shadow database;
- comments on tables, columns and indexes;
- views;
- users, groups and permissions;
- a table's own settings: TTL, partitioning, column families and changefeeds;
- vector, full-text, JSON and column-table indexes;
- `ptah inference` and the inference tools of `ptah mcp`, which wait for the vector index family.
<!-- END GENERATED YDB GAPS -->

The work is planned in [#4015](https://github.com/stokaro/ptah/issues/4015).

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
- URL forms for every engine: [Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/).
