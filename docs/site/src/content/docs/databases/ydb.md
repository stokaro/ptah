---
title: YDB
description: YDB in Ptah - connecting with ydb:// URLs, what renders and plans for row tables and views, type mappings, keys, defaults and indexes, users, groups and permissions, what each release line can do, linting YQL, seeds, declared rows and the query builder, and what is not supported yet.
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

Ptah renders YQL for YDB row tables, views, topics and the users, groups and
permissions of a database, plans a migration between two schemas,
connects to a live database, reads its tables back, applies DDL to it, runs
versioned migrations against it, lints YQL for it, and writes data to it:
seeds, declared rows and the statements the query builder renders. The
dialect name is `ydb`. YDB is its own dialect rather than a PostgreSQL-family
one: Ptah writes YQL and talks to the server through the YDB Go SDK. A schema
Ptah applies reads back as itself, which the integration suite checks in CI
against live YDB 26.2 and 25.1 servers. The nightly capability matrix runs the
same suite on each YDB line it probes.

`ptah-compat` takes a YDB URL on every verb; see [ptah-compat](#ptah-compat).
Inference and the YDB object families such as full-text and JSON
indexes are not supported yet. See
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
`prefetch_query_result_parts`, `monitoring`, which names the cluster's
monitoring endpoint (see [Feature flags](#feature-flags)), and `dev_realm`,
which Ptah writes when it gives a run a dev database of its own (see
[Dev, shadow and scratch databases](#dev-shadow-and-scratch-databases)). Any
other parameter is refused with the list of the accepted ones.

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
`.metadata`. Two names at the database root are Ptah's own and never part of a
schema it reads, plans or drops: the coordination node `ptah_locks` and the
directory `ptah_dev`.

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
| `VECTOR(n)` | `String`; see [Vector indexes](#vector-indexes) |

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
`XML`, ranges, `HALFVEC` and `SPARSEVEC`, and a bare `DECIMAL` with no
precision. Ptah never writes `Varchar`: YDB accepts the word and makes the
column bytes rather than text.

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
keeps what the index holds, as a table's does (see
[Table partitioning](#table-partitioning-read-replicas-and-key-bloom-filter)):
a new index takes it from YDB, which gives every new index the same settings,
split by size at 2048 MB, not by load, at least one partition, no maximum and
no read replicas. An index does not take its table's settings.

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
ALTER TABLE `orders` ALTER INDEX `orders_customer_ix` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 16);
```

No statement that creates an index takes the settings, so they are an
`ALTER INDEX` of their own, which runs as its own query and names the settings
the index declares.

A change of the settings is made in place with the same statement, and read
back from the index's implementation table. Setting one setting can reset
another: setting `AUTO_PARTITIONING_BY_LOAD` resets the minimum partition
count to 1, and setting `AUTO_PARTITIONING_BY_SIZE` resets the size and the
minimum. So a change names every splitting setting, the ones the index
declares and the held value of the rest. A partition size on an index that
does not split by size is refused, as YDB refuses it. Other dialects refuse an
index that declares its partitioning.

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

### Vector indexes

A vector index finds the rows whose vectors are nearest a given one. YDB keeps
a vector in a `String` column, as the bytes `Knn::ToBinaryStringFloat` writes,
and builds the index with `vector_kmeans_tree`. An index declares that method
as its type, and its settings with attributes named after YDB's:

| Attribute | Value |
| --- | --- |
| `distance` | `cosine`, `euclidean` or `manhattan` |
| `similarity` | `inner_product` or `cosine` |
| `vector_type` | `float`, `uint8`, `int8` or `bit` |
| `vector_dimension` | 1 to 16384 |
| `levels` | the depth of the tree, 1 to 16 |
| `clusters` | the clusters each level splits into, 2 to 2048 |

An index names one of `distance` and `similarity`, and every other setting.
`clusters` to the power of `levels` is at most 2^30. YDB 25.1 and 25.2 check
none of these limits and take an index without `levels` and `clusters`; 25.3
and later refuse such an index, so Ptah holds every line to their limits and an
index that applies today applies after an upgrade. The same keys work on an
index in a YAML schema. The columns before the vector column are a prefix,
which a search names in its `WHERE`, and `include` columns become `COVER`.

This field:

```go
//ptah:schema:field name="embedding" type="vector(1536)"
//ptah:schema:index name="docs_embedding_ix" fields="tenant,embedding" type="vector_kmeans_tree" distance="cosine" vector_type="float" vector_dimension="1536" levels="2" clusters="128"
Embedding []byte
```

renders inside its table's `CREATE TABLE` as:

```sql
INDEX `docs_embedding_ix` GLOBAL USING vector_kmeans_tree ON (`tenant`, `embedding`) WITH (distance=cosine, vector_type=float, vector_dimension=1536, levels=2, clusters=128)
```

`vector(n)` is the `String` column the index reads, and the index keeps the
dimension: YDB stores none for the column, and a vector of another length is
left out of the index without an error. So a column declared `vector(n)`
under an index of another `vector_dimension` is refused, and the dimension is
reported as dropped from the column, as a `VARCHAR` length is. The column
reads back as `String`, and a comparison reads a declared `vector(n)` and a
`String` as one type. A Utf8 or numeric column under a vector index is
refused, as YDB refuses it.

A declaration written for pgvector reads where it has a YDB meaning: the
operator classes `vector_cosine_ops`, `vector_l2_ops`, `vector_l1_ops` and
`vector_ip_ops` name the metric `distance=cosine`, `distance=euclidean`,
`distance=manhattan` and `similarity=inner_product`. The methods `hnsw` and
`ivfflat` are refused with `vector_kmeans_tree` named in their place, their
storage parameters `m`, `ef_construction` and `lists` are refused by the method
they belong to, and so are the other pgvector operator classes. A vector index
is not unique, and it keeps the partitioning YDB gives it: `ALTER INDEX ...
SET` on one answers `Only index with one impl table is supported`.

No setting changes in place, so an index whose settings, columns or prefix
change is dropped and added again, after the column it reads exists and
before any column it reads is dropped. A renamed index keeps its settings and
is renamed in place.

What a line does with a vector index:

| Line | Vector indexes |
| --- | --- |
| 26.1, 26.2 | built and kept current by every write; bit vectors |
| 25.3, 25.4 | built and kept current by every write; no bit vectors |
| 25.2 | built; a row written after the build is not found through the index |
| 25.1 | behind `EnableVectorIndex`, off by default; as 25.2 with the flag on |

The keys are `vector_indexes`, `vector_index_maintained_on_write` and
`vector_bit_type`. On a line without `vector_index_maintained_on_write`, a
search through the index answers as the table stood when the index was built,
while a full scan sees every row; `YD131` warns about a migration that writes
rows into such a table. A search through the index is approximate, and names
it with `VIEW`:

```sql
SELECT id FROM docs VIEW docs_embedding_ix
WHERE tenant = 1
ORDER BY Knn::CosineDistance(embedding, $target)
LIMIT 10;
```

Other dialects refuse an index that declares vector settings.

### Serial columns and their sequences

A Serial column fills itself from a sequence YDB creates with the column, at
`<table>/_serial_column_<column>`. `SERIAL`, `BIGSERIAL` and `SMALLSERIAL` are
Serial columns, and so is an integer column with `auto_increment`. The sequence
starts at 1 and steps by 1 unless the column declares `identity_start` or
`identity_increment`, in a Go annotation or a YAML schema:

```go
//ptah:schema:field name="id" type="BIGSERIAL" primary="true" identity_start="1000" identity_increment="10"
```

YDB sets both with `ALTER SEQUENCE` and nothing else, and that statement takes
the sequence only by its absolute path, which begins with the database's own.
So a plan made against a database writes it, after the table's `CREATE TABLE`:

```sql
ALTER SEQUENCE `/local/orders/_serial_column_id` START WITH 1000 INCREMENT BY 10 RESTART WITH 1000;
```

`START` alone changes the start YDB records and leaves the next value where it
was, even on a new sequence, so a new table's sequence is restarted at the
declared start. A sequence of a table that exists is never restarted, because a
restart onto a value a row holds fails the next insert with `Conflict with
existing key`. A changed start there is recorded and moves no value; a changed
increment applies from the next value on.

`ptah schema render` has no database to name, so it writes the table without
the statement and reports the start and the increment as skipped. A migration
file names the database it was planned against, and applying it to a database
at another path fails with `Path does not exist`.

A plan refuses a change to a sequence that YDB would turn against the table:

- a start or an increment on a 16-bit or 32-bit Serial. Any `ALTER SEQUENCE`
  raises its maximum to the Int64 maximum, and the column then stores the value
  after 32767 or 2147483647 as a negative number without an error. A
  `BIGSERIAL` has no such limit to lose. The `serial_sequence_keeps_range` key
  is false on every YDB line;
- a change to a sequence that was restarted, including the one of a table
  created with a start other than 1. YDB replays the last restart on every
  later `ALTER SEQUENCE`, so the next insert takes that value again and
  collides with the row that holds it.

`identity_options` and `GENERATED ALWAYS` are refused, and so is a start or an
increment below 1. The read reports a sequence's start, increment and last
restart. `YD107` and `YD108` in `ptah migrations lint` report the same traps in
a migration written by hand.

## Views

A view renders with the security clause YDB requires on every view:

```sql
CREATE VIEW `shop/active_users` WITH (security_invoker = TRUE) AS
SELECT id, email FROM `shop/users` WHERE deleted_at IS NULL
;
```

YDB runs a view's query with the rights of the user who reads the view, and
refuses a view without `security_invoker = TRUE`. Ptah writes the clause on
every view and takes no other `WITH` attribute. A view's name follows the
table rules: a view in the schema `shop` is the path `shop/active_users`, and
creating it creates the directory.

YDB resolves the names in a view's query from the database root, not from the
view's directory. Name a table by its whole path, `` `shop/users` `` rather than
`users`.

YDB stores a view's query in its own form: comments are dropped, and the
tokens are joined by single spaces. Ptah reads the declared query into the same
form before it compares the two, so a view applied once plans nothing, and a
change of case is a change. A `PRAGMA` that ran before the `CREATE VIEW` in the
same query, such as `TablePathPrefix`, is stored with the view and changes what
its names mean. Such a view does not match a declaration of the query alone,
and a plan replaces it once.

YDB has no `CREATE OR REPLACE VIEW` and no `ALTER VIEW`, which the
`create_or_replace_view` key records. A view whose query changed is dropped and
created again, so for a moment the view does not exist.

A view's `WITH CHECK OPTION` is refused: a YDB view cannot be written through,
and YDB reads the words after the query as a table hint that checks nothing. A
comment on a view waits for comment support.

YDB records no dependency on a view. It drops a table or a view that another
view reads, and the reading view fails from then on. A plan drops views before
anything else and creates them after every table change, and lint rule `YD106`
reports a migration that drops a table a view still reads.

## Users, groups and permissions

A role is a YDB user, and a role declared with `group="true"` is a group.
`member_of` names the groups a user or a group joins. A grant is on the
database (`on_database="true"`), a directory (`on_schema`) or a table
(`on_table`), and names YDB permissions, by name or as `GRANT` spells them:

```go
//ptah:schema:role name="readers" group="true"
//ptah:schema:role name="app" login="true" password="Secret1!" member_of="readers,DATA-READERS"
//ptah:schema:grant role="readers" privilege="SELECT ROW,ydb.generic.list" on_table="shop.orders"
//ptah:schema:grant role="app" privilege="CONNECT" on_database="true"
type Access struct{}
```

renders as:

```sql
CREATE GROUP `readers`;
CREATE USER `app` PASSWORD 'Secret1!';
ALTER GROUP `readers` ADD USER `app`;
ALTER GROUP `DATA-READERS` ADD USER `app`;
GRANT 'ydb.granular.select_row', 'ydb.generic.list' ON `shop/orders` TO `readers`;
GRANT 'ydb.database.connect' ON `/local` TO `app`;
```

A user that does not log in is `CREATE USER ... NOLOGIN`. A password is
written as declared, under a warning that it sits in the file in plain text;
a password hash, the JSON object YDB keeps, is written with `HASH` instead.
The read never asks for a password, so a declared password is set once and
never compared. A name holds lower-case letters and digits only, as YDB
requires. `superuser`, `createdb`, `createrole`, `replication` and
`with_option` are refused: YDB keeps the grant option as the permission
`ydb.access.grant`, which a grant names instead. YDB has no `PUBLIC` and no
default privileges; a permission on a directory is inherited by every object
created in it. A grant on a view is refused, because Ptah does not read a
view's permissions yet; grant on the view's directory instead.

A grant names its object by a path relative to the database root, so a
migration applies to a database of another name. A grant on the database
itself, and on 25.1 to 25.4 a grant on a table or a directory at the database
root, takes an absolute path, which a plan reads from the database it runs
against and `ptah schema render` cannot know; such a render is refused.

The read reports every user and group from `.sys/auth_users`, `.sys/auth_groups`
and `.sys/auth_group_members`, and the permissions and the owner of the
database, each directory and each table the read covers. A description leaves
out, while knowing they exist, the principals no migration run there could have
made: the cluster's own groups, such as `ADMINS` and `DATA-READERS`, whose names
no declaration could create; the database's owner, such as local-ydb's `root`,
which exists before any migration runs; and every user and group when the read
is of a dev realm, which shares them with its database. Reading the users and
groups needs a connection that may read rows of the database; one that may only
list it, such as a member of `METADATA-READERS` alone, reads the rest of the
database and plans no user or group.

A plan removes a membership only when the schema declares both its group and
its member, so a user keeps the cluster's `USERS` group, which grants the right
to connect. It grants a rebuilt table the permissions the old table held.
[Planning changes](#planning-changes) says where each statement goes.

## TTL

A table's TTL is its row deletion policy: YDB deletes a row once an interval
has passed since the time one of its columns holds. A table declares it with
three attributes:

| Attribute | Value |
| --- | --- |
| `row_deletion_column` | the column the interval is measured from |
| `row_deletion_interval` | an ISO 8601 duration such as `P30D` or `PT1H30M` |
| `row_deletion_unit` | for an integer column, the unit it counts since 1970 |

This table:

```go
//ptah:schema:table name="events" row_deletion_column="created_at" row_deletion_interval="P30D"
```

renders as:

```sql
CREATE TABLE `events` (
    `id` Int64 NOT NULL,
    `created_at` Timestamp64,
    PRIMARY KEY (`id`)
) WITH (TTL = Interval("P30D") ON `created_at`);
```

The same keys work on a table in a YAML schema. The column is either a date or
time column (`Date`, `Datetime`, `Timestamp` or their 64-bit forms) and names
no unit, or a `Uint32`, `Uint64` or `DyNumber` column that counts `SECONDS`,
`MILLISECONDS`, `MICROSECONDS` or `NANOSECONDS` since the Unix epoch. YDB
refuses any other column type, and so does Ptah, before anything runs.

The interval takes weeks, days, hours, minutes and seconds. YDB keeps it as a
whole number of seconds and drops the rest in silence, so an interval with a
fraction of a second is refused. Months and years have no fixed length and are
not intervals. The comparison reads both sides as seconds: `PT720H` and `P30D`
are one TTL, and a TTL reads back in the form YDB shows, `P30D`.

On a table that exists, `ALTER TABLE ... SET (TTL = ...)` adds or replaces the
TTL, and `ALTER TABLE ... RESET (TTL)` removes it. YDB refuses to drop the
column a TTL reads, so a plan changes the TTL after it adds columns and before
it drops them.

A TTL tier that moves rows to an external data source is for column-oriented
tables only, and a row table refuses it. A run interval set with
`ydb table ttl set --run-interval` has no YQL spelling, and
`SET (TTL = ...)` resets it to YDB's default. Ptah reads such a table's TTL,
records its run interval as not described, and refuses a change to the TTL that
would reset it. Removing the TTL removes the run interval with it.

HCL and DBML have no spelling for a TTL. `schema inspect` warns about each TTL
it leaves out of an HCL document, on `ptah` and `ptah-compat` alike. A desired
state read from either format keeps every table's TTL as the database holds it,
rather than reading its silence as a request to remove it, and a table it
rebuilds gets that TTL on the new table.

Spanner takes the same attributes, with its own interval spelling (`30 days`)
and no unit. Every other dialect refuses a row deletion policy.

## Changefeeds

A changefeed is YDB's stream of a row table's changes, kept in a topic at
`<table>/<changefeed>` that readers read through consumers. A table declares
one with `//ptah:schema:changefeed`, on its struct or on a holder field naming
the table with `table` in the same file, and a consumer of its topic with
`//ptah:schema:changefeed:consumer`:

```go
//ptah:schema:table name="orders"
//ptah:schema:changefeed name="updates" mode="NEW_AND_OLD_IMAGES" format="JSON" retention_period="PT12H" initial_scan
//ptah:schema:changefeed:consumer changefeed="updates" name="billing" important
type Order struct {
	//ptah:schema:field name="id" type="BIGINT UNSIGNED" primary="true"
	ID uint64
}
```

YDB adds a changefeed only to a table that exists, one per statement, so it
follows the table's `CREATE TABLE`:

```sql
ALTER TABLE `orders` ADD CHANGEFEED `updates` WITH (MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'JSON', RETENTION_PERIOD = Interval('PT12H'), INITIAL_SCAN = TRUE);
ALTER TOPIC `orders/updates` ADD CONSUMER `billing` WITH (important = TRUE);
```

The [annotation reference](../../reference/go-annotations/#ptahschemachangefeed)
lists every attribute, and a YAML table takes the same keys under its
`changefeeds` map. An interval is an ISO 8601 duration such as `PT12H`. YDB
keeps whole seconds and drops a fraction without a word (`PT0.5S` resolved
timestamps read back as none), so a fraction is refused. So is what YDB refuses
on every line:

- `DEBEZIUM_JSON` in `UPDATES` mode, or with virtual timestamps, resolved
  timestamps or schema changes;
- `DYNAMODB_STREAMS_JSON`, which YDB writes only for a document table;
- a topic starting with several partitions on a first key column that is not
  `Uint32` or `Uint64`;
- a changefeed named like one of the table's indexes.

A server with default settings holds a table to five changefeeds. Options that
came in later releases each need a key: `user_sids` (26.1), `schema_changes`
(25.3), `topic_auto_partitioning` (a flag on 25.1) and a consumer's
`availability_period` (25.4). Other dialects refuse a table that declares a
changefeed.

### Changing a changefeed

YDB changes a changefeed's retention and consumers in place, through
`ALTER TOPIC` on its path, and nothing else about it. A retention left out is
set to 24 hours, because `RESET` changes nothing on 26.2 and is refused on
25.1. A consumer that is to take any codec again is dropped and added, since
YDB keeps a consumer's codecs once set.

Any other change drops the changefeed and adds it again (`MODE alter is not
supported`). **The stream restarts: the records nobody read are lost, and every
consumer starts again from the beginning of the new stream.** The plan says so
above the statements. YDB has no statement that disables a changefeed (`ALTER
CHANGEFEED ... DISABLE` answers `Name not found: quote`), so one the server
disabled is added again too.

`DROP TABLE` drops a table's changefeeds with it. YDB refuses to rename a table
that carries one (`Cannot move table with cdc streams`, lint rule `YD109`), and
on 26.2 to `TRUNCATE` it. The consumers of a changefeed's topic belong to the
changefeed, which creates and drops the topic; a topic made with `CREATE
TOPIC` is a different object, described under [Topics](#topics).

## Topics

A topic is YDB's persistent message queue: a path that writers append to and
named consumers read from, each consumer with a read position of its own. Ptah
declares, reads, plans and applies topics and their consumers under the
capability key `topics`, which only the YDB presets carry, and every other
target refuses a declared topic by name. A changefeed's topic lives under its
table and belongs to the changefeed, so it is not declared as a topic.

Declare a topic on a struct, and its consumers in the same file:

```go
//ptah:schema:topic name="order_events" schema="app" min_active_partitions="2" retention_period="PT36H" supported_codecs="raw,gzip"
//ptah:schema:topic:consumer name="billing" topic="order_events" schema="app" important="true"
//ptah:schema:topic:consumer name="audit" topic="order_events" schema="app" read_from="2026-01-01T00:00:00Z"
type OrderEvents struct{}
```

The same topic in a YAML schema:

```yaml
topics:
  order_events:
    schema: app
    min_active_partitions: 2
    retention_period: PT36H
    supported_codecs: [raw, gzip]
    consumers:
      billing: { important: true }
      audit: { read_from: "2026-01-01T00:00:00Z" }
```

The attributes carry the YQL setting names, and the
[annotation reference](../../reference/go-annotations/#ptahschematopic) lists
them. A setting the declaration leaves out stands for the value YDB gives a new
topic: one partition, auto-partitioning disabled, a retention of 24 hours, a
write speed of 1 MiB per second per partition, a burst equal to the write speed,
and no codec list. A topic created with a strategy splits a partition at 90% of
its write speed, merges at 30%, and waits five minutes before either.

A declaration YDB would accept and keep as something else is refused when it is
read:

- `max_active_partitions`, a utilization threshold or the stabilization window
  on a topic whose `auto_partitioning_strategy` is absent or `disabled`. YDB
  keeps none of them, and `max_active_partitions = 5` reads back as 1.
- a codec other than `raw`, `gzip`, `lzop`, `zstd` and `custom`. YDB keeps a
  list that names one as no list at all.
- a retention period or a window with a fraction of a second, which YDB drops,
  and a `max_active_partitions` below `min_active_partitions`.
- an `important` consumer with an `availability_period`, which YDB refuses.
- a consumer's `availability_period` on a line without the
  `topic_consumer_availability_period` key, which YDB 25.4 and later have.

A plan changes a topic with one `ALTER TOPIC` that names every setting, not
only the changed ones, because YDB keeps the value a topic was created with for
a setting a statement leaves out: the burst does not follow a write speed
changed later, and a topic given a strategy after it was created splits at 80%
and merges at 20%. The same statement drops, changes and adds consumers. YDB
cannot empty a consumer's codec list (`unknown codec found or codecs list is
malformed`), so a consumer whose list the declaration removes is dropped and
added again, and starts reading from the beginning. A plan refuses the changes
YDB refuses in place:

- fewer partitions (`Invalid total groups count specified`). Drop the topic and
  create it again to lower the count.
- auto-partitioning disabled once it is on (`Can't disable auto partitioning.`).
  Declare `paused` to stop it.

A dropped topic loses every message it holds, and a dropped or re-added consumer
loses its read position, so the safety report classifies each as destructive.

The read describes each topic with `DescribeTopic`. A topic that holds a setting
Ptah does not model, such as a storage limit, a partition count limit, a
metering mode, a read speed quota or a shared consumer, is refused rather than
read without it. YQL sets none of them outside a serverless database. Atlas HCL
has no block for a topic, so a plan from an HCL document leaves every topic
alone, and an HCL export reports each topic it leaves out.

## Column families

A column family is a group of a row table's columns that YDB stores together,
with a storage pool, a compression and a cache mode of their own. Every row
table has the family `default`, which holds the key and every column no other
family names. A table declares its families with `//ptah:schema:columnfamily`,
on its struct or on a holder field naming the table with `table` in the same
file:

```go
//ptah:schema:table name="documents"
//ptah:schema:columnfamily name="default" compression="lz4"
//ptah:schema:columnfamily name="cold" data="hdd" compression="lz4" fields="body,attachment"
type Document struct {
	//ptah:schema:field name="id" type="BIGINT UNSIGNED" primary="true"
	ID uint64
	//ptah:schema:field name="title" type="TEXT"
	Title string
	//ptah:schema:field name="body" type="TEXT"
	Body string
	//ptah:schema:field name="attachment" type="BYTEA"
	Attachment []byte
}
```

renders as:

```sql
CREATE TABLE `documents` (
    `id` Uint64 NOT NULL,
    `title` Utf8,
    `body` Utf8 FAMILY `cold`,
    `attachment` String FAMILY `cold`,
    PRIMARY KEY (`id`),
    FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'lz4'),
    FAMILY `default` (COMPRESSION = 'lz4')
);
```

| Attribute | Value |
| --- | --- |
| `name` | the family's name; `default` sets the default family |
| `data` | the kind of storage pool, such as `ssd` or `hdd` |
| `compression` | `off` or `lz4` |
| `cache_mode` | `regular` or `in_memory` |
| `fields` | the columns the family holds |

A YAML table takes the same keys under its `column_families` map, with `fields`
as a list. A column names its family right after its type, the only place 25.1
takes it. Measured on 25.1 and 26.2:

- Which pool kinds exist is the database's own configuration, so Ptah cannot
  check one before the statement runs. local-ydb has only `hdd`, and answers
  any other kind with `database doesn't have required storage pools`.
- A row table compresses with `off` or `lz4`. `zstd` and a compression level are
  for column-oriented tables (`Unsupported compression value 3`, `Field
  COMPRESSION_LEVEL is not supported for OLTP tables`), and Ptah refuses them
  where they are written.
- A key column stays in the default family (`Key column 'id' must belong to the
  default family`), and the default family lists no columns.
- `cache_mode` needs the key `column_family_cache_mode`: 25.4 and later take
  it, 25.3 takes it with the `EnableTableCacheModes` flag on, and older lines
  answer `Unknown table setting: CACHE_MODE`.
- A row table has no compression or encoding per column: 26.2 answers `Column
  Compression is not supported in row tables. Use COLUMN FAMILY.` and `Column
  encoding is supported only for column tables`, and 25.1 does not parse
  either. A family is how a row table compresses.

### Settings a declaration leaves out

A new table's families take settings from the cluster's table profile too, so
YDB's documented defaults are not what every cluster gives. Measured on 25.1 and
26.2 with a dynamic configuration whose default storage policy names families:
a codec on family 0 compresses the default family of every new table,
`column_cache: ColumnCacheEver` turns its `keep_in_memory` on, and
`column_cache_mode` gives it a cache mode on 26.2. A family with an id and a
name in the policy is added to every new table, holding no column. A setting
the statement names wins over the profile, and `ALTER TABLE` keeps what the
profile set.

So a setting the declaration leaves out means "keep what the table holds",
never YDB's documented default:

- A family setting the declaration does not state is neither compared nor
  written. State `compression="off"` or `cache_mode="regular"` to hold a family
  to that value.
- A family the table holds and the declaration does not name stays, with its
  settings. YQL could not drop it anyway, and a profile would give it back to
  a new table.
- Where a statement has to name the whole table, the `CREATE TABLE` of a
  [rebuild](#table-rebuilds), each setting the declaration leaves out takes the
  value the table holds, and each family the table holds is written.

Where each column sits is the declaration's own: a profile places no column,
and a column the declaration puts in no family moves to the default family. A
read reports every setting at the value the table holds, YDB's own included,
and DescribeTable names the family of every column outside the default one,
which is how a read lists each family's columns.

### Changing column families

On a table that exists, one `ALTER TABLE` per table adds the families the
declaration names and the table lacks (`ADD FAMILY`), sets each setting the
declaration states and the table holds otherwise (`ALTER FAMILY ... SET DATA`,
`SET COMPRESSION`, `SET CACHE_MODE`), and moves each column whose family
differs (`ALTER COLUMN ... SET FAMILY`). It runs after the columns the plan
adds, so a new column can move into its family, and before the columns it
drops. Setting one family setting resets no other.

YQL has no `DROP FAMILY` and resets no family setting, and no family change
needs a rebuild, since what the declaration leaves out stays. A rollback of a
change that added a family or gave one a storage pool moves the columns back
and keeps the family and the pool.

YDB does not refuse a statement that names a family the table does not have: it
creates the family with its own settings. Ptah adds a family before it moves a
column into it, and `ptah migrations lint` reports a hand-written statement
that names a family the directory never declared (`YD119`).

HCL and DBML have no spelling for a column family. `schema inspect` warns about
each table whose families it leaves out of an HCL document, and a desired state
read from either format keeps the families the database holds, through a
rebuild too.

`keep_in_memory` has no YQL spelling, and with default flags YDB refuses it
through the table service too (`Setting keep_in_memory to ENABLED is not
allowed`), but a table profile's `column_cache` sets it. A read reports it, no
statement Ptah writes changes it, and a [rebuild](#table-rebuilds) of a table
holding it is refused, since the new table's `CREATE TABLE` cannot say it.

Other dialects refuse a table that declares a column family. CockroachDB's
`FAMILY` clause groups columns with no settings of their own, and Ptah models
none.

## Coordination nodes

A coordination node holds an application's semaphores, which serve as
distributed locks, and its rate limiter resources. Ptah declares, reads,
creates, changes and drops a node and its configuration. The semaphores and the
resources inside a node belong to the application. A Go annotation declares one:

```go
//ptah:schema:coordinationnode name="locks" schema="app" self_check_period="PT2S" read_consistency_mode="strict"
type Locks struct{}
```

So does the `coordination_nodes` key of a YAML schema, with the same settings:

```yaml
coordination_nodes:
  locks:
    schema: app
    self_check_period: PT2S
    read_consistency_mode: strict
```

| Setting | Value | YDB's default |
| --- | --- | --- |
| `self_check_period` | how often the node checks that it is alive, from `PT0.5S` to `PT10S` | `PT1S` |
| `session_grace_period` | how long a session keeps its semaphores while the node changes its leader, from the self-check period plus `PT1S` to `PT30S` | `PT10S` |
| `read_consistency_mode` | `strict` or `relaxed` | `relaxed` |
| `attach_consistency_mode` | `strict` or `relaxed` | `strict` |
| `rate_limiter_counters_mode` | `aggregated` or `detailed` | `aggregated` |

A setting left out takes YDB's default. YDB stores only the settings a node was
given, so the comparison fills in the defaults on both sides: a declaration that
names a default and a node that never had the setting are the same node. YDB
stores a period outside its range and runs the node with the period moved into
the range, so Ptah refuses such a period where it is written.

YQL has no statement for a coordination node: `CREATE COORDINATION NODE` is a
parse error, and YDB creates, changes and drops one through its coordination
service. So Ptah writes a statement of its own, and Ptah's YDB connection runs
it through that service instead of sending it to the server:

```sql
CREATE COORDINATION NODE `app/locks` WITH (self_check_period = Interval('PT2S'), read_consistency_mode = 'strict');
ALTER COORDINATION NODE `app/locks` SET (read_consistency_mode = 'relaxed');
DROP COORDINATION NODE `app/locks`;
```

A plan, a plan file and a migration file carry these statements as text, and
`ptah migrations up` runs and records them like any other schema statement.
Only Ptah runs them: another client, `ydb sql` included, answers with a parse
error. The connection refuses one inside a transaction, beside another
statement in one query, and for a node that already exists, because the
service answers a second creation with success and keeps the node as it was.
In a [dev realm](#dev-shadow-and-scratch-databases) a relative path names a node
under the realm, as it names a table there.

A change names only the settings that differ, and YDB keeps every setting a
change leaves out. A node the declaration does not name is dropped with its
semaphores and rate limiter resources, and YDB drops it even while a session
holds a lock on it. The safety report counts such a drop as destructive, and
`DS107` in `ptah migrations lint` reports it, as it reports a dropped topic, so
`ptah migrations up` stops before it by default. A schema that leaves the nodes
to the application declares
`//ptah:schema:notdescribed kind="coordination_node"`.

The node `ptah_locks` at the database root is Ptah's own lock. A declaration
that names it is refused, and so is a statement that does.

## Table partitioning, read replicas and key bloom filter

A row table declares how YDB splits it into partitions, its read replicas and
its key bloom filter, with table attributes named after the YDB settings, in
lower case. The same keys work on a table in a YAML schema.

| Attribute | Value |
| --- | --- |
| `auto_partitioning_by_size` | `ENABLED` or `DISABLED` |
| `auto_partitioning_partition_size_mb` | megabytes, at least 1 |
| `auto_partitioning_by_load` | `ENABLED` or `DISABLED` |
| `auto_partitioning_min_partitions_count` | at least 1 |
| `auto_partitioning_max_partitions_count` | at least 1 |
| `read_replicas_settings` | `PER_AZ:<n>` or `ANY_AZ:<n>` |
| `key_bloom_filter` | `ENABLED` or `DISABLED` |
| `uniform_partitions` | at least 1 |
| `partition_at_keys` | `10, 20` or `(10, 'a'), (20)` |

A setting a table leaves out keeps what the table holds. Ptah never changes
it, and removing a setting from the declaration changes nothing on the
server. A new table takes it from the cluster's table profile, which need not
match YDB's documentation: a cluster whose dynamic configuration replaces the
default one creates tables that do not split by size, where a default cluster
splits them at 2048 MB. To return a setting to a value, declare the value, for
example `auto_partitioning_by_size="ENABLED"` or
`auto_partitioning_min_partitions_count="1"`. To remove read replicas,
declare `read_replicas_settings="PER_AZ:0"`, and to remove the key bloom
filter, `key_bloom_filter="DISABLED"`. YDB cannot remove a maximum partition
count (`Can't set max partition count to 0`, and no `RESET`), so no
declaration does either.

This table:

```go
//ptah:schema:table name="events" auto_partitioning_by_load="ENABLED" auto_partitioning_max_partitions_count="64" key_bloom_filter="ENABLED" uniform_partitions="8"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT UNSIGNED" primary="true"
	ID uint64
}
```

renders as:

```sql
CREATE TABLE `events` (
    `id` Uint64 NOT NULL,
    PRIMARY KEY (`id`)
) WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 64, KEY_BLOOM_FILTER = ENABLED, UNIFORM_PARTITIONS = 8);
```

A change of the settings is one `ALTER TABLE ... SET (...)`. Setting one
setting can reset another: setting `AUTO_PARTITIONING_BY_LOAD` resets the
minimum partition count to 1, and setting `AUTO_PARTITIONING_BY_SIZE` resets
the partition size to 2048 MB and the minimum to 1. So the statement names
every splitting setting whenever it changes one: the declared ones, and the
held value of the rest. Read replicas and the key bloom filter reset nothing,
and are named alone. YDB refuses `RESET` for every setting.

`uniform_partitions` and `partition_at_keys` give a new table the partitions
it starts with. `uniform_partitions` splits a `Uint32` or `Uint64` first key
column evenly. `partition_at_keys` names the split points: a value of the first
key column, or a parenthesized list of values of the leading key columns. A
value is a number for an integer column or a string for a text column, because
YDB takes no other literal there. YDB takes both settings only in
`CREATE TABLE` and keeps no record of them, only the minimum partition count
they set: the number of partitions they create, unless the table declares its
own minimum. A comparison sees a starting layout through that minimum.

A starting layout on a table that does not hold the minimum it gives is a
change YDB cannot make in place. It is refused, and planned as a
[table rebuild](#table-rebuilds) with `--allow-table-rebuild`. To change the
minimum in place, declare `auto_partitioning_min_partitions_count`. A starting
layout beside a declared minimum leaves a comparison nothing to see, so it
takes effect only when the table is created.

HCL and DBML cannot spell these settings, so a desired state in either format
leaves each one out and the table keeps what it holds. `ptah-compat schema
inspect` warns about each table it leaves them out of. Other dialects refuse a
table that declares them.

## Planning changes

YDB changes a table in place less than the SQL engines do, and runs a schema
statement outside any transaction. A plan therefore refuses what the server
cannot do before it emits anything, and orders what it emits so that no
statement needs one that has not run yet:

1. Drop the views the plan removes or replaces, a view before the view it
   reads.
2. Revoke the permissions and remove the memberships the plan takes away, then
   create and change users and groups.
3. Drop the removed topics, then the removed coordination nodes, so a table
   created at one's path finds it free.
4. Create the added tables, with their indexes and changefeeds, each followed
   by the `ALTER SEQUENCE` that gives a Serial column its declared start and
   increment.
5. Drop the indexes the plan removes, before any column they name. YDB refuses
   to drop an indexed or a covered column.
6. Rename the indexes the declaration renames, then change the partitioning of
   the indexes that keep their definition.
7. Per table: add columns, then change columns in place, then set or reset
   the TTL, then change the column families, then change the table's
   partitioning, read replicas and key bloom filter, then drop columns. YDB
   refuses to drop the column a TTL reads.
8. Change the start and the increment of the Serial columns of existing tables.
9. Add the new indexes of existing tables.
10. Per table: drop changefeeds, then add changefeeds with their consumers,
    then change topics in place. Drops come first, so a table that swaps one
    changefeed for another stays within YDB's limit.
11. Drop the removed tables.
12. Create the added topics, then change the changed ones, then create and
    change coordination nodes, so an object created at a dropped table's path
    finds it free.
13. Create the added and replaced views, a view after the view it reads. YDB
    checks a view's query against the schema when it creates the view.
14. Add memberships and grants, once the tables they name exist.
15. Drop the removed users and groups, after revoking what they hold:
    `DROP USER` leaves its permissions behind, and a user created later under
    the name would hold them.

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
changes as a rebuild of the table, and a
[starting layout](#table-partitioning-read-replicas-and-key-bloom-filter) YDB
gives only a new table. `ptah-compat` takes no flag the Atlas
community CLI lacks, so its `schema apply`, `schema diff` and `schema plan new`
ask with the variable `PTAH_ALLOW_TABLE_REBUILD=1` instead (see
[ptah-compat](#ptah-compat)). The rebuild is the same:

1. `CREATE TABLE` a scratch table, `__ptah_rebuild_<table>`, from the
   declaration, with its indexes, its column families and its TTL inside it.
   Each family the old table holds is written, with each setting the
   declaration leaves out at the value the old table holds. Every
   partitioning, read replica and key bloom filter setting is named in `WITH`,
   and an `ALTER INDEX ... SET` follows for each index: a setting the
   declaration names takes the declared value and every other one the value
   the old table or index holds, so the rebuild changes no setting nobody
   declared, since a new table would otherwise take the cluster's table
   profile.
2. `INSERT INTO` the scratch table `SELECT` the old rows, converting each
   changed column.
3. `ALTER TABLE` the old table `DROP CHANGEFEED`, for each changefeed it
   carries, since YDB moves no table that carries one.
4. `ALTER TABLE` the old table `RENAME TO __ptah_replaced_<table>`.
5. `ALTER TABLE` the scratch table `RENAME TO` the table's name.
6. `ALTER TABLE` the table `ADD CHANGEFEED`, for each changefeed the
   declaration names, with its consumers. A declaration that cannot name one,
   in HCL or DBML, keeps the database's.
7. `DROP TABLE` the renamed old table.

YDB has no transactional DDL, and YQL has no statement that swaps two tables
at once, so the steps are not atomic. **Rows written to the table between the
copy and the swap are lost, and YDB has no lock to stop them**: stop writing to
the table until the last step has run. The plan says so in a comment above the
steps. The old rows stay until step 7, and the table's name is free only between
steps 4 and 5. A changefeed's stream restarts across steps 3 to 6, and the plan
says that too.

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
one with `migrations up --allow-dirty`. `ptah migrations lint` reads the five
steps as a rebuild, so it does not report the final `DROP TABLE` as a lost
table; it does when the copy leaves out a column the directory's earlier
migrations gave the table.

Even with the flag, a rebuild is refused when it would damage the table:

- a table with a Serial column. The new table's sequence would start at 1 while
  the copied rows keep their values, so the next insert would collide, and YDB's
  `ALTER SEQUENCE ... RESTART WITH` takes only a literal;
- a table whose column family keeps its columns in memory with
  `keep_in_memory`, which no `CREATE TABLE` can say;
- a table carrying a setting Ptah does not model yet: a TTL run interval, a
  column family holding a setting Ptah does not read, storage settings such as
  external blobs, or a changefeed holding a setting Ptah does not read.
  Recreating the table would drop them.

## What each release line does

Ptah measures six YDB release lines. Each has a capability preset, and a
server's `SELECT Version()` selects it, whether the server reports `26.2.1.14`
or `stable-25-4-1`:

| Preset | Lines | Compared with the line above, lacks |
| --- | --- | --- |
| `YDB262` | 26.2 | — |
| `YDB261` | 26.1 | `SET DEFAULT` and `DROP DEFAULT` on an existing column |
| `YDB254` | 25.4 | a column added with a default, a changefeed's `USER_SIDS`, a `GRANT` on a root object by its relative name, bit vectors in a vector index |
| `YDB253` | 25.3 | a consumer's `availability_period`, a family's `CACHE_MODE` |
| `YDB252` | 25.2 | a `JsonDocument` or `DyNumber` default, `UPDATE ... RETURNING` on a table with a unique index, a changefeed's `SCHEMA_CHANGES`, a vector index that takes in the rows written after its build |
| `YDB251` | 25.1 | the 64-bit date and time types, `Decimal` precision other than 22,9, an `Int16` or `Uint16` default, an auto-partitioned changefeed topic, vector indexes |

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
| `EnableTopicAutopartitioningForCDC` | `changefeed_topic_auto_partitioning` |
| `EnableVectorIndex` | `vector_indexes` |
| `EnableTableCacheModes` | `column_family_cache_mode` |

`EnableAsyncIndexes` decides no capability: a cluster with the flag off still
builds a `GLOBAL ASYNC` index, so `async_indexes` keeps the preset's answer.

`EnableViews` is not read. Turned off on 25.1, it refuses every view statement
with `Views are disabled`; 26.2 creates and reads views with it off, so the flag
does not say whether a cluster has views.

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
`Serial` columns with their sequence's start, increment and last restart,
primary key, TTL, column families with the columns each holds, and global
indexes, with each index's partitioning and read replicas, its vector indexes
with their settings, its changefeeds, each with the retention and the
consumers of its topic, the table's own partitioning, read replicas and key
bloom filter, every view with the query the server stores, every topic with
its settings and consumers, every coordination node with its configuration,
and the users, groups and permissions; see
[Users, groups and permissions](#users-groups-and-permissions).

What Ptah does not model yet is recorded rather than dropped:
column-oriented tables, sequences other than a `Serial` column's, the settings
of a table such as a TTL run interval and storage settings, a column family
kept in memory with `keep_in_memory`, and a changefeed holding a setting Ptah
does not read, such as attributes, an AWS region, trace identifiers or a shared
consumer. A command reports them, and a plan neither drops nor changes them.

An index kind Ptah cannot read, such as a full-text index, is refused by name
rather than read as a plain index, and so is a vector index holding a setting
Ptah does not model, such as the `overlap_clusters` 26.2 takes.

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
statement does not state, under the `YD` family:

- a unique index added to an existing table;
- a block that mixes schema and data statements;
- an `ADD COLUMN` the line refuses;
- a dropped column an index or the TTL uses;
- a partitioning change that resets the minimum partition count or the
  partition size;
- a table a view reads that is dropped or renamed;
- a renamed table that carries a changefeed;
- a `REVOKE GRANT OPTION FOR`, which takes the permission too;
- a dropped user or group, which leaves its permissions behind;
- a topic setting reset that changes nothing, and a topic setting YDB keeps as
  nothing;
- a statement that names a column family the table does not have, which YDB
  creates rather than refuses;
- a vector index the line does not build, and rows written into a table whose
  vector index does not take them in.

`DS107` reports a dropped user or group as it reports a dropped role
elsewhere, and a dropped topic or coordination node.
[Lint rules](../../reference/lint-rules/#ydb) lists each rule with its
meaning.

`YD104`, `YD105`, `YD106`, `YD109`, `YD118`, `YD119` and `YD131` read the
indexes, TTL, minimum partition count and partition size, views, changefeeds,
column families and vector indexes the directory's own earlier migrations
declare; a table the directory never created is unknown to them. `YD105`
stays silent where that history left the minimum at 1, and `YD118` where it
left the size at 2048 MB or splitting by size off; each warns where it does
not know. With `--dev-url`, lint first replays the directory
in a [dev realm](#dev-shadow-and-scratch-databases), so a statement YDB
refuses fails the run, and the rules that read a baseline schema read it
there. The rules for
every dialect run too, and the
[lint rules](../../reference/lint-rules/#what-the-rules-for-every-dialect-do-on-ydb)
say what each does on YDB.

`ptah sql lint` reads YQL without the SQL parser, which has no YQL grammar. It
reports a `CREATE TABLE` without a primary key, which YDB refuses, as `DDL001`,
and a capability a `CREATE TABLE` or an `ALTER TABLE` needs that the line lacks
as `CAP001`.

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

## Dev, shadow and scratch databases

SQL cannot create a YDB database, so a dev, shadow or test database on YDB is a
dev realm: a directory Ptah creates for the run under `ptah_dev` in the
database the URL names, and removes when the run ends. Every query the run
sends starts with `PRAGMA TablePathPrefix` naming the realm, and the run reads
and resets only what is under it, so it works in an empty database of its own:

```bash
ptah migrations validate --dir migrations --dev-url "ydb://localhost:2136/local"
```

A run that reads or changes a database leaves `ptah_dev` out, as it leaves out
`ptah_locks`. So a `--dev-url` or `--shadow-db` may name the target database
itself: a realm is never part of the target's schema, of a plan against it or
of a `ptah db drop-all`. It may also name a database on another server. Two
local-ydb servers both serve `local`, and every node of a cluster serves each
of its databases, so Ptah does not tell databases apart by their URLs; the
realm keeps the dev database apart from the target on either server. `ptah migrations test` and `ptah schema test` run their
cases in one realm, and a case marked `parallel` in a realm of its own.

Creating a realm needs the `ydb.granular.create_directory` right on the
database, and removing it needs `ydb.granular.remove_schema`. A run that is
killed before it removes its realm leaves `ptah_dev/<name>` behind; remove it
with `ydb scheme rmdir -r ptah_dev/<name>`.

The prefix confines every relative path, and replay refuses a statement whose
effect it does not confine:

- a write whose target is an absolute path, climbs out with `..`, is named
  through a `$` expression or names a cluster, an `ALTER TABLE ... RENAME TO`
  either, and `PRAGMA TablePathPrefix`;
- `DEFINE ACTION`, `DO` and `EVALUATE`, which run statements they compute;
- users, groups, `GRANT` and `REVOKE`, secrets, resource pools, backups and
  `ALTER DATABASE`, which belong to the whole database;
- external data sources and tables, async replication, transfers and streaming
  queries, which reach outside the server.

A read outside the realm is allowed, since it leaves nothing behind. A
coordination node is confined like a table, and the realm's reset drops it.

`docker://ydb/<tag>[/local]` starts `ydbplatform/local-ydb:<tag>` for one
command and removes it afterwards. The image serves the one database `local`,
so another database name is refused. The server is the run's own, so replay
there runs everything in the list above except what reaches outside the
server. local-ydb takes every connection without a credential, so a container
runtime on another machine, which would publish the port on every interface of
its host, is refused before anything starts:

```bash
ptah migrations validate --dir migrations --dev-url docker://ydb/26.2.1.14/local
```

`PTAH_DEV_SERVER_DISPOSABLE=1` makes the server a YDB dev URL names the run's
own in the same way. The run then gets no realm: the database is the dev
database, and it has to be empty. Ptah connects to both databases and refuses
a dev database that is the target's own.

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
kind as its `type`, such as `"GLOBAL SYNC"` or `"GLOBAL ASYNC"`. A vector index
carries its settings as attributes named as the annotation names them, such as
`distance = "cosine"` and `vector_dimension = 1536`. A directory is a
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

HCL and DBML have no block for a changefeed or a column family, so a document
in either says nothing about one. Applying it leaves the database's
changefeeds and column families as they are, and a rebuild adds them to the new
table. `schema inspect` and `ptah schema export` warn about each changefeed and
each table's column families they leave out, and `--cleanup-go-annotations`
refuses to delete them.

The YDB driver reports a row count it did not measure, so a `script exec` or
`script loop` step reports its count as not reported, and `expect_rows` is
refused rather than judged against the number. A script spells parameters the
way YQL reads them, `$p1`, `$p2` and so on; `?` is not YQL.

A primary key change, a column type change and `SET NOT NULL` are refused unless
`PTAH_ALLOW_TABLE_REBUILD=1` asks `schema apply`, `schema diff` or `schema plan
new` for a [table rebuild](#table-rebuilds), and the refusal names the
variable. `migrate diff` reads it too, and plans with it once YDB can be its
dev database. A malformed value fails the run before it does anything, and
strict mode refuses the variable.

A `--dev-url` or a project `dev` URL naming a YDB database gets a dev realm, as
on the native commands; see
[Dev, shadow and scratch databases](#dev-shadow-and-scratch-databases).
`docker://ydb` starts a local-ydb server for the run in the default profile.

`PTAH_ATLAS_STRICT_COMPAT=1` reproduces the community binary, which answers
`sql/sqlclient: unknown driver "ydb". See: https://atlasgo.io/url` on every verb,
with `ydbs` for a `ydbs://` URL, and `unsupported docker image "ydb"` for a
`docker://ydb` dev URL. Strict mode refuses a YDB URL in those words
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

`ptah schema lineage --db-url ydb://...` traces views from the queries the
server stores. A view's source is its table's Ptah name, such as `shop.items`
for the path `shop/items`, so a view over a view links to the view it reads,
and a double-quoted text is a YQL string, which feeds no column. YDB has no
routines, so a directory without views has nothing to trace.

`ptah schema security --db-url ydb://...` analyzes the users, groups,
memberships, permissions and owners the reader reports: a group nobody is a
member of, two groups held by one member that grant nearly the same
permissions, and an object owned by a user who logs in. `--schemas` names the
directories whose permissions it reads. A connection that may not read the
users and groups is refused, naming what it needs, rather than reported as a
clean access model.

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
- comments on tables, columns and indexes;
- full-text, JSON and column-table indexes;
- `ptah inference` and the inference tools of `ptah mcp`, which store their vectors through pgvector.
<!-- END GENERATED YDB GAPS -->

The work is planned in [#4015](https://github.com/stokaro/ptah/issues/4015).

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
- URL forms for every engine: [Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/).
