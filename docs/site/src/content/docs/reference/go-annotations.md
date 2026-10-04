---
title: Go annotation reference
description: Every //ptah directive and attribute accepted by Ptah's Go annotation parser.
type: reference
audience:
  - "all-users"
readerQuestion: "Which Go annotation directives and attributes does Ptah accept?"
goal: "Look up every accepted Go annotation directive and attribute."
sourceOfTruth:
  - "internal/annotationmeta"
  - "docs/site/public/ptah-annotations.schema.json"
generated: false
searchAliases:
  - "Go annotations"
overlaps: []
disposition: keep
owns:
  - cli-ptah-schema-annotations
---

Every `//ptah` comment directive and attribute accepted by Ptah's Go annotation
parser is listed below. The same metadata is exported as a JSON Schema
document by `ptah schema annotations`, and that document is published at
<https://docs.ptah.run/ptah-annotations.schema.json>, which is the address it
declares as its own `$id`. Point an editor's JSON Schema setting at that URL to
get completion and validation for `//ptah` directives. Each published version
also serves its own copy, so `/edge/ptah-annotations.schema.json` is the one
built from the development source.
For the workflow — modeling, rendering, and generating migrations from
annotated structs — see [Go annotations](../../schema/go-annotations/).

## Syntax

A directive is a single Go comment line: the directive name followed by
space-separated `key="value"` attributes.

```go
//ptah:schema:table name="products"
```

- Attributes marked "bare form allowed" may be written without a value:
  `primary` is equivalent to `primary="true"`.
- An unknown attribute fails parsing with
  `unknown annotation attribute "<name>" on //<directive>`.
- Directives marked "Platform overrides: yes" also accept
  `platform.<dialect>.<attribute>="..."` pairs that override the base
  attribute for one dialect, for example
  `platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT"`.
- The Required column records what the parser rejects. An attribute the
  renderer needs for valid SQL (such as `name` on a table) can still be
  omitted at parse time; the rendered SQL is then invalid. `ptah schema
  render` is the cheapest way to catch that early.

## Placement

Each directive attaches to one of three places in Go source:

- **struct** — the comment lines directly above a `type ... struct`
  declaration. Any struct works, including empty marker structs declared only
  to carry directives, and one struct can carry several directives.
- **field** — the comment line directly above a struct field. Directives that
  allow both struct and field placement (`index`, `constraint`) can use a
  blank `_ int` placeholder field inside the struct.
- **file** — a detached comment line anywhere in a Go file, separated from
  declarations by blank lines (row-level security directives only). The named
  `table` must be declared in the same file; a file-scoped RLS comment naming
  a table from another file is silently ignored.

```go
//ptah:schema:table name="users"
//ptah:schema:constraint name="users_email_check" type="CHECK" check="email <> ''"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//ptah:schema:index name="idx_users_email" fields="email"
	_ int
}

//ptah:schema:enum name="user_status" values="active,disabled"
type StatusEnumMarker struct{}
```

## Directive index

| Directive | Declares | Placement |
| --- | --- | --- |
| [`ptah:schema:table`](#ptahschematable) | A database table | struct |
| [`ptah:schema:field`](#ptahschemafield) | A table column | field |
| [`ptah:embedded`](#ptahembedded) | Columns or relations from an embedded Go field | field |
| [`ptah:schema:index`](#ptahschemaindex) | An index | struct or field |
| [`ptah:schema:constraint`](#ptahschemaconstraint) | A table constraint | struct or field |
| [`ptah:schema:changefeed`](#ptahschemachangefeed) | A YDB changefeed of a table | struct or field |
| [`ptah:schema:changefeed:consumer`](#ptahschemachangefeedconsumer) | A consumer of a YDB changefeed's topic | struct or field |
| [`ptah:schema:enum`](#ptahschemaenum) | A reusable enum type | struct |
| [`ptah:schema:domain`](#ptahschemadomain) | A PostgreSQL domain type | struct |
| [`ptah:schema:composite`](#ptahschemacomposite) | A PostgreSQL composite type | struct |
| [`ptah:schema:range`](#ptahschemarange) | A PostgreSQL range type | struct |
| [`ptah:schema:schema`](#ptahschemaschema) | A database schema/namespace | struct |
| [`ptah:schema:extension`](#ptahschemaextension) | A PostgreSQL extension | struct |
| [`ptah:schema:sequence`](#ptahschemasequence) | A standalone PostgreSQL sequence | struct |
| [`ptah:schema:function`](#ptahschemafunction) | A database function | struct |
| [`ptah:schema:procedure`](#ptahschemaprocedure) | A stored procedure | struct |
| [`ptah:schema:trigger`](#ptahschematrigger) | A database trigger | struct |
| [`ptah:schema:view`](#ptahschemaview) | A database view | struct |
| [`ptah:schema:matview`](#ptahschemamatview) | A materialized view | struct |
| [`ptah:schema:coordinationnode`](#ptahschemacoordinationnode) | A YDB coordination node | struct |
| [`ptah:schema:role`](#ptahschemarole) | A database role | struct |
| [`ptah:schema:grant`](#ptahschemagrant) | Database grants | struct |
| [`ptah:schema:revoke`](#ptahschemarevoke) | Privileges a role must not hold | struct |
| [`ptah:schema:defaultprivilege`](#ptahschemadefaultprivilege) | A PostgreSQL default privilege | struct |
| [`ptah:schema:rls:enable`](#ptahschemarlsenable) | Row-level security enablement | file or struct |
| [`ptah:schema:rls:policy`](#ptahschemarlspolicy) | A row-level security policy | file or struct |
| [`ptah:schema:data`](#ptahschemadata) | Reference/seed row data for a table | struct |
| [`ptah:schema:notdescribed`](#ptahschemanotdescribed) | What this schema does not describe | struct |

Placement is semantic, not cosmetic. A struct directive belongs in the doc
comment of a Go struct declaration; a field directive belongs in the doc comment
of the field it describes. File-scoped RLS directives may use a separate comment
group, but they still need enough attributes to resolve to a parsed RLS object.
`ptah schema export --cleanup-go-annotations` refuses to remove any recognized
standalone directive that does not meet those conditions. Directive names use an
exact token boundary: `//ptah:schema:tableau` is an ordinary comment, not a table
directive.

## Scoping an object to dialects

Every directive that declares a standalone database object accepts `dialects`, a
comma-separated list of the targets the object belongs to:

```go
//ptah:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" dialects="postgres,cockroachdb,yugabytedb" body="BEGIN RETURN current_setting('app.tenant_id', true); END;"
type CurrentTenant struct{}
```

An object whose `dialects` excludes the target is **absent** from that target's
desired state. It is not skipped and not refused: nothing compares it, nothing
plans it, and `ptah schema render` prints a note on stderr naming what was left
out and which dialects it was declared for.

Absence is what makes a shared schema converge. Without a scope, a
PostgreSQL-only object on a MySQL target is either passed over with a comment —
in which case `schema apply` exits 0 having created nothing and the next
comparison asks for the same object again, forever — or refused outright, which
makes one schema across `postgres`, `mysql` and `mariadb` impossible.

Rules:

- **Omitting `dialects` means every dialect.** Declarations written before this
  attribute existed are unaffected, and a scope can only narrow an object, never
  widen one.
- **Every accepted spelling resolves to the same target.** `dialects="postgresql"`
  and `dialects="postgres"` select the same dialect.
- **A dialect family is not implied.** `dialects="postgres"` does not include
  `cockroachdb`, `yugabytedb` or `spanner`; name each target you mean.
- **A scope that names no supported dialect is a parse error**, including an
  empty `dialects=""`. Reading a typo as "belongs to nothing" would drop the
  object from every target with every command still exiting 0.
- **`ptah schema export --to hcl` reports the scope as an export loss.** Atlas
  HCL cannot carry it, so `--cleanup-go-annotations` refuses to delete an
  annotation whose scope the exported file would not preserve.

`dialects` is accepted on `extension`, `sequence`, `domain`, `composite`,
`range`, `function`, `trigger`, `view`, `matview`, `role`, `grant`, `revoke`,
`rls:enable` and `rls:policy`. Directives that describe table structure —
`table`, `field`, `index`, `constraint`, `embedded`, `enum` and `schema` — do
not accept it.

## Tables and columns

### `//ptah:schema:table`

Maps a Go struct to a database table.

| Attribute | Required | Description |
| --- | --- | --- |
| `checks` | No | Comma-separated table-level check expressions. |
| `comment` | No | Table comment. |
| `depends_on` | No | Comma-separated tables this table must be created after. |
| `custom` | No | Raw custom CREATE TABLE SQL. |
| `engine` | No | MySQL/MariaDB table engine shortcut; see the note below the table. |
| `name` | No | Table name. |
| `primary_key` | No | Comma-separated primary key columns. |
| `primary_key_block_size` | No | MySQL-family primary index block-size hint. Zero uses the engine default. |
| `primary_key_comment` | No | MySQL-family primary index comment. |
| `row_deletion_column` | No | Row deletion policy (Spanner and YDB TTL): the column a row's age is measured from. Needs `row_deletion_interval`. |
| `row_deletion_interval` | No | Row deletion policy: how long after the column's time a row is deleted, such as `P30D` on YDB or `30 days` on Spanner. |
| `row_deletion_unit` | No | YDB TTL on an integer column: what the column counts since the Unix epoch, `SECONDS`, `MILLISECONDS`, `MICROSECONDS` or `NANOSECONDS`. |
| `schema` | No | Database schema name. |
| `ttl_delete_batch_size` | No | CockroachDB row-level TTL: rows deleted per batch; at least 1. |
| `ttl_delete_rate_limit` | No | CockroachDB row-level TTL: rows deleted per second; at least 1. |
| `ttl_disable_changefeed_replication` | No | CockroachDB row-level TTL: omits the job's deletes from changefeeds. `true`/`false`. |
| `ttl_expiration_expression` | No | CockroachDB row-level TTL: SQL expression whose value is when a row expires. Enables the TTL. |
| `ttl_expire_after` | No | CockroachDB row-level TTL: interval after a row is written at which it expires, such as `3 days`. Enables the TTL. |
| `ttl_job_cron` | No | CockroachDB row-level TTL: cron schedule for the deletion job. |
| `ttl_label_metrics` | No | CockroachDB row-level TTL: labels the job's metrics with the table name. `true`/`false`. |
| `ttl_pause` | No | CockroachDB row-level TTL: pauses the deletion job without removing the policy. `true`/`false`. |
| `ttl_select_batch_size` | No | CockroachDB row-level TTL: rows selected per batch; at least 1. |
| `ttl_select_rate_limit` | No | CockroachDB row-level TTL: rows selected per second; at least 1. |

The table engine, character set, collation and `AUTO_INCREMENT` start are
MySQL-family options. A PostgreSQL-family target renders none of them and
names each one it was handed on a `skipped` comment above the statement;
declare a key's start there with `identity_start` on the column.

The `ttl_` attributes declare CockroachDB row-level TTL and are refused on every
other dialect before anything is applied. Either `ttl_expiration_expression` or
`ttl_expire_after` enables the policy, and the rest are refused without one. One
real CockroachDB parameter is deliberately absent — `ttl_row_stats_poll_interval`
— because the server rewrites the duration it stores and drops a value below one
second entirely, so a declaration could never read back as written; declaring it
is refused by name with the reason.
See [CockroachDB row-level TTL](../../databases/distributed/#cockroachdb-row-level-ttl).

The `row_deletion_` attributes declare a row deletion policy, which Spanner and
YDB have and every other target refuses. A policy needs its column and its
interval, and each engine reads the interval in its own spelling. See
[YDB TTL](../../databases/ydb/#ttl).

Platform overrides: yes.

### `//ptah:schema:field`

Maps a Go struct field to a database column.

| Attribute | Required | Description |
| --- | --- | --- |
| `auto_increment` | No | Marks the column as auto-incrementing. `true`/`false`; bare form allowed. |
| `check` | No | Column CHECK expression. |
| `check_name` | No | Explicit CHECK constraint name. |
| `comment` | No | Column comment. |
| `default` | No | Literal column default. |
| `default_expr` | No | SQL default expression. |
| `enum` | No | Comma-separated enum values. |
| `foreign` | No | Foreign key reference in table(column) form. |
| `foreign_key_name` | No | Explicit foreign key constraint name. |
| `generated` | No | Generated column expression. |
| `generated_kind` | No | Generated column kind, such as STORED or VIRTUAL. |
| `identity_generation` | No | SQL identity generation mode. |
| `identity_increment` | No | SQL identity increment value. |
| `identity_options` | No | Raw SQL identity options. |
| `identity_start` | No | SQL identity start value. |
| `name` | No | Column name. |
| `not_null` | No | Marks the column NOT NULL. `true`/`false`; bare form allowed. |
| `on_delete` | No | Foreign key ON DELETE action. |
| `on_update` | No | Foreign key ON UPDATE action. |
| `primary` | No | Marks the column as part of the primary key. `true`/`false`; bare form allowed. |
| `stored` | No | Shortcut controlling generated column storage. `true`/`false`. |
| `type` | No | Database column type. |
| `unique` | No | Adds a single-column unique constraint. `true`/`false`; bare form allowed. |
| `unique_expr` | No | Uniqueness over an expression. Not implemented: rendering fails with `column "…" declares unique_expr`, because emitting the column's own `UNIQUE` instead would enforce a different constraint. |

Platform overrides: yes.

### `//ptah:embedded`

Controls how an embedded Go field contributes schema objects.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Generated column comment. |
| `field` | No | Generated relation field name. |
| `mode` | No | Embedding mode: inline, json, or relation. |
| `name` | No | Column name for json embedding. |
| `nullable` | No | Marks generated embedded columns nullable. `true`/`false`; bare form allowed. |
| `on_delete` | No | Generated foreign key ON DELETE action. |
| `on_update` | No | Generated foreign key ON UPDATE action. |
| `prefix` | No | Column prefix for inline embedded fields. |
| `ref` | No | Relation target in table(column) form. |
| `type` | No | Generated column type for json or relation embedding. |

Platform overrides: yes.

For relation embedding, set `type` to the referenced column's physical type.
When it is omitted, Ptah uses a conservative numeric or string heuristic,
which cannot infer every user-defined or dialect-specific key type.

### `//ptah:schema:index`

Declares an index for a table.

| Attribute | Required | Description |
| --- | --- | --- |
| `columns` | No | Synonym for `fields`. |
| `comment` | No | Index comment. |
| `condition` | No | Partial index condition. |
| `fields` | No | Comma-separated Go field or column names. |
| `granularity` | No | ClickHouse data-skipping index granularity. |
| `key_block_size` | No | MySQL-family index block-size hint. Zero uses the engine default. See [MySQL and MariaDB](../../databases/mysql/) for retention and range limits. |
| `include` | No | Comma-separated INCLUDE columns for PostgreSQL, YugabyteDB, CockroachDB, or the Spanner PostgreSQL dialect, and `COVER` columns on YDB. Order is preserved, and a changed list rebuilds the index. |
| `invisible` | No | Hides the index from the optimizer: `INVISIBLE` on MySQL, `IGNORED` on MariaDB, `NOT VISIBLE` on CockroachDB. `true`/`false`; bare form allowed. A target whose capability set does not carry `invisible_indexes` refuses it at render time rather than building a visible index. |
| `name` | No | Index name. |
| `nulls_distinct` | No | Controls NULLS DISTINCT behavior. `true`/`false`. The clause is PostgreSQL's; a target whose capability set does not carry `unique_nulls_distinct_clause` refuses it at render time rather than dropping it, in either spelling. |
| `ops` | No | PostgreSQL operator class. |
| `table` | No | Explicit target table. |
| `type` | No | Index type or method. On YDB, `async` builds a `GLOBAL ASYNC` index. |
| `unique` | No | Creates a unique index. `true`/`false`; bare form allowed. |
| `where` | No | Atlas-style partial index condition alias. |

Omit `include` when the index has no payload columns. A present value with any
empty element fails parsing instead of silently removing that element. This
includes empty, whitespace-only, comma-only, sparse, and trailing-comma lists.

The accepted access methods depend on the target: PostgreSQL accepts the
default, `BTREE`, and `GIST`, plus `SPGIST` on PostgreSQL 14 and newer;
YugabyteDB accepts the default and `LSM`, with `BTREE` normalized to its
documented default-LSM alias; CockroachDB accepts the default and `BTREE`, and
refuses `GIN` and `GIST`, both of which name an inverted index there; the
Spanner PostgreSQL dialect accepts only the default; and YDB writes the columns
as `COVER (...)` on a synchronous or an asynchronous global index. Every other
dialect rejects `include` before emitting SQL.

A YDB global index also takes its partitioning, in attributes spelled as YDB
names the settings, in lower case. Every other dialect refuses an index that
declares one, rather than build it with the server's defaults. See
[index partitioning](../../databases/ydb/#index-partitioning).

| Attribute | Value |
| --- | --- |
| `auto_partitioning_by_size` | `ENABLED` or `DISABLED` |
| `auto_partitioning_partition_size_mb` | megabytes, at least 1 |
| `auto_partitioning_by_load` | `ENABLED` or `DISABLED` |
| `auto_partitioning_min_partitions_count` | at least 1 |
| `auto_partitioning_max_partitions_count` | at least 1 |
| `read_replicas_settings` | `PER_AZ:<n>` or `ANY_AZ:<n>` |

CockroachDB's catalog names its access methods `prefix` and `inverted`, and it
refuses both as input. `ptah db read` reports them as `btree` and `gin`, the
spellings the same server prints in `pg_get_indexdef`, so a description it
produces replays against the database it came from.

### `//ptah:schema:constraint`

Declares a table constraint.

| Attribute | Required | Description |
| --- | --- | --- |
| `check` | No | CHECK expression. |
| `columns` | No | Comma-separated local columns. |
| `comment` | No | Constraint comment. |
| `condition` | No | Constraint WHERE condition. |
| `elements` | No | EXCLUDE constraint elements. |
| `foreign_column` | No | Single referenced column for FOREIGN KEY constraints. |
| `foreign_columns` | No | Comma-separated referenced columns for composite FOREIGN KEY constraints. |
| `foreign_table` | No | Referenced table for FOREIGN KEY constraints. |
| `include` | No | Comma-separated INCLUDE columns for a covering UNIQUE or PRIMARY KEY constraint. Order is preserved. |
| `key_block_size` | No | Block-size hint for a MySQL-family `PRIMARY KEY`. Declare a unique index to give a `UNIQUE` key this option. |
| `name` | No | Constraint name. |
| `nulls_distinct` | No | Controls NULLS DISTINCT behavior. `true`/`false`. The clause is PostgreSQL's; a target whose capability set does not carry `unique_nulls_distinct_clause` refuses it at render time rather than dropping it, in either spelling. |
| `on_delete` | No | Foreign key ON DELETE action. |
| `on_update` | No | Foreign key ON UPDATE action. |
| `table` | No | Explicit target table. |
| `type` | No | Constraint type: CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY, or EXCLUDE. |
| `using` | No | Access method: an EXCLUDE constraint's index method, or a PRIMARY KEY's on MySQL and MariaDB (`BTREE` or `HASH`). Refused on UNIQUE, CHECK and FOREIGN KEY. |

`include` belongs to a UNIQUE or a PRIMARY KEY constraint, and the two accept it
on different targets:

| `type` | Targets that accept `include` |
| --- | --- |
| `UNIQUE` | PostgreSQL, YugabyteDB, CockroachDB |
| `PRIMARY KEY` | PostgreSQL, YugabyteDB |

Every other target refuses the constraint before emitting SQL, naming the
constraint and the targets that accept it. CockroachDB stores a covering UNIQUE
constraint as a unique index with a `STORING` clause, which is its spelling of
the same payload; it has no covering primary key, and the Spanner PostgreSQL
dialect answers `<INCLUDE> clause is not supported in constraints` for either
kind, so both are refused there.

`include` on a CHECK, FOREIGN KEY, or EXCLUDE constraint is refused on every
target: those constraints carry no payload to render. Omit `include` when there
are none; a present list with an empty element is refused rather than trimmed.

The targets for a constraint are not the targets for an index. The Spanner
PostgreSQL dialect takes `include` on an index and refuses it on a constraint,
and CockroachDB takes it on an index and on a UNIQUE constraint but not on a
primary key. See
[`//ptah:schema:index`](#ptahschemaindex) for the index list.

A PRIMARY KEY constraint's `name` is the name the key is built with on
PostgreSQL; without one the server names it `<table>_pkey`. MySQL and MariaDB
call every primary key `PRIMARY` whatever it is declared as, so there the
comparison matches the key without its name.

### `//ptah:schema:changefeed`

Declares a YDB changefeed: a stream of a table's changes, which YDB keeps in a
topic at `<table>/<name>`. It belongs to the table of the struct it is on, or
to the one `table` names, which the same file declares. Every other dialect
refuses a table that declares one. See
[changefeeds](../../databases/ydb/#changefeeds).

| Attribute | Required | Description |
| --- | --- | --- |
| `name` | Yes | Changefeed name, unique among the table's changefeeds and indexes. |
| `mode` | Yes | What a record carries: `KEYS_ONLY`, `UPDATES`, `NEW_IMAGE`, `OLD_IMAGE` or `NEW_AND_OLD_IMAGES`. |
| `format` | Yes | How a record is written: `JSON` or `DEBEZIUM_JSON`. |
| `table` | No | Table the changefeed belongs to, when not the struct's own. |
| `virtual_timestamps` | No | Each record carries its change's virtual timestamp. `true`/`false`; bare form allowed. |
| `resolved_timestamps` | No | Interval of the barrier records, an ISO 8601 duration of whole seconds. |
| `retention_period` | No | How long the topic keeps a record; 24 hours when omitted. |
| `initial_scan` | No | The stream opens with a record for every row. `true`/`false`; bare form allowed. |
| `user_sids` | No | Each record names its change's user. Needs `changefeed_user_sids`. |
| `schema_changes` | No | The stream carries schema change records. Needs `changefeed_schema_changes`. |
| `topic_min_active_partitions` | No | Partitions the topic starts with, at least 1. |
| `topic_auto_partitioning` | No | The topic gains partitions as writes grow. Needs `changefeed_topic_auto_partitioning`. |

### `//ptah:schema:changefeed:consumer`

Declares a consumer of a changefeed's topic: a named reader that keeps its own
position in the stream.

| Attribute | Required | Description |
| --- | --- | --- |
| `name` | Yes | Consumer name, unique within the topic. |
| `changefeed` | Yes | Changefeed whose topic the consumer reads. |
| `table` | No | Table of the changefeed, when not the struct's own. |
| `important` | No | The topic keeps what this consumer has not read past the retention. `true`/`false`; bare form allowed. |
| `read_from` | No | RFC 3339 time a partition this consumer has not read is read from, in whole seconds. |
| `supported_codecs` | No | Codecs the consumer reads: `raw`, `gzip`, `lzop`, `zstd`, `custom`. |
| `availability_period` | No | How long the topic keeps what this consumer has not read past the retention. Needs `topic_consumer_availability_period`. |

## Reusable types

### `//ptah:schema:enum`

Declares a reusable enum type.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Enum type comment. |
| `name` | Yes | Enum type name. |
| `values` | Yes | Comma-separated enum values. |

### `//ptah:schema:domain`

Declares a PostgreSQL domain type.

| Attribute | Required | Description |
| --- | --- | --- |
| `check` | No | CHECK constraint expression (uses VALUE). |
| `comment` | No | Domain comment. |
| `default` | No | Literal DEFAULT value. |
| `default_expr` | No | DEFAULT expression. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `name` | Yes | Domain name. |
| `not_null` | No | Marks the domain NOT NULL. `true`/`false`. |
| `schema` | No | Target schema/namespace. |
| `type` | Yes | Underlying base data type. |

### `//ptah:schema:composite`

Declares a PostgreSQL composite type.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Composite type comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `fields` | Yes | Comma-separated name:type field list. |
| `name` | Yes | Composite type name. |
| `schema` | No | Target schema/namespace. |

### `//ptah:schema:range`

Declares a PostgreSQL range type.

| Attribute | Required | Description |
| --- | --- | --- |
| `canonical` | No | Canonicalization function. |
| `collation` | No | Collation for the subtype. |
| `comment` | No | Range type comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `name` | Yes | Range type name. |
| `schema` | No | Target schema/namespace. |
| `subtype` | Yes | Element subtype the range is built over. |
| `subtype_diff` | No | Subtype difference function. |
| `subtype_opclass` | No | Operator class for the subtype. |

**Omitting an attribute and writing it empty are different instructions.**
Omission says nothing about the attribute, so a range in an existing database
that carries a `SUBTYPE_DIFF` keeps it when the declaration does not mention
one — which is what makes pointing Ptah at a database somebody else built safe.
Writing the attribute empty says the range has none, and is the spelling that
removes one:

```go
//ptah:schema:range name="measurement" subtype="int8" subtype_diff=""
type Measurement struct{}
```

This applies to `canonical`, `collation`, `subtype_diff` and `subtype_opclass`.
PostgreSQL has no `ALTER TYPE … AS RANGE`, so removing one is planned as a drop
and a create: the drop is non-`CASCADE` and fails while the type is in use.
That is the reason omission cannot mean removal.

## Database objects

### `//ptah:schema:schema`

Declares a database schema or namespace.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Schema comment. |
| `name` | Yes | Schema name. |

### `//ptah:schema:extension`

Declares a PostgreSQL extension.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Extension comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `if_not_exists` | No | Adds IF NOT EXISTS where supported. `true`/`false`. |
| `name` | No | Extension name. |
| `schema` | No | PostgreSQL installation schema. Empty means the target's default schema. |
| `version` | No | Extension version. |

### `//ptah:schema:notdescribed`

Declares that this description does not describe an object family, or one named
object in it, so its absence is not a removal.

| Attribute | Required | Description |
| --- | --- | --- |
| `kind` | Yes | Object family, such as `extension`, `schema`, `role` or `sequence`. |
| `name` | No | One object of that family. Omitted, the whole family is declined. |

```go
//ptah:schema:notdescribed kind="extension" name="pg_trgm"
//ptah:schema:notdescribed kind="role"
type _ struct{}
```

An object a schema declines is neither created nor dropped: the comparison
treats the silence as a limit of the description rather than as a statement that
the database should not have the object. An extension a bootstrap step installs
is the usual case — declaring it instead hands Ptah the object, and that
includes removing it.

`kind` comes from a closed list, and one this build does not know is refused
rather than ignored: a directive nothing understands reads as no directive at
all, and the absence it was protecting becomes a removal.

A serialized description says the same thing in its own grammar, as a directive
in the leading comment header:

```hcl illustration
// ptah:not-described extension "pg_trgm"
```

The two produce one plan. Which one fits depends on where the description lives,
not on what it means.

### `//ptah:schema:sequence`

Declares a standalone PostgreSQL sequence.

| Attribute | Required | Description |
| --- | --- | --- |
| `as` | No | Underlying integer type, such as bigint. |
| `cache` | No | CACHE size. |
| `comment` | No | Sequence comment. |
| `cycle` | No | Enables CYCLE wrap-around. `true`/`false`. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `if_not_exists` | No | Adds IF NOT EXISTS where supported. `true`/`false`. |
| `increment` | No | INCREMENT BY value; must be non-zero. |
| `maxvalue` | No | MAXVALUE bound. |
| `minvalue` | No | MINVALUE bound. |
| `name` | Yes | Sequence name. |
| `owned_by` | No | Owning table.column association (OWNED BY). |
| `schema` | No | Target schema/namespace. |
| `start` | No | START WITH value. |

### `//ptah:schema:function`

Declares a database function.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | No | Function body SQL. |
| `comment` | No | Function comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `language` | No | Function language. |
| `leakproof` | No | Marks the function `LEAKPROOF`. `true`/`false`. |
| `name` | No | Function name. |
| `parallel` | No | Parallel level: `SAFE`, `RESTRICTED` or `UNSAFE`. |
| `params` | No | Function parameter list. |
| `returns` | No | Return type. |
| `schema` | No | Target schema/namespace. |
| `security` | No | Security mode, such as DEFINER. |
| `settings` | No | Routine configuration settings, `name=value`, separated by `;`. |
| `strict` | No | Marks the function `STRICT`. `true`/`false`. |
| `volatility` | No | Volatility class. |

`strict` makes the function return NULL without running its body when any
argument is NULL. PostgreSQL also spells it `RETURNS NULL ON NULL INPUT`, and
`false` is `CALLED ON NULL INPUT`, the server's default, which is left out of the
rendered statement.

`leakproof` and `parallel` decide how the query planner may use the routine, and
the first decides it across a security boundary: a filter using a leakproof
function may be pushed past a row-level-security predicate, so a function marked
leakproof by mistake can reveal rows a policy withholds. PostgreSQL reserves the
attribute for a superuser for that reason. `UNSAFE` is the server's default and
is left out of the rendered statement, so a routine that states no level renders
as it did; a level that is none of the three is refused at parse time rather
than read as a default, because neither direction of that guess is safe.

`settings` pins the routine's own configuration, and `search_path` is the entry
that matters: a `SECURITY DEFINER` routine without one resolves unqualified
names through whatever the caller had set.

`schema` places the routine in a named schema, and is also accepted on
`view` and `matview`. A function, view or materialized view carries its schema
inside its NAME rather than in a field of its own — `schema="app" name="fn"`
becomes `app.fn` — which is the same spelling the `atlas.hcl` frontend produces
for these three kinds, and what the renderers split back apart when they emit
DDL. A declaration naming no schema keeps its name exactly as written.

### `//ptah:schema:procedure`

Declares a stored procedure: a routine that returns nothing and is invoked with `CALL`.

A procedure is the same catalog object as a function with one property removed, so it takes
the same attributes minus `returns`. Declaring `returns` is refused rather than ignored:
`CREATE PROCEDURE ... RETURNS` does not parse on either engine that has procedures, and
accepting the attribute would mean a declaration that says one thing while the database holds
another.

```go
//ptah:schema:procedure name="archive_tenant" params="tenant_id integer" language="sql" body="DELETE FROM tenants WHERE id = tenant_id"
type ArchiveTenant struct{}
```

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | No | Procedure body SQL. |
| `comment` | No | Procedure comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `language` | No | Procedure language. |
| `name` | No | Procedure name. |
| `params` | No | Procedure parameter list. |
| `schema` | No | Target schema/namespace. |
| `security` | No | Security mode, such as DEFINER. |
| `volatility` | No | Volatility class. |

### `//ptah:schema:trigger`

Declares a database trigger.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | Trigger body SQL. |
| `comment` | No | Trigger comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `event` | Yes | Trigger event, such as INSERT or UPDATE. |
| `for` | No | Trigger granularity; defaults to ROW. |
| `name` | Yes | Trigger name. |
| `new_table` | No | Transition table holding the rows after the change (REFERENCING NEW TABLE). PostgreSQL family. |
| `old_table` | No | Transition table holding the rows before the change (REFERENCING OLD TABLE). PostgreSQL family. |
| `table` | Yes | Target table. |
| `timing` | Yes | Trigger timing, such as BEFORE or AFTER. |
| `when` | No | WHEN condition; the trigger fires only where it is true. PostgreSQL family. |

### `//ptah:schema:view`

Declares a database view.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | View SELECT body. |
| `depends_on` | No | Comma-separated objects this view must be created after. |
| `comment` | No | View comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `name` | Yes | View name. |
| `schema` | No | Target schema/namespace. |
| `with_check` | No | Controls WITH CHECK OPTION where supported. `true`/`false`. |

### `//ptah:schema:matview`

Declares a materialized view.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | Materialized view SELECT body. |
| `depends_on` | No | Comma-separated objects this view must be created after. |
| `comment` | No | Materialized view comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `name` | Yes | Materialized view name. |
| `schema` | No | Target schema/namespace. |
| `refresh` | No | ClickHouse refresh schedule, as ClickHouse spells it. See below. |

`refresh` carries a ClickHouse **scheduled** materialized view's schedule, in
the engine's own words: `every 1 hour`, `after 30 minute`,
`every 1 day offset 2 hour randomize for 30 minute append`. Omitting it leaves
an ordinary materialized view, maintained by inserts into its source.

The server rewrites intervals — `every 60 minute` is stored as `EVERY 1 HOUR` —
so the declaration is normalized to the stored spelling when it is parsed. Any
spelling of the same schedule converges. A schedule the server would refuse is
refused here instead: an interval mixing calendar units with clock units, a
zero interval, or `offset` on an `after` schedule.

It is a ClickHouse property and only that. It is not the retired
cross-dialect strategy below, which described an operation rather than state:

`refresh_strategy` is not an attribute. Ptah does not refresh materialized
views: one is populated when it is created, a changed `body` is reconciled as a
drop and a create that populates it again, and it goes stale only when its
source data changes, which schema reconciliation cannot observe. Refresh from
your own scheduler.

An annotation that still declares it is refused while the source file is
parsed, with that reason, on every dialect -- including the bare form with no
value, and including a view scoped away from the current target by `dialects`.
The name stays recognized so the refusal explains itself instead of reading as
a misspelling.

### `//ptah:schema:coordinationnode`

Declares a YDB coordination node, which holds an application's semaphores and
rate limiter resources. A setting left out takes YDB's default.

| Attribute | Required | Description |
| --- | --- | --- |
| `attach_consistency_mode` | No | `strict` or `relaxed`. YDB's default is `strict`. |
| `name` | Yes | Node name. |
| `rate_limiter_counters_mode` | No | `aggregated` or `detailed`. YDB's default is `aggregated`. |
| `read_consistency_mode` | No | `strict` or `relaxed`. YDB's default is `relaxed`. |
| `schema` | No | Directory holding the node, relative to the database root. |
| `self_check_period` | No | How often the node checks it is alive, as an ISO 8601 duration from `PT0.5S` to `PT10S`. YDB's default is `PT1S`. |
| `session_grace_period` | No | How long a session keeps its semaphores while the node changes its leader, from the self-check period plus one second to `PT30S`. YDB's default is `PT10S`. |

```go
//ptah:schema:coordinationnode name="locks" schema="app" self_check_period="PT2S"
type Locks struct{}
```

A coordination node is YDB's own object, and every other target refuses the
declaration. `ptah_locks` at the database root is Ptah's lock node and is
refused. [Coordination nodes](../../databases/ydb/#coordination-nodes) says how
Ptah applies one.

## Security

### `//ptah:schema:role`

Declares a database role.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Role comment. |
| `create_db` | No | Alias for `createdb`. `true`/`false`. |
| `create_role` | No | Alias for `createrole`. `true`/`false`. |
| `createdb` | No | Allows database creation. `true`/`false`. |
| `createrole` | No | Allows role creation. `true`/`false`. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `group` | No | Declares a group, which never logs in and has members. YDB only. `true`/`false`. |
| `inherit` | No | Controls role inheritance; defaults to true. `true`/`false`. |
| `login` | No | Creates the role with LOGIN. `true`/`false`. |
| `member_of` | No | Comma-separated groups the role is a member of. YDB only. |
| `name` | No | Role name. |
| `password` | No | Role password. |
| `replication` | No | Allows replication. `true`/`false`. |
| `superuser` | No | Creates the role as SUPERUSER. `true`/`false`. |

Most of these attributes are PostgreSQL-family notions. A ClickHouse role
carries none of them — `system.roles` is `(name, id, storage)` — so declaring
`password`, `login`, `superuser`, `createdb`, `createrole`, or `replication`
for a ClickHouse target is refused rather than dropped, and `comment` is
emitted as a leading SQL comment because the engine cannot store one. See
[ClickHouse roles and grants](../../databases/clickhouse/#roles-and-grants).
A MySQL or MariaDB role carries none of them either, and the same attributes
are refused there. `inherit="false"` is refused too: a role on those engines
always passes on the privileges of the roles granted to it. See
[MySQL and MariaDB](../../databases/mysql/).

On YDB a role is a user, and `group="true"` declares a group instead. A user
takes `login` and `password`; a group takes neither. `member_of` names the
groups a user or a group joins, a group of the cluster's own such as
`DATA-READERS` included. The other attributes are refused, and so is a name
holding anything but lower-case letters and digits. See
[YDB users, groups and permissions](../../databases/ydb/#users-groups-and-permissions).

### `//ptah:schema:grant`

Declares database grants.

| Attribute | Required | Description |
| --- | --- | --- |
| `columns` | No | Comma-separated columns of `on_table` the privileges are limited to, such as `state,decided_at`. PostgreSQL only. |
| `comment` | No | Grant comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `grant_option` | No | Alias for `with_option`. `true`/`false`. |
| `on_database` | No | Targets the database itself. YDB only. `true`/`false`. |
| `on_function` | No | Target function with its argument types, such as `purge(uuid)`. PostgreSQL only. |
| `on_procedure` | No | Target procedure with its argument types, such as `archive(uuid)`. PostgreSQL only. |
| `on_schema` | No | Target schema. |
| `on_sequence` | No | Target sequence. |
| `on_table` | No | Target table. |
| `privilege` | No | Privilege or comma-separated privileges. |
| `privileges` | No | Alias for `privilege`. |
| `role` | No | Target role. |
| `with_option` | No | Adds WITH GRANT OPTION where supported. `true`/`false`. |

A function or procedure is named with its argument types in parentheses,
because PostgreSQL tells overloads apart by them. A name without them is
refused while the file is parsed.

On ClickHouse a grant names one scope, and `on_table` must be qualified as
`database.table` because rendering is offline and has no current database to
resolve a bare name against. `on_sequence`, wildcard scopes and column-scoped
privileges such as `SELECT(id)` are refused, as is declaring the same privilege
on both `db.*` and `db.t` — the server would absorb the narrower grant, so the
pair could never converge. `role` must name a role the same schema declares,
because ClickHouse resolves a grantee across users and roles and a user of that
name would win. Privilege names the server rewrites on the way in — `ALL`,
`CREATE`, `DROP`, `SYSTEM` and the rest — are refused too, because they never
read back as written. See
[ClickHouse roles and grants](../../databases/clickhouse/#roles-and-grants).

On YDB a grant is on the database (`on_database="true"`), a directory
(`on_schema`) or a table (`on_table`). A privilege is a YDB permission, by its
name, such as `ydb.granular.select_row`, or as `GRANT` spells it, such as
`SELECT ROW`. `with_option` is refused: YDB records the grant option as a
permission of its own, so grant `ydb.access.grant` instead.

### `//ptah:schema:revoke`

Declares privileges a role must not hold, including ones it holds without a
grant: the `EXECUTE` every new function gives `PUBLIC`, or what `ALTER DEFAULT
PRIVILEGES` gives a role on a new table.

| Attribute | Required | Description |
| --- | --- | --- |
| `columns` | No | Comma-separated columns of `on_table` the privileges are limited to, such as `state,decided_at`. PostgreSQL only. |
| `comment` | No | Comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `on_database` | No | Targets the database itself. YDB only. `true`/`false`. |
| `on_function` | No | Target function with its argument types, such as `purge(uuid)`. PostgreSQL only. |
| `on_procedure` | No | Target procedure with its argument types, such as `archive(uuid)`. PostgreSQL only. |
| `on_schema` | No | Target schema. |
| `on_sequence` | No | Target sequence. |
| `on_table` | No | Target table. |
| `privilege` | No | Privilege or comma-separated privileges. |
| `privileges` | No | Alias for `privilege`. |
| `role` | Yes | Role the privileges are taken from; `PUBLIC` names every role. |

```go
//ptah:schema:revoke role="PUBLIC" privilege="EXECUTE" on_function="purge_workspace(uuid)"
//ptah:schema:grant role="app" privilege="EXECUTE" on_function="purge_workspace(uuid)"
type AccessControl struct{}
```

Ptah plans the `REVOKE` whenever the database holds the privilege, and in the
same plan that creates the object, since the privilege arrives with it. The
same privilege declared both granted and revoked to one role on one object is
refused: annotations have no order, so neither declaration could win.

### `//ptah:schema:defaultprivilege`

Declares a PostgreSQL default privilege: what a grantee receives on objects a
named role creates, in a named schema or in every schema. It is
`ALTER DEFAULT PRIVILEGES`, and no other engine has the statement.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Default privilege comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `for_role` | Yes | Role whose newly created objects the privileges apply to. |
| `grantable` | No | The subset of `privileges` carrying `WITH GRANT OPTION`. |
| `grantee` | Yes | Role receiving the privileges; `PUBLIC` names every role. |
| `object_type` | Yes | `TABLES`, `SEQUENCES`, `FUNCTIONS` or `TYPES`, and without `schema` also `SCHEMAS` or `LARGE OBJECTS`. |
| `privileges` | No | Comma-separated privileges, such as `SELECT,INSERT`. Required unless `revoked` is set. |
| `revoked` | No | Comma-separated privileges the grantee must not hold by default, such as `INSERT,UPDATE`; `ALL` names every privilege of the object type. A name also in `privileges` is refused. |
| `schema` | No | Schema the default applies in. Left out, the directive is the global default, which applies in every schema. |

The directive needs a holder struct. Written at file level, below the closing
brace of the declaration above it, it contributes no object and reports
nothing, the same trap index annotations carry. Give it a struct of its own:

```go
//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT,INSERT" grantable="INSERT"
type AccessControl struct{}
```

The object's identity is `for_role`, `schema`, `object_type` and `grantee`
together. Two declarations differing only in `for_role` are two objects:
PostgreSQL enforces the grantor and refuses the statement from a role that is
not a member of it.

`grantable` names a subset of `privileges`, not a second list. Grantability is
recorded per privilege, so one identity granted `SELECT` plainly and `INSERT
WITH GRANT OPTION` reads back from the catalog as two rows. A name in
`grantable` that is absent from `privileges` is refused while the file is
parsed, rather than kept as a declaration nothing can render.

`object_type` accepts those keywords and nothing else; any other value is
refused at parse time. Without `schema` the directive declares the global
default, `ALTER DEFAULT PRIVILEGES` without `IN SCHEMA`, which applies in every
schema and starts from the built-in default:

```go
//ptah:schema:defaultprivilege for_role="app_owner" object_type="FUNCTIONS" grantee="PUBLIC" revoked="EXECUTE"
type NoPublicExecute struct{}
```

`SCHEMAS` and `LARGE OBJECTS` exist only in that form, so either one with a
`schema` is refused. See
[global default privileges](../../databases/postgresql/#global-default-privileges)
for how the comparison treats the built-in default.

### `//ptah:schema:rls:enable`

Enables row-level security on a table.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | RLS enablement comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `force` | No | Apply the table's policies to its owner too. `true`/`false`. |
| `table` | No | Target table. |

Enabling row-level security leaves the table's owner exempt from every policy on
it. `force="true"` adds `ALTER TABLE ... FORCE ROW LEVEL SECURITY`, which binds
the owner as well. The two are separate flags on the relation, so the render
emits two statements.

### `//ptah:schema:rls:policy`

Declares a row-level security policy.

| Attribute | Required | Description |
| --- | --- | --- |
| `as` | No | `PERMISSIVE` (the default) or `RESTRICTIVE`. |
| `comment` | No | Policy comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `for` | No | Policy command, such as ALL or SELECT. |
| `name` | No | Policy name. |
| `table` | No | Target table. |
| `to` | No | Comma-separated roles; omitted means PUBLIC on PostgreSQL. |
| `using` | No | USING expression. |
| `with_check` | No | WITH CHECK expression. |

A row passes when at least one permissive policy admits it and every restrictive
policy admits it, so the two combine differently and neither substitutes for the
other. `PERMISSIVE` is the server's default and is left out of the rendered
statement. Any value other than the two is refused at parse time rather than
read as the default: permissive is the weaker of the two, so a misspelled
`RESTRICTIVE` folded into it would grant the access the policy was written to
withhold.

## Reference data

### `//ptah:schema:data`

Declares external reference/seed row data for a table.

| Attribute | Required | Description |
| --- | --- | --- |
| `file` | Yes | Path to the YAML row-data file, relative to the Go source file. |
| `key` | Yes | Comma-separated key column(s) forming each row's identity. |
| `schema` | No | Database schema the table belongs to. |
| `table` | Yes | Target table the rows belong to. |

## Editor support

The repository ships `ptah-ls`, a language server for `//ptah` annotations
in Go source. It speaks the Language Server Protocol over stdio and provides
hover documentation, attribute completion, and diagnostics backed by the same
directive metadata as this page. Build it from the repository root:

```bash
go build -o bin/ptah-ls ./cmd/ptah-ls
```

A Visual Studio Code extension that starts `ptah-ls` for Go files lives in
[`editors/vscode`](https://github.com/stokaro/ptah/tree/master/editors/vscode);
its `ptah.languageServer.path` setting points at the built binary. Any other
LSP-capable editor can run `ptah-ls` directly as a stdio language server.

Serve mode takes no arguments. `ptah-ls version` and `ptah-ls --version` print
build metadata and exit; anything else on the command line is a usage error and
exits `2`. Earlier releases silently discarded arguments and started serving
anyway, so an editor configuration that passes a document or workspace path now
fails loudly instead of appearing to work.

## Next steps

- Modeling a schema with these directives: [Go annotations](../../schema/go-annotations/).
- Declaring rows, not only structure: [Reference data](../../versioned/reference-data/).
- Checking which features your dialect supports: [Capabilities](../capabilities/).
