---
title: YAML Schema Reference
description: Ptah's strict YAML schema-file format.
type: reference
audience:
  - "all-users"
readerQuestion: "Which fields and values does Ptah's YAML schema format accept?"
goal: "Look up the fields and values accepted by Ptah's YAML schema format."
sourceOfTruth:
  - "cmd"
  - "core"
  - "migration"
  - "core/yamlschema"
generated: false
overlaps: []
disposition: keep
sourceMode: static-file-only
owns:
  - gopkg-core-yamlschema
---

Ptah YAML is a language-neutral desired-schema format. It feeds the same schema
IR as Go annotations and HCL schema files, then uses the normal Ptah
finalization, dependency ordering, planner, and renderer paths.

Use YAML when a project wants a compact Ptah-owned schema file without tying the
schema to Go structs or HCL syntax.

## Command

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

`--schema-file` accepts `.yaml`, `.yml`, `.hcl`, `.sql`, and `.dbml` inputs. This page
documents the YAML shape only. Relative inputs are confined to the process
working directory after symbolic-link resolution; use an absolute pathname for
an intentional source outside it, as detailed under [schema file paths](../native-commands/#schema-file-paths).

## Minimal schema

```yaml
tables:
  users:
    columns:
      id:
        type: SERIAL
        primary: true
      email:
        type: VARCHAR(255)
        not_null: true
        unique: true
      email_lc:
        type: TEXT
        generated: lower(email)
        stored: true
    indexes:
      idx_users_email:
        fields: [email]
      idx_active_users_email:
        fields: [email]
        where: deleted_at IS NULL
```

## Top-level objects

Top-level objects are maps. Their keys are used as default object names when a
`name` field is not provided.

| Object | Purpose |
| --- | --- |
| `tables` | Tables, columns, indexes, constraints, checks, and table-local RLS enablement. |
| `enums` | Standalone enum types and values. |
| `extensions` | PostgreSQL extension declarations. |
| `functions` | PostgreSQL-style function metadata and SQL bodies. |
| `views` | View definitions. |
| `materialized_views` | Materialized view definitions. |
| `triggers` | Trigger definitions. |
| `rls_policies` | Row-level security policies, with the attributes of `//ptah:schema:rls:policy`, `as` included. An entry and an `rls_enabled_tables` entry take `dialects`: without it, or scoped to the PostgreSQL family, it is PostgreSQL row-level security and other targets refuse it. An entry scoped to ClickHouse is refused. |
| `row_policies` | ClickHouse row policies, keyed by name, with the attributes of `//ptah:schema:rowpolicy`; `struct_name` may name the table instead. See [row policies](../../databases/clickhouse/#row-policies). |
| `roles` | Role declarations. On YDB `group: true` declares a group, and `member_of` lists the groups a role joins. |
| `grants` | Permission grants on a table, schema, sequence, function or procedure, and on YDB on the database with `on_database: true`. A routine is `on_function` or `on_procedure` with its argument types, such as `purge(uuid)`. |
| `revokes` | Privileges a role must not hold, named like a grant: `role`, `privileges`, one target, and `comment`. |
| `default_privileges` | PostgreSQL default privileges: what a grantee receives on objects a role creates later. |
| `topics` | YDB topics: the settings their annotation takes, and `consumers` keyed by name. See [Topics](../../databases/ydb/#topics). |
| `streaming_queries` | YDB streaming queries, keyed by name: `schema`, `text`, `run`, `resource_pool` and `allow_state_reset`. See [streaming queries](../../databases/ydb/#streaming-queries). |
| `resource_pools` | YDB resource pools, keyed by name, with the settings of `//ptah:schema:resourcepool`. See [resource pools](../../databases/ydb/#resource-pools-and-classifiers). |
| `resource_pool_classifiers` | YDB resource pool classifiers, keyed by name, with `resource_pool`, `member_name` and `rank`. |
| `coordination_nodes` | YDB coordination nodes: `schema` and the settings of [`//ptah:schema:coordinationnode`](../go-annotations/#ptahschemacoordinationnode). |
| `async_replications` | YDB async replications: the connection and consistency settings their annotation takes, and `items`, a list of `source` and `target` pairs. See [Async replications and transfers](../../databases/ydb/#async-replications-and-transfers). |
| `transfers` | YDB transfers: `source`, `target`, `using` and the settings their annotation takes. |
| `secrets` | YDB secrets, each by its directory and the environment variable its value comes from; see [Secrets](#secrets). |
| `external_data_sources` | YDB external data sources; see [External data sources and tables](#external-data-sources-and-tables). |
| `external_tables` | YDB external tables over files in object storage; see [External data sources and tables](#external-data-sources-and-tables). |

Unknown keys fail. Ptah does not silently ignore fields that look meaningful but
are outside the supported schema.

### What this format has no key for

A sequence, a domain, a composite type and a range have no top-level key here,
and neither has a SQL Server synonym or extended property, nor a TimescaleDB
hypertable or continuous aggregate. **Silence about one of them is not a request to remove it**: a YAML
schema declaring one table, compared against a database holding one of each,
planned `DROP SEQUENCE`, `DROP DOMAIN` and both `DROP TYPE`s until this was
recorded. Loading a `.yaml` or `.yml` file now marks those families as not
described, so the comparison withholds the removal.

The two TimescaleDB objects are the ones whose silence looks like a complete
answer. The table IS in the document and only its partitioning is missing, so a
YAML description of a hypertable describes an ordinary table — replaying it
produces a table that is not partitioned, and a diff between the two reports no
difference. A continuous aggregate is worse: the hypertable underneath it is
described, so its absence reads as an object deliberately left out, and the drop
that would follow discards a materialization no rollback rebuilds.

The other formats keep their own answer. HCL **does** have a block for all
eight, so an HCL document that omits one is still asking for it to go; a `.sql`
document has the syntax for the first four and a Go schema for every one of
them. What a document can say is a property of its format, and each loader
records only its own limits.

## Extensions

Each entry under `extensions` declares one PostgreSQL extension. The map key is
the default extension name.

| Key | Meaning |
| --- | --- |
| `name` | Extension name. Defaults to the map key. |
| `schema` | PostgreSQL installation schema. Empty uses the target's default schema. |
| `if_not_exists` | Adds `IF NOT EXISTS` to creation SQL. |
| `version` | Requested extension version. |
| `comment` | Extension comment. |

```yaml
extensions:
  pgcrypto:
    schema: extensions
    if_not_exists: true
```

## Default privileges

Each entry under `default_privileges` declares one PostgreSQL default
privilege. The map key names the entry in error messages and nothing else: a
default privilege has no name of its own, and `for_role`, `schema`,
`object_type` and `grantee` together identify it.

| Key | Meaning |
| --- | --- |
| `for_role` | Role whose newly created objects the privileges apply to. Required. |
| `schema` | Schema the default applies in. Left out, the entry is the global default, which applies in every schema. |
| `object_type` | `TABLES`, `SEQUENCES`, `FUNCTIONS`, or `TYPES`, and without a `schema` also `SCHEMAS` or `LARGE OBJECTS`. Required. |
| `grantee` | Role receiving the privileges. `PUBLIC` names every role. Required. |
| `privileges` | Privileges granted, such as `SELECT`. Required unless `revoked` is set. |
| `grantable` | The subset of `privileges` carrying `WITH GRANT OPTION`. A name outside `privileges` is refused. |
| `revoked` | Privileges the grantee must not hold by default, such as `INSERT`; `ALL` names every privilege of the object type. A name also in `privileges` is refused. |
| `comment` | Default privilege comment. |
| `dialects` | Target dialects this entry belongs to. Written with no dialect in it, it is refused rather than read as every dialect. |

```yaml
default_privileges:
  owner_tables_to_reader:
    for_role: app_owner
    schema: public
    object_type: TABLES
    grantee: app_reader
    privileges: [SELECT, INSERT]
    grantable: [INSERT]
```

`grantable` is a subset rather than one boolean over the whole entry because
PostgreSQL records grantability per privilege. The entry above renders two
statements: `SELECT` plainly, and `INSERT` with the grant option. Reading the
database back reports the same two rows, so the comparison converges.

`revoked` is what a SQL schema file writes as `ALTER DEFAULT PRIVILEGES ...
REVOKE`. The comparison revokes each listed privilege wherever the database
holds it for that identity, whether or not the schema declares the role in
`for_role`. A database the schema creates has no schema-scoped default to
revoke, so a schema-scoped entry renders no statement of its own.

An entry without `schema` is the global default, `ALTER DEFAULT PRIVILEGES`
without `IN SCHEMA`. It starts from the built-in default, so its `revoked`
can take a built-in privilege away, and it renders as a statement even for a
database the schema creates:

```yaml
default_privileges:
  no_public_execute:
    for_role: app_owner
    object_type: FUNCTIONS
    grantee: PUBLIC
    revoked: [EXECUTE]
```

See [global default privileges](../../databases/postgresql/#global-default-privileges)
for how the comparison treats the built-in default.

## Tables

Each entry under `tables` declares one table.

| Key | Meaning |
| --- | --- |
| `name` | Database table name. Defaults to the map key. |
| `struct_name` | Internal Go-schema owner name. Defaults to the map key. |
| `api_name` | Shared OpenAPI, GraphQL, and Protobuf table-name fallback. |
| `openapi_name` | Exact OpenAPI component key for this table. |
| `graphql_name` | GraphQL type-name stem for this table. |
| `proto_name` | Protobuf message-name stem for this table. |
| `engine` | Table engine value for the MySQL family; a PostgreSQL-family target names it on a `skipped` comment instead. |
| `comment` | Table comment. |
| `primary_key` | Table-level primary key column list. |
| `checks` | Table-level check expressions. |
| `custom_sql` | Custom SQL attached to the table. |
| `columns` / `fields` | Ordered column map. Use one or the other. |
| `indexes` | Ordered table-local index map. |
| `constraints` | Ordered table-local constraint map. |
| `column_families` | Ordered map of a YDB table's column families; see [Column families](#column-families). |
| `changefeeds` | Ordered map of a YDB table's changefeeds; see [Changefeeds](#changefeeds). |
| `rls_enabled` | Enables row-level security for the table. |
| `platform` / `overrides` | Dialect-specific override map. A [Spanner row deletion policy](../../databases/distributed/#spanner-row-deletion-policy) sits in its `spanner` group and a [YDB TTL](../../databases/ydb/#ttl) in its `ydb` group, as `row_deletion_column`, `row_deletion_interval` and, on YDB, `row_deletion_unit`. |
| `auto_partitioning_by_size`, `auto_partitioning_partition_size_mb`, `auto_partitioning_by_load`, `auto_partitioning_min_partitions_count`, `auto_partitioning_max_partitions_count`, `read_replicas_settings`, `key_bloom_filter`, `uniform_partitions`, `partition_at_keys` | A YDB row table's [partitioning, read replicas and key bloom filter](../../databases/ydb/#table-partitioning-read-replicas-and-key-bloom-filter), with the values the annotation attributes of the same names take. Every other dialect refuses them. |

Table-local `columns`, `fields`, `indexes`, and `constraints` preserve YAML
author order. Top-level maps render deterministically by sorted key.

## Columns

| Key | Meaning |
| --- | --- |
| `name` | Database column name. Defaults to the column key. |
| `field_name` | Internal Go-schema field name. Defaults to the column key. |
| `api_name` | Shared OpenAPI, GraphQL, and Protobuf column-name fallback. |
| `openapi_name` | Exact OpenAPI property key for this column. |
| `graphql_name` | Exact GraphQL field identifier for this column. |
| `proto_name` | Exact lower-snake-case Protobuf field name for this column. |
| `api_type` | Contract-only type override shared by all three export targets. It must name a type Ptah maps or a declared enum. |
| `api_expose` | Contract exposure: `read`, `write`, `read-write`, or `none`. |
| `type` | SQL type or enum type name. |
| `nullable` | Explicit nullability. |
| `not_null` | Marks the column `NOT NULL`. |
| `primary` | Marks the column as a primary key. |
| `auto_increment` / `auto_inc` | Marks the column as auto-incrementing. |
| `identity_generation` | PostgreSQL identity mode: `ALWAYS` or `BY_DEFAULT`. |
| `identity_start` | Identity `START WITH` value; on YDB, the start of a Serial column's sequence. |
| `identity_increment` | Identity `INCREMENT BY` value; on YDB, the step of a Serial column's sequence. |
| `identity_options` | Raw PostgreSQL identity option clause. |
| `unique` | Adds a unique constraint. |
| `unique_expr` | Uniqueness over an expression. Not implemented; rendering refuses it rather than enforcing uniqueness on the column instead. |
| `index` | Requests an index for the column. |
| `generated` | Generated-column SQL expression. |
| `generated_kind` | Generated-column kind, such as `STORED` or `VIRTUAL`. |
| `stored` | Convenience boolean for `generated_kind: STORED`. |
| `default` | Literal default value. |
| `default_expr` | Default SQL expression, such as `NOW()`. |
| `foreign` | Foreign key reference in `table(column)` form. |
| `foreign_key_name` | Explicit foreign key constraint name. |
| `on_delete` / `on_update` | Foreign key actions. |
| `enum` | Inline enum values. |
| `check` | Column check expression. |
| `check_name` | Explicit column check constraint name. |
| `comment` | Column comment. |
| `platform` / `overrides` | Dialect-specific overrides. |

If `enum` is provided and `type` is empty or `ENUM`, Ptah creates a generated
enum type name and uses that type for the column.

API names resolve from the target-specific key, then `api_name`, then the
database name. GraphQL and Protobuf table values are stems that Ptah
singularizes and PascalCases into a type or message name; their column values
are exact field identifiers. API metadata changes generated OpenAPI, GraphQL,
and Protobuf contracts, not DDL or migration planning. Unknown keys, invalid
explicit target names, and per-target collisions fail before output. See
[API schema export](../../schema/export/#names-in-the-contract) for complete
semantics and examples.

A column needs a name. An empty column key, or an explicit `name: ""`, fails
rendering on every dialect with `table "<name>" declares a column that has no
name`; PostgreSQL answers `zero-length delimited identifier` and the MySQL
family answers `Incorrect column name ''` for the DDL that used to be produced.

## Indexes

An index sits under `tables.<table>.indexes`, or under the top-level `indexes`
map with a `table` key.

| Key | Meaning |
| --- | --- |
| `name` | Index name. Defaults to the map key. |
| `table` | Target table. Required for a top-level index. |
| `fields` / `columns` | Indexed columns. Required. |
| `include` | Covered columns: `INCLUDE` on the PostgreSQL family, `COVER` on YDB. |
| `unique` | Builds a unique index. |
| `type` | Dialect-specific index type; `async`, `vector_kmeans_tree`, `fulltext_plain` or `fulltext_relevance` on YDB. |
| Full-text analyzer attributes | YDB [full-text indexes](../../databases/ydb/#full-text-indexes). |
| `where` / `condition` | Partial-index condition where the target has one. |
| `ops` | Operator class. |
| `platform` | Source properties grouped by target. A selected provider decodes the keys it owns; ClickHouse reads `type` and `granularity` under `clickhouse` for a data-skipping index. |
| `comment` | Index comment. |
| `auto_partitioning_by_size`, `auto_partitioning_partition_size_mb`, `auto_partitioning_by_load`, `auto_partitioning_min_partitions_count`, `auto_partitioning_max_partitions_count`, `read_replicas_settings` | A YDB global index's [partitioning](../../databases/ydb/#index-partitioning), with the values the annotation attributes of the same names take. Every other dialect refuses them. |
| `distance`, `similarity`, `vector_type`, `vector_dimension`, `levels`, `clusters` | A YDB [vector index](../../databases/ydb/#vector-indexes)'s settings, with the values the annotation attributes of the same names take. Every other dialect refuses them. |

## Column families

A YDB table's column families sit under `tables.<table>.column_families`, keyed
by name. The keys are the attributes of `//ptah:schema:columnfamily`, with the
same values; `fields` is a list of the columns the family holds. Every other
dialect refuses a table that declares a column family. See
[column families](../../databases/ydb/#column-families).

```yaml
tables:
  documents:
    column_families:
      default:
        compression: lz4
      cold:
        data: hdd
        compression: lz4
        fields: [body, attachment]
```

## Changefeeds

A YDB table's changefeeds sit under `tables.<table>.changefeeds`, keyed by
name, and each changefeed's consumers under its `consumers` map, keyed by
name. The keys are the attributes of `//ptah:schema:changefeed` and
`//ptah:schema:changefeed:consumer`, with the same values; `supported_codecs`
is a list. Every other dialect refuses a table that declares a changefeed. See
[changefeeds](../../databases/ydb/#changefeeds).

```yaml
tables:
  orders:
    changefeeds:
      updates:
        mode: NEW_AND_OLD_IMAGES
        format: JSON
        retention_period: PT12H
        consumers:
          billing:
            important: true
          search:
            supported_codecs: [raw, gzip]
```

## Secrets

A YDB secret sits under `secrets`, keyed by name, with the attributes of
`//ptah:schema:secret`: `name` when the key is not the name, `schema` for its
directory, and `value_env` for the environment variable that holds the value,
whose name starts with `PTAH_SECRET_`. A document never holds the value: a
`value` key is refused, and the error names the key and not what it held. A
dot is part of the name, and a slash in it is refused. Every other dialect
refuses a secret. See [secrets](../../databases/ydb/#secrets).

```yaml
secrets:
  pg_password:
    schema: ext
    value_env: PTAH_SECRET_PG_PASSWORD
```

## External data sources and tables

A YDB external data source sits under `external_data_sources` and an external
table under `external_tables`, each keyed by name, with the attributes of
`//ptah:schema:externaldatasource` and `//ptah:schema:externaltable`. `options`
is a map of option names, in any letter case, to values, and each column of a
table has a `name`, a `type` and `not_null`. Every other dialect refuses both.
See [external data sources](../../databases/ydb/#external-data-sources-and-external-tables).

```yaml
external_data_sources:
  events_bucket:
    schema: ext
    source_type: ObjectStorage
    location: https://storage.example.test/events/
    auth_method: NONE
external_tables:
  events:
    schema: ext
    data_source: ext/events_bucket
    location: 2026/
    columns:
      - {name: id, type: Int64, not_null: true}
      - {name: kind, type: Utf8}
    options:
      FORMAT: json_each_row
```

## Platform overrides

Use `platform` when one dialect needs a different type or option:

```yaml
tables:
  users:
    columns:
      email:
        type: VARCHAR(255)
        not_null: true
        platform:
          mysql:
            type: VARCHAR(191)
```

Prefer overrides for real dialect differences. Do not use them to hide a schema
shape that the main IR cannot represent.

## Validate the file

Render before applying or generating migrations:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres >/tmp/schema.sql
```

The rendered SQL is the proof that Ptah understood the schema and dialect.
