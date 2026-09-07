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
| `rls_policies` | Row-level security policies. |
| `roles` | PostgreSQL role declarations. |
| `grants` | Table or schema permission grants. |

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
| `rls_enabled` | Enables row-level security for the table. |
| `platform` / `overrides` | Dialect-specific override map. |

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
| `identity_start` | PostgreSQL identity `START WITH` value. |
| `identity_increment` | PostgreSQL identity `INCREMENT BY` value. |
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
