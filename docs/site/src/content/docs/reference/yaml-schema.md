---
title: YAML Schema Reference
description: Ptah's strict YAML schema-file format.
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

`--schema-file` accepts `.yaml`, `.yml`, `.hcl`, and `.sql` inputs. This page
documents the YAML shape only.

## Minimal Schema

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

## Top-Level Objects

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

## Tables

Each entry under `tables` declares one table.

| Key | Meaning |
| --- | --- |
| `name` | Database table name. Defaults to the map key. |
| `struct_name` | Internal Go-schema owner name. Defaults to the map key. |
| `engine` | Table engine value for dialects that support it. |
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
| `unique_expr` | Unique expression. |
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

## Platform Overrides

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

## Validate The File

Render before applying or generating migrations:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres >/tmp/schema.sql
```

The rendered SQL is the proof that Ptah understood the schema and dialect.
