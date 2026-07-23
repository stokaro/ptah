---
title: HCL Schema Reference
description: Atlas-compatible HCL schema subset supported by Ptah.
---

Ptah can read HCL schema files as desired schema input. The parser builds the
same schema IR as Go annotations and YAML schema files, then uses Ptah's normal
rendering and planning paths.

Ptah's HCL schema syntax is compatible with the supported subset of Atlas HCL
schema files. Ptah is an independent implementation and is not affiliated with
or endorsed by Ariga or Atlas.

## Command

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

`--schema-file` accepts `.hcl` files for HCL schema input. Project
configuration in `atlas.hcl` is a different file type; see
[Atlas project config](../atlas-project-config/).

## Minimal Schema

```hcl
schema "main" {}

table "users" {
  schema = schema.main

  column "id" {
    type = int
  }

  column "email" {
    type = varchar(255)
    null = false
  }

  primary_key {
    columns = [column.id]
  }

  index "idx_users_email" {
    unique  = true
    columns = [column.email]
  }
}
```

## Supported Object Subset

| Object | Supported shape |
| --- | --- |
| `schema` | Labels and comments for table namespace references. |
| `table` | Table blocks with columns, primary keys, indexes, uniques, foreign keys, checks, and row security. |
| `column` | `type`, `null`, `auto_increment`, `unique`, `default`, `identity`, and comments. |
| `primary_key` | `columns`; PostgreSQL also supports `include`. |
| `index` | `columns`, `on { column = ... }`, `on { expr = ... }`, `desc`, `unique`, `type`, `where`, and PostgreSQL include/storage options. |
| `unique` | `columns`; PostgreSQL also supports `include` and `nulls_distinct`. |
| `foreign_key` | One local `columns` entry and one table-qualified `ref_columns` entry. |
| `check` | `expr`. |
| `extension` | PostgreSQL `version` and comments. |
| `role` | PostgreSQL role attributes such as `login`, `superuser`, `create_db`, and `inherit`. |
| `permission` | PostgreSQL table and schema permissions. |
| `function` | PostgreSQL function metadata and raw body. |
| `view` / `materialized` | SQL body plus schema and comments. |
| `trigger` | Trigger timing, target, execution mode, function body, and comments. |
| `policy` | PostgreSQL RLS policy fields. |

Unsupported semantics fail explicitly. Ptah does not silently drop HCL objects
that it cannot represent in the schema IR.

## PostgreSQL Include Columns

```hcl
table "users" {
  column "id" {
    type = int
  }

  column "covering" {
    type = int
  }

  primary_key {
    columns = [column.id]
    include = [column.covering]
  }

  index "idx_users_id" {
    columns = [column.id]
    include = [column.covering]
  }
}
```

Ptah preserves supported include columns through HCL parsing, SQL rendering,
SQL parsing, schema diffing, and database introspection paths where the dialect
supports the feature.

## Function Bodies

Function bodies are stored as raw SQL text. Ptah does not parse the dialect
sub-language inside each function body today. That is intentional: PostgreSQL,
MySQL, SQL Server, and other dialects have different procedural languages and
require dialect-specific parsers.

## Unsupported Constructs

Unsupported HCL constructs return errors rather than partial output. Treat these
errors as compatibility gaps. Check [Conformance](../../operate/conformance/)
and [Atlas docs coverage](../atlas-docs-coverage/) before deciding whether the
gap is already tracked.
