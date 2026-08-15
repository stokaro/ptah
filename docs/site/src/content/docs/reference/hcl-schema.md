---
title: HCL schema reference
description: Atlas-compatible HCL schema subset and Ptah parity extensions.
---

Ptah can read HCL schema files as desired schema input. The parser builds the
same schema IR as Go annotations and YAML schema files, then uses Ptah's normal
rendering and planning paths.

Ptah's HCL schema syntax includes a supported subset of Atlas HCL schema files
plus Ptah extensions for Go annotation parity. Ptah is an independent
implementation and is not affiliated with or endorsed by Ariga or Atlas.

## Command

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

`--schema-file` accepts `.hcl` files for HCL schema input. Project
configuration in `atlas.hcl` is a different file type; see
[Atlas project config](../../atlas/project-config/).

Relative schema-file inputs are confined to the process working directory after
symbolic-link resolution; use an absolute pathname for an intentional source
outside it, as detailed under [schema file paths](../native-commands/#schema-file-paths).

`ptah schema fmt [path ...]` rewrites `.hcl` schema files into HashiCorp
HCL's canonical layout, walking directory arguments recursively and printing
only the files that changed. `--check` reports non-canonical files without
rewriting them and exits non-zero, for CI formatting gates.

## Minimal schema

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

### How many `schema` blocks a document may declare

A document may declare as many schemas as the run can reach. A run whose URL
names one schema — any SQLite URL, a PostgreSQL-family URL carrying
`search_path=<one name>`, a MySQL-family URL naming a database — reaches one,
and a document declaring more than one top-level `schema` block is refused
there rather than narrowed. The count is of blocks: repeating
`schema "main" {}` in two files of a schema directory is two.

This holds on the native commands, not only the compatibility binary:
`ptah schema inspect --schema-file two-schemas.hcl --dev-url sqlite://dv?mode=memory`
refuses, because narrowing a desired state to the scope and reporting success
is a wrong answer wherever it happens. Give the run a realm-scoped URL to
describe every schema the document declares. A desired state with no URL to be
scoped by — Go annotation roots, `ptah schema test` — is unaffected.

See
[the Atlas-compatible schema commands](../../atlas/schema-commands/#a-schema-limited-run-refuses-a-multi-schema-hcl-desired-state)
for the message and the flag it names.

## Supported object subset

| Object | Supported shape |
| --- | --- |
| `schema` | Labels and comments for table namespace references. |
| `table` | Columns, keys, indexes, constraints, checks, row security, and Ptah `checks`, `custom`, and `platform` extensions. |
| `column` | Type, nullability, defaults, generated/identity metadata, comments, checks, and Ptah `enum` and `platform` extensions. |
| `primary_key` | `columns`; PostgreSQL also supports `include`. |
| `index` | `columns`, `on { column = ... }`, `on { expr = ... }`, `desc`, `on { nulls_first = ... }` or `on { nulls_last = ... }`, `unique`, `type`, `where`, `comment`, ClickHouse `granularity`, and PostgreSQL include/storage options. |
| `constraint` | Ptah block used when annotation metadata cannot fit the Atlas-native constraint blocks, and for `EXCLUDE` constraints. |
| `unique` | `columns`; PostgreSQL also supports `include` and `nulls_distinct`. |
| `foreign_key` | One local `columns` entry and one table-qualified `ref_columns` entry. |
| `check` | `expr`. |
| `enum` | `values`, plus the `schema` that owns the type. A PostgreSQL enum is created in that schema and a column declared against it is qualified with it. |
| `extension` | PostgreSQL installation `schema`, `if_not_exists`, `version`, and comments. |
| `role` | PostgreSQL role attributes, including `password`. |
| `permission` | PostgreSQL table, schema, and sequence permissions. |
| `function` | PostgreSQL metadata and raw body, with Atlas-style `arg` blocks or a Ptah raw `params` string. |
| `view` / `materialized` | SQL body plus schema and comments; `materialized` also parses `refresh_strategy`. |
| `trigger` | Trigger timing, target, execution mode, function body, and comments. |
| `policy` | PostgreSQL RLS policy fields. |
| `sequence` | PostgreSQL `type`, `start`, `increment`, `min_value`, `max_value`, `cache`, `cycle`, `owned_by`, and `if_not_exists`. |
| `domain` | PostgreSQL `type`, `null`, `default`, and `check`. |
| `composite` | PostgreSQL composite type with ordered `field` sub-blocks. |
| `range` | PostgreSQL `subtype`, `subtype_opclass`, `collation`, `canonical`, and `subtype_diff`. |
| `data` | Ptah managed-data declaration with a table reference, key columns, and a file path relative to the HCL file. |

`refresh_strategy` defaults to `manual`, which means Ptah emits no separate
refresh operation. It is the only currently supported value. After a target
dialect is selected, another value is refused before rendering or comparison;
the error names the dialect, materialized view, and value. The HCL codec keeps
the attribute so target validation can diagnose it instead of silently
dismissing it.

Every `schema "pg_catalog" {}` or `schema "information_schema" {}` block is an
explicit schema declaration, even when an extension also refers to it, and is
refused before SQL, including when comparison would otherwise report no changes.
To preserve an extension already installed in a
server-owned namespace without requesting `CREATE SCHEMA`, write the placement
as a string, for example `schema = "pg_catalog"`, and omit the schema block.
Ptah's generated HCL uses that spelling. CockroachDB likewise refuses its exact
`crdb_internal` namespace. Quoted lookalikes such as
`schema "PG_CATALOG" {}` or `schema "CRDB_INTERNAL" {}` remain user schemas.

The `extension.schema` attribute accepts an HCL string template or an exact
one-name schema traversal such as `schema.extensions` or
`schema["Extension Store"]`. A string template may evaluate declared variables;
a direct `var.*` traversal, another object namespace such as `table.*`, and an
over-qualified traversal such as `schema.extensions.extra` are refused instead
of being reinterpreted as schema names.

The two-label form `extension "extensions" "citext" {}` uses its first label
as the installation schema. If the block also carries a `schema` attribute,
that value must resolve exactly to the first label. An explicit empty value is
still present, so `schema = ""` conflicts with a nonempty schema label; on the
one-label form it explicitly selects the target's default schema.

Unsupported semantics fail explicitly. Ptah does not silently drop HCL objects
that it cannot represent in the schema IR. The Ptah `ops` index attribute
preserves a Go annotation operator class.

## Variables, locals, and expressions

A schema file may declare `variable` and `locals` blocks and read them back
through the `var.` and `local.` namespaces:

```hcl
variable "status" {
  type    = string
  default = "active"
}

locals {
  state_column = "state_${var.status}"
}

schema "app" {}

table "t" {
  schema = schema.app
  column "state" {
    type    = text
    default = var.status
    comment = local.state_column
  }
}
```

A `variable` block requires `type`, which is one of `bool`, `int`, `number`,
`string`, or `list`, `map` or `set` of those. It accepts `default` and
`description`. A variable with no `default` needs a value from `--var`:

```bash
ptah-compat schema diff --dev-url "sqlite://file?mode=memory" \
  --from file://empty.hcl --to file://schema.hcl --var status=live
```

One `--var` occurrence carries comma-separated `name=value` assignments, and the
flag may be repeated. A variable that ends with no value fails with
`missing value for required variable "status"`. `--var` does not require an
`atlas.hcl`; when one is present it also supplies that file's variables.

Attribute values are evaluated. A function call resolves against the function
set, a `var.` or `local.` reference resolves against the blocks above, and
anything that will not resolve is an error rather than the expression's own
source text. References that name schema objects — `schema.app`, `column.state`,
`enum.status` — and type expressions such as `text` or `varchar(255)` are read
as written and are not evaluated, so a `var.` reference in a `type` is rejected.

## Go annotation parity

Every schema semantic accepted by Ptah's Go annotation parser has an HCL
representation. Export may use Ptah-specific blocks and attributes when the
Atlas-compatible shape would lose information:

```hcl
schema "app" {}

enum "enum_user_status" {
  values = ["active", "disabled"]
}

table "users" {
  schema = schema.app
  checks = ["id > 0"]
  custom = "WITHOUT OIDS"

  column "status" {
    type = enum_user_status
    enum = ["active", "disabled"]
  }

  platform "mysql" {
    override "engine" {
      value = "InnoDB"
    }
  }

  constraint "users_no_overlap" {
    type      = "EXCLUDE"
    using     = "gist"
    elements  = "id WITH ="
    condition = "id > 0"
  }
}

function "lookup_user" {
  params = "IN user_id BIGINT, OUT display_value DOUBLE PRECISION"
  return = "DOUBLE PRECISION"
  lang   = SQL
  as     = "SELECT user_id::double precision"
}

data {
  table = table.users
  keys  = ["id"]
  file  = "users.yaml"
}
```

Embedded Go annotations export as finalized concrete columns and foreign keys.
Go struct and field names are provenance rather than schema semantics and are
intentionally not written to HCL. Role passwords are written as string
literals, and Ptah forces generated files containing them to owner-only `0600`
permissions. Treat those files as sensitive.

## PostgreSQL include columns

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

## Function bodies

Function bodies are stored as raw SQL text. Ptah does not parse the dialect
sub-language inside each function body today. That is intentional: PostgreSQL,
MySQL, SQL Server, and other dialects have different procedural languages and
require dialect-specific parsers. Go-annotation export writes function, view,
materialized-view, and trigger bodies as opaque HCL strings and reports a
warning for each body. Because cleanup is destructive, any such warning prevents
`--cleanup-go-annotations` from publishing HCL or removing annotations.

Cleanup also refuses any recognized standalone Go directive that is attached at
an unsupported scope or did not produce a parsed schema object. The diagnostic
names the source file and line, and the operation leaves both the output and Go
sources unchanged. Near-prefix comments are not directives and are preserved.

## Unsupported constructs

Unsupported HCL constructs return errors rather than partial output. Treat these
errors as compatibility gaps. Check [Conformance](../../atlas/conformance/)
and [Atlas docs coverage](../../atlas/docs-coverage/) before deciding whether the
gap is already tracked.
