# Atlas HCL Schema Input

Ptah can generate SQL from an Atlas schema HCL file instead of scanning Go
source annotations. The Atlas HCL frontend builds the same `goschema.Database`
intermediate representation as Go annotations and YAML schema input, then runs
the normal finalization, dependency ordering, AST conversion, and dialect
renderers.

Use Atlas HCL input when an existing Atlas schema file should be used as a Ptah
schema source.

## Generate SQL

```bash
go run ./cmd generate --schema-file schema.hcl --dialect postgres
```

`--schema-file` accepts `.hcl` files for Atlas HCL input, plus `.yaml` and
`.yml` files for YAML input. When it is set, `--root-dir` is ignored. If
`--dialect` is omitted, Ptah renders every supported dialect.

## Supported Shape

The parser supports the schema-object subset that maps directly to Ptah's
current schema IR:

- `schema` labels and `comment`, for table namespace references such as
  `schema = schema.main`
- `table` blocks
- `column` blocks with `type`, `null`, `auto_increment`, `unique`, `default`,
  and `comment`
- `primary_key` blocks with `columns`
- `index` blocks with `columns`, `on { column = ... }`, `unique`, `type`, and
  `where`
- `foreign_key` blocks with one local `columns` entry and one table-qualified
  `ref_columns` entry
- `check` blocks with `expr`
- `default = sql("...")` as a default expression

Unsupported schema semantics are rejected with an explicit parse error instead
of being silently dropped from the generated Ptah IR.

## Minimal Example

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
    unique = true
    columns = [column.email]
  }
}
```

## Foreign Key Example

```hcl
table "users" {
  column "id" {
    type = int
  }

  primary_key {
    columns = [column.id]
  }
}

table "posts" {
  column "id" {
    type = int
  }

  column "owner_id" {
    type = int
    null = true
  }

  foreign_key "owner_id" {
    columns = [column.owner_id]
    ref_columns = [table.users.column.id]
    on_delete = SET_NULL
  }
}
```

## Current Limitations

The Atlas HCL frontend is intentionally conservative. It does not yet model
Atlas features that Ptah cannot represent without losing semantics, including:

- schema-level charset and collation attributes
- generated columns
- composite foreign keys
- index prefix parts
- Atlas project `env` execution semantics
- Atlas HCL objects outside direct schema definitions, such as variables,
  realms, extensions, and other dialect-specific object types

Project-level `env` and `variable` blocks may appear next to schema objects, but
they are not executed by `ptah generate --schema-file`.
