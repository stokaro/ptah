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
go run ./cmd schema render --schema-file schema.hcl --dialect postgres
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
  `identity`, and `comment`
- `primary_key` blocks with `columns`; PostgreSQL primary keys also support
  `include`
- `index` blocks with `columns`, `on { column = ..., prefix = ... }`,
  `on { expr = "..." }`, `desc`, `unique`, `type`, and `where`; PostgreSQL
  indexes also support `include`, BRIN `page_per_range`, and
  `nulls_distinct`
- `unique` blocks with `columns` and PostgreSQL `nulls_distinct`
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

  column "bio" {
    type = text
  }

  primary_key {
    columns = [column.id]
  }

  index "idx_users_email" {
    unique = true
    columns = [column.email]
  }

  index "idx_users_bio" {
    type = FULLTEXT
    parser = ngram
    columns = [column.bio]
  }
}
```

## PostgreSQL Primary Key Include Example

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
}
```

## PostgreSQL Index Include Example

```hcl
table "users" {
  column "name" {
    type = text
  }

  column "active" {
    type = bool
  }

  index "idx_users_name" {
    columns = [column.name]
    include = [column.active]
  }
}
```

## PostgreSQL BRIN Storage Parameter Example

```hcl
table "users" {
  column "c" {
    type = int
  }

  index "idx_users_c" {
    type = BRIN
    columns = [column.c]
    page_per_range = 2
  }
}
```

## PostgreSQL NULLS NOT DISTINCT Example

```hcl
table "users" {
  column "c" {
    type = int
  }

  index "users_c_idx" {
    unique = true
    columns = [column.c]
    nulls_distinct = false
  }

  unique "users_c_key" {
    columns = [column.c]
    nulls_distinct = false
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
  column "slug" {
    type = text
    as = "lower(name)"
  }
  column "name_key" {
    type = text
    as {
      expr = "lower(name)"
      type = STORED
    }
  }

  foreign_key "owner_id" {
    columns = [column.owner_id]
    ref_columns = [table.users.column.id]
    on_delete = SET_NULL
  }
}
```

## PostgreSQL Identity Columns

Atlas-style `identity` blocks map to PostgreSQL `GENERATED ... AS IDENTITY`
columns:

```hcl
table "users" {
  column "id" {
    type = int
    null = false
    identity {
      generated = BY_DEFAULT
      start = 10
      increment = 5
    }
  }
}
```

`generated` accepts `ALWAYS` or `BY_DEFAULT`. When omitted, Ptah follows
PostgreSQL and Atlas defaults and renders `BY DEFAULT`. Ptah currently supports
the Atlas `start` and `increment` identity options in HCL input. Other identity
block attributes are rejected instead of being silently dropped.

## Current Limitations

The Atlas HCL frontend is intentionally conservative. It does not yet model
Atlas features that Ptah cannot represent without losing semantics, including:

- composite foreign keys
- Atlas HCL objects outside direct schema definitions, such as variables,
  realms, extensions, and other dialect-specific object types

Project-level `env` and `variable` blocks may appear next to schema objects in
schema HCL files, but they are not executed by `ptah schema render --schema-file`.
Command-level `atlas.hcl` project config support is documented in
[Atlas Project Config](atlas_project_config.md).
