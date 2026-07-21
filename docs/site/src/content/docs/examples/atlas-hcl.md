---
title: Atlas HCL example
description: A minimal supported Atlas HCL schema file.
---

Use this example when you already have Atlas HCL schema files and want Ptah to
read the supported schema subset.

Create `schema.hcl`:

```hcl
schema "public" {}

table "accounts" {
  schema = schema.public

  column "id" {
    type = int
  }

  column "email" {
    type = varchar(255)
    null = false
  }

  index "accounts_email_key" {
    unique = true
    columns = [column.email]
  }
}
```

Render it:

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

## Plan against a database

```bash
ptah migrations plan \
  --schema-file schema.hcl \
  --db-url "$DATABASE_URL"
```

Use `atlas.hcl` project config separately from schema HCL. A project config can
provide database URLs, migration directories, and environment selection; a
schema HCL file provides desired schema objects.

Ptah supports a deliberate Atlas HCL subset. Unsupported constructs are errors,
not silent no-ops. See
[Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md)
and [Atlas project config subset](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md).
