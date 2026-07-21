---
title: Atlas HCL example
description: A minimal supported Atlas HCL schema file.
---

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

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

Ptah supports a deliberate Atlas HCL subset. Unsupported constructs are errors, not silent no-ops. See [Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md).
