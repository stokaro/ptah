---
title: Schema files
description: Use YAML schema files or Atlas HCL schema files as Ptah input.
---

Ptah can render and migrate from schema files when Go annotations are not the source of truth.

## YAML schema

YAML is Ptah-owned and strict. Use it when you want a compact, explicit schema file without Atlas HCL syntax:

```yaml
tables:
  users:
    columns:
      id:
        type: int
        primary_key: true
      email:
        type: varchar
        nullable: false
```

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

Reference: [YAML schema](https://github.com/stokaro/ptah/blob/master/docs/yaml_schema.md).

## Atlas HCL schema

Use Atlas HCL when you already maintain schema files in Atlas syntax and want Ptah to read the supported subset:

```hcl
schema "public" {}

table "users" {
  schema = schema.public

  column "id" {
    type = int
  }

  column "email" {
    type = varchar(255)
    null = false
  }
}
```

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

Reference: [Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md).

:::caution[Supported subset]
Ptah intentionally rejects unsupported Atlas HCL constructs instead of silently guessing. If a construct is not implemented, treat the error as a compatibility gap and check the conformance reports.
:::
