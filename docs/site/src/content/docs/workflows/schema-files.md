---
title: Schema files
description: Use YAML, Atlas HCL, or SQL schema files as Ptah input.
---

Ptah can render and migrate from schema files when Go annotations are not the
source of truth.

## Pick a source format

| Format | Best for | Notes |
| --- | --- | --- |
| Ptah YAML | Ptah-owned schema files with compact structure. | Strict parser; unknown keys fail. |
| Atlas HCL schema | Reusing supported Atlas schema files. | Supported subset only; unsupported constructs fail. |
| SQL schema | Reusing local SQL DDL files for render and Atlas-compatible local diff workflows. | Parsed through Ptah's compatibility SQL parser; unsupported DDL fails explicitly. |
| Live database | Introspection, drift checks, and migration planning. | Requires a database URL. |

## YAML schema

YAML is Ptah-owned and strict. Use it when you want a compact, explicit schema file without Atlas HCL syntax:

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
```

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

Use the same input to plan against a live database:

```bash
ptah migrations plan \
  --schema-file schema.yaml \
  --db-url "$DATABASE_URL"
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

Ptah reads schema HCL as desired schema input. Project configuration HCL is a
different file type and is described in [Configuration](../../reference/configuration/).

Reference: [Atlas HCL schema](https://github.com/stokaro/ptah/blob/master/docs/atlas_hcl_schema.md).

:::caution[Supported subset]
Ptah intentionally rejects unsupported Atlas HCL constructs instead of silently guessing. If a construct is not implemented, treat the error as a compatibility gap and check the conformance reports.
:::

## SQL schema

Use SQL schema files when the desired state is already represented as local DDL:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
```

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
```

The same local SQL files can be compared through the Atlas-compatible command
surface:

```bash
ptah atlas schema diff \
  --from file://old.sql \
  --to file://schema.sql \
  --dev-url "sqlite://dev?mode=memory"
```

## Validate before applying

Keep schema-file workflows reviewable:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres >/tmp/schema.sql
ptah migrations plan --schema-file schema.yaml --db-url "$DATABASE_URL" >/tmp/plan.sql
```

Review both files. The render output proves Ptah understood the desired schema;
the plan output proves what would change in the target database.
