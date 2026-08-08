---
title: HCL schema
description: Reuse Atlas-compatible HCL schema files as Ptah's desired schema.
---

Use HCL schema files when you already maintain schema files in Atlas-compatible
HCL syntax and want Ptah to read the supported subset. Unsupported constructs
fail explicitly instead of being silently guessed.

## Write a schema file

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

  column "created_at" {
    type = timestamptz
  }

  index "accounts_email_key" {
    unique = true
    columns = [column.email]
  }

  unique "accounts_email_covering" {
    columns = [column.email]
    include = [column.created_at]
  }
}
```

## Render it

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

Expected output includes:

```sql
CREATE SCHEMA IF NOT EXISTS "public";

CREATE TABLE "public"."accounts" (
  "id" int NOT NULL,
  "email" varchar(255) NOT NULL,
  "created_at" timestamptz NOT NULL,
  CONSTRAINT "accounts_email_covering" UNIQUE ("email") INCLUDE ("created_at")
);

CREATE UNIQUE INDEX IF NOT EXISTS "accounts_email_key" ON "public"."accounts" ("email");
```

`--schema-file` is accepted wherever Ptah needs a desired schema:
`ptah schema render`, `ptah schema compare`, `ptah schema drift`, and the
migration commands (`ptah migrations plan` / `ptah migrations generate`).

To replace Go annotations with an HCL source, use the review-aware one-time
export workflow in [Go annotations](../go-annotations/#move-the-schema-to-hcl).

## Plan against a database

```bash
ptah migrations plan \
  --schema-file schema.hcl \
  --db-url "$DATABASE_URL"
```

## Schema HCL is not project config

Ptah reads schema HCL as desired-schema input. An `atlas.hcl` project
configuration is a different file type: it provides database URLs, migration
directories, and environment selection, while a schema HCL file provides
desired schema objects. See [Configuration](../../reference/configuration/) and
the [Atlas project config subset](../../atlas/project-config/).

:::caution[Supported subset]
Ptah's HCL schema format is compatible with the Atlas HCL schema language for
the supported subset and adds documented Ptah extensions for Go annotation
parity. Export reports opaque SQL bodies and any byte-level
normalization before automatic cleanup. Cleanup also verifies that every
removable directive produced parsed schema intent. Ptah is not affiliated with
or endorsed by Ariga or Atlas. If a construct is not implemented, the command
fails with an explicit error; treat that as a compatibility gap and check the
conformance reports.
:::

## Next steps

- Need the supported blocks and types? [HCL schema reference](../../reference/hcl-schema/).
- Combining HCL with Go packages or other files? [Composite desired schema](../composite/).
- Coming from Atlas commands, not only Atlas files? [Atlas compatibility overview](../../atlas/overview/).
