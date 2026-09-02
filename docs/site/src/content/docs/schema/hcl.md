---
title: HCL schema
description: Reuse Atlas-compatible HCL schema files as Ptah's desired schema.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I reuse Atlas-compatible HCL schema files as Ptah's desired schema?"
goal: "Render a desired schema from Atlas-compatible HCL."
sourceOfTruth:
  - "cmd/schema"
  - "internal/schemaload"
  - "internal/atlashcl"
  - "internal/atlashclrender"
generated: false
overlaps: []
disposition: keep
sourceMode: static-file-only
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

schema "extensions" {}

extension "pgcrypto" {
  schema        = schema.extensions
  if_not_exists = true
}
```

## Render it

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

Expected output includes:

```sql
CREATE SCHEMA IF NOT EXISTS "public";

CREATE SCHEMA IF NOT EXISTS "extensions";

CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";

CREATE TABLE "public"."accounts" (
  "id" int NOT NULL,
  "email" varchar(255) NOT NULL,
  "created_at" timestamptz NOT NULL,
  CONSTRAINT "accounts_email_covering" UNIQUE ("email") INCLUDE ("created_at")
);

CREATE UNIQUE INDEX IF NOT EXISTS "accounts_email_key" ON "public"."accounts" ("email");
```

`--schema-file` is accepted wherever Ptah needs a desired schema:
`ptah schema render`, `ptah schema compare`, `ptah schema drift`, the
migration commands (`ptah migrations plan` / `ptah migrations generate`), and
every target of [`ptah schema export`](../export/#sources) except `hcl`, whose
source is `--root-dir`. That includes the two documentation targets, so
[a Markdown or HTML reference](../document/) can be generated from this file.

Path confinement is shared by every `--schema-file` source; see
[Schema file paths](../../reference/native-commands/#schema-file-paths).

To replace Go annotations with an HCL source, use the review-aware one-time
export workflow in [Go annotations](../go-annotations/#move-the-schema-to-hcl).

## Declare API export metadata

Ptah HCL extends the Atlas-compatible table and column blocks with contract
metadata. Tables accept `api_name`, `openapi_name`, `graphql_name`, and
`proto_name`. Columns accept those four attributes plus `api_type` and
`api_expose`:

```hcl
table "billing_invoices" {
  api_name     = "invoices"
  openapi_name = "invoice_documents"
  graphql_name = "invoice_records"
  proto_name   = "invoice_records"

  column "billing_amount_minor" {
    type         = integer
    api_name     = "amount"
    openapi_name = "amount_value"
    graphql_name = "amountMinor"
    proto_name   = "amount_minor"
    api_type     = "TEXT"
    api_expose   = "read"
  }
}
```

The target-specific name wins over `api_name`, which wins over the database
name. Table GraphQL and Protobuf values are type/message stems; column GraphQL
and Protobuf values are exact field names. `api_type` changes only the generated
contract type, and `api_expose` accepts `read`, `write`, `read-write`, or
`none`. Canonical HCL export and OCI schema artifacts preserve all six column
attributes and all four table attributes. Unknown or non-string attributes fail
before output. See [API schema export](../export/#names-in-the-contract) for
target naming rules and collision behavior.

These attributes are Ptah extensions, not Atlas CE schema syntax. Native Ptah
and the default compatibility profile preserve them; strict Atlas CE mode
refuses a schema that contains API export metadata instead of discarding it.

## Use it

Everything a desired schema is for — comparing, gating on drift, generating
migrations, applying directly, composing sources, validating across dialects —
is the same for every source and lives on
[Work with a desired schema](../work-with-a-source/). For HCL the flag is
`--schema-file`:

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
