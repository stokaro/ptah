---
title: YAML schema
description: Author the desired schema in Ptah's strict YAML format and feed it to render, compare, and migration commands.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I author the desired schema in Ptah's strict YAML format and feed it to render, compare, and migration commands?"
goal: "Render and validate a desired schema from Ptah YAML."
sourceOfTruth:
  - "cmd/schema"
  - "internal/schemaload"
generated: false
overlaps: []
disposition: keep
---

Use YAML when Ptah owns the schema file and you want compact, explicit input
without HCL syntax. The parser is strict: unknown keys fail instead of being
silently ignored, so a typo cannot masquerade as an intentional setting.

## Write a schema file

Create `schema.yaml`:

```yaml
tables:
  accounts:
    columns:
      id:
        type: SERIAL
        primary: true
      email:
        type: VARCHAR(255)
        not_null: true
    indexes:
      accounts_email_key:
        fields: [email]
        unique: true
extensions:
  pgcrypto:
    schema: extensions
    if_not_exists: true
```

## Render it

Preview the SQL Ptah derives before any database is involved:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

Expected output includes:

```sql
CREATE SCHEMA IF NOT EXISTS "extensions";

CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";

CREATE TABLE "accounts" (
  "id" SERIAL PRIMARY KEY NOT NULL,
  "email" VARCHAR(255) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "accounts_email_key" ON "accounts" ("email");
```

The rendered SQL proves Ptah understood the desired schema. `--schema-file` is
accepted wherever Ptah needs a desired schema: `ptah schema render`,
`ptah schema compare`, `ptah schema drift`, the migration commands
(`ptah migrations plan` / `ptah migrations generate`), and every target of
[`ptah schema export`](../export/#sources) except `hcl`. That includes the two
documentation targets, so
[a Markdown or HTML reference](../document/) can be generated from this file.

Path confinement is shared by every `--schema-file` source; see
[Schema file paths](../../reference/native-commands/#schema-file-paths).

## Use it

Everything a desired schema is for — comparing, gating on drift, generating
migrations, applying directly, composing sources, validating across dialects —
is the same for every source and lives on
[Work with a desired schema](../work-with-a-source/). For YAML the flag is
`--schema-file`. Plan first, then generate files only after reviewing the plan:

```bash
ptah migrations plan \
  --schema-file schema.yaml \
  --db-url "$DATABASE_URL"

ptah migrations generate \
  --schema-file schema.yaml \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations
```

Then seal and check the migration directory:

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

## Next steps

- Need the exact accepted keys and shapes? [YAML schema reference](../../reference/yaml-schema/).
- Combining YAML with Go packages or other files? [Composite desired schema](../composite/).
- Ready to run the lifecycle? [Versioned migrations](../../versioned/overview/).
