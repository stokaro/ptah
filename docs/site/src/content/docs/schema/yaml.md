---
title: YAML schema
description: Author the desired schema in Ptah's strict YAML format and feed it to render, compare, and migration commands.
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
```

## Render it

Preview the SQL Ptah derives before any database is involved:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

Expected output includes:

```sql
CREATE TABLE "accounts" (
  "id" SERIAL PRIMARY KEY NOT NULL,
  "email" VARCHAR(255) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "accounts_email_key" ON "accounts" ("email");
```

The rendered SQL proves Ptah understood the desired schema. `--schema-file` is
accepted wherever Ptah needs a desired schema: `ptah schema render`,
`ptah schema compare`, `ptah schema drift`, and the migration commands
(`ptah migrations plan` / `ptah migrations generate`).

## Use it for migrations

Plan first, then generate files only after reviewing the plan:

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
- Ready to run the lifecycle? [Migrations](../../workflows/migrations/).
