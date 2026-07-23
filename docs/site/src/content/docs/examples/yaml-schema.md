---
title: YAML schema example
description: A minimal Ptah YAML schema file.
---

Use YAML when Ptah owns the schema file and you want strict, compact input.

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

Render it:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

Expected output includes a `CREATE TABLE` statement for `accounts` and a unique
index or constraint for `email`, depending on dialect rendering.

## Use it for migrations

```bash
ptah migrations plan \
  --schema-file schema.yaml \
  --db-url "$DATABASE_URL"

ptah migrations generate \
  --schema-file schema.yaml \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations
```

Then hash and validate the directory:

```bash
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
```

Use YAML for Ptah-owned schema files. See the full reference in
[YAML schema](../../reference/yaml-schema/).
