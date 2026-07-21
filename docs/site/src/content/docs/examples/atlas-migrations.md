---
title: Atlas-style migrations example
description: Use an Atlas-style migration directory with Ptah.
---

Atlas-style migration files can include `migration.sql` and `down.sql` sections
inside txtar archives. Ptah executes those known sections and ignores unrelated
embedded files.

Create an Atlas-style migration:

```text
-- atlas:txtar

-- migration.sql --
CREATE TABLE users (
  id integer PRIMARY KEY,
  email text NOT NULL UNIQUE
);

-- down.sql --
DROP TABLE users;
```

Name the file with a migration version, for example:

```text
migrations/20260721120000_create_users.sql
```

Hash and validate the directory:

```bash
ptah migrations hash --dir ./migrations --dir-format atlas
ptah migrations validate --dir ./migrations --dir-format atlas
```

Apply it:

```bash
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --dir-format atlas
```

Use `ptah atlas migrate apply --dir ./migrations --url "$DATABASE_URL"` when you need the Atlas-compatible command path.

## Roll back

The `down.sql` section is used by rollback:

Use the native command when you need an explicit rollback target:

```bash
ptah migrations down \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --dir-format atlas \
  --target 0 \
  --confirm
```

The `ptah atlas migrate down` command path exists as a Ptah extension path, but
current conformance does not track it as an Atlas OSS drop-in target. Use the
native command for rollback recipes until the Atlas-compatible path documents
the same runtime contract.

## Troubleshooting

If validation or apply fails, force Atlas directory parsing with
`--dir-format atlas` and inspect the migration file for section names. Ptah
recognizes `migration.sql` and `down.sql`; other section names are not executed.
