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

Use `ptah atlas migrate apply --dir ./migrations --url "$DATABASE_URL"` when
you need the Atlas-compatible command path. That path uses Atlas revision-table
metadata by default and supports Atlas-style apply controls such as positional
`amount`, `--baseline`, `--allow-dirty`, `--tx-mode`, `--exec-order`,
`--revisions-schema`, `--lock-timeout`, `--dry-run`, and Go-template
`--format` output.

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

The `ptah atlas migrate down` command path exists and forwards to native Ptah
rollback behavior. It maps compatible Atlas flags such as `--url`, `--dir`,
`--to-version`, `--dry-run`, `--revisions-schema`, and `--lock-timeout`.
Atlas dynamic down-planning flags such as `--dev-url`, `--to-tag`,
`--skip-checks`, and `--plan` fail explicitly until Ptah implements equivalent
planning behavior. Atlas Go template output formatting via `--format` also fails
explicitly until Ptah supports that output contract.

## Troubleshooting

If validation or apply fails, force Atlas directory parsing with
`--dir-format atlas` and inspect the migration file for section names. Ptah
recognizes `migration.sql` and `down.sql`; other section names are not executed.
