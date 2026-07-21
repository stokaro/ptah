---
title: Atlas-style migrations example
description: Use an Atlas-style migration directory with Ptah.
---

Atlas-style migration files can include `migration.sql` and `down.sql` sections inside txtar archives.

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
