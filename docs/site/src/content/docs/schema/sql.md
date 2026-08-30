---
title: SQL schema
description: Use plain SQL DDL files as Ptah's desired schema.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I use plain SQL DDL files as Ptah's desired schema?"
goal: "Render a desired schema from SQL DDL."
sourceOfTruth:
  - "cmd/schema"
  - "internal/schemaload"
generated: false
overlaps: []
disposition: keep
---

Use SQL schema files when the desired schema is already written as local DDL
(Data Definition Language). Ptah parses the file through its compatibility SQL
parser; unsupported DDL fails explicitly instead of being skipped.

## Write a schema file

Create `schema.sql`:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
```

PostgreSQL extension placement is preserved too. In a separate
`extensions.sql`, Ptah accepts both the optional `WITH` spelling and the bare
`SCHEMA` clause:

```sql
CREATE SCHEMA extensions;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions VERSION '1.3';
```

Render that PostgreSQL-specific file with the PostgreSQL dialect:

```bash
ptah schema render --schema-file extensions.sql --dialect postgres
```

Expected output includes the schema precondition before the extension:

```sql
CREATE SCHEMA IF NOT EXISTS "extensions";

CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions" VERSION '1.3';
```

## Render it

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
```

Expected output includes:

```sql
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL
);
```

Rendering SQL back out of a SQL file is not a no-op: it proves the parser
understood every statement, and it can retarget the schema at another dialect.
`--schema-file` is accepted wherever Ptah needs a desired schema:
`ptah schema render`, `ptah schema compare`, `ptah schema drift`, the
migration commands (`ptah migrations plan` / `ptah migrations generate`), and
every target of [`ptah schema export`](../export/#sources) except `hcl`. That
includes the two documentation targets, so
[a Markdown or HTML reference](../document/) can be generated from this file.

Path confinement is shared by every `--schema-file` source; see
[Schema file paths](../../reference/native-commands/#schema-file-paths).

## Use it

Everything a desired schema is for — comparing, gating on drift, generating
migrations, applying directly, composing sources, validating across dialects —
is the same for every source and lives on
[Work with a desired schema](../work-with-a-source/). For SQL the flag is
`--schema-file`. What follows is specific to this source.

## Diff two SQL files locally

`ptah schema diff` compares local SQL files directly. With `old.sql`
describing the deployed shape and `schema.sql` adding a `pets` table, a dev
database replays both sides:

```bash
ptah schema diff \
  --from old.sql \
  --to schema.sql \
  --dev-url "sqlite://dev?mode=memory"
```

Expected output includes:

```sql
CREATE TABLE "pets" (
  "id" INTEGER PRIMARY KEY,
  "name" TEXT NOT NULL,
  "user_id" INTEGER NOT NULL CONSTRAINT "fk_pets_user_id" REFERENCES "users" ("id")
);
```

## Failure modes

- A change that a SQLite dev database cannot express as an in-place `ALTER`
  is refused loudly rather than turned into an incomplete diff. For example,
  adding a `NOT NULL` column to an existing table exits with
  `sqlite: adding column email to table users requires a table rebuild plan`.
- Unsupported DDL constructs fail with a parse error naming the statement.
  Treat the error as a compatibility gap and check the conformance reports.
- A constraint name on `DEFAULT` is refused. Ptah keeps a name on `NOT NULL`,
  `CHECK`, `REFERENCES`, `UNIQUE` and `PRIMARY KEY`; the last two are read as
  the table constraint they describe, which is the level a name lives at. A
  default has no such level and no engine Ptah supports records one:

  ```sql
  CREATE TABLE t (b INTEGER CONSTRAINT c_x DEFAULT 1);
  ```

  ```text
  named column constraint "c_x" at position 41: Ptah has nowhere to keep a name
  on DEFAULT, and does not read one back from a database, so write the
  constraint without a name; a name is kept on NOT NULL, CHECK, REFERENCES,
  UNIQUE and PRIMARY KEY
  ```

  Write `b INTEGER DEFAULT 1` instead. A name Ptah accepts and cannot read back
  would make every later comparison report a difference no apply can settle.

- A routine whose body Ptah did not parse is refused rather than dropped. The
  parser understands the outer boundary of every `CREATE PROCEDURE` and
  `CREATE FUNCTION` it accepts; where it cannot model the body, it keeps the
  text — and text nothing read cannot be compared, so it has no place in a
  desired schema:

  ```sql
  CREATE PROCEDURE bump() SET @counter = @counter + 1;
  ```

  ```text
  the schema model has no place for this statement: a mysql procedure whose
  body was kept as text rather than parsed, so nothing here can compare it:
  CREATE PROCEDURE bump() SET @counter = @counter + 1
  ```

  Write the body in a form Ptah reads — a `BEGIN ... END` block, or a
  `RETURN` — or keep the routine out of the desired schema and manage it
  separately. Carried silently, the routine would be missing from the desired
  schema: a comparison against a database that has it reports no difference,
  and a migration against one that does not plans it out of existence.

- A constraint name on `NOT NULL` is carried where the target **persists** it.
  The distinction is not whether the syntax parses: PostgreSQL 17 accepts
  `CONSTRAINT c_x NOT NULL` and stores nothing, while PostgreSQL 18 records one
  row per `NOT NULL` in `pg_constraint` with `contype = 'n'`, keyed to the
  column through `conkey`, and can drop, add and rename it by name. MariaDB 12.3
  answers `ERROR 1064 (42000)` for the syntax outright. So the name is gated on
  the target's measured capability, and a target that cannot keep it refuses the
  declaration rather than silently dropping the name.

## Next steps

- Combining SQL files with Go packages or other sources? [Composite desired schema](../composite/).
- Planning versioned migrations from this file? [Generate migrations](../../versioned/generate/).
- Using Atlas-style commands end to end? [Atlas compatibility overview](../../atlas/overview/).
