---
title: SQL schema
description: Use plain SQL DDL files as Ptah's desired schema.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I use plain SQL DDL files as Ptah's desired schema?"
goal: "Render a desired schema from SQL DDL."
sourceOfTruth:
  - "internal/cli/schema"
  - "internal/schemaload"
generated: false
overlaps: []
disposition: keep
sourceMode: static-file-only
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

## Add a column after the table

A column can be added after its table with `ALTER TABLE ... ADD COLUMN`, in the
same file or in a later file of a schema directory. It joins the table in the
order the document adds it:

```sql
CREATE TABLE users (id integer PRIMARY KEY);
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at timestamptz;
```

`IF NOT EXISTS` makes the statement do nothing for a column the table already
declares the same way. A column declared again without `IF NOT EXISTS` is
refused, as the server refuses it. One declared again with `IF NOT EXISTS` but
differently is refused too: the server keeps the first declaration and ignores
the second, so either reading would drop what the other one says.

## Limit ON DELETE to some columns

PostgreSQL 15 and later take a column list after `ON DELETE SET NULL` and
`ON DELETE SET DEFAULT`. The action then changes only the listed columns, so
the other columns of a composite key may be `NOT NULL`:

```sql
CREATE TABLE parents (tenant integer, id integer, PRIMARY KEY (tenant, id));
CREATE TABLE children (
  id integer PRIMARY KEY,
  tenant integer NOT NULL,
  parent_id integer,
  FOREIGN KEY (tenant, parent_id) REFERENCES parents (tenant, id)
    ON DELETE SET NULL (parent_id)
);
```

Deleting a parent row sets `parent_id` to NULL and keeps `tenant`. The list is
compared as a set of columns, and a list that names every column of the key
means the same as no list.

A declaration the server would refuse, or would accept and then fail on when a
parent row is deleted, is refused:

- a `NOT NULL` column in the list, or a `NOT NULL` key column with no list;
- a list after any action other than `SET NULL` or `SET DEFAULT`, or after
  `ON UPDATE`;
- a column that is not part of the key. A column-level `REFERENCES` can list
  only its own column.

A target without the clause refuses the list instead of widening the action to
every column: PostgreSQL before 15, YugabyteDB 2024.2, CockroachDB, and the
other engines. The Go annotation and Atlas HCL exports refuse such a key for
the same reason, because neither format can write the list.

## Row-level security

A PostgreSQL schema file declares row-level security with the statements a
migration would run:

```sql
CREATE TABLE sites (id uuid PRIMARY KEY, tenant_id uuid NOT NULL);
ALTER TABLE sites ENABLE ROW LEVEL SECURITY;
ALTER TABLE sites FORCE ROW LEVEL SECURITY;
CREATE POLICY sites_tenant ON sites
  USING (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY sites_scope ON sites AS RESTRICTIVE
  USING (current_setting('app.site_scope', true) = 'all');
```

`FORCE` binds the table's owner to its policies; without it the owner reads and
writes past every one of them. `ENABLE` and `FORCE` may come in either order.
`AS RESTRICTIVE` narrows what the permissive policies admit, and `AS
PERMISSIVE` is the default. Both flags are compared with the database and
planned in both directions; [PostgreSQL](../../databases/postgresql/#row-level-security)
has the details.

Three spellings are refused rather than read. `DISABLE ROW LEVEL SECURITY` and
`NO FORCE ROW LEVEL SECURITY` take a protection away, and a schema file says a
table has none by not declaring it. A `FORCE` for a table the file never
enables is refused too: PostgreSQL accepts it, and it changes nothing until the
table enables row-level security.

## API export metadata

SQL DDL cannot author Ptah's export-only `api_name`, `openapi_name`,
`graphql_name`, `proto_name`, `api_type`, or `api_expose` metadata. OpenAPI,
GraphQL, and Protobuf exports still work from a SQL schema, but their public
names, types, and exposure are derived from the persistence schema. Use
[YAML](../yaml/), [HCL](../hcl/), or [Go annotations](../go-annotations/) when
the published contract must differ from database names and types.

## `--dialect` decides how the file is read, not only how it is written

The dialect selects the tokenizer as well as the renderer: whether a backslash
escapes inside a string, whether `E'...'` is an escape string, whether `--x`
without a space is a comment, and whether `[name]` is an identifier.

Two consequences are worth knowing before you pick one.

**A file that the named engine would reject is rejected here.** PostgreSQL runs
with `standard_conforming_strings` on, so a backslash is an ordinary character
and `DEFAULT 'a\'b'` is an unterminated string — PostgreSQL 18 answers
`unterminated bit string literal`. Read with `--dialect postgres`, Ptah refuses
it too. Read with `--dialect mysql`, where a backslash escapes, the same bytes
are a valid default.

**A version-guarded span is stepped over, and the one clause a schema needs is
read out of it.** `mysqldump` writes a full-text index's parser as
``FULLTEXT KEY `ft` (`bio`) /*!50100 WITH PARSER `ngram` */``, and that clause
now reaches the schema. The rest of a guard is not read: those spans hold
version-conditional fragments Ptah does not model, and `mariadb-dump` opens
every file with `/*M!999999\- enable the sandbox mode */` — a guard no server
executes, because no server is version 999999.

**Omitting `--dialect` keeps a permissive reader.** No dialect means no
dialect's rules, which is what lets one file mixing conventions be read at all.
Name the dialect when the file belongs to one engine, which is nearly always.

## Use it

Everything a desired schema is for — comparing, gating on drift, generating
migrations, applying directly, composing sources, validating across dialects —
is the same for every source and lives on
[Work with a desired schema](../work-with-a-source/). For SQL the flag is
`--schema-file`. What follows is specific to this source.

## Split a schema across files

A SQL schema file can pull in other SQL files with an `atlas:import` comment,
one per line:

```sql
-- schema/main.sql
-- atlas:import ./tables/orders.sql
-- atlas:import ./tables/users.sql
```

Point `--schema-file` at the entry point and the declarations merge in the order
the file lists them. The entry point may declare objects of its own as well, and
an imported file may import in turn.

This is the layout `ptah-compat schema inspect` writes when its output goes
through `split`, so an export of a live database reads back as the schema it was
taken from.

Each path is relative to the file that writes it and must stay inside the entry
point's own directory. An absolute path, a path that climbs out with `..`, a
file that does not exist, one that is not `.sql`, and a cycle are each refused
by name. An entry point that imports nothing is read exactly as any other SQL
file.

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

- An index name between `FOREIGN KEY` and its column list is read under
  `--dialect mysql` and `--dialect mariadb`, and refused elsewhere. On those
  engines the name declares the index that backs the key, so Ptah reads it as
  the index it builds; no other engine has the syntax:

  ```sql
  CREATE TABLE child (a INT, FOREIGN KEY zidx (a) REFERENCES parents (id));
  ```

  ```text
  an index name after FOREIGN KEY at position 39 is the MySQL family's alone;
  postgres has no such syntax, so write the key as FOREIGN KEY (columns) and
  declare the index "zidx" separately
  ```

  A name written beside an explicit `CONSTRAINT` symbol is accepted and
  ignored, because both engines record the symbol for the backing index too.
- A column-level `REFERENCES` clause is refused under `--dialect mysql`. MySQL
  accepts the syntax and builds nothing from it, so reading it as a foreign key
  would make rendering add a constraint the source schema never had:

  ```sql
  CREATE TABLE child (a INT REFERENCES parents (id));
  ```

  ```text
  a column-level REFERENCES clause at position 26: MySQL accepts the clause and
  creates neither a foreign key nor an index: SHOW CREATE TABLE reports the
  column alone, and information_schema.referential_constraints stays empty, so
  Ptah refuses it rather than reading a foreign key the source schema does not
  have; write a table-level FOREIGN KEY clause to declare an enforced
  relationship
  ```

  Write the relationship as a table-level `FOREIGN KEY (a) REFERENCES parents
  (id)` instead. MariaDB enforces the column-level spelling and builds a backing
  index for it, so `--dialect mariadb` reads it unchanged.

- `ALTER TABLE ... ADD KEY` adds a secondary index on MySQL and MariaDB, in
  every spelling the engines take: `ADD KEY`, `ADD INDEX`, `ADD SPATIAL KEY` and
  `ADD FULLTEXT KEY`, with a key part's prefix length and direction. A
  `UNIQUE` key stays a constraint, because it is a uniqueness guarantee rather
  than an index alone. `ADD INDEX` still declares ClickHouse's data-skipping
  index on that dialect; which one a statement means is decided by the dialect,
  as it is for the same keyword inside a table body.
- `ALTER TABLE ... ADD PRIMARY KEY` is read onto the table it names, with its
  prefix length and direction, exactly as the same key written inside the
  `CREATE TABLE` would be. A statement naming a table the file does not declare
  is refused rather than dropped:

  ```sql
  ALTER TABLE nosuch ADD PRIMARY KEY (a);
  ```

  ```text
  the schema model has no place for this statement: ALTER TABLE nosuch ADD
  PRIMARY KEY names a table this schema does not declare
  ```

  A primary key has nowhere to live without its table, and the document is not
  one any engine would run either. Declare the table in the same file, or drop
  the statement.

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
