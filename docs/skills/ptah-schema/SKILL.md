---
name: ptah-schema
description: "Change a database schema in a project that uses Ptah. Edit the desired schema (Go structs with //ptah: comments, or SQL, HCL, YAML or DBML schema files), then let Ptah validate it, plan and generate the migration, lint it and apply it to a disposable database. Use it when a task adds, removes or changes tables, columns, indexes, constraints, views or other database objects, and the project has a ptah.yaml, //ptah: comments or a migration directory sealed by ptah.sum."
---

# Change a schema with Ptah

Ptah turns a desired schema into the SQL that brings a database to it. Your
part is the desired schema. Ptah's part is the plan, the migration files and
the checks. Keep to that split:

- Change the desired schema, never a live database by hand.
- Do not write migration SQL that Ptah can plan. Write SQL by hand only for
  what no schema declares, such as a data backfill, and say so in your summary.
- Never edit a migration that a database has already applied. Add a new one.
- Run commands that change a database only against a database you were told is
  disposable. A shared, staging or production database is for a person to
  migrate.
- When Ptah refuses something, read the refusal and change the schema. Do not
  look for a flag that silences it.

## Find the project's inputs

Look for these before you change anything:

- `ptah.yaml` or `atlas.hcl` at the project root. An environment in it names a
  database URL and a migration directory, and `--env <name>` selects it.
- Go structs with `//ptah:` comments. Ptah reads them from the directories
  `--root-dir` names, the current directory by default. The comments sit on the
  struct and on its fields; an index comment belongs on a field.
- Schema files in SQL, HCL, YAML or DBML, passed with `--schema-file`.
- A migration directory with a `ptah.sum` file in it. Each migration is a pair
  of files, `<version>_<name>.up.sql` and `<version>_<name>.down.sql`.
- The database engine. Commands that read no database need `--dialect`, for
  example `postgres`, `mysql`, `sqlite` or `clickhouse`.

If none of these exist, the project does not manage its schema with Ptah. Stop
and ask before you add it.

## The example these commands use

The commands below run on a small example, and this skill's own tests run them
exactly as written: a desired schema in `schema.sql`, migrations in
`./migrations`, an application database `sqlite://app.db` and a scratch
database `sqlite://dev.db`. In a real project, use its schema sources in place
of `--schema-file schema.sql`, for example `--root-dir ./models` for Go
structs, its migration directory, and the database URLs the person gave you.

Two rules about those databases decide whether the migration is right:

- `migrations plan` and `migrations generate` diff the desired schema against
  the database `--db-url` names. That database must hold every migration in the
  directory and nothing else. Run `migrations up` on it first. A database
  behind the directory puts other people's pending changes into your migration,
  and one ahead of it, or changed by hand, gives a wrong or empty one. With no
  such database, add `--replay --dev-url <url>` to `migrations generate`, and
  Ptah builds the current state from the directory instead.
- A `--dev-url` database is reset destructively. It is never the application
  database, and never one a person did not name as scratch.

The example starts with one table. Save this as `schema.sql`:

```sql
CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    email TEXT NOT NULL
);
```

A project with no migration directory yet starts its history with a first
migration, and seals the directory:

```console
ptah migrations generate --schema-file schema.sql --db-url sqlite://app.db --migrations-dir ./migrations --name create_users
```

Expected output includes, on standard output:

```text
Generated migration files for sqlite://app.db:
```

```console
ptah migrations hash --dir ./migrations
```

Expected output on standard output:

```text
Wrote ./migrations/ptah.sum
2 migration file(s) hashed
```

The example database then takes the first migration:

```console
ptah migrations up --db-url sqlite://app.db --migrations-dir ./migrations
```

Expected output includes, on standard output:

```text
✅ Migrations completed successfully!
```

## Make a change

### 1. Change the desired schema

Edit the schema source, and nothing else. Here the task is a name for each
user, so `schema.sql` now reads:

```sql
CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    name  TEXT
);
```

A new `NOT NULL` column on a table that holds rows needs a default, or the
plan is refused. Ptah says which column and why.

### 2. Validate it

Validation reads no database. It reports structural problems, such as a
foreign key to a missing column, and prints nothing when there are none:

```console
ptah schema validate --schema-file schema.sql --dialect sqlite
```

### 3. Preview the plan

`ptah migrations plan` prints the SQL the change needs and how risky each
statement is. It writes no file and changes no database:

```console
ptah migrations plan --schema-file schema.sql --db-url sqlite://app.db
```

Expected output includes, on standard output:

```text
Safety classification:
ALTER TABLE "users" ADD COLUMN "name" TEXT;
```

Read the safety classification. A statement that drops data or tightens a
constraint is something to tell the person about, not something to work
around.

### 4. Generate the migration and seal it

```console
ptah migrations generate --schema-file schema.sql --db-url sqlite://app.db --migrations-dir ./migrations --name add_user_name
```

Expected output includes, on standard output:

```text
Generated migration files for sqlite://app.db:
```

Read both new files. The up file must do what the plan showed, and the down
file must undo it. Then seal the directory again, because every later command
refuses a directory whose files do not match `ptah.sum`:

```console
ptah migrations hash --dir ./migrations
```

Expected output includes, on standard output:

```text
4 migration file(s) hashed
```

### 5. Lint it

The linter replays the migrations on the scratch database, which it resets,
and reports risky statements. `--latest 1` lints the new migration only:

```console
ptah migrations lint --dir ./migrations --dev-url sqlite://dev.db --latest 1
```

Expected output on standard output:

```text
No lint findings.
```

Fix every error the linter reports in the desired schema, then generate again.
Pass warnings on to the person with your summary.

### 6. Apply it to a disposable database, and check for drift

Only when the person told you the database is disposable:

```console
ptah migrations up --db-url sqlite://app.db --migrations-dir ./migrations
```

Expected output includes, on standard output:

```text
Pending migrations: 1
✅ Migrations completed successfully!
```

The database now matches the desired schema, and a drift check says so:

```console
ptah schema drift --schema-file schema.sql --db-url sqlite://app.db
```

Expected output on standard output:

```text
No schema drift detected.
```

## When Ptah is connected as an MCP server

A client that started `ptah mcp` offers Ptah's tools directly. Use them in
place of the commands where they fit:

- `validate_schema` and `render_schema` check and render a declared schema with
  no database.
- `read_database` reads the schema a configured database holds, if the session
  allows it.
- `search_docs` answers a question from Ptah's own documentation. Prefer it to
  what you remember about Ptah's flags.
- `preview_patch` and `apply_patch` change a migration, schema or test file
  only after a preview, and only in the classes the session allows.

`describe_session` says what the session may do. A tool it denies is a decision
the person made; ask, do not work around it.

## Report back

End with what you changed in the desired schema, the files Ptah generated, the
plan's safety classification, every lint finding, and what you applied and
where. Name the steps you skipped and why.
