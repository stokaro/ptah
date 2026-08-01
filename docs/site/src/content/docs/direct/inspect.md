---
title: Inspect a database
description: Read a live database schema as SQL statements, annotated Go models, or Atlas-shaped HCL, SQL, and JSON.
---

You want to see the schema a live database actually has — to review it in the
terminal, commit it as a file, or turn it into a schema source Ptah can manage.
Ptah reads a live schema through three commands, each producing a different
representation:

| Command | Output | Reach for it when |
| --- | --- | --- |
| `ptah db read` | SQL `CREATE` statements with a status banner | You want a readable snapshot in the terminal |
| `ptah introspect` | Annotated Go model files | You want the live schema to become your desired schema |
| `ptah schema inspect` | HCL, SQL, or JSON without banners | You want machine-readable output for files and scripts |

Prerequisites:

- A `ptah` binary on your machine ([Install Ptah](../../start/install/)).
- The URL of the database to inspect.

The examples use a local SQLite database, `sqlite://$PWD/app.db`, containing one
`users` table, so every command runs without a database daemon. Substitute your
own database URL throughout.

## Print the schema as SQL

`ptah db read` connects, reads every schema object, and prints the schema as
SQL:

```bash
ptah db read --db-url "sqlite://$PWD/app.db"
```

Expected output includes:

```text
=== DATABASE SCHEMA ===

Connected to sqlite database successfully!

CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT
);
```

On PostgreSQL-family databases, `--schemas` accepts a comma-separated list of
database schemas to read; when empty, Ptah reads the connection's default
schema.

## Turn the schema into Go models

`ptah introspect` writes the live schema as annotated Go structs — Ptah's
desired-schema representation — so the database you already have becomes the
model you edit from now on:

```bash
ptah introspect \
  --db-url "sqlite://$PWD/app.db" \
  --out ./models \
  --package models
```

Expected output includes:

```text
Generated 1 Go file(s) in .../models
Imported 1 table(s), 3 field(s), 0 enum(s)
```

Introspection is step one of bringing a database under Ptah management.
[Adopt an existing database](../../start/adopt-an-existing-database/) continues
from here: it verifies the round trip with a drift check, generates the initial
migration, and baselines the revision table.

## Export machine-readable output

`ptah schema inspect` writes schema output without status banners, so it can
be redirected straight into files. The default format is Atlas-shaped HCL:

```bash
ptah schema inspect --db-url "sqlite://$PWD/app.db" > schema.hcl
```

`schema.hcl` then contains:

```hcl
table "users" {
  column "email" {
    type = TEXT
    unique = true
  }
  column "id" {
    type = INTEGER
    auto_increment = true
  }
  column "name" {
    type = TEXT
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}
```

`--format sql` and `--format json` select SQL and JSON output. `--schemas`,
`--include`, and `--exclude` select what is inspected, in that order:
`--schemas` names the database schemas, `--include` picks top-level resources
inside them with Atlas-style glob patterns, and `--exclude` subtracts from the
result.

```bash
ptah schema inspect --db-url "sqlite://$PWD/app.db" --include users
```

Child resources — columns, indexes, constraints, triggers, policies, grants —
ride along with their parent and cannot be selected on their own; a selector
that names one with `[type=column]` or with a literal dot (`users.email`)
fails before the database is contacted. Glob metacharacters match a dot too,
so `users*email` escapes that check and selects nothing instead
([#979](https://github.com/stokaro/ptah/issues/979)). A selection that keeps
an object whose dependency it dropped is refused rather than rendered, so the
output never references an object it omits.

The source does not have to be a live database: `--schema-file` inspects a
local `.hcl`, `.yaml`, `.yml`, or `.sql` schema file, and `--migrations-dir`
inspects an Atlas-format migration directory. Both require `--dev-url` — a
disposable database that is reset, has the source materialized on it, and is
then introspected, so the output is normalized by a real database of the
target dialect.

With `--out-dir` the inspected schema is exported as files instead of one
stream — one file per object by default, or grouped with `--split schema` /
`--split type`:

```bash
ptah schema inspect --db-url "sqlite://$PWD/app.db" --format sql --out-dir ./schema
```

The Atlas-compatible spelling, `ptah-compat schema inspect --url ...`, adds
custom Go templates, Mermaid output, and template-driven split exports; see
[Atlas schema commands](../../atlas/schema-commands/#inspect-a-schema-source).

## Failure modes

An unreachable database fails with exit code `2` on every native command. The
`ptah db read` output ends with a connection checklist and the underlying
error:

```text
Make sure:
1. The database URL is correct
2. The database server is running
3. You have the correct permissions
4. The database exists
5. The connection completes within --connect-timeout (currently 10s)
error: failed to ping database: ...
```

Raise `--connect-timeout` for databases that are slow to accept connections.
For symptoms beyond connectivity, see
[Troubleshooting](../../operate/troubleshooting/).

## Next steps

- See how this schema differs from what you want:
  [Compare and drift](../compare-and-drift/).
- Make the inspected schema the one Ptah manages:
  [Adopt an existing database](../../start/adopt-an-existing-database/).
- Render the schema as a diagram instead of text:
  [Visualize the schema](../../schema/visualize/).
