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
| `ptah db read` | Executable SQL on stdout; connection status on stderr | You want a SQL snapshot you can review or redirect |
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

Standard output contains SQL only:

```sql
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT
);
```

Connection progress and diagnostics go to standard error. You can redirect the
SQL without filtering the command output:

```bash
ptah db read --db-url "sqlite://$PWD/app.db" >schema.sql
sqlite3 restored.db <schema.sql
```

On PostgreSQL-family databases, `--schemas` accepts a comma-separated list of
database schemas to read; when empty, Ptah reads the connection's default
schema.

PostgreSQL roles are cluster-wide rather than per-database, so a read reports
only the roles the schemas being read actually use: a role that holds a
privilege on a relation in them or on one of the schemas, a role that granted
one, or a role a row-level security policy on a table in them applies to. A
role that merely exists elsewhere on the server belongs to no schema being read
and is not described.

Equivalently, a read describes a role exactly when some other statement in the
same output names it, so the output never refers to a role it does not create.
Ownership alone is not a reason: Ptah writes no `OWNER TO` and no
`CREATE SCHEMA ... AUTHORIZATION`, so an owner would be a role the output
creates and never mentions again. For the same reason a read no longer reports
the built-in privileges an owner holds on a relation nobody has granted
anything on -- no `GRANT` produced them, and `CREATE TABLE` re-establishes them
for the new owner when the output is replayed.

A read that leaves roles out says so on standard error, and the fuller read is
still available on this same command:

```bash
PTAH_POSTGRES_INSPECT_ALL_ROLES=1 ptah db read --db-url "$PG_URL"
```

That describes every role Ptah manages on the server — what a read produced
before the scoping — which is what you want when the point of the read is to
reproduce a cluster's roles somewhere else. It changes the description only:
comparison already treats those roles as present either way. Reserved `pg_`
names and the bootstrap `postgres` superuser stay out of both. See
[PostgreSQL roles and grants](../../databases/postgresql/#roles-and-grants).

Role creation statements in PostgreSQL output are intended for
a clean target. If a role already exists, running the SQL by hand
fails before its description or grants are changed; this prevents privileges
from being attached to a role with unverified security attributes. Role
descriptions are restored with `COMMENT ON ROLE`; schema and table grants are
still emitted after successful role creation.

Feeding that description back through Ptah is different, because Ptah knows the
role is already there. `ptah-compat schema apply` plans no `CREATE ROLE` for a
role the target's server has, and evaluating the document on a `--dev-url` dev
database does not re-create one either — a dev database is reset before the
document is materialized on it, and resetting a database does not clear the
server's roles. Roles the dev database was not given are named on standard
error and are left exactly as the server has them; a role the server does not
have is still created there.

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

Native inspection describes every construct Ptah models, including PostgreSQL
extensions, sequences, and row-level security policies. The Atlas-compatible
binary leaves those three block types out of its HCL by default, because the
tool it stands in for refuses a file containing them; that filtering is a
property of `ptah-compat` alone, never reaches this command, and
`PTAH_ATLAS_INSPECT_ALL_BLOCKS` has no effect here. See
[Blocks the compatibility surface leaves out by default](../../atlas/schema-commands/#blocks-the-compatibility-surface-leaves-out-by-default).

Because this command leaves nothing out, its output claims to describe
everything, and it carries no `ptah:not-described` header. Delete a block from
it and the plan that follows removes the object, which is what you asked for.
The compatibility surface, which does leave blocks out, says so in the document;
see [The document says what it does not describe](../../atlas/schema-commands/#the-document-says-what-it-does-not-describe).

Both commands write a `permission` block by the same three rules: a schema is
declared whenever anything references one, a grantee is a `role.<name>`
reference only where the document declares that role block, and a target names
the kind of block the document declares for it — `view.<name>` for a view,
`materialized.<name>` for a materialized view. See
[Three rules a permission block is written by](../../atlas/schema-commands/#three-rules-a-permission-block-is-written-by).

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
that names one with `[type=column]` fails before the database is contacted.
A positional spelling such as `users.email` is not refused on its shape,
because it is indistinguishable from a table literally named `users.email`.
Whether it matched is decided by the projection: `path.Match` treats `.` as an
ordinary character, so `users.email`, `users*email`, `users?email`, and
`users[.]email` all select nothing, and inspection says so on standard error
while keeping exit status 0 and its rendered bytes.

A table whose own name contains a dot is selectable three ways —
`--include 'a.b.c'`, `--include 'a\.b\.c'`, or `--include 'main."a.b.c"'`.
The escaped and quoted spellings were once the documented workaround for the
bare one; all three work.

A selection that keeps an object whose dependency it dropped is refused rather
than rendered, so the output never references an object it omits.

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
The `ptah db read` standard error stream ends with a connection checklist and
the underlying error:

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
