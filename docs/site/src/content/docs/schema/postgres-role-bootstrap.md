---
title: Conditional role bootstrap
description: Declare PostgreSQL application roles in supported procedural SQL blocks.
type: reference
audience:
  - "schema-author"
readerQuestion: "Which PostgreSQL role-bootstrap declarations can Ptah interpret?"
goal: "Understand supported role-bootstrap syntax and its evaluation boundaries."
sourceOfTruth:
  - "internal/sqlschema/postgres_bootstrap.go"
  - "internal/schemafile"
  - "integration/postgres_role_bootstrap_e2e_test.go"
generated: false
overlaps: []
disposition: keep
sourceMode: static-file-only
---

This reference defines the PostgreSQL role-bootstrap SQL that Ptah can read as
a desired schema without executing the procedural body.

## Declaration syntax

PostgreSQL desired SQL can keep a conditional role declaration:

```sql
DO $bootstrap$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'app_reader'
  ) THEN
    CREATE ROLE app_reader NOLOGIN;
  END IF;
END;
$bootstrap$;

CREATE TABLE documents (id bigint PRIMARY KEY, body text NOT NULL);
GRANT SELECT ON TABLE documents TO app_reader;
```

For this declaration saved as `roles.sql`, the native rendering command is:

```bash
ptah schema render --schema-file roles.sql --dialect postgres
```

Ptah reads the selected declarations into the same role model as top-level
`CREATE ROLE`. Generated migrations contain ordinary role DDL before dependent
grants. Adding another conditional role and its grants produces an incremental
migration; existing migration files do not need rewriting.

This is a bounded interpretation, with no SQL execution or database connection.
Each desired document starts with no application roles. Earlier declarations in
the file, ordered source list, or imported SQL establish the roles that later
conditions see. An existing declaration makes `IF NOT EXISTS` keep its original
attributes. An executed `CREATE ROLE` for an already declared role is refused,
including across blocks, top-level statements, and ordered source files.
Roles on a dev server or deployment target never determine this
result. A grant or policy reference alone does not declare an external role.

## Supported forms

Supported statement forms:

- `BEGIN`/`END` groups declarations.
- `NULL` makes no change.
- `CREATE ROLE` declares an application role.
- Nested `IF`/`ELSE` selects declarations using the conditions below.

Conditions may be `TRUE`, `FALSE`, or `[NOT] EXISTS (SELECT 1 FROM pg_roles WHERE
rolname = 'name')`; the catalog may be qualified with `pg_catalog`. Ordinary
string literals and quoted role identifiers preserve their PostgreSQL meaning.
Supported role attributes are `LOGIN`, `SUPERUSER`, `CREATEDB`, `CREATEROLE`,
`INHERIT`, and `REPLICATION`, including their `NO` forms, plus a literal
`PASSWORD`. Each attribute may appear once; an optional `WITH` comes before
the options. These use the existing top-level role model and password policy.

Ptah refuses unknown conditions and statements, including in unselected
branches. Dynamic SQL, variables, loops, exception handlers, nested block
comments, `ALTER ROLE`, `DROP ROLE`, and languages other than PL/pgSQL are
outside this interpretation.
Conditions and declarations naming `pg_*` or `postgres` are refused. Role names
longer than 63 bytes are refused rather than truncated. Nesting is limited to
64 levels. A refusal reports the source file and body position; it does not
publish a partially interpreted schema.

## Migration replay and dev databases

The same reader serves native commands and `ptah-compat`. Reading the desired
block needs no disposable server. **Replaying migration history is separate:**
existing migrations execute SQL and still require
[a disposable server](../../concepts/database-urls-and-dev-databases/#a-server-declared-disposable)
for procedural or role effects. Applying the generated migration requires a
database user authorized to create the declared roles.

Commands that materialize a schema on a dev database still execute generated
DDL. Use a fresh `docker://` server for role-bearing schemas: resetting a
database does not remove cluster-wide roles or restore their attributes.

For file loading and rendering, see [SQL schema](../sql/). To turn the desired
state into versioned changes, see [Generate migrations](../../versioned/generate/).
