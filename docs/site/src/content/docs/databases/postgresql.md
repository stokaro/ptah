---
title: PostgreSQL
description: What Ptah manages on PostgreSQL - schema objects, roles and grants, RLS, extensions, sequences, user-defined types, and version-dependent behavior.
---

PostgreSQL is Ptah's primary first-party target, and this page is the map of
what Ptah manages on it beyond portable table DDL. Every object family below
is declared in your schema sources, flows through the full generate / compare
/ migrate / rollback lifecycle, and has its exact directive syntax in the
[Go annotation reference](../../reference/go-annotations/).

## Connecting

Use a `postgres://` or `postgresql://` URL:

```bash
ptah db read --db-url "postgres://user:pass@localhost:5432/app"
```

Commands that introspect PostgreSQL-family targets accept `--schemas` to scope
reading to a comma-separated list of database schemas; empty means the
connection's default schema. On connection, Ptah reads the server version and
selects the matching capability preset, so planning adapts to the concrete
server — see
[Dialects and capabilities](../../concepts/dialects-and-capabilities/).

## Version-dependent behavior

Three PostgreSQL preset lines exist: 12–13, 14–16, and 17+. The differences
that reach generated SQL:

- Trigger modification uses single-statement `CREATE OR REPLACE TRIGGER` on
  PostgreSQL 14+; older lines get an explicit drop-and-create sequence.
- In-place `ALTER COLUMN ... SET EXPRESSION` for generated columns requires
  PostgreSQL 17+.

## Schema objects

The annotation grammar covers, per object family:

- **Namespaces and infrastructure**: schemas, extensions, functions.
- **Types**: enum types (`CREATE TYPE ... AS ENUM`), domains, composite
  types, and range types.
- **Relations**: tables, views, materialized views, triggers, and standalone
  sequences.
- **Security**: roles, grants, and row-level security policies.

Several of these are features Atlas keeps out of its open-source core; Ptah
provides them as open, local, no-account capabilities. The sections below
summarize behavior that affects how you plan changes.

## Roles and grants

`//ptah:schema:role` and `//ptah:schema:grant` declare roles and their
privileges next to your entities. Ptah emits `CREATE ROLE` for new roles,
`ALTER ROLE` for attribute changes, and `GRANT`/`REVOKE` as declared grants
change. Grants target a table, a schema, or a sequence, and table grants are
compared per individual privilege, so a `privilege="SELECT,INSERT"` list
round-trips cleanly through introspection.

Ordering is dependency-aware: roles are created before the functions and
policies that reference them, and grants are emitted after the roles and
target objects exist.

:::caution
Ptah never drops a role automatically. A role that disappears from the desired
schema stays in the database, because roles may be shared with DBAs,
infrastructure, or other applications; remove one manually with `DROP ROLE`
when you are certain it is safe. `REVOKE` is narrower: Ptah revokes only
privileges attached to roles that are still declared in the desired schema.
:::

Do not put plaintext passwords in `password` attributes. Ptah recognizes
common encrypted formats (MD5, SCRAM-SHA-256, bcrypt, SHA-256/512) and adds a
warning comment to generated SQL when a value looks like a plaintext password.

## Row-level security

`//ptah:schema:rls:enable` switches RLS on for a table and
`//ptah:schema:rls:policy` declares each policy, including the roles it
applies to and its `USING`/`WITH CHECK` expressions. Policies are created
after the roles they reference, so a role and the policy that uses it can land
in the same migration.

## Extensions

`//ptah:schema:extension` manages `CREATE EXTENSION`. Some extensions are
pre-installed and should not be migration-managed, so Ptah keeps an ignore
list, with `plpgsql` ignored by default. An ignored extension can still be
created when your schema declares it, but it is never dropped and never
appears in a diff. Embedders can replace or extend the ignore list through the
Go API — see [Reusable components](../../extend/components/).

## Standalone sequences

PostgreSQL creates an implicit sequence for every `SERIAL` column and identity
column; you do not declare those, and Ptah's introspection deliberately
excludes them, so a plain `SERIAL` column never produces a spurious diff. A
*standalone* sequence — declared with `//ptah:schema:sequence`, typically
to share one number generator across tables — is a first-class object with the
full lifecycle.

Ordering matters twice: a sequence consumed by a column default is created
before the table that uses it, while an `owned_by="table.column"` association
requires the table to exist, so Ptah emits it as a separate
`ALTER SEQUENCE ... OWNED BY` after table creation — the same ordering
`pg_dump` uses. Options you leave unset follow PostgreSQL defaults and are
never reported as drift.

## User-defined types

Domains, composite types, and range types are declared with
`//ptah:schema:domain`, `:composite`, and `:range`. They are created after
extensions and enums but before tables, and their drops are classified as
destructive by the safety gate. Reconciliation is deliberately conservative:

- A domain's `check` and `default` are create-only. PostgreSQL rewrites those
  expressions on read-back, so Ptah does not diff them; change them with a
  manual migration.
- A domain base-type or nullability change has no in-place `ALTER`, so it is
  emitted as a non-`CASCADE` drop and recreate; if a column still uses the
  domain, the drop fails loudly instead of dropping the column.
- Range types are matched by name only; a changed range is dropped and
  recreated.

## Concurrent index creation

`CREATE INDEX CONCURRENTLY` avoids blocking writes but cannot run inside a
transaction block. With `diff.concurrent_index: true` in `ptah.yaml`
([Configuration](../../reference/configuration/)), migration generation emits
`CONCURRENTLY` for new indexes on populated tables and pairs the file with
`-- +ptah no_transaction` so the migrator runs it outside a transaction. The
Atlas-compatible `ptah atlas migrate diff` with `diff.concurrent_index.create`
in `atlas.hcl` tags such files with the Atlas `-- atlas:txmode none` directive
instead ([Atlas migrate commands](../../atlas/migrate-commands/)); the
migrator honors both directives.

## Migration locking

Migration runs serialize through a session-level advisory lock
(`pg_advisory_lock`, lock name `ptah_migrate`), so two concurrent
`ptah migrations up` runs against one database cannot interleave. Timeout
flags for the lock are on [Apply migrations](../../versioned/apply/).

## Next steps

- Declaring these objects in Go sources: [Go annotation reference](../../reference/go-annotations/).
- Targeting CockroachDB, YugabyteDB, or Spanner instead: [Database support matrix](../support-matrix/).
- Gating destructive changes before they run: [Integrity and safety](../../versioned/integrity-and-safety/).
