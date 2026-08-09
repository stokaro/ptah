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
round-trips cleanly through introspection. New-role SQL fails closed when the
role already exists, so later comments and grants cannot be applied to a role
with unverified security attributes. Role descriptions are applied with
`COMMENT ON ROLE` after successful creation.

Ordering is dependency-aware: roles are created before the functions and
policies that reference them, and grants are emitted after the roles and
target objects exist.

Reading a live database describes only the roles the schemas being read
actually use, because a PostgreSQL role belongs to the cluster rather than to
one database. A role counts as used when it holds a privilege on a relation in
those schemas or on one of the schemas themselves, when it granted one, or when
a row-level security policy on a table in them applies to it. A role that
merely exists elsewhere on the server is not part of the schema being
described, so it is left out — of `ptah db read` and of `ptah-compat schema
inspect` alike.

The rule is exact in both directions: a description defines a role when some
other statement in it names that role, and not otherwise. Ownership alone does
not qualify, because Ptah describes no ownership — it writes no `OWNER TO` and
no `CREATE SCHEMA ... AUTHORIZATION` — so an owner would be created and then
never referred to.

For the same reason, grant introspection reports privileges somebody granted
rather than the built-in privileges an owner holds by default. A relation whose
`pg_class.relacl` is null has had no `GRANT` run against it, and its owner's
implicit privileges are no longer emitted as `GRANT` statements; replaying
`CREATE TABLE` gives the new owner exactly those privileges again.

Leaving a role out of a description does not mean Ptah thinks it is missing.
Which roles a schema uses and which roles Ptah manages on the server are
separate questions, and comparison asks the second one: a managed role that
already exists anywhere in the cluster is never planned as a `CREATE ROLE`,
whether or not the schema you are reading refers to it. Declare such a role in
your entities and Ptah still applies `ALTER ROLE` when its attributes drift, so
comparison is unchanged in both directions. Roles outside the described scope
are used for that comparison only — they are never written to any output.

Scoping the description does cost one thing, and it is not comparison: a
description no longer reproduces a cluster's ungranted roles somewhere else.
Measured on PostgreSQL 17.10, on a database holding one table and roles nothing
grants anything to, `ptah db read` went from four `CREATE ROLE` statements to
none, and `ptah-compat schema apply --dry-run` against an empty database in a
**second** cluster went from planning three of them to planning none. Copying
one cluster's roles into another is something you could do before, so it stays
available on the same commands rather than being removed:

```bash
# Describe every role Ptah manages on the server, not only the ones in scope.
PTAH_POSTGRES_INSPECT_ALL_ROLES=1 ptah db read --db-url "$PG_URL"
PTAH_POSTGRES_INSPECT_ALL_ROLES=1 ptah-compat schema inspect --url "$PG_URL"
```

The variable widens the description only. Both reads still happen, so
comparison sees exactly the same set of existing roles either way and turning
it on can never plan a `CREATE ROLE` for a role that is already there. It is an
environment variable rather than a flag because `ptah-compat` registers exactly
the flags the Atlas community CLI registers. Reserved roles stay out of the
widened read too, for the reason the next paragraph gives.

You are never left to infer the omission. When a read leaves roles out, Ptah
says so on standard error, alongside the schema on standard output:

```text
note: 4 roles Ptah manages on this server are not described, because nothing in the inspected schemas refers to them; comparison still treats them as present, so none of them is planned as a CREATE ROLE. Set PTAH_POSTGRES_INSPECT_ALL_ROLES=1 to describe every role Ptah manages.
```

The note reports a count and never the names: on a shared instance those names
belong to other tenants, which is half the reason the description is scoped at
all.

A scoped description is also a replayable one, which is the point of scoping it
at all. Feed `ptah-compat schema inspect` output straight back in against a
clean sibling database on the same server and it materializes at exit 0, and
the document that comes back is the one that went in. The roles a scoped
description names are precisely the roles the server already has — they hold
privileges on the inspected tables — so the dev database is not given them
again; they are left exactly as the server has them, never altered, and named
on standard error. A role the server does **not** have is still created there,
so the same document also materializes on a server that has never seen it.

Reserved roles sit outside that rule in both directions. Ptah manages neither
the `pg_` roles nor the bootstrap `postgres` superuser: it never describes them
and never compares them, so a declaration naming one would be compared against
nothing and planned as a `CREATE ROLE` the server refuses. Ptah refuses the
declaration instead, before anything is compared or planned:

```text
Error: compare database schema: invalid schema diff: desired schema declares reserved PostgreSQL role "pg_monitor" (PostgreSQL reserves the "pg_" prefix for system roles and refuses CREATE ROLE at SQLSTATE 42939); Ptah manages reserved roles in neither direction, so the declaration is compared against nothing and would be planned as a CREATE ROLE the server refuses; rename the role, or set PTAH_ALLOW_RESERVED_ROLE_NAMES=1 to plan it anyway
```

The two names fail for different reasons and both are covered: the superuser
because it already exists (`role "postgres" already exists`, SQLSTATE 42710),
and a `pg_` name because the prefix is reserved (`role name "pg_monitor" is
reserved`, SQLSTATE 42939). The prefix is matched literally, so `pgbouncer`,
`pgadmin` and `pgpool` are ordinary roles and are planned as before.

One thing the refusal costs, so it stays reachable rather than being removed:
on a cluster bootstrapped under another name, `CREATE ROLE "postgres"` succeeds.
Measured on PostgreSQL 17.10, a cluster whose superuser is `admin` accepts the
statement and the role appears in `pg_roles`.

```bash
# Plan a declared reserved role anyway, as Ptah did before the refusal existed.
PTAH_ALLOW_RESERVED_ROLE_NAMES=1 ptah-compat schema apply --dry-run --url "$PG_URL" --to file://schema.hcl --dev-url "$PG_DEV_URL"
```

The variable changes only whether Ptah refuses first or the server does. The
reads are untouched, so a `pg_` name still fails at the server whatever you set
it to. It is an environment variable rather than a flag because `ptah-compat`
registers exactly the flags the Atlas community CLI registers.

Both variables on this page are booleans and read the same way: unset selects
the default described above, a valid boolean is honored, and anything else fails
the command before it reads or compares anything — including on a run that
declares no reserved role and leaves no role out, which is what makes a typo
visible before the day it would have mattered. See
[Boolean environment variables](../../reference/configuration/#boolean-environment-variables).

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

Enablement is compared against `pg_class.relrowsecurity` and planned in both
directions: a table your schema declares that the database has row-level
security off for gets `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, and a table
the database secures that your schema does not declare gets the matching
`DISABLE`. A table that is being dropped is not disabled first. A table that
declares a policy without declaring enablement is enabled with the table,
because `CREATE POLICY` on a table whose row-level security is off protects
nothing.

Which table a policy belongs to is decided under the target's identifier rules
rather than by spelling, so a policy declared on `orders` and a table created
as `public.orders` are one table and the enablement is emitted once. Matching
them as plain strings left the `CREATE POLICY` in the plan without its
`ENABLE`, and the migration reported success while the policy sat inert on an
unprotected table.

A policy name is scoped to its table rather than to the schema, which is what
PostgreSQL itself enforces: `CREATE POLICY tenant_isolation` succeeds on two
tables in one schema and is refused only when repeated on the same table. Ptah
identifies each policy by its table and its name together, so two tables can
each carry a `tenant_isolation` policy and both are rendered, compared, and
migrated independently.

The owning table is that table's identity rather than the string you spelled
it with. A table declared without a schema is reached both as `orders` and as
`public.orders`, and PostgreSQL treats those as one table: declaring `p` on
each spelling is one policy declared twice, and the second `CREATE POLICY` is
refused with `policy "p" for table "orders" already exists`. Ptah keeps the
first declaration and renders it once. Two tables of the same name in
different schemas — `tenanta.orders` and `tenantb.orders` — remain two tables,
and a policy name on each remains two policies.

Letter case is the other spelling that reaches one table, and it folds in one
direction only. An unquoted PostgreSQL identifier folds to lower case, so a SQL
schema file declaring `CREATE TABLE orders` and then naming `ORDERS` in
`CREATE POLICY` or in `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` is naming
that same table. Ptah binds each declaration to the table it names and renders
the declared spelling, so a policy or an enablement written as `ORDERS` no
longer renders `ON "ORDERS"` — a table nothing declared, answered by
`relation "ORDERS" does not exist`.

The rule is that a reference folds down onto a table declared in lower case,
and a declaration that preserves case is never a fold target. Ptah does not
record whether an identifier was quoted, so `CREATE TABLE ORDERS` and
`CREATE TABLE "ORDERS"` reach it as the same declaration while PostgreSQL reads
them as two different relations — `orders` and `ORDERS`. A schema that declares
only `"ORDERS"` and then writes `ON orders` therefore names a relation it does
not declare, and Ptah keeps that spelling rather than guessing: the render
reproduces PostgreSQL's own `relation "orders" does not exist` instead of
quietly moving the policy onto `ORDERS`. Declaring both `orders` and `"ORDERS"`
gives two tables, and a policy on each is two policies.

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
- Within the group the three kinds are ordered by what their definitions name,
  not by kind: `CREATE TYPE addr AS (...)` precedes `CREATE DOMAIN d AS addr`,
  and `CREATE DOMAIN qty AS integer` precedes a composite with a `qty` field.
  Both directions occur, and PostgreSQL has no forward declaration for a type.
- The drops a recreation emits are ordered by the shape the **database holds**,
  which is a different graph from the one above and does not have to agree with
  it. A `DROP` runs against the current schema, so only a reference that schema
  carries can block it; when the change is what moves the reference, the create
  order and the drop order are not mirror images. Both orders come out of the
  same plan.
- A domain, composite or range type that an **extension** owns is not
  described. `CREATE EXTENSION` creates those types, so they cannot be created
  or dropped independently, and describing one as a user type made the
  description declare something the extension already makes — replaying it
  failed with `type "lo" already exists`. Ownership is read from `pg_depend`,
  so a type of your own named close to an extension's is unaffected. The same
  rule has always applied to extension-owned functions.

## Concurrent index creation

`CREATE INDEX CONCURRENTLY` avoids blocking writes but cannot run inside a
transaction block. With `diff.concurrent_index: true` in `ptah.yaml`
([Configuration](../../reference/configuration/)), migration generation emits
`CONCURRENTLY` for new indexes on populated tables and pairs the file with
`-- +ptah no_transaction` so the migrator runs it outside a transaction. The
Atlas-compatible `migrate diff` (in the `ptah-compat` binary) with `diff.concurrent_index.create`
in `atlas.hcl` tags such files with the Atlas `-- atlas:txmode none` directive
instead ([Atlas migrate commands](../../atlas/migrate-commands/)); the
migrator honors both directives.

## Concurrent index removal

`DROP INDEX CONCURRENTLY` is the matching non-blocking removal, and it carries
the same restriction: it cannot run inside a transaction block.

The rollback of a concurrent index build always uses it where the target
supports it, so the down file undoes a non-blocking build without taking the
write lock the build avoided. For the up direction it is opt-in:
`diff.concurrent_index_drop: true` in `ptah.yaml`, or
`diff.concurrent_index.drop = true` in `atlas.hcl`, requests it for standalone
index removals. An index that is dropped and recreated under the same identity
is a redefinition rather than a standalone removal and keeps the blocking drop
the planner pairs with the rebuild.

## Migration locking

Migration runs serialize through a session-level advisory lock
(`pg_advisory_lock`, lock name `ptah_migrate`), so two concurrent
`ptah migrations up` runs against one database cannot interleave. Timeout
flags for the lock are on [Apply migrations](../../versioned/apply/).

## Next steps

- Declaring these objects in Go sources: [Go annotation reference](../../reference/go-annotations/).
- Targeting CockroachDB, YugabyteDB, or Spanner instead: [Database support matrix](../support-matrix/).
- Gating destructive changes before they run: [Integrity and safety](../../versioned/integrity-and-safety/).
