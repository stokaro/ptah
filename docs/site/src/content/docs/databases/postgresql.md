---
title: PostgreSQL
description: What Ptah manages on PostgreSQL - schema objects, roles and grants, RLS, extensions, sequences, user-defined types, and version-dependent behavior.
type: reference
audience:
  - "database-engineer"
readerQuestion: "Which PostgreSQL objects and release lines does Ptah manage?"
goal: "Identify the PostgreSQL objects and release lines Ptah manages."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema"
generated: false
searchAliases:
  - "PostgreSQL extension"
overlaps: []
disposition: keep
sourceMode: live-database-only
owns:
  - dialect-postgres
---

PostgreSQL is Ptah's primary first-party target, and this page is the map of
what Ptah manages on it beyond portable table DDL. Every object family below
is declared in your schema sources, flows through the full generate / compare
/ migrate / rollback lifecycle, and has its exact directive syntax in the
[Go annotation reference](../../reference/go-annotations/).
Commands that introspect this behavior require a live PostgreSQL database URL.

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

## Materialized view refresh

Ptah does not refresh materialized views, and a declaration cannot ask it to.
A `refresh_strategy` attribute is refused when the schema is parsed, on every
dialect and in every frontend.

That is a boundary rather than a missing feature. `REFRESH MATERIALIZED VIEW`
is a statement someone runs, and `CONCURRENTLY` is an option of that statement;
neither is state the database holds, so nothing Ptah reads back could report a
refresh policy or diff one. What Ptah does manage keeps the view current on its
own terms: `CREATE MATERIALIZED VIEW` populates the view, and a changed body is
reconciled as a `DROP` and a `CREATE` that populates it again. Measured on
PostgreSQL 18.

The case that is left over is a view no schema change touched, going stale
because its **source data** moved. Schema reconciliation cannot observe that,
so a refresh emitted there would be an unbounded data operation attached to a
migration on a relationship Ptah inferred. Issue it yourself, from whatever
already knows when the data changed:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.user_counts;
```

`CONCURRENTLY` needs a unique index on the view, or the server answers
`cannot refresh materialized view "public.mv" concurrently`.

ClickHouse is a different matter: an ordinary materialized view there is
maintained by inserts into its source, and the scheduled form the server does
own, `REFRESH EVERY|AFTER`, is engine-native DDL and is managed — see
[ClickHouse](../clickhouse/).

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

Set the annotation's `schema` attribute, or the equivalent HCL/YAML field, to
install an extension outside the default schema. Ptah creates the schema first
and renders `CREATE EXTENSION ... WITH SCHEMA ...`; live inspection preserves
that placement, so a second comparison is synced. Moving an existing extension
between schemas is detected but currently refused before SQL is emitted,
because Ptah does not yet plan `ALTER EXTENSION ... SET SCHEMA`. Creation,
removal, and identical-placement comparisons remain supported.

```go
//ptah:schema:extension name="pgcrypto" schema="extensions" if_not_exists="true"
type PostgreSQLExtensions struct{}
```

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

### A declared CONCURRENTLY is honored

The paragraph above is the heuristic: Ptah choosing `CONCURRENTLY` for you. A
desired state can also ask for it directly, and that request is carried through
to the plan:

```sql
CREATE INDEX CONCURRENTLY idx_widget_a ON widget (a);
```

`ptah schema diff` and `ptah schema apply` plan that index with `CONCURRENTLY`,
on a PostgreSQL-family target that has the capability, without any
`diff.concurrent_index` setting. The apply path refuses the statement inside a
transaction with an actionable message rather than failing at the server, so
`--tx-mode none` is the answer it names.

Two things bound it, and they are the same two the heuristic uses:

- **A partitioned parent is excluded**, not refused. PostgreSQL has no
  concurrent index form for `relkind` `'p'`, and refusing would leave a project
  with a partitioned table unable to plan an index change at all.
- **`diff.concurrent_index.create = false` turns it off.** An explicit `false`
  is an instruction, and a description does not overrule it. Leaving the setting
  out is not the same answer: it leaves the description in charge.

The heuristic's "table already holds rows" test does not apply here. That test
exists because the heuristic is guessing what you would have wanted; a
declaration is not guessing, so an index declared concurrent on an empty table
is still built concurrently.

### What "populated" means when the database cannot say

The decision reads `pg_class.reltuples` and `pg_stat_all_tables.n_live_tup`,
and PostgreSQL reports **no row statistics at all** in more situations than it
reports zero rows: `reltuples` is `-1` until something vacuums or analyzes the
relation, and `n_live_tup` reads `0` after the cumulative counters are reset —
which happens on a crash-recovery restart, on `pg_stat_reset()`, and for a
table restored into a fresh cluster. A table holding millions of rows reports
exactly what an empty one reports.

Missing statistics alone would be a poor test, because `reltuples = -1` is also
the state of every table that has never had a row inserted — so reading it as
"row count unknown" would put every freshly created table into a
non-transactional migration of its own. Ptah asks the file system instead:
`pg_relation_size` reports the table's main fork, which is not reset by an
analyze, by a counter reset, or by a restore.

- **No statistics and no storage** is an empty table. It gets the plain,
  transactional `CREATE INDEX`.
- **No statistics but storage in use** is a row count Ptah genuinely does not
  know, and it counts as **populated** — the non-blocking build. Failing the
  other way was the wrong direction: a blocking `CREATE INDEX` immediately after
  a bulk load or a restore held writes for the length of the scan.

Running `ANALYZE` on the table before generating gives the decision a real
number in either case. A table that does not exist in the database yet is a
separate case and stays transactional — the migration creates it, so it starts
empty.

### Partitioned parents

PostgreSQL has no concurrent index statement for a declaratively partitioned
parent: both `CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` are
refused on `relkind = 'p'` with SQLSTATE `0A000`, and they are refused at
execution time — after the migration file, its checksum and its commit exist.

Ptah reads the partitioned flag from the catalog (`information_schema` reports
a partitioned parent as an ordinary `BASE TABLE`, so it cannot carry this) and
handles the two cases differently:

- **The populated-table heuristic** excludes a partitioned parent and generates
  the plain, transactional statement. Nothing asked for a concurrent build, and
  the plain form is legal SQL that `ptah migrations lint` still reports as
  [`PG101`](../../reference/lint-rules/).
- **An explicit `diff.concurrent_index.create` / `diff.concurrent_index.drop`**
  fails generation before any file is written, naming the index and the
  partitioned table. Silently downgrading an explicit request would hand a
  project that asked for a non-blocking build a blocking one without saying so.

To build a partitioned index without locking every partition at once, use the
documented sequence by hand: `CREATE INDEX ... ON ONLY` the parent, then
`CREATE INDEX CONCURRENTLY` on each partition, then `ALTER INDEX ... ATTACH
PARTITION`. The parent index stays `indisvalid = false` until every partition is
attached, and Ptah's migration guards recognize that shape rather than treating
it as failed-build residue.

### The index copies a partitioned parent creates

Creating an index on a partitioned parent makes PostgreSQL create one copy of it
on every partition, under a name the server chooses (`events_2026_tenant_idx`).
Those copies are attached to the parent index and cannot be dropped on their own:

```sql
DROP INDEX "events_2026_tenant_idx";
-- ERROR:  cannot drop index events_2026_tenant_idx because index idx_events_tenant requires it
```

A desired state written against the parent never names them, so a comparison
that reads them as ordinary indexes plans exactly that refused statement — and
only on the *second* generate, because the copies do not exist until the first
migration has been applied. Ptah reads the attachment from `pg_inherits` and
plans neither a create nor a drop for an attached copy; the parent index is
where the plan acts.

The attachment is the test, not the name and not the table. An index created on
a partition directly is still managed, including one that happens to carry the
name PostgreSQL would have generated for a copy, and a copy attached under a
name of your own choosing is still left alone.

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

### Transaction poolers

A PostgreSQL advisory lock belongs to a **session**. A transaction pooler such
as PgBouncer in `pool_mode = transaction` hands a client whichever backend is
free between transactions, so two clients can be given the same backend — and
there the lock is reentrant and answers "acquired" to both.

That does not weaken the lock. It removes it, and it fails open: both runs
believe they hold it. Measured through PgBouncer 1.25.2 in transaction mode, two
independent client connections and one key:

```text
first handle         acquired=true  backend=113
second handle        acquired=true  backend=113
first handle again   acquired=true  backend=113
```

So Ptah checks, rather than trusts. After taking the lock it asks a second
connection for the same one; if that succeeds, the lock excludes nothing and the
command refuses before it changes anything:

```console
$ ptah migrations up --db-url "postgres://…@pgbouncer:6432/app"
error: postgres advisory lock "ptah_migrate" excludes nothing on this connection:
a second connection took the same lock, which is what a transaction-pooling proxy
such as PgBouncer produces — it hands both clients one backend session, where the
lock is reentrant. …
```

The check asks the property rather than identifying the proxy, so it holds for
any topology that shares backends, named or not, and costs one query against a
direct server.

**Point schema-mutating commands at the database directly.** Reads through a
pooler are unaffected — one server read both ways gives the same description,
and a live test asserts it — it is the lock that cannot survive one. Where the
risk is understood and accepted — a single deploy job that no other run can
race — `PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK=1` skips the refusal. It does not
skip the lock, only the proof that the lock excludes anybody.

A **transaction**-scoped advisory lock does survive a pooler, because it is held
to the end of a transaction and a pooler keeps one backend for the whole of one.
Measured through PgBouncer 1.25.2 in transaction mode, two concurrent
transactions and one key:

```text
first transaction    pg_try_advisory_xact_lock = true   backend 79
second transaction   pg_try_advisory_xact_lock = false  backend 82
```

It is not what Ptah's migration lock uses, because that lock is held across
planning and applying rather than inside one transaction. The property is
pinned by a live test so it is a measured fact rather than a plan.

### `search_path` in the URL

A `?search_path=` on a PostgreSQL URL is sent as a **startup parameter**, and
PgBouncer refuses the connection outright rather than ignoring it:

```text
FATAL: unsupported startup parameter: search_path (SQLSTATE 08P01)
```

Nothing about the schema is wrong there, so Ptah reconnects without the
parameter and carries the selection itself. The server still resolves it —
`set_config('search_path', …, is_local => true)` inside a transaction that is
rolled back — so PostgreSQL's own rule decides, including a list, a `$user`
entry, and a schema the connected role may not use. A transaction is what makes
that safe through a pooler: the setting dies with the transaction, and a pooler
keeps one backend for the whole of one.

Measured on PostgreSQL 17 behind PgBouncer 1.25.2 in transaction mode, one
server reached two ways, the pooled answer beside the direct one for the same
URL:

| `search_path=` | schema selected |
| --- | --- |
| `app` | `app` |
| `app,public` | `app` |
| `nosuch,public` | `public` |
| `nosuch` | refused: `database URL selects schema "nosuch", which does not exist in this database` |

The refusal is carried too. A URL naming a schema the database does not have is
rejected rather than folded back to `public`, because a caller who named a
schema and silently got a different one is the failure Ptah's realm cleanup
turns into a dropped schema.

**Only the schema selection is carried.** Every other startup parameter is the
operator's, and running a command without one would be running it under
settings nobody asked for, so it stays a failure that names the parameter:

```console
$ ptah schema inspect --db-url "postgres://…@pgbouncer:6432/app?statement_timeout=5000"
error: connect to --url: failed to ping database: the database URL carries the
startup parameter "statement_timeout", which the server or the proxy in front of
it refuses: server error: FATAL: unsupported startup parameter:
statement_timeout (SQLSTATE 08P01)
```

The proxy's own `FATAL` is kept, so nothing is taken on trust. Configuring the
proxy to pass a parameter through (PgBouncer: `track_extra_parameters`) is the
other way out, and it is the only way out for a parameter Ptah does not own.

## TimescaleDB

TimescaleDB is PostgreSQL with an extension installed. Ptah manages both of the
objects it adds — hypertables and continuous aggregates — and the two sections
below say what each declaration carries.

### Continuous aggregates

A TimescaleDB continuous aggregate is a view to PostgreSQL: `pg_class` reports
`relkind = 'v'`, and a reader that asks only PostgreSQL describes it as one.
Ptah reads it, declares it and plans it as itself.

Describing one as a view is wrong in both directions, and both were measured on
TimescaleDB 2.29.2 / PostgreSQL 17.11:

- a plan that dropped it emitted `DROP VIEW`, and the server answered
  `cannot drop continuous aggregate using DROP VIEW`, hinting at
  `DROP MATERIALIZED VIEW`. The plan could not apply, and the next run reported
  the same pending change.
- a plan that created it emitted `CREATE VIEW` with the body `pg_get_viewdef`
  answers, which is not the body anybody wrote: TimescaleDB rewrites the
  definition to select from the materialization hypertable, so the emitted view
  named a relation in a schema the extension owns.

So a continuous aggregate is read from `timescaledb_information.continuous_aggregates`
— which keeps the `SELECT` as it was written — and is left out of the view list.

Declare one over the hypertable it materializes:

```go
//ptah:schema:continuousaggregate name="readings_hourly" body="SELECT time_bucket('1 hour', time) AS bucket, device, avg(temperature) AS avg_temp FROM readings GROUP BY bucket, device"
type ReadingsHourly struct{}
```

```hcl
continuous_aggregate "readings_hourly" {
  schema = schema.public
  as     = "SELECT time_bucket('1 hour', time) AS bucket, device, avg(temperature) AS avg_temp FROM readings GROUP BY bucket, device"
}
```

which renders

```sql
CREATE MATERIALIZED VIEW "public"."readings_hourly" WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, device, avg(temperature) AS avg_temp FROM readings GROUP BY bucket, device
WITH NO DATA;
```

**`WITH NO DATA` is not a preference.** Creating an aggregate with data
materializes the whole history the hypertable holds, which is an unbounded
amount of work a schema change must not start on its own; the first refresh is
an operation an operator runs. It is also what makes the statement usable inside
a transaction at all — without it the server answers
`CREATE MATERIALIZED VIEW ... WITH DATA cannot run inside a transaction block`.

`materialized_only` is optional, and an omitted one takes the server's own
default rather than `false`. The default is not a constant across TimescaleDB
versions — measured on 2.29.2, an aggregate created without the option is
reported with it `true` — so a declaration that did not choose is not compared
against it.

The create runs after `create_hypertable`, because the extension checks: over an
ordinary table, `WITH (timescaledb.continuous)` answers
`invalid continuous aggregate view`.

#### A changed body is a drop and a create

There is no `CREATE OR REPLACE MATERIALIZED VIEW` — measured, it is
`syntax error at or near "MATERIALIZED"` — so a changed declaration is planned
as a `DROP MATERIALIZED VIEW` followed by the create, in that order. **The drop
discards the materialization**, and the aggregate starts empty again.

#### The body is compared through the server

TimescaleDB rewrites the definition before storing it, and the rewrite is not a
formatting difference. Measured on 2.29.2:

```text
declared                          stored
time_bucket('1 hour', time)    -> time_bucket('01:00:00'::interval, "time")
GROUP BY bucket, sensor        -> GROUP BY (time_bucket('01:00:00'::interval, "time")), sensor
```

The `GROUP BY` key written by its output name comes back as the whole expression
that name stood for, so no textual fold can decide whether two spellings are the
same aggregate. A comparison that holds a connection asks the server instead:
the declaration is created under a probe name inside a transaction, its stored
definition is read back, and the transaction is rolled back — the aggregate, its
materialization hypertable and everything else it made go with it.

Where there is no connection — `ptah schema render`, a comparison between two
files — the body stays **uncompared**, and only the options are. Reporting a
difference between two spellings of one `SELECT` would drop and recreate the
aggregate on every run, and each drop discards the materialization it exists to
keep.

A declared **view, materialized view or table** whose name a continuous
aggregate already occupies is refused before anything is compared, naming the
aggregate and the hypertable it materializes. The server's own answer at apply
time would be `relation "…" already exists`, halfway through a script.

### Hypertables

A hypertable is an ordinary table partitioned on a range dimension, and nothing
in an ordinary catalog can tell you that it is one. Measured on TimescaleDB
2.29.2 / PostgreSQL 17.11, after `create_hypertable('conditions', by_range('time'))`:
`pg_class` reports `relkind = 'r'`, `pg_depend` reports no extension ownership
for the table, and the index the call created carries the same `deptype` an
ordinary user index does. The extension's own catalog is the only evidence
there is.

Declare one beside the table it partitions:

```go
//ptah:schema:hypertable table="readings" column="time" chunk_interval="1 day" if_not_exists="true"
type ReadingsHypertable struct{}
```

```hcl
hypertable "readings" {
  schema         = schema.public
  column         = "time"
  chunk_interval = "1 day"
  if_not_exists  = true
}
```

which renders

```sql
SELECT create_hypertable('public.readings', by_range('time', INTERVAL '1 day'),
  if_not_exists => TRUE, create_default_indexes => FALSE);
```

`chunk_interval` is optional and is kept in the server's own spelling, because
that is what `timescaledb_information.dimensions` reports back: an omitted one
takes TimescaleDB's default, which is 7 days for a `timestamptz` column.

**`create_default_indexes => FALSE` is not a preference.** The call creates an
index on the dimension unless told not to, and nothing declared it — measured on
2.29.2, the next comparison planned `DROP INDEX IF EXISTS "readings_time_idx"`,
Ptah dropping an index the server had made a moment earlier, on every apply.
Suppressing it keeps the description the whole truth about which indexes exist;
declare an index on the dimension if you want one.

The call runs after the table exists and before anything writes rows. Measured
on the same server, `create_hypertable` against a missing relation answers
`relation "conditions" does not exist`, and against a table holding one row it
answers `table "loaded" is not empty`, hinting at `migrate_data => true`.
**Ptah never sends `migrate_data`**: it rewrites the whole table, which is a
decision an operator makes rather than one a migration takes on their behalf.

#### What cannot be undone

TimescaleDB has no `drop_hypertable` — measured, the call answers
`function drop_hypertable(unknown) does not exist` — and no call repartitions an
existing hypertable. So two changes are refused before anything is applied:

```console
$ ptah schema apply --db-url "$TIMESCALE_URL" --to file://schema.hcl
error: unsupported feature: readings is a hypertable and the desired schema does
not declare one; TimescaleDB has no statement that turns a hypertable back into
an ordinary table, so this needs an explicit migration that drops and recreates it
```

Planning nothing would be worse. The table stays partitioned, the description
says otherwise, and an operator reading "no changes" believes the two agree.

#### One range dimension

A second dimension is a separate call — `add_dimension` with `by_hash` — and no
declaration carries one. A table partitioned on two is described with the first,
and the read says so:

```text
note: 1 hypertable is described with the first partitioning dimension only,
because a declaration carries one range dimension and a second is a separate
call; replaying this description partitions on less than the server does, and a
diff between the two reports no difference: conditions (on time and 1 more
dimension).
```

A hypertable on one dimension is named nowhere, because the description is
complete for it.

#### The capability follows the connection

TimescaleDB is PostgreSQL with an extension installed, not a dialect or a
version of its own, and it puts no token in `version()`. So no preset sets the
`hypertables` or `continuous_aggregates` capability: what decides both is
`pg_extension`, read once when the connection opens. A PostgreSQL target without the extension skips the statement
and says so in the plan rather than failing at apply time on
`function create_hypertable(unknown, unknown) does not exist`.

**Offline, the declaration is the evidence.** `ptah schema render` has no
connection to ask, so a document that declares the `timescaledb` extension
alongside a hypertable renders both — an extension the same script installs is
installed by the time the call runs. A document that declares the hypertable and
not the extension gets the skip comment instead, because nothing in it says the
target has the function.

The same rule reaches an apply that adds the extension in the same plan: the
connection was opened before the extension existed, so its answer is about the
past.

Only HCL and a Go schema can declare a hypertable or a continuous aggregate. A
YAML or `.sql` document records that it could not say so, and the comparison
then withholds the removal its silence would otherwise mean.

## Next steps

- Declaring these objects in Go sources: [Go annotation reference](../../reference/go-annotations/).
- Targeting CockroachDB, YugabyteDB, or Spanner instead: [CockroachDB, YugabyteDB, and Spanner](../distributed/).
- Gating destructive changes before they run: [Lint and gate unsafe SQL](../../versioned/lint/).
