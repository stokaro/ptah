# ADR 0085: A schema read does not depend on the connecting role

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5498](https://github.com/stokaro/ptah/issues/5498)

## Context

A comparison reads a missing row as a missing object. That is right only if the
read could have seen the object. `Schemas are synced` is the answer people act on
against production, so a read that loses objects to the connecting role's
privileges gives the most damaging wrong answer a comparison has.

At cda568c0d the PostgreSQL reader takes tables, columns, constraints, a view's
check option and table grants from `information_schema`. Those views answer for the
role that asks. Measured there on PostgreSQL 18.6 with two databases that differ:

| Connecting role | What the read loses | `schema diff` |
| --- | --- | --- |
| no table privileges | every table, column, constraint and grant | `Schemas are synced`, exit 0, for a difference in those |
| `pg_read_all_data` | every constraint and grant | `Schemas are synced`, exit 0, for a difference in those |

Indexes, views and sequences come from `pg_catalog`, so the result is partial: the
same role gets a `DROP INDEX` for a table it cannot see. `schema drift` reports a
table that exists as missing and rates that safe.

## Decision

A schema read gives the same answer for every role that can connect, or it fails.

On PostgreSQL and YugabyteDB the reader takes those facts from `pg_catalog`, which
lists every object to every role. `internal/pgcatalogview` holds a statement for
each view it replaces, with the view's columns and expressions and without its
privilege condition. A live test compares each statement with its view on the
connection whose role the views filter nothing for.

Where the catalog cannot answer, the command fails at exit 2 and the message names
what could not be read and the privilege that is missing. Two facts are in that
position:

- A role with no `USAGE` on any schema on its `search_path` has no current schema:
  PostgreSQL answers NULL for it, as for a schema that does not exist.
- Whether a role has a password is in `pg_authid`, which only a superuser can read
  by default. A comparison that declares a password has to decide this.

A warning that keeps exit 0 is not an option. The reader of `Schemas are synced` in
a pipeline does not read standard error.

## Alternatives

**Refuse any role that lacks privileges.** Asking `has_table_privilege` for every
relation, and refusing if one answer is no, also stops the false answer. It also
stops the read for a role that could have given the full answer, and a read-only
comparison role is what an operator should want to use.

**Keep `information_schema` and add what it hides.** The read would still differ by
role wherever a new branch is missed, and each view hides something different.
`check_constraints` shows only to the owner, `table_constraints` leaves SELECT out
of the privileges it counts, and `role_table_grants` follows the enabled roles.

**Call `information_schema._pg_*` from the new statements.** They are the functions
the view uses, so the numbers could not drift. They also leave a read depending on
`USAGE` on that schema, and a guard that no statement names `information_schema`
could no longer be written. The live test covers the drift instead.

## Other targets

CockroachDB stays on `information_schema` because its view does not filter this
way. Measured on v26.3.5, a role refused `SELECT` on a table reads the same
description of it as `root`.

Spanner's `pg_catalog` is an emulation over `information_schema`, without the
helper functions the statements call.

MySQL and MariaDB have the same exposure and no catalog to move to: an ordinary
account has only `information_schema`, filtered by its privileges. The fix there
has to be a refusal that knows which privilege each object kind needs
([stokaro/ptah#5497](https://github.com/stokaro/ptah/issues/5497)).
[The MySQL page](../site/src/content/docs/databases/mysql.md) states the account to
use.

## A complete desired side

The same false answer reaches a superuser from another direction. The grant
comparison removes only a grant to a role the desired state manages, because a
file that says nothing about a role makes no claim about what the role may do. A
database read as `--to` is not such a file. It holds every grant on what it
describes, so a grant only `--from` holds is a difference, whoever holds it.

`config.CompareOptions.DesiredGrantsComplete` carries that fact, and `schema diff`
sets it for a `--to` database. Two rows stay out, because the other side holds them
by default and reads as holding nothing: an object owner's privileges, and PUBLIC's
`EXECUTE` on a routine. Revoking either to match takes away what the other side
holds.

## Consequences

A comparison can run as a role with no table privileges and needs no membership in
the owner's role.

A role with no usable schema still fails at connection, for every verb, including
one that names `--schemas`.

The reader needs a statement in `internal/pgcatalogview` for a new fact a view would
have given. A read that reaches for `information_schema` on these targets fails
`TestReadSchemaContext_AsksInformationSchemaNothing`.
