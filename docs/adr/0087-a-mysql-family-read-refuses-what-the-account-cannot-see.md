# ADR 0087: A MySQL-family read refuses what the account cannot see

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5497](https://github.com/stokaro/ptah/issues/5497)

## Context

[ADR 0085](0085-a-schema-read-does-not-depend-on-the-connecting-role.md) states the
rule: a schema read gives the same answer for every role that can connect, or it
fails. PostgreSQL meets it by reading `pg_catalog`. MySQL and MariaDB have no such
catalog for an ordinary account: `information_schema` lists only what the account
holds a privilege on, and a comparison reads a missing row as a missing object.
Measured with a table, a foreign key, a `CHECK`, an index, a view, a trigger and a
procedure on one side only:

| Account | MySQL 8.4 and 26.10 lose | MariaDB 11.8 and 12.3 lose |
| --- | --- | --- |
| `SELECT` on one table | the other table, the view, the trigger, the procedure | also the keys and the `CHECK` |
| `SELECT` on the database | the view's definition, the trigger, the procedure | also the keys and the `CHECK` |
| `SELECT, SHOW VIEW, TRIGGER` on the database | the procedure | the procedure |

## Decision

The reader asks `SHOW GRANTS` what the session holds, and refuses a read that cannot
prove it saw a kind of object. The message names the privilege, and the command exits
2. The check is in `internal/dbschema/mysql`, below the CLI, so every command that
reads gets it and `ptah-compat` gets it at exit 1. `internal/mysqlgrants` reads the
lines, including the rules that make a grant apply: a database-level row is a
pattern, the most specific matching row is the one used, and a role counts while it
is active. A command marks the read of its target, so a machine-readable document
reports that refusal with the error kind `permission_denied` and the privileges as
fields, and the same refusal from a dev database or a `--to` source has no kind.

What can be read off the rows is read off the rows. A view listed with an empty
definition, a trigger with no body and a routine with a NULL body say that the
account may list the object and not read it, and that needs no knowledge of grants.
What has no row, a table the account holds nothing on, can only be known from the
privileges.

A writing command gets the same refusal, because an apply that believes an object is
absent plans from that belief.

## What does not refuse

A false refusal is a regression. These read without the proof:

- **A run that leaves a kind out.** A selector naming a whole kind,
  `--exclude '*[type=trigger]'`, says the run does not depend on it. A selector naming
  objects says nothing: a read that cannot see a trigger cannot say what it is called.
  `internal/readscope` carries this from the run to the reader in the context, because
  the selectors apply after the read and the read sits behind `catalog.SchemaReader`.
- **A dev or shadow database.** The read covers what the same account created there
  in the same run, and an account is shown its own triggers and routines. The account
  still has to hold a privilege that lists columns, and a view or routine that comes
  back without a body is still refused.
- **A read of the columns of a declared table**, as `internal/datamigrate` makes, and
  the agent API's list of tables and views, which depend on nothing else.

`diff.skip` relieves nothing: it leaves destructive changes out of a plan, and the
comparison still reads the kind to find what to add.

These blind spots stay, written down rather than refused:

- A URL that names no database covers the databases the account can see, as the plan
  and apply pages state, and each of them is held to this rule. A database the account
  holds nothing on is outside the comparison. Refusing there would refuse every
  account fenced to a name pattern, whose whole server is the point of the grant.
- Reading roles and grants needs `SELECT` on the `mysql` database. An account without
  it records them as not described, and a comparison decides nothing about them.
  Refusing there would refuse every account that holds `ALL PRIVILEGES` on its own
  database.

## Alternatives

**A warning that keeps exit 0.** The reader of `Schemas are synced` in a pipeline does
not read standard error.

**`information_schema.SCHEMA_PRIVILEGES` and `USER_PRIVILEGES`.** They need no parser
and match a pattern with `LIKE`, but they list the account's own rows and not an active
role's, and a partial revoke is not in them.

**Probing each object kind by asking for one.** An account that sees nothing gets an
empty answer whether or not anything exists, so a probe cannot tell the two apart.

## Consequences

An account that compared what it could see now has to be given what the comparison
depends on, or to leave the rest out. The page on
[MySQL and MariaDB](../site/src/content/docs/databases/mysql.md#what-the-connecting-account-needs)
lists the privilege for each kind.

A server or proxy that does not answer `SHOW GRANTS` fails the read, because nothing
then proves it complete.

Ptah answers where the pinned community binary says `Schemas are synced` at exit 0, so
this is stricter than Atlas CE, deliberately; `docs/conformance.md` records the
measurement.
