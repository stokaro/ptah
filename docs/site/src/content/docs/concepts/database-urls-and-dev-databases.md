---
title: Database URLs and dev databases
description: Accepted database URL formats, and the difference between the target, dev, shadow, and throwaway databases.
type: concept
audience:
  - "all-users"
readerQuestion: "How does Ptah model database URLs and dev databases?"
goal: "Explain Ptah's model for database URLs and dev databases."
sourceOfTruth:
  - "dbschema/connection.go"
  - "cmd"
generated: false
overlaps: []
disposition: keep
---

Every Ptah command that touches a database takes a URL, and the URL's scheme
selects the engine. The same syntax names databases in four roles: the target
you are changing, and up to three kinds of disposable databases that exist so
mistakes happen somewhere harmless. This page defines all four; other pages
link here instead of redefining them.

## URL formats

| Engine | Example |
| --- | --- |
| PostgreSQL | `postgres://user:pass@localhost:5432/app` |
| MySQL | `mysql://user:pass@localhost:3306/app` (Go-driver form `mysql://user:pass@tcp(localhost:3306)/app` is also accepted) |
| MariaDB | `mariadb://user:pass@localhost:3306/app` (the Atlas CLI spelling `maria://user:pass@localhost:3306/app` is also accepted) |
| SQLite | `sqlite://relative.db`, `sqlite:///absolute/path/app.db`, `sqlite:file:C:/absolute/windows/path/app.db`, `sqlite:///:memory:`, `sqlite:file:memdb1?mode=memory&cache=shared` |
| SQL Server | `sqlserver://sa:pass@localhost:1433?database=app` (plus a Ptah-only `schema` parameter — see [SQL Server](../../databases/sqlserver/)) |
| ClickHouse | `clickhouse://user:pass@localhost:9000/app` |
| CockroachDB | `cockroachdb://user:pass@localhost:26257/app` |
| YugabyteDB | `yugabytedb://user:pass@localhost:5433/app` |
| Oracle | `oracle://user:pass@localhost:1521/service` (renders, plans, reads a live catalog, and runs versioned migrations) |
| YDB, through its provider | `ydb://localhost:2136/local`, or `ydbs://` over TLS (see [YDB](../../extend/providers/ydb/usage/#connecting)) |
| Spanner's PostgreSQL interface, through its provider | `spanner://localhost:5432/app`, with `cloudspanner://` and `google-spanner://` as the same address (see [Spanner](../../extend/providers/spanner/usage/#connecting)) |

Scheme aliases normalize to the canonical dialect (`postgresql://`,
`sqlite3://`, `mssql://`, `crdb://`, `ysql://`, `ch://`, and more) — the full
alias list is on the
[Database support matrix](../../databases/support-matrix/). A URL with an
unrecognized scheme fails with `unsupported database dialect`.

A MySQL or MariaDB server behind a Unix socket takes the Atlas CLI socket
form, `mysql+unix://user:pass@/run/mysqld/mysqld.sock?database=app`, or
`mariadb+unix://` and `maria+unix://`: the path is the socket, `database`
names the database, and a host is ignored. The Go-driver form
`mysql://user:pass@unix(/run/mysqld/mysqld.sock)/app` reaches the same socket.

A MySQL or MariaDB server whose certificate a private certificate authority
signed is verified with `ssl-ca`, the path of a PEM file of CA certificates:
`mysql://user:pass@db.internal:3306/app?ssl-ca=/etc/ptah/mysql-ca.pem`. The
connection then requires TLS and verifies the certificate and the host name
against that file alone, for that URL only. A relative path is read from the
directory the command runs in. The rules, the refused combinations and a
local example are in
[MySQL and MariaDB](../../databases/mysql/#tls-with-a-private-certificate-authority).

A MySQL or MariaDB URL naming no database is the whole server, as in the
Atlas CLI; as a dev database it is a
[dev server](../../atlas/migrate-commands/#a-whole-dev-server).

A password holding a character a URL reserves, such as `@ : / ? # %`, is
percent-encoded in a URL. The Go-driver form takes it as written, because that
driver reads a password up to the last `@` before the last `/`. A URL written with
such a character raw is refused, and the error says so.

Ptah never repeats a password. Output, errors and reports show an address with
its password as `***` and the value of a secret parameter, such as `sslpassword`,
`authToken` or `TOKEN`, as `redacted`, in every form an engine takes: a URL, a
Go-driver DSN with or without a scheme, and a keyword, ADO or ODBC string. A
secret-named run-time parameter that PostgreSQL's `options` setting passes,
such as `-c app.jwt_secret=...`, is shown as `redacted` too. The rest of the
address stays, so the line still says which database it means.

### Secrets in errors and program output

A driver's or a server's error can quote the address, or what the driver read
from it, and a hook or another program Ptah starts can print a password it was
given. Ptah takes each secret it knows of out of that text:

- A secret of five characters or more is taken out wherever it occurs, glued
  to other text too, as in `-pSecret1` or `\nSecret1`.
- A shorter one is taken out only where it stands as a credential:
  - between the `:` or the `/` and the `@` of an address, as written or
    percent-encoded: `app:...@host`, `app/...@host`;
  - after a `:` and before a space or the end of a line, as curl's `-u
    app:...` and a line of a password file write it;
  - as the whole value of a secret setting after an `=` or a `:`, such as
    `password=`, `PGPASSWORD=`, `password: ` or the JSON `"password": "`;
  - after `-p`, `--password`, `Bearer`, `PASSWORD '` and `IDENTIFIED BY '`.

A short password in any other place stays, so with the password `x` an error
still reads `database=execute`. A word that only holds a short password's
letters stays whole, and does not show the password by what is missing from
it. A program that writes a short password in another shape prints it.

## The four database roles

**The target database** is the one a command reads or changes: `--db-url` on
native commands, `--url` on Atlas-compatible ones. It is the only database
whose state matters after the command exits.

**A dev database** (`--dev-url`) is a disposable replay target used for
validation:

- `ptah migrations validate` and `ptah migrations lint` clean it and replay the
  migration directory on it to prove the SQL executes;
- `schema apply` rehearses its plan on it before touching the target;
- `schema diff` replays a migration directory on it, and creates a `--from`
  schema file on it when `--to` is a database or migration directory;
- Atlas-compatible verbs use it for planning, linting, and rollback
  verification.

A dev or shadow URL given for the run, on the command line or through its
`PTAH_*` variable, is opened even when the run needs nothing from it, such as
`ptah schema diff` of two schema files, `ptah schema compare`, `ptah schema
plan` or a `ptah migrations down` with nothing to roll back. A URL nothing
answers on then fails this run, while the mistake is in front of the operator,
rather than the first later run that needs the database. The `dev:` key in
`ptah.yaml` is different: it is a default for every command in the project, so
a native command that needs nothing from the dev database does not fail on a
`dev:` URL nothing answers on. `ptah schema inspect` of a database never opens
a dev URL, wherever it came from.

`ptah-compat` opens every dev URL it is given, from a flag or from `atlas.hcl`,
because the Atlas CLI fails on one it cannot reach. Its `schema inspect` of a
database is the exception, as it is on the Atlas CLI.

Two kinds of URL are read without a connection in such a run. A docker URL,
`docker://` or `docker+<engine>://`, is read for the server its tag names, and
no container is started. When a selected
[executable provider](../../extend/executable-providers/) provisions the URL's
engine, only the engine is read, and the provider is asked for no database. A
URL whose engine is not the target's is refused. A `sqlite://` URL or a file
path is not opened, since opening it would create the file, so a SQLite dev
path that does not exist is not refused there.

A dev database must hold nothing the reset would drop when a command starts.
Ptah refuses a table before anything is dropped, as the Atlas CLI does, and
any other such object too. Ptah then cleans the
replay realm before migration execution and after the replay, failed or not.
Commands read the replayed state before the final cleanup, on the same session.
No fixed time limit applies to a cleanup: it takes as long as emptying the
realm takes. After an interrupt, the cleanup has 30 seconds to finish; a second
interrupt stops the process at once.

Everything a dev database runs stays inside it. Where a schema names a database
rather than a namespace — MySQL, MariaDB, and ClickHouse — a plan carrying the
target's schema name is re-scoped onto the dev database before it is rehearsed,
and a statement naming some third database is refused instead of run.

`mysql://`, `mariadb://` and `maria://` open one driver, and the server's
version banner, not the scheme, says which engine answers. A dev URL may spell
the family differently from its target, and two spellings of one database are
one database. `schema apply` and `migrate diff` refuse a MySQL dev database for
a MariaDB target, and the reverse.

**A shadow database** (`--shadow-db`) is a disposable verification target for
commands that write or record migrations: `ptah migrations generate` replays
the directory — including the new migration, up, down, and up again — before
keeping any files, and `ptah migrations checkpoint` and
`ptah migrations baseline` use it to verify that migrations reproduce the
expected schema before anything is recorded. `ptah migrations down` uses it to
verify the rollback plan before changing the target.

The shadow database must meet the same rule and must not be the target's live
database realm. Ptah checks both before changing either database, and empties
the shadow database again after each run.

**A throwaway test database** is what `ptah migrations test` and
`ptah schema test` run cases against: by default a fresh ephemeral SQLite
database per case, or the database passed with `--db-url` when tests must
exercise a real server dialect — see
[Test migrations and schemas](../../testing/migrations-and-schema/). On YDB the
provider runs the cases in a dev realm in that database.

## Consequences

- **Disposable means Ptah may drop everything it supports.** Dev and shadow
  database workflows clean user objects, and test seed steps bypass the
  seeder's protected-environment guards. Point these flags at scratch databases
  only, never at a real environment. Rollback verification, `schema apply`,
  `schema diff`, and `migrate diff` refuse, before any reset, a dev or shadow
  URL naming a database they read, by URL or by the realm each server reports.
  Equal network database names fail closed across different endpoints because
  DNS aliases and replicated members cannot be proven independent before
  destructive cleanup. Cleanup rejects system, template and metadata databases,
  and a server's default database unless the run owns it.
- **Migration-diff scope does not reduce the replay realm.** Repeated
  `--schema` values select which schemas `migrate diff` compares and emits. They do not
  limit which schemas a migration may create or which user schemas final
  cleanup removes. PostgreSQL extensions remain in both scoped projections:
  an extension is a database-wide identity, and its schema records installation
  placement rather than ownership by that schema. An extension declared on
  both sides remains synced even when installed outside the named schemas; an
  extension the replay created and the desired schema omits remains an explicit
  global removal. One the dev database held when the command started is its
  environment: a comparison leaves it out unless the desired schema declares
  it, so no migration drops it.
- **The replay realm follows the database engine.** PostgreSQL, CockroachDB,
  and YugabyteDB cleanup treats all user schemas and user-installed extensions
  in the selected database as one dependency graph. The row-security policies
  of its tables go before any relation: a policy holds back each relation it
  reads, so tables whose policies read each other's table are dropped too.
  Extensions the dev
  database held when the run started stay installed, with everything they own,
  so a migration may use a type from one without creating it, as under Atlas;
  a schema one is installed in is emptied in place. A dev URL that selects a
  schema with `search_path` also keeps the database's other schemas as they
  were, `public` included. An extension the run created is dropped with the
  schemas and tables it owns; TimescaleDB's catalog schemas are this shape.
  So are PostgreSQL and YugabyteDB default privileges: the global ones, and
  with `search_path` those set in the selected schema. A default the run set is
  taken back, so a Supabase image works as a dev database.
  MySQL, MariaDB, and ClickHouse cleanup owns the selected database. SQL
  Server cleanup owns all supported user schemas in the selected database.
  SQLite cleanup owns `main` on one pinned session. YDB has no SQL that
  creates a database, so its provider gives a run a dev realm instead: a
  directory under `ptah_dev` in the database its dev, shadow or test URL names,
  which the run treats as its whole database and the provider removes at the
  end. A run against the database itself leaves `ptah_dev` out, so the URL may
  name the target; see
  [YDB](../../extend/providers/ydb/usage/#dev-shadow-and-scratch-databases).
- **PostgreSQL cleanup gives back the schema it empties.** The schema the dev
  URL selects, `public` when it selects none, comes back after the cleanup
  with the owner, grants and comment it had when the run started. A role
  created on the server afterwards can use `public` as it could before. A run
  that revokes `USAGE` on `public` or comments it does not hand the change to
  the next run, and neither does one whose dev database keeps an extension in
  `public`, where the schema is emptied in place. A schema the connecting
  role does not own is emptied in place too, since only its owner may drop
  it. On PostgreSQL 13 and 14 the bootstrap superuser owns `public` in every
  new database, so a dev database an ordinary role owns is this case. Ptah
  refuses such a schema before it drops anything when the role holds a grant
  option on it, because the cleanup could not take back what a run grants
  there.
- **MySQL-family cleanup needs global catalog visibility.** MySQL cleanup
  credentials require global `SELECT`, `DROP`, `ALTER`, `ALTER ROUTINE`,
  `EVENT`, `LOCK TABLES`, and `PROCESS`. MySQL also requires global `TRIGGER`
  and, on MySQL 8.0.20 and newer, `SHOW_ROUTINE`; MariaDB requires global
  `SHOW VIEW`. Ptah checks these privileges before destructive DDL. It fails
  closed when another user database contains a routine, event, or trigger
  because stored-program bodies can reference the cleanup realm without a
  catalog dependency. Use dedicated server instances and credentials only for
  disposable dev databases.
- **ClickHouse realm cleanup requires 24.11 or newer.** Ptah uses `CHECK GRANT`
  with global `SHOW DATABASES` and `SHOW TABLES` to prove complete catalog
  visibility before dropping objects. ClickHouse does not expose ordinary-view
  dependencies, so Ptah fails closed when another user database contains a
  view, materialized view, live/window view, dictionary, or `Buffer`,
  `Distributed`, or `Merge` table. Older servers fail before cleanup because
  role-aware visibility cannot be proven safely.
- **PostgreSQL-family cleanup keeps the database-scoped artifacts it found.**
  Publications, subscriptions, logical replication slots, event triggers, and
  non-extension foreign-data wrappers, servers, or user mappings the dev
  database held when the command started are its environment, as under Atlas:
  the realm cleanup leaves them and checks they are still there. PostgreSQL and
  YugabyteDB reject one the run created before DDL, since the cleanup cannot
  remove it. A PostgreSQL publication is the exception: replaying a desired
  schema that declares one creates it, so the cleanup drops each publication
  the dev database did not hold when the command started, after the check that
  refuses the rest. A replay may create, change and drop its own publications.
  It may not change or drop one the dev database held, such as
  `supabase_realtime`, since the cleanup keeps that publication as it finds it.
  Where the server cannot list its publications when the command starts, every
  publication statement is refused. Ptah lists them from catalogs and views any
  role may read, user mappings from `pg_user_mappings`, so the check runs the
  same for an ordinary role that owns the dev database as for a superuser. An
  object the server created when it built the database template is not counted:
  YugabyteDB 2026.1.2 puts the foreign server `yb_global_views_server` into
  every database, and the cleanup leaves it, and the extensions it depends on,
  in place. PostgreSQL also removes and verifies database large objects
  transactionally. YugabyteDB does not run that PostgreSQL-specific
  large-object operation.
- **A docker block's starting point is kept whole.** A PostgreSQL dev
  database an `atlas.hcl`
  [`docker` block](../../atlas/project-config/#docker-dev-database-block)
  provisions starts from what its image and baseline leave. Cleanup returns
  that database to the starting point instead of emptying the realm, and
  refuses when the run dropped part of it or changed what no statement
  restores.
- **SQL Server cleanup rejects replication state.** A replication-enabled
  database or replicated table fails before DDL, along with other unsupported
  database-scoped artifacts. Remove replication configuration or use a
  dedicated disposable database before replay.
- **Cross-realm operations fail before execution.** Ptah rejects direct
  statements that switch or mutate another database, protected namespace,
  server, cluster, another session's temporary namespace, external file, or attached SQLite
  database. It also rejects statement forms whose nested SQL cannot be
  confined safely during replay.
- **Replay cleanup is serialized by realm.** PostgreSQL, YugabyteDB, MySQL,
  MariaDB, and SQL Server use database advisory locks, and the YDB provider a
  semaphore on the coordination node `ptah_locks`, keyed by the database and
  the dev realm.
  SQLite, ClickHouse, and
  CockroachDB use an operating-system file lock keyed by the normalized
  database identity. That file lock coordinates only Ptah processes that
  resolve the same temporary lock path and can access it, normally processes
  running as the same operating-system user with the same temporary-directory
  configuration. Different users, different temporary directories, other
  hosts, and non-Ptah clients are not coordinated. Cross-host ClickHouse and
  CockroachDB replay is unsupported because neither engine provides the
  required effective session advisory lock.
- **Match the target engine.** Replay verification proves the SQL executes on
  the engine it ran against, so a dev or shadow database should run the same
  engine (and ideally the same version) as the target.
- **Every flag has an environment variable.** `PTAH_DB_URL`, `PTAH_DEV_URL`,
  and `PTAH_SHADOW_DB` set the corresponding flags, which keeps credentials
  out of CI command lines. A malformed non-empty value fails before command
  execution. See [Configuration](../../reference/configuration/).

### Statement forms that fail closed

Replay rejects SQL sublanguages and storage mechanisms whose effects cannot be
proven to stay inside the disposable realm:

- PostgreSQL-family `DO`, `CALL`, routine creation or alteration, foreign-table
  creation or alteration, foreign servers, `IMPORT FOREIGN SCHEMA`,
  `SET search_path`, `SELECT INTO` protected namespaces, and CockroachDB's
  `ALTER DEFAULT PRIVILEGES FOR ALL ROLES` without `IN SCHEMA`.
- MySQL and MariaDB executable comments, `CALL`, events, triggers, routines,
  `LOAD DATA`/`LOAD XML`, and externally backed `FEDERATED` or `CONNECT`
  tables.
- SQL Server `EXEC`/`EXECUTE`, procedure/function/trigger creation or
  alteration, synonym/external-table creation, `BULK INSERT`, `BACKUP`,
  `RESTORE`, and server-level maintenance statements.
- ClickHouse remote, distributed, replicated, and unknown table engines,
  external dictionary sources, `FREEZE`/`UNFREEZE`, and `SYSTEM` statements but
  `SYSTEM STOP MERGES` and `SYSTEM START MERGES` of one table in the dev
  database, which a [table rebuild](../../databases/clickhouse/#creating-a-table-again)
  runs. Tables and standalone materialized views must select an explicitly
  allowlisted engine.
- SQLite `ATTACH`, `DETACH`, temporary views and triggers, any statement that names
  the `temp` schema, and non-restorable pragmas.
- YDB, through its provider's replay guard, writes whose target is an absolute
  path, climbs out of the dev realm
  with `..`, is named through a `$` expression or names a cluster,
  `PRAGMA TablePathPrefix`, `DEFINE ACTION`, `DO`, `EVALUATE`, users, groups,
  `GRANT`, `REVOKE`, secrets, resource pools, backups, `ALTER DATABASE`,
  topics, external data sources and tables, async replication, transfers, and
  streaming queries.

Replay runs every migration of a directory on one session, as `migrate apply`
runs the migrations of one run, so a PostgreSQL `SET` or `RESET` that changes
the session would carry into the migrations after it, and is refused. A
setting that ends with its transaction is accepted:

- `SET LOCAL` and `SET CONSTRAINTS`;
- `set_config(name, value, true)`, with the name written as a string and `true`
  written as the keyword.

PostgreSQL `CREATE TEMP TABLE` and `CREATE TEMPORARY TABLE` are accepted on the
replay's pinned physical session. Later statements and migrations can use those
tables, including through `pg_temp`. Ptah discards that session after success,
failure, or cancellation. Explicit `pg_temp_<number>` targets remain protected;
the replay cannot take ownership of another session's temporary tables.

SQLite `CREATE TEMP TABLE` and `CREATE TEMPORARY TABLE` are accepted too. Later
statements and migrations can use the table. A SQLite temporary table lives on
the connection, and an in-memory dev database keeps its only connection, so Ptah
drops the replay's temporary objects after success, failure, or cancellation
instead of discarding the session. A statement that names the `temp` schema, such
as `CREATE TABLE temp.t`, remains refused.

A PostgreSQL-family `COMMENT ON SCHEMA` is accepted when the dev URL pins no
schema. The reset that ends the replay returns each schema it keeps to the comment
it held, and drops the others. A URL pinned to one schema is reset without reading
that state, so there the comment is refused. A privilege on a schema is refused in
every replay, because it depends on the server's roles, and so is a comment on a
protected namespace such as `pg_catalog`. The pinned community binary
replays all of these, so `ptah-compat` is stricter here, deliberately.

Replay runs each migration the way `migrations up` does, in one transaction
unless the file opts out with `-- +ptah no_transaction` or
`-- atlas:txmode none`. Such a setting holds for the rest of its migration and
is gone before the next. In a file that opts out, PostgreSQL ignores a
`SET LOCAL` with a warning, on replay and on apply alike.

A PostgreSQL migration file that opens and closes its own transaction replays
as written. Histories written for golang-migrate have files that begin with
`BEGIN` and end with `COMMIT`. Replay reads such a file as a whole before it
runs a statement and runs it without a transaction of its own. A file that fails
is rolled back on the pinned session before the dev database is cleaned. The
whole file is refused, with the statement named, when it:

- leaves a transaction open, closes one it did not open, or opens one inside
  another;
- uses `ROLLBACK`, `ABORT`, `SAVEPOINT`, `RELEASE` or `ROLLBACK TO`;
- writes `AND CHAIN`, `PREPARE TRANSACTION`, `COMMIT PREPARED`, or a `BEGIN`
  mode other than an isolation level, access mode or `DEFERRABLE`
  (`COMMIT AND NO CHAIN` is a plain `COMMIT`);
- asks for `-- atlas:txmode file` and opens its own transaction.

This holds for PostgreSQL only, and only where Ptah replays an existing
migration directory. Elsewhere in a replay each of these statements is refused
on its own. In a golang-migrate directory, and in one that `--runner
golang-migrate` or `migration.runner` names, replay runs a file the way
`ptah-compat migrate apply` runs it, which is how golang-migrate's default mode
runs it: a file that opens its own transaction runs in the transactions the
file sent as one query runs in, and a file of one statement runs without one.
The reading there also accepts what golang-migrate applies: a `COMMIT` outside
a block, a `BEGIN` inside one, and `SAVEPOINT`, `RELEASE` and `ROLLBACK TO`
inside one; see
[Transactions in a golang-migrate directory](../../atlas/migrate-commands/#transactions-in-a-golang-migrate-directory).
`LOCK TABLE` is accepted for the same reason as `SET LOCAL`; a lock on a table in
a protected namespace is refused.

A rollback that `ptah migrations down --runner golang-migrate --shadow-db`
verifies runs each down file on the shadow database the same way, and
`--plan` builds the target version there by running each up file the same
way.

Three parameters stay refused even for one transaction. `search_path` (and its
alias `SET SCHEMA`) decides where an unqualified name lands, which would let a
`CREATE FUNCTION` reach `pg_catalog`. `role` and `session_authorization` change
who the rest of the migration runs as.

The rejection happens while Ptah validates the whole migration, before its
first statement executes. Realm-local removals Ptah can classify without
reading a routine body, such as `DROP FUNCTION`, `DROP FOREIGN TABLE`,
`DROP SYNONYM` and `DROP EXTERNAL TABLE`, stay allowed.

### A server Ptah provisions

A `docker://` or
[`docker+<driver>://`](../../atlas/migrate-commands/#a-dev-database-from-a-named-image)
dev URL starts a server for one command and removes it afterwards, so the
server holds only what the run put there. On such a server,
replay also runs the statements it refuses elsewhere only because their effect
reaches past the dev database:

- PostgreSQL-family `DO`, `CALL`, routine creation and alteration,
  `CREATE` and `DROP` of a role, user, group or database, `ALTER ROLE`
  without `SET` or `RESET`, role membership, privileges on a role, database,
  schema, language or parameter, CockroachDB's
  `ALTER DEFAULT PRIVILEGES FOR ALL ROLES` without `IN SCHEMA`, `DROP OWNED`,
  `REASSIGN OWNED`, `COMMENT ON` a role, a database, or an extension or
  schema of the dev database, and `ALTER EXTENSION ... UPDATE [TO ...]` whose
  name and version Ptah reads. A server declared disposable lifts all of these
  except the comment on an extension or a schema and the extension update,
  because that server outlives the run and its reset keeps them. Replay does
  not read a `DO` block or a routine body; outside one, any other
  `ALTER EXTENSION` stays refused. A
  [rehearsal](#the-baseline-a-rehearsal-writes) allows a schema's own comment
  and privileges where its reset returns them.
- MySQL and MariaDB routines, triggers, `CALL`, `GRANT`, `REVOKE`, `CREATE`
  and `DROP` of a user, role or database, and writes to another database.
- YDB writes anywhere in the server, `PRAGMA TablePathPrefix`, actions, users,
  groups, permissions, secrets, resource pools, backups and topics, on
  `docker://ydb/<tag>`, which the YDB provider starts. YDB gets no dev realm
  there: the database is the dev database.

So a migration directory that creates its role in a `DO` block or defines a
function replays on `docker://postgres/18/dev`.

The rest of the list above still fails closed on a provisioned server. Those
statements change the replay session or the sessions the rest of the command
opens (`SET search_path`, `ALTER ROLE ... SET`, any `ALTER DATABASE`,
transaction control), change how the command's later SQL runs (`ALTER SYSTEM`,
event triggers, casts, languages), reach past the container (foreign servers,
subscriptions, `dblink`, an external `COPY`), or mutate a protected namespace
such as `pg_catalog`. A MySQL or MariaDB event stays refused because the
scheduler runs it during the rest of the command.

A YDB external data source or table, async replication, a transfer and a
streaming query reach outside the container and stay refused. local-ydb takes
every connection without a credential, so the YDB provider refuses
`docker://ydb` when the container runtime is on another machine, where its port
would be published on every interface of that host.

A `docker://<engine>/...` or `docker+<engine>://...` URL whose engine belongs to
a selected [executable provider](../../extend/executable-providers/) goes to
that provider instead. The provider picks the image, starts the server and
removes it when the command ends. Ptah records the address it gets back as a
server the run owns, as for a server it starts itself. A provider that will not
provision answers with a refusal that says whether it does not serve the request
or its runtime is unavailable, and the command stops with that reason.

The decision reads what Ptah recorded when it started the server, not the
URL's spelling. A named server keeps the whole list, since it may hold
databases and roles that are not the run's. Its refusal names the two ways to
this realm, a docker URL and the declaration below, and
[trust in the SQL](#sql-you-trust), which lifts the refusal without making the
server the run's.

### A server declared disposable

A server Ptah did not start can be as disposable: a CI service container
the job throws away, or a container the operator runs. `PTAH_DEV_SERVER_DISPOSABLE=1` declares that the server
`--dev-url` names is the run's own. Replay then runs the statements listed
above on it, as on a server Ptah provisions, except a comment on an extension
or a schema of the dev database and an extension update, and refuses the same
rest.

```bash
PTAH_DEV_SERVER_DISPOSABLE=1 ptah migrations validate --dir migrations --dev-url "$DEV_URL"
```

The declaration covers the whole server, its default database (`postgres`)
included. After a replay Ptah cleans the dev database. On PostgreSQL,
CockroachDB and YugabyteDB it also drops every role the server did not hold
when the command claimed the dev database. On a server nothing else uses, those
roles are the run's, so a later command that replays the same directory creates
them again. A role the server already held stays, whoever created it. A YDB dev
URL declared this way gets no dev realm: its database is the dev database, and
it has to be empty. A database a replay creates, and a role or a user on any
other engine, stays on the server until the container is removed. A later
command that replays the same directory on the same server meets it, so a
migration that creates one without checking for it first fails the second
time. Declare only a server that nothing else uses.

Every command that replays a migration directory or rehearses a plan on a dev
database reads the variable, on both binaries, and refuses a non-boolean value
before any work, whether or not it replays. Strict Atlas
compatibility keeps the variable, because the pinned community binary runs
these statements on any dev database.

### SQL you trust

The refusals above protect a shared server from SQL the run cannot confine to
its dev database. When you wrote that SQL or reviewed it, you can tell Ptah to
run it on a dev server that other work shares: pass `--trust-replay-sql` to a
native command, or set `PTAH_TRUST_REPLAY_SQL=1` for either binary. The flag
takes precedence over the variable. With neither, the guard refuses as
described above, and each refusal that trust would lift says so.

```bash
ptah migrations validate --dir migrations --dev-url "$DEV_URL" --trust-replay-sql
PTAH_TRUST_REPLAY_SQL=1 ptah-compat migrate validate --dir file://migrations --dev-url "$DEV_URL"
```

**Trust does not confine anything.** The SQL runs with the privileges of the
dev connection, and what it does to the rest of the server is yours to accept.
Ptah still cleans only the dev database it claimed, and never drops a role, a
database or another server-level object the SQL created. A role a `DO` block
creates stays on the server after the run. The next run that replays the same
directory meets it: a `DO` block that checks for the role first passes, and a
bare `CREATE ROLE` fails with the server's own error, such as
`role "app_admin" already exists (SQLSTATE 42710)`. Give the dev connection
only the privileges you are willing to hand to that SQL, and write role
creation so that it can run twice.

A `schema apply` whose target is on the dev server meets the same thing in one
run. The rehearsal creates the roles the plan declares, so the apply that
follows fails on `CREATE ROLE` with that error. The next run finds the role on the server and plans no `CREATE ROLE`.
Keep the dev database on a server of its own, or create the role first.

Trust is not ownership. It does not declare the server disposable and does not
imply `PTAH_DEV_SERVER_DISPOSABLE`. On a server Ptah provisions or one declared
disposable, the cleanup drops the roles the run created, with trust or without
it.

Trust lifts every refusal whose only reason is that the statement's effect
reaches past the dev database, and not a list of spellings. A `DO` block can
`EXECUTE` a `CREATE ROLE`, so refusing a bare `CREATE ROLE` while running the
`DO` block would protect nothing. On a server the run does not own, trust
accepts:

- on PostgreSQL, CockroachDB and YugabyteDB, the statements a
  [server Ptah provisions](#a-server-ptah-provisions) accepts except a comment
  on an extension or a schema of the dev database and an extension update, and
  also an external `COPY`,
  `dblink` and `IMPORT FOREIGN SCHEMA`;
- on MySQL and MariaDB, routines, triggers, `CALL`, prepared statements, users,
  roles, `GRANT`, `REVOKE`, loadable functions, other databases and writes to
  them, `FEDERATED` and `CONNECT` tables, `LOAD DATA`, `LOAD XML`, and
  `SELECT ... INTO OUTFILE` or `DUMPFILE`;
- on SQL Server, `EXEC`, functions, procedures and DML triggers, databases,
  logins, credentials, synonyms, a target in another database, `BACKUP`,
  `RESTORE`, `BULK INSERT`, `OPENQUERY` and `OPENROWSET`;
- on ClickHouse, SQL functions, databases, users, roles, row policies, named
  collections, `GRANT`, `REVOKE`, `ON CLUSTER`, writes to another database or
  through a table function, any table engine, dictionary sources, `BACKUP`,
  `FREEZE`, `UNFREEZE` and `INTO OUTFILE`;
- on SQLite, `VACUUM INTO`.

A refusal that protects the run itself stays under trust. That covers a
statement that changes the replay session, the identity it runs as or its
transaction; a write to a system catalog such as `pg_catalog`; server
configuration such as `ALTER SYSTEM`, `SET GLOBAL` or `RECONFIGURE`; an object
the dev database cleanup cannot remove, such as an event trigger, a YugabyteDB
publication or a SQL Server ledger table; a MySQL event or a SQL Server DDL
trigger, which would run on their own during the rest of the command; and a
MySQL executable comment, which hides SQL from the guard. SQLite keeps
`ATTACH`, `load_extension` and a pragma that changes state refused.

An executable provider, such as the YDB provider, guards its own dev databases
and decides in its dialect what trust lifts. A provider that declares
`database.dev-replay-trust` (provider contract 25.43) receives the declaration
with each replay guard request and with each dev session Ptah opens for the
run, so a statement its guard accepts under trust also runs. When you declare
trust and the provider does not declare the feature, Ptah stops before the
replay and names the provider: install a provider release that supports it, or
run without the declaration. Without the declaration, every provider replays
under its own default confinement.

The YDB provider lifts a write outside its dev realm, actions, users, groups,
permissions, backups and statements that reach past the server. It keeps a
statement that changes the session, such as `PRAGMA TablePathPrefix` or `USE`,
refused, inside the body of an action or a `DO` block too.

Trust covers the SQL a run executes on its dev database: a migration directory
it replays, a desired schema file it loads there, and the baseline and plan a
rehearsal writes. An apply to the target never needs it, and a command that
parses or compares its input without executing it runs no input SQL. The
native commands that take `--trust-replay-sql` are `migrations validate`,
`migrations lint`, `migrations generate`, `schema inspect`, `schema diff` and
`schema apply`. On `ptah-compat`, `migrate validate`, `migrate diff`,
`migrate lint`, `schema inspect`, `schema diff`, `schema apply`,
`schema plan lint` and `schema plan validate` read the variable. Each of them
refuses a value that is not a boolean before any work. Strict Atlas
compatibility keeps the variable, because the pinned community binary runs
these statements on any dev database.

There is no `ptah.yaml` key for trust, and a `ptah.yaml` that sets
`trust_replay_sql` is refused as an unknown key. A command discovers
`ptah.yaml` in its working directory, so the file belongs to the repository
whose SQL the run replays. If that file could trust its own SQL, a pull request
from a fork that runs `migrations lint` or `schema diff` against a dev URL from
the CI secrets could lift the guard for itself. Trust comes from whoever runs
the command, on the command line or in the job's environment. The permission to
run a program a project file names follows the same rule; see
[Programs a project file names](../../reference/configuration/#programs-a-project-file-names).

### A role a desired schema file declares

`schema inspect` of a schema file and `schema diff` with a file on either side
load that file on the dev database, from the schema Ptah reads out of it.
Tables, routines, policies and views land in the dev database and need no
declaration on any server. A role the file declares, as `CREATE ROLE` or as a
[role-bootstrap block](../../schema/postgres-role-bootstrap/), belongs to the
server. When the dev server does not hold that role, the load creates it only
where something accounts for it:

- on a server Ptah provisions or one declared disposable, the role is created
  and the cleanup drops it with the run;
- under trust, the role is created and stays on the server;
- on any other server, the load refuses before it writes the dev database. The
  refusal names each role and the three remedies.

A role the server already holds is not created again, and the result
describes it as the server has it. `migrate diff`, `migrations generate` and
`schema plan` do not load the desired file on the dev database: they plan the
role into their output and create nothing.

### The baseline a rehearsal writes

Before `schema apply` rehearses its plan, it recreates the target's current
schema in the dev database. Every statement of that baseline is checked
against the same rules as a replay before the first one runs. A routine or
trigger body can write outside the dev database when it runs, and a role, a
user or a privilege the cleanup does not remove outlives it, so a target that
holds one is rehearsed only on a server the run owns, one Ptah provisions from
a docker URL or one declared with `PTAH_DEV_SERVER_DISPOSABLE=1`, or when the
run [trusts the SQL](#sql-you-trust). The refusal names the baseline statement
and these remedies.

A selection narrows the comparison, not what the target holds. With
`--include`, `--exclude` or `--schemas`, or a URL bound to one schema with
`search_path`, the compared state can leave out an object that a statement of
the baseline or of the plan names: a policy of an included table that calls
`auth.uid()` with `auth` left out, or a planned foreign key to a table in an
excluded schema. The target applies such a statement, so the baseline also
recreates each object the statements name, read whole from the target, and
then what those objects name in turn: tables with their keys and indexes,
views, routines, sequences, types, domains and extensions, and their schemas.
An object nothing names stays out, because a selection often leaves out what
the dev server cannot build, such as an extension it does not have.

To find those objects the rehearsal reads the target again, and only the
schemas the statements name, beside the schema the URL pins. A word a statement
uses as a qualifier, folded to lower case, is a candidate, and a schema nothing
names is not read, so a schema whose objects the reader cannot inspect does not
stop the rehearsal. How a word becomes a schema depends on the engine:

- An engine whose connection selects schemas along a search path, as
  PostgreSQL, CockroachDB and YugabyteDB do, matches it against the schemas the
  target holds.
- An [executable provider](../../extend/executable-providers/) whose engine
  carries namespaces (`schema.common-namespaces`) receives the words as the
  schema selectors of its filtered inspection. The provider decides which
  schema a word selects, and a word that selects none, such as a table alias,
  reads nothing. A quoted schema name that keeps upper-case letters, such as
  `"App"`, is sent as `app`, so a provider that compares schema names exactly
  does not find it.
- Every other engine, and a provider whose engine carries no namespaces, reads
  what its URL reaches once. For such a provider that is its whole database:
  when the read fails, the rehearsal fails with a message that names the
  missing feature.

The comparison itself is not widened. A role belongs to the server rather than
to one database, so a plan has no `CREATE ROLE` for a role the target already
holds, and a dev server of its own may still lack one a statement names, such
as the role of a policy's `TO app_admin`. On a server the run owns, the
baseline creates each such role that the dev server did not hold when the run
claimed it, with the attributes the target gives it and no password, and the
cleanup drops it with the run. A role is named the way an
object is, by its bare name in the code below, and the rule errs toward
naming: a column called like the role counts too. On any other server no role
is created, so the dev server has to hold the roles the statements name.

The objects recreated this way are checked against the same rules as the rest
of the baseline, so a routine such as `auth.uid()` is recreated only on a
server Ptah provisions from a docker URL or one declared with
`PTAH_DEV_SERVER_DISPOSABLE=1`, or when the run
[trusts the SQL](#sql-you-trust), and refused by name otherwise.

A name counts as named where it appears in the code of a statement, or in what
PostgreSQL resolves in a routine body before the plan goes on. That is the
whole of an SQL body, and the declarations of a PL/pgSQL body, such as
`r auth.accounts%ROWTYPE` or `v extensions.vector(3)`, without their defaults:
PostgreSQL resolves a call in a PL/pgSQL body, and a default, only when the
block runs. A `DO` block runs at
once, so the whole of it counts, and one that names no language is PL/pgSQL.
The rule is the same for a routine the target holds and for a planned `CREATE
FUNCTION`, `CREATE PROCEDURE` or `DO` block.

A name qualified by its schema counts wherever it stands. A bare name reached along the dev session's
`search_path` counts only where it can name the object: a function where a call
names it, a table, view or sequence where SQL names a relation, such as after
`FROM`, `JOIN` or `REFERENCES`, and a type anywhere. So a planned column
`title` does not recreate a function `title()`.

On PostgreSQL, CockroachDB and YugabyteDB, a string counts where the server
reads it as the name of a relation as it reads the statement: a constant cast
to `regclass`, as in `'app.ids'::regclass` or `CAST('app.ids' AS regclass)`,
and the first argument of `nextval`, `currval` or `setval` written as a
constant, as in `nextval('app.ids')`. So a column default `nextval('app.ids')`
with `app` excluded recreates the sequence `app.ids` and its schema, whether the
plan adds the default or the target already holds it. The text is read as the
server reads it: a dot outside double quotes separates the schema from the
name, an unquoted part is folded to lower case, and a bare name is a table, view
or sequence found along the dev session's `search_path`. The sequence stays
outside the comparison: the plan neither creates nor drops it.

Any other name inside a string literal names nothing, so `DEFAULT 'app.ids'`
and a string passed to another function recreate nothing. Neither does a name
built from other strings, such as `nextval('app.' || 'ids')`, nor one passed as
text, such as `nextval('app.ids'::text)`, which PostgreSQL looks up only when
the expression runs. CockroachDB looks up the text form as it reads the
statement, so there that default fails the rehearsal with `relation "app.ids"
does not exist`. A sequence the target does not hold either fails it the same
way on every engine. When a statement of the baseline or of the plan fails after objects were
recreated this way, the error lists them.

A dev URL that pins one schema with `search_path` leaves the other schemas the
dev database held at the claim as it finds them, with what they hold, so an
object written into one of them would outlive the run and fail the next one.
On PostgreSQL, CockroachDB and YugabyteDB, and on an engine whose provider
session lists its schemas, the claim records those schemas. There the
rehearsal refuses a baseline that would write an object into one of them
before it writes anything, names the schema and the objects, and writes
nothing beyond the reset, whether or not the server is disposable. Point
`--dev-url` at a database that does not hold the schema: a schema the
baseline creates is the run's, and its cleanup drops it.

Comments follow a narrower rule. A comment on a table or a column is written.
The comment an extension's control file gives is not written, because `CREATE
EXTENSION` sets it; Ptah reads that comment from the dev server. A comment the
target gave an extension itself is written only on a server Ptah provisions
from a docker URL, because the cleanup does not restore it and a server declared
disposable keeps it for the next run.

A schema's own comment and the privileges on it are written on any server,
because they end with the run. A target often holds them, on `public` or on a
schema of its own, and a dev database whose `public` was dropped and created
again has neither. On PostgreSQL, CockroachDB and YugabyteDB, the claim records
the owner, access list and comment of the schemas a reset keeps, and every reset
returns those schemas to them and drops the others. A dev URL that pins one schema
with `search_path` is the exception: its cleanup leaves that schema as it finds
it, so they are refused there, with the remedies above. Its cleanup does drop a
schema beside the pinned one that the claim did not find, with everything in
it, so a schema the baseline recreated does not reach the next run. A
privilege for a role the dev server does not hold fails on that server, as it
does on a table.

The plan is held to the same rules, before the first statement of the
baseline or the plan runs. The plan comes from the desired schema, so a role,
a routine or trigger body, or a comment the cleanup keeps is rehearsed only
on a server Ptah provisions from a docker URL or one declared with
`PTAH_DEV_SERVER_DISPOSABLE=1`, and a role or a body also under trust. On any
other server, without trust, the rehearsal refuses, names the plan statement
and the remedies, and leaves the target and the dev server as they were.
Without this check, a role the rehearsal created would outlive the cleanup on
the dev server, and the real apply would fail on it when the target is on the
same server. Under trust that is what happens; see
[SQL you trust](#sql-you-trust).

In a YDB dev realm, the baseline leaves out the permissions the target holds
on its database root. The realm's root would stand in for the target's, but a
reset in the middle of a run keeps the realm's root and its permissions, so a
grant written there would reach the next target rehearsed in the realm.

## Where it appears

- Replay validation with a dev database: [Integrity and safety](../../versioned/integrity-and-safety/).
- Linting against a dev database: [Lint and gate unsafe SQL](../../versioned/lint/).
- Shadow-verified generation and baselining: [Generate migrations](../../versioned/generate/) and [Adopt an existing database](../../start/adopt-an-existing-database/).
- Engine-specific URL behavior: [Database support matrix](../../databases/support-matrix/).
