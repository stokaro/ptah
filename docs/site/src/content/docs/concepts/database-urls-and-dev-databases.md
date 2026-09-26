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
selects the engine. The same URL syntax names databases in four different
roles, though — the target you are changing, plus up to three kinds of
disposable databases that exist so mistakes happen somewhere harmless. This
page defines all four; other pages link here instead of redefining them.

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
| Spanner (PostgreSQL interface) | `spanner://user:pass@localhost:5432/app` |
| Oracle | `oracle://user:pass@localhost:1521/service` (renders, plans, reads a live catalog, and runs versioned migrations) |

Scheme aliases normalize to the canonical dialect (`postgresql://`,
`sqlite3://`, `mssql://`, `crdb://`, `ysql://`, `ch://`, and more) — the full
alias list is on the
[Database support matrix](../../databases/support-matrix/). A URL with an
unrecognized scheme fails with `unsupported database dialect`.

A MySQL or MariaDB server behind a Unix socket takes the Atlas CLI socket
form, `mysql+unix://user:pass@/run/mysqld/mysqld.sock?database=app`, with
`mariadb+unix://` and `maria+unix://` for MariaDB. The URL path is the socket
and the `database` parameter names the database; a host in that URL is
ignored. The Go-driver form `mysql://user:pass@unix(/run/mysqld/mysqld.sock)/app`
reaches the same socket.

A MySQL or MariaDB URL must name a database, over TCP or through a socket. The
Atlas CLI reads a URL that names none as the whole server; Ptah refuses it
([stokaro/ptah#3761](https://github.com/stokaro/ptah/issues/3761)). An empty
`database` parameter is refused too.

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

Ptah cleans the replay
realm before migration execution, after a failed replay, and after a successful
replay. Commands that inspect the replayed state do so between execution and
the final cleanup on the same pinned database session. No fixed time limit
applies to a cleanup: it runs as long as emptying the realm takes, and that
grows with the number of objects and the round trip to the server. After an
interrupt, the cleanup has 30 seconds to finish; a second interrupt stops the
process at once.

Everything a dev database runs stays inside it. Where a schema names a database
rather than a namespace — MySQL, MariaDB, and ClickHouse — a plan carrying the
target's schema name is re-scoped onto the dev database before it is rehearsed,
and a statement naming some third database is refused instead of run.

**A shadow database** (`--shadow-db`) is a disposable verification target for
commands that write or record migrations: `ptah migrations generate` replays
the directory — including the new migration, up, down, and up again — before
keeping any files, and `ptah migrations checkpoint` and
`ptah migrations baseline` use it to verify that migrations reproduce the
expected schema before anything is recorded. `ptah migrations down` uses it to
verify the rollback plan before changing the target.

The shadow database must identify a different live database realm from the
target. Ptah compares both connections before cleanup and fails before changing
either database when they resolve to the same realm.

**A throwaway test database** is what `ptah migrations test` and
`ptah schema test` run cases against: by default a fresh ephemeral SQLite
database per case, or the database passed with `--db-url` when tests must
exercise a real server dialect — see
[Test migrations and schemas](../../testing/migrations-and-schema/).

## Consequences

- **Disposable means Ptah may drop everything it supports.** Dev and shadow
  database workflows clean user objects, and test seed steps bypass the
  seeder's protected-environment guards. Point these flags at scratch databases
  only, never at a real environment. Rollback verification rejects a dev or
  shadow URL that identifies the target database, including equivalent URL
  aliases. Before reset it also compares the live dialect and selected
  database/catalog realm from both connections. Equal network database names
  fail closed across different endpoints because DNS aliases and replicated
  members cannot be proven independent before destructive cleanup. Cleanup
  rejects known system, template, metadata, and administrative database names.
- **Migration-diff scope does not reduce the replay realm.** Repeated
  `--schema` values select which schemas `migrate diff` compares and emits. They do not
  limit which schemas a migration may create or which user schemas final
  cleanup removes. PostgreSQL extensions remain in both scoped projections:
  an extension is a database-wide identity, and its schema records installation
  placement rather than ownership by that schema. An extension declared on
  both sides remains synced even when installed outside the named schemas; an
  extension omitted from desired state remains an explicit global removal.
- **The replay realm follows the database engine.** PostgreSQL, CockroachDB,
  and YugabyteDB cleanup treats all user schemas and user-installed extensions
  in the selected database as one dependency graph. Extensions the dev
  database already held when the run started are its environment and stay
  installed, with everything they own, so a migration may use a type from one
  without creating it, as it may under Atlas. A schema such an extension is
  installed in is emptied where it stands. An extension the run created is
  dropped, and that drop removes the schemas it owns and its member tables,
  so the cleanup does not queue their objects one by one; TimescaleDB's
  catalog schemas are this shape. MySQL, MariaDB, and
  ClickHouse cleanup owns the selected database. SQL Server cleanup owns all
  supported user schemas in the selected database. SQLite cleanup owns `main`
  on one pinned session.
- **PostgreSQL cleanup gives back the schemas it drops.** The schema the dev
  URL selects comes back after the cleanup, and so does `public` when the URL
  selects another schema. Each keeps the owner, grants and comment it had.
  A role created on the server afterwards can use `public` as it could before.
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
- **PostgreSQL-family cleanup rejects database-scoped artifacts.** PostgreSQL
  and YugabyteDB reject publications, subscriptions, logical replication
  slots, event triggers, and non-extension foreign-data wrappers, servers, or
  user mappings before DDL. An object the server created when it built the
  database template is not counted: YugabyteDB 2026.1.2 puts the foreign
  server `yb_global_views_server` into every database, and the cleanup leaves
  it, and the extensions it depends on, in place. PostgreSQL also removes and
  verifies database large objects transactionally. YugabyteDB does not run
  that PostgreSQL-specific large-object operation.
- **SQL Server cleanup rejects replication state.** A replication-enabled
  database or replicated table fails before DDL, along with other unsupported
  database-scoped artifacts. Remove replication configuration or use a
  dedicated disposable database before replay.
- **Cross-realm operations fail before execution.** Ptah rejects direct
  statements that switch or mutate another database, protected namespace,
  server, cluster, temporary namespace, external file, or attached SQLite
  database. It also rejects statement forms whose nested SQL cannot be
  confined safely during replay.
- **Replay cleanup is serialized by realm.** PostgreSQL, YugabyteDB, MySQL,
  MariaDB, and SQL Server use database advisory locks. SQLite, ClickHouse, and
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
  `SET search_path`, and `SELECT INTO` protected namespaces.
- MySQL and MariaDB executable comments, `CALL`, events, triggers, routines,
  `LOAD DATA`/`LOAD XML`, and externally backed `FEDERATED` or `CONNECT`
  tables.
- SQL Server `EXEC`/`EXECUTE`, procedure/function/trigger creation or
  alteration, synonym/external-table creation, `BULK INSERT`, `BACKUP`,
  `RESTORE`, and server-level maintenance statements.
- ClickHouse remote, distributed, replicated, and unknown table engines,
  external dictionary sources, and `FREEZE`/`UNFREEZE`. Tables and standalone
  materialized views must select an explicitly allowlisted engine.
- SQLite `ATTACH`, `DETACH`, temporary objects, and non-restorable pragmas.

Replay runs every migration of a directory on one session, so a PostgreSQL
`SET` or `RESET` that changes the session would carry into the migrations after
it, and is refused. A setting that ends with its transaction is accepted:

- `SET LOCAL` and `SET CONSTRAINTS`;
- `set_config(name, value, true)`, with the name written as a string and `true`
  written as the keyword.

Replay runs each migration the way `migrations up` does, in one transaction
unless the file opts out with `-- +ptah no_transaction` or
`-- atlas:txmode none`. Such a setting holds for the rest of its migration and
is gone before the next. In a file that opts out, PostgreSQL ignores a
`SET LOCAL` with a warning, on replay and on apply alike.

Three parameters stay refused even for one transaction. `search_path` (and its
alias `SET SCHEMA`) decides where an unqualified name lands, which would let a
`CREATE FUNCTION` reach `pg_catalog`. `role` and `session_authorization` change
who the rest of the migration runs as.

The rejection happens while Ptah validates the whole migration, before its
first statement executes. Realm-local removal forms that Ptah can classify
without interpreting a routine body, such as `DROP FUNCTION`,
`DROP FOREIGN TABLE`, `DROP SYNONYM`, and `DROP EXTERNAL TABLE`, remain
allowed.

### A server Ptah provisions

A `docker://` dev URL starts a server for one command and removes it when the
command ends, so the server holds nothing that is not the run's. On such a
server, replay also runs the statements it refuses elsewhere only because
their effect reaches past the dev database:

- PostgreSQL-family `DO`, `CALL`, routine creation and alteration,
  `CREATE` and `DROP` of a role, user, group or database, `ALTER ROLE`
  without `SET` or `RESET`, role membership, privileges on a role, database,
  schema, language or parameter, `ALTER DEFAULT PRIVILEGES` without
  `IN SCHEMA`, `DROP OWNED`, `REASSIGN OWNED`, and `COMMENT ON ROLE` or
  `DATABASE`.
- MySQL and MariaDB routines, triggers, `CALL`, `GRANT`, `REVOKE`, `CREATE`
  and `DROP` of a user, role or database, and writes to another database.

So a migration directory that creates its application role in a `DO` block or
defines a function replays on `docker://postgres/18/dev`.

The rest of the list above still fails closed on a provisioned server. Those
statements change the replay session or the sessions the rest of the command
opens (`SET search_path`, `ALTER ROLE ... SET`, any `ALTER DATABASE`,
transaction control), change how the command's later SQL runs (`ALTER SYSTEM`,
event triggers, casts, languages), reach past the container (foreign servers,
subscriptions, `dblink`, an external `COPY`), or mutate a protected namespace
such as `pg_catalog`. A MySQL or MariaDB event stays refused because the
scheduler runs it during the rest of the command.

The decision reads what Ptah recorded when it started the server, not the
spelling of the URL. A server URL keeps the whole list, because such a server
may hold databases and roles that are not the run's: a role a replay created
there would outlive the command.

### A server declared disposable

A server Ptah did not start can be as disposable: a CI service container
the job throws away, or a container running an image `docker://` cannot name,
such as TimescaleDB. `PTAH_DEV_SERVER_DISPOSABLE=1` declares that the server
`--dev-url` names is the run's own. Replay then runs the statements listed
above on it, as on a server Ptah provisions, and refuses the same rest.

```bash
PTAH_DEV_SERVER_DISPOSABLE=1 ptah migrations validate --dir migrations --dev-url "$DEV_URL"
```

The declaration covers the whole server, and Ptah cleans only the dev
database after a replay. A role or database a replay creates stays on the
server until the container is removed. A later command that replays the same
directory on the same server meets it, so a migration that creates a role
without checking for it first fails the second time. Declare only a server
that nothing else uses.

Every command that replays a migration directory on a dev database reads the
variable, on both binaries. A value that is not a boolean fails the command
before it does any work, whether or not that run replays. Strict Atlas
compatibility keeps the variable, because the pinned community binary runs
these statements on any dev database.

## Where it appears

- Replay validation with a dev database: [Integrity and safety](../../versioned/integrity-and-safety/).
- Linting against a dev database: [Lint and gate unsafe SQL](../../versioned/lint/).
- Shadow-verified generation and baselining: [Generate migrations](../../versioned/generate/) and [Adopt an existing database](../../start/adopt-an-existing-database/).
- Engine-specific URL behavior: [Database support matrix](../../databases/support-matrix/).
