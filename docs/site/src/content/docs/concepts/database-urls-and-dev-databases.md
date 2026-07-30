---
title: Database URLs and dev databases
description: Accepted database URL formats, and the difference between the target, dev, shadow, and throwaway databases.
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
| MariaDB | `mariadb://user:pass@localhost:3306/app` |
| SQLite | `sqlite://relative.db`, `sqlite:///absolute/path/app.db`, `sqlite:///:memory:`, `sqlite:file:memdb1?mode=memory&cache=shared` |
| SQL Server | `sqlserver://sa:pass@localhost:1433?database=app` (plus a Ptah-only `schema` parameter — see [SQL Server](../../databases/sqlserver/)) |
| ClickHouse | `clickhouse://user:pass@localhost:9000/app` |
| CockroachDB | `cockroachdb://user:pass@localhost:26257/app` |
| YugabyteDB | `yugabytedb://user:pass@localhost:5433/app` |
| Spanner (PostgreSQL interface) | `spanner://user:pass@localhost:5432/app` |

Scheme aliases normalize to the canonical dialect (`postgresql://`,
`sqlite3://`, `mssql://`, `crdb://`, `ysql://`, `ch://`, and more) — the full
alias list is on the
[Database support matrix](../../databases/support-matrix/). A URL with an
unrecognized scheme fails with `unsupported database dialect`.

## The four database roles

**The target database** is the one a command reads or changes: `--db-url` on
native commands, `--url` on Atlas-compatible ones. It is the only database
whose state matters after the command exits.

**A dev database** (`--dev-url`) is a disposable replay target used for
validation: `ptah migrations validate` and `ptah migrations lint` clean it and
replay the migration directory on it to prove the SQL executes, and
Atlas-compatible verbs use it for planning, linting, and rollback verification.
Ptah cleans the replay realm before migration execution, after a failed replay,
and after a successful replay. Commands that inspect the replayed state do so
between execution and the final cleanup on the same pinned database session.

**A shadow database** (`--shadow-db`) is a disposable verification target for
commands that write or record migrations: `ptah migrations generate` replays
the directory — including the new migration, up, down, and up again — before
keeping any files, and `ptah migrations checkpoint` and
`ptah migrations baseline` use it to verify that migrations reproduce the
expected schema before anything is recorded. `ptah migrations down` uses it to
verify the rollback plan before changing the target.

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
- **Comparison scope does not reduce the replay realm.** Repeated
  `--schema` values select which schemas Ptah compares and emits. They do not
  limit which schemas a migration may create or which user schemas final
  cleanup removes.
- **The replay realm follows the database engine.** PostgreSQL, CockroachDB,
  and YugabyteDB cleanup treats all user schemas and user-installed extensions
  in the selected database as one dependency graph. MySQL, MariaDB, and
  ClickHouse cleanup owns the selected database. SQL Server cleanup owns all
  supported user schemas in the selected database. SQLite cleanup owns `main`
  on one pinned session.
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
  user mappings before DDL. PostgreSQL also removes and verifies database large
  objects transactionally. YugabyteDB does not run that PostgreSQL-specific
  large-object operation.
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
  out of CI command lines — see [Configuration](../../reference/configuration/).

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

The rejection happens while Ptah validates the whole migration, before its
first statement executes. Realm-local removal forms that Ptah can classify
without interpreting a routine body, such as `DROP FUNCTION`,
`DROP FOREIGN TABLE`, `DROP SYNONYM`, and `DROP EXTERNAL TABLE`, remain
allowed.

## Where it appears

- Replay validation and linting with a dev database: [Integrity and safety](../../versioned/integrity-and-safety/).
- Shadow-verified generation and baselining: [Generate migrations](../../versioned/generate/) and [Adopt an existing database](../../start/adopt-an-existing-database/).
- Engine-specific URL behavior: [Database support matrix](../../databases/support-matrix/).
