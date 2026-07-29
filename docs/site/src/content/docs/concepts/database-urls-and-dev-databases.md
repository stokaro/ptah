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
Atlas-compatible verbs use it for planning and linting the same way. Ptah
cleans the replay realm before migration execution, after a failed replay, and
after a successful replay. Commands that inspect the replayed state do so
between execution and the final cleanup on the same pinned database session.

**A shadow database** (`--shadow-db`) is a disposable verification target for
commands that write or record migrations: `ptah migrations generate` replays
the directory — including the new migration, up, down, and up again — before
keeping any files, and `ptah migrations checkpoint` and
`ptah migrations baseline` use it to verify that migrations reproduce the
expected schema before anything is recorded.

**A throwaway test database** is what `ptah migrations test` and
`ptah schema test` run cases against: by default a fresh ephemeral SQLite
database per case, or the database passed with `--db-url` when tests must
exercise a real server dialect — see
[Test migrations and schemas](../../testing/migrations-and-schema/).

## Consequences

- **Disposable means Ptah may drop everything it supports.** Dev and shadow
  database workflows clean user objects, and test seed steps bypass the
  seeder's protected-environment guards. Point these flags at scratch databases
  only, never at a real environment.
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
- **Cross-realm operations fail before execution.** Ptah rejects direct
  statements that switch or mutate another database, protected namespace,
  server, cluster, temporary namespace, external file, or attached SQLite
  database. It also rejects statement forms whose nested SQL cannot be
  confined safely during replay.
- **Replay cleanup is serialized by realm.** PostgreSQL, YugabyteDB, MySQL,
  MariaDB, and SQL Server use database advisory locks. SQLite, ClickHouse, and
  CockroachDB use an operating-system file lock keyed by the normalized
  database identity, which coordinates Ptah processes on the same host.
  Cross-host ClickHouse and CockroachDB replay is unsupported because neither
  engine provides the required effective session advisory lock.
- **Match the target engine.** Replay verification proves the SQL executes on
  the engine it ran against, so a dev or shadow database should run the same
  engine (and ideally the same version) as the target.
- **Every flag has an environment variable.** `PTAH_DB_URL`, `PTAH_DEV_URL`,
  and `PTAH_SHADOW_DB` set the corresponding flags, which keeps credentials
  out of CI command lines — see [Configuration](../../reference/configuration/).

## Where it appears

- Replay validation and linting with a dev database: [Integrity and safety](../../versioned/integrity-and-safety/).
- Shadow-verified generation and baselining: [Generate migrations](../../versioned/generate/) and [Adopt an existing database](../../start/adopt-an-existing-database/).
- Engine-specific URL behavior: [Database support matrix](../../databases/support-matrix/).
