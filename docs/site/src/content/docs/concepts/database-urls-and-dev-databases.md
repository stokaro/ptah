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
Atlas-compatible verbs use it for planning and linting the same way.

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
- **Cleanup ordering depends on the workflow.** `ptah-compat migrate diff`
  validates its inputs before cleaning the dev database and cleans again after
  success or a failure once replay starts. Validation, lint, and shadow
  workflows reset their disposable database before replay but do not promise
  that it is empty when the command exits.
- **Cleanup follows the configured scope.** SQLite cleanup affects the selected
  attached schema, while MySQL and MariaDB cleanup affects the selected
  database and refuses to proceed when foreign keys or views from another
  database reference it. PostgreSQL cleanup refuses to proceed when objects
  outside the selected schema depend on managed objects. It also refuses a
  schema-owned PostgreSQL extension: `DROP EXTENSION` is database-scoped and
  can remove extension members outside the selected schema, so schema cleanup
  cannot make that operation safe. Use a disposable database, not only a
  disposable schema, and install required extensions outside the replayed
  migration history until Ptah has an explicit database-wide cleanup mode.
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
