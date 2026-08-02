# Ptah Migrator

The Ptah Migrator provides versioned database migration capabilities with up/down directions, inspired by the PostgreSQL migrations from the registry package but adapted for the Ptah ecosystem.

## Features

- **Versioned Migrations**: Each migration has a unique version number and description
- **Up/Down Migrations**: Support for both applying and rolling back migrations
- **Transaction Safety**: Each migration runs in its own transaction unless it explicitly opts out with `no_transaction`
- **SQL File Support**: Migrations can be defined as SQL files
- **Go Function Support**: Migrations can also be defined as Go functions for complex logic
- **Multiple Database Support**: Works with PostgreSQL and MySQL through Ptah's executor package
- **Dry Run Mode**: Preview what migrations would do without actually applying them
- **Migration Status**: Check current migration state and pending migrations
- **Configurable Migration State**: Store migration history in a custom schema/table

## Migration File Structure

Migrations are stored with the following naming convention:

```text
NNNNNNNNNN_description.up.sql    # Up migration
NNNNNNNNNN_description.down.sql  # Down migration
```

Where:
- `NNNNNNNNNN` is a 10-digit version number (e.g., `0000000001`)
- `description` is a snake_case description of the migration
- Each migration must have both `.up.sql` and `.down.sql` files

### Filesystem Requirements

The `RegisterMigrations` function accepts an `fs.FS` parameter where migrations should be located in the root directory. It's the caller's responsibility to prepare the filesystem correctly:

```go
// For embedded migrations, use a subdirectory
migrationsFS := must.Must(fs.Sub(GetMigrations(), "source"))
err := RegisterMigrations(migrator, migrationsFS)

// For directory on disk
migrationsFS := os.DirFS("/path/to/migrations")
err := RegisterMigrations(migrator, migrationsFS)

// For convenience, use helper functions
err := RegisterMigrationsFromEmbedded(migrator)  // Uses embedded source/ directory
err := RegisterMigrationsFromDirectory(migrator, "/path/to/migrations")
```

### Example Migration Files

**0000000001_create_users_table.up.sql:**
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

**0000000001_create_users_table.down.sql:**
```sql
DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;
```

## Command Line Interface

Migration directory commands use `--dir-format=auto` by default.

Auto mode prefers Ptah paired files (`NNNNNNNNNN_description.up.sql` and
`NNNNNNNNNN_description.down.sql`) when they are present, and otherwise
accepts Atlas-style timestamp files such as `20220318104614_team_A.sql` or
`20240112070806.sql`, plus numeric migration names produced by Atlas importers
such as `1_initial.sql`, `2.sql`, `1_initial.up.sql`, `1_initial.down.sql`,
and `1.my.sql`.

Use `--dir-format=ptah` or `--dir-format=atlas` on `migrations up`,
`migrations down`, `migrations status`, `migrations hash`, and `migrations
validate` when detection should be explicit. Ordinary Atlas and imported
single SQL files are forward migrations. Imported `.down.sql` files are paired
with their matching `.up.sql` version for rollback.

Atlas txtar files can also embed a `down.sql` section that Ptah executes
during rollback:

```sql
-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
```

For Atlas txtar migrations, Ptah executes only the `migration.sql` section for
`migrations up` and only the `down.sql` section for `migrations down`. A
`checks.sql` and `checks/*.sql` sections are pre-migration gates, matching
Atlas semantics:

- Each statement must be a top-level `SELECT` that returns exactly one column
  and one row containing a truthy scalar.
- Assertions run on a dedicated physical session that Ptah discards afterward.
  Transaction-capable drivers always roll back; ClickHouse uses the disposable
  session directly because its driver does not implement transactions.
  PostgreSQL-family and MySQL-family drivers also request database-enforced
  read-only mode.
- A failing assertion aborts the migration before any `migration.sql`
  statement runs, through the same machinery as `-- +ptah check` directives
  (see `docs/pre-migration-checks.md`).
- Assertions in one check file use all-of semantics by default. A file-level
  `-- atlas:assert oneof` directive requires at least one assertion in that
  file to pass; an empty `oneof` file fails closed.
- Assertion SQL is preserved through execution. Dialect-aware splitting handles
  PostgreSQL `E'...'` strings and MySQL/MariaDB comment boundaries and active
  block comments without changing query semantics. Numeric prefixes shorter
  than five digits remain executable SQL rather than version guards.
- Executable-comment bodies are expanded for validation, and version guards use
  the connected server version. The effective SQL must remain one top-level
  `SELECT`; hidden delimiters and non-`SELECT` effective bodies fail closed.
- `--skip-checks` on `migrations up` bypasses txtar checks too, and
  `--tx-mode all` refuses checked files exactly as it refuses
  `-- +ptah check` files.

Other embedded txtar files, such as `schema.sql`, are ignored by the migrator;
ordinary SQL comments that look like `-- keep this comment --` remain
comments, not txtar section boundaries. Ptah's txtar support is intentionally
limited to Atlas SQL migration containers and is not a general-purpose txtar
parser.

Atlas-format SQL template migrations are rendered with Go `text/template`
before execution and linting. Root versioned files such as `1.sql` and `2.sql`
are executable migrations; shared template files in subdirectories can define
helpers such as `{{ define "shared/users" }}` and are not executed as standalone
migrations. The template data object exposes `.Env`; CLI commands set it with
`--atlas-env`, and programmatic callers can pass `WithAtlasTemplateData`.

`--dir-format` controls only migration-file discovery. To continue a database
that already uses Atlas's runtime history table, pass `--revision-format
atlas` on the CLI or `WithRevisionTableFormat(RevisionTableFormatAtlas)` in
Go.

Atlas revision mode uses `atlas_schema_revisions` by default, stores string
migration versions, reads the Atlas `applied`/`total` and `error` state
fields, and writes the Atlas `hash` value from `atlas.sum` when it is
available. Successful rows created by migration execution use the migration
filename description, store empty `error` and `error_stmt` values, and
identify Ptah as the operator. Dot-prefixed versions — such as the
`.atlas_cloud_identifier` row Atlas's `migrate down` writes even in local
mode — are metadata, not migrations: Ptah skips them in version, status, and
pending calculations and never rewrites or deletes them.

Ptah records a coherent timing interval for executed SQL: `executed_at` is the
migration lifecycle start and `execution_time` is the full elapsed duration in
nanoseconds. Failed rows preserve their execution diagnostics. Metadata-only
baseline, set, and force operations record their write timestamp with zero
duration, or preserve existing timing metadata when updating a row.

Atlas CE can read these values, but exact dynamic timing equality is not
claimed: Atlas CE v1.2.0 can persist a near-final timestamp and
write-order-dependent duration. On PostgreSQL-family databases, Ptah creates
the Atlas-compatible `executed_at TIMESTAMPTZ` column. Custom
`--migrations-schema` / `--migrations-table` values still override the
metadata table location.

If an Atlas migration does not provide `down.sql`, `migrations down` returns a typed
error explaining that Atlas dynamic down-plan synthesis is not implemented yet.
The migrator validates the complete rollback selection before execution, so a
missing down body leaves schema and revision state unchanged. This is distinct
from transaction rollback on a failed migration: transaction rollback undoes
an in-progress failure, Ptah paired `.down.sql` files and Atlas txtar
`down.sql` sections revert already-applied migrations, and Atlas dynamic
`migrate down` would synthesize a downgrade plan from database/dev state.

`-- +ptah` directives inside `migration.sql` and `down.sql` are parsed per
section for timeout, validation, and transaction handling. `no_transaction`
applies only to the direction whose SQL section contains it, so a
non-transactional `down.sql` does not force `migration.sql` to run outside a
transaction. Atlas `-- atlas:txmode none` comments are accepted as the Atlas
equivalent for the section where they appear.

### Migrate Up
Apply all pending migrations:
```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations
```

With dry run:
```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dry-run
```

Dry-run reads existing revision metadata and selects only pending migrations.
When the metadata table is absent, it models an empty revision history without
creating the table. It reads the legacy three-column Ptah revision layout
without adding the newer state and checksum columns. A table containing only a
subset of the current revision columns fails with an explicit layout diagnostic
and remains unchanged. For the current revision layout, dry-run still enforces
dirty-state and checksum validation. It enforces execution-order,
transaction-mode, checkpoint, and down-body validation for both current and
legacy revision layouts.

Allow applying a migration whose version is below the current high-water mark:
```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --exec-order non-linear
```

With a custom migration state table:
```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --migrations-schema infra --migrations-table ptah_migrations
```

With an Atlas-style versioned migration directory:
```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dir-format atlas
```

Continue an Atlas-managed database using `atlas_schema_revisions`:
```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dir-format atlas --revision-format atlas
```

### Migrate Down
Roll back to a specific version:
```bash
go run ./cmd migrations down --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --target 5
```

With confirmation skip (dangerous!):
```bash
go run ./cmd migrations down --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --target 5 --confirm
```

With a custom migration state table:
```bash
go run ./cmd migrations down --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --target 5 --migrations-schema infra --migrations-table ptah_migrations
```

### Migration Status
Check current migration status:
```bash
go run ./cmd migrations status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations
```

Verbose output:
```bash
go run ./cmd migrations status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --verbose
```

JSON output:
```bash
go run ./cmd migrations status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --json
```

With a custom migration state table:
```bash
go run ./cmd migrations status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --migrations-schema infra --migrations-table ptah_migrations
```

With an Atlas-style versioned migration directory:
```bash
go run ./cmd migrations status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dir-format atlas
```

Check an Atlas-managed revisions table:
```bash
go run ./cmd migrations status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dir-format atlas --revision-format atlas
```

## Observability

The migrator exposes a small observer interface for tracing and metrics without
binding the core package to a specific telemetry backend. Programmatic callers
can pass `WithObserver` to receive migration spans, counters, and duration
measurements. The built-in no-op observer keeps existing library usage silent
unless a caller explicitly opts in.

CLI migration commands add observability flags on top of this API:

```bash
ptah migrations up --log-format json --log-level debug --metrics-addr :9090
```

Text logging preserves the traditional human output. JSON logging emits
structured log records with a `correlation_id` so automation can parse stdout.
When `--metrics-addr` is set, Ptah serves Prometheus text metrics on
`/metrics`. OpenTelemetry OTLP tracing is intentionally behind the
`observability` build tag so default binaries do not link the OTLP exporter.

## API Overview

The migrator package provides a clean, modular API with the following key components:

### Core Types

- **`Migrator`**: Main migration engine that executes migrations
- **`Migration`**: Represents a single database migration with up/down functions
- **`MigrationProvider`**: Interface for providing migrations to the migrator
- **`MigrationFunc`**: Function type for migration operations
- **`MigrationStatus`**: Represents the current state of migrations

### Execution Order Policy

Ptah derives pending migrations from the applied version set, not from `MAX(version)`.
This catches an ordinary branch merge race: migration `5` may already be applied while
a later-merged migration `3` is present on disk but missing from the database.

The default policy is `linear`, which fails loudly when a pending version is below the
current high-water mark. Use `WithExecOrder(migrator.ExecOrderNonLinear)` or
`--exec-order=non-linear` to apply the missing migration in version order. Use
`linear-skip` only when you intentionally want to leave those versions unapplied; Ptah
logs a warning for each skipped version and `migrations status` continues to report it as
pending and out of order.

### Migration Providers

- **`RegisteredMigrationProvider`**: In-memory provider for programmatically registered migrations
- **`FSMigrationProvider`**: Filesystem-based provider that loads migrations from SQL files

### Factory Functions

- **`NewMigrator(conn, provider)`**: Creates a migrator with a custom provider
- **`NewFSMigrator(conn, fsys)`**: Creates a migrator that loads migrations from a filesystem
- **`NewRegisteredMigrationProvider(migrations...)`**: Creates an in-memory migration provider
- **`WithMigrationsTable(schema, table)`**: Configures the migration history table
- **`WithExecOrder(policy)`**: Configures out-of-order migration handling
- **`WithMigrationDirFormat(format)`**: Selects `auto`, `ptah`, or `atlas` filesystem discovery
- **`WithAtlasTemplateData(data)`**: Supplies data, including `.Env`, for Atlas SQL template migrations
- **`WithStatementInterceptor(interceptor)`**: Lets an external executor take over selected statements
- **`WithStatementValidator(validator)`**: Validates every statement before the migration executes its first statement
- **`WithStatementObserver(observer)`**: Reports each statement after successful execution without replacing the execution path
- **`WithRevisionTableFormat(format)`**: Selects Ptah's native `schema_migrations` layout and bookkeeping semantics or Atlas's `atlas_schema_revisions` layout and Atlas-compatible bookkeeping semantics
- **`Baseline(ctx, version)` / `BaselineWithOptions(ctx, opts)`**: Records provider migrations without executing their SQL bodies; Atlas metadata records only the exact baseline revision
- **`SetAtlasRevision(ctx, version)`**: Moves Atlas metadata to an exact version and returns version-and-description `AtlasRevisionChange` entries in an `AtlasRevisionSetResult`; it preserves clean rows through the target, adds missing manually-set rows, converts dirty rows to the combined applied and manually-set type without discarding diagnostics, and removes rows above it
- **`GetMigrationStatusSnapshot(ctx)`**: Returns migration status and the exact revision rows used to derive it from one metadata query

## Programmatic Usage

### Basic Migration Execution

```go
package main

import (
    "context"
    "os"
    "time"

    "github.com/stokaro/ptah/dbschema"
    "github.com/stokaro/ptah/migration/migrator"
)

func main() {
    // Connect to database. Supply a context so the initial Ping cannot block
    // indefinitely on a stuck host.
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    conn, err := dbschema.ConnectToDatabase(ctx, "postgres://user:pass@localhost/db")
    if err != nil {
        panic(err)
    }
    defer dbschema.CloseAndWarn(conn)

    // Create filesystem from migrations directory
    migrationsFS := os.DirFS("/path/to/migrations")

    // Create migrator from filesystem
    m, err := migrator.NewFSMigrator(conn, migrationsFS)
    if err != nil {
        panic(err)
    }

    // Run all pending migrations
    err = m.MigrateUp(context.Background())
    if err != nil {
        panic(err)
    }
}
```

### Custom Migration Registration

```go
import (
    "context"
    "os"
    "github.com/stokaro/ptah/dbschema"
    "github.com/stokaro/ptah/migration/migrator"
)

// Option 1: Create migrator with registered migrations
provider := migrator.NewRegisteredMigrationProvider()
m := migrator.NewMigrator(conn, provider)

// Register a Go-based migration
upFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
    return conn.Writer().ExecuteSQL(ctx, "CREATE TABLE test (id SERIAL PRIMARY KEY)")
}

downFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
    return conn.Writer().ExecuteSQL(ctx, "DROP TABLE test")
}

migration := &migrator.Migration{
    Version:     1001,
    Description: "Create test table",
    Up:          upFunc,
    Down:        downFunc,
}
provider.Register(migration)

// Option 2: Create migrator from filesystem
customFS := os.DirFS("/custom/path")
m, err := migrator.NewFSMigrator(conn, customFS)

// Option 3: Create migration from SQL strings
sqlMigration := migrator.CreateMigrationFromSQL(
    1002,
    "Add users table",
    "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255));",
    "DROP TABLE users;",
)
provider.Register(sqlMigration)
```

### Observing Executed Statements

Use a statement observer for auditing, metrics, or consumer-controlled replay
analysis that must follow the real migration execution path. The callback runs
after each statement succeeds, including statements handled by a configured
`StatementInterceptor`.

```go
observer := migrator.StatementObserverFunc(func(
    _ context.Context,
    event migrator.StatementEvent,
) error {
    log.Printf(
        "executed migration statement %d/%d from %s",
        event.Index,
        event.Total,
        event.SourcePath,
    )
    return nil
})

provider, err := migrator.NewFSMigrationProvider(
    os.DirFS("/path/to/migrations"),
    migrator.WithStatementObserver(observer),
)
if err != nil {
    panic(err)
}
```

### Validating statements before execution

Use a statement validator when an embedder must reject SQL that can escape a
disposable database or is unsupported by its execution policy. The provider
splits and validates every statement in a migration before executing the first
statement. A rejected later statement therefore leaves the migration
untouched.

```go
provider, err := migrator.NewFSMigrationProvider(
    os.DirFS("/path/to/migrations"),
    migrator.WithStatementValidator(replayGuard),
)
if err != nil {
    panic(err)
}
```

`replayGuard` implements `migrator.StatementValidator`. The validator observes
SQL only; combine it with `WithStatementInterceptor` when accepted statements
may be executed by an external tool.

`StatementEvent.Directives` is an event-local copy. An observer must not
modify migration execution. A database-aware observer may capture a connection
owned by its consumer, but that consumer is responsible for transaction
visibility. Returning an error stops the migration with a
`StatementObservationError`; its event preserves the source path, statement
ordinal, and statement text and records the already-executed statement in
dirty-migration progress.

### Migration Status Checking

```go
// Create migrator from filesystem
migrationsFS := os.DirFS("/path/to/migrations")
m, err := migrator.NewFSMigrator(conn, migrationsFS)
if err != nil {
    panic(err)
}

status, err := m.GetMigrationStatus(context.Background())
if err != nil {
    panic(err)
}

fmt.Printf("Current version: %d\n", status.CurrentVersion)
fmt.Printf("Pending migrations: %d\n", len(status.PendingMigrations))

if status.HasPendingChanges {
    fmt.Println("Database needs migration!")
}
```

### Brownfield Baseline

Use baseline mode when the target database schema already exists and should
become managed by Ptah from this point forward. Baseline writes migration
metadata only; it does not execute the migration bodies.

```go
provider, err := migrator.NewFSMigrationProvider(os.DirFS("/path/to/migrations"))
if err != nil {
    panic(err)
}

m := migrator.NewMigrator(conn, provider).
    WithMigrationsTable("infra", "ptah_migrations")

err = m.BaselineWithOptions(context.Background(), migrator.BaselineOptions{
    Version: 20260718120000,
})
if err != nil {
    panic(err)
}
```

`BaselineWithOptions` refuses to write when the metadata table already contains
rows unless `Force` is set. `Force` can fill or update metadata at or below the
baseline version, but it refuses to rewrite history when rows above that version
already exist. The CLI `migrations baseline` adds pre-flight schema verification:
with `--shadow-db`, it replays baselined migrations on a disposable database and
compares the result to the target; without `--shadow-db`, it uses the weaker
entity drift check against `--root-dir`.

## Migration Table

The migrator automatically creates a `schema_migrations` table to track applied migrations:

```sql
CREATE TABLE schema_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
);
```

Rows are written as `pending` before migration SQL executes, then marked
`applied` after success. Failed or interrupted runs leave a dirty row with
statement progress and error details; later migration operations refuse to
continue until `RepairMigration` or the `migrations repair` CLI resolves it.
Applied rows store an up-SQL checksum, so editing an already-applied migration
file is detected before new work starts.

## Best Practices

1. **Always create both up and down migrations**: Every migration should be reversible
2. **Use descriptive names**: Make migration purposes clear from the filename
3. **Keep migrations small**: Each migration should make one focused change
4. **Test migrations**: Always test both up and down migrations before deploying
5. **Use transactions**: The migrator automatically wraps migrations in transactions
6. **Backup before rollbacks**: Down migrations can cause data loss
7. **Handle out-of-order files deliberately**: Use the default `linear` policy in CI so
   a migration merged below the current version cannot be skipped silently
8. **Use production timeouts**: Run production DDL with `--lock-timeout 3s --statement-timeout 30s` so hot-table locks and runaway statements fail fast
9. **Version numbers**: Use sequential version numbers or timestamps

### Migration Advisory Locks

PostgreSQL, MySQL, MariaDB, and SQL Server migrators acquire a session-level
advisory lock around the planning and apply window for `MigrateUp`,
`MigrateDown`, `MigrateDownTo`, and `MigrateTo`. This prevents concurrent
runners from reading the same pending migration set and applying it more than
once.

By default the migrator waits until the lock is available. Use
`WithMigrationLockName` to coordinate on a custom lock name, and use
`WithMigrationLockTimeout` or the CLI `--migration-lock-timeout` flag to bound
that wait. Timed-out callers receive a typed error that can be detected with
`migrator.IsMigrationLockTimeout`.

### Per-Migration Timeouts

Set CLI defaults for every pending migration:

```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --lock-timeout 3s --statement-timeout 30s --migration-lock-timeout 30s
```

Override those defaults in a specific migration file with top-of-file directives:

```sql
-- +ptah lock_timeout=3s
-- +ptah statement_timeout=30s

ALTER TABLE users ADD COLUMN email TEXT;
```

PostgreSQL runs `SET LOCAL lock_timeout` and `SET LOCAL statement_timeout` inside the migration transaction. MySQL and MariaDB run `SET SESSION innodb_lock_wait_timeout`; statement timeouts use MySQL `max_execution_time` and MariaDB `max_statement_time`.

### Transaction Modes

`WithTransactionMode` and `ptah migrations up --tx-mode` accept the
Atlas-compatible values:

- `file` wraps each pending migration file in its own transaction unless the
  file opts out with `no_transaction`.
- `all` wraps the pending migration SQL bodies in one transaction. It is
  limited to dialects where Ptah can safely run DDL transactionally and rejects
  file-level `no_transaction` directives and migration timeouts.
- `none` applies pending migrations without creating migration transactions.
  Failed runs record statement-level dirty progress. Timeouts are rejected
  until Ptah has a dedicated single-session timeout setup and restore path.

### Non-Transactional Migrations

Most migrations should stay transactional. When the database rejects
transactional execution, mark the migration explicitly:

```sql
-- +ptah no_transaction
ALTER TYPE status ADD VALUE 'archived';
ALTER TABLE users ALTER COLUMN status SET DEFAULT 'archived';
```

`no_transaction` executes the migration body and metadata update outside the
normal per-migration transaction. This is intended for narrow database
requirements such as PostgreSQL enum value additions that must be used by a
later statement in the same migration, or PostgreSQL `CREATE INDEX
CONCURRENTLY` operations. Programmatic migrations set `UpNoTransaction` or
`DownNoTransaction` explicitly; the two directions never share an execution-mode
flag.

Migration timeouts are rejected for `no_transaction` migrations because Ptah
cannot safely apply writer/session timeouts to raw autocommit statements. Ptah
rejects that combination before running the migration body or changing its
revision row, so fixing the directives and retrying does not require dirty-state
repair. If execution is canceled after an autocommit statement, Ptah uses a
bounded cleanup context to record committed progress or finalize the revision
state before returning.

SQL-backed migrations executed through `Migrator` use a two-phase progress
record around each autocommit statement. Before execution, Ptah records the
last known completed statement and marks the next statement's outcome as
unknown. After success, it advances `applied/total`, clears that marker, and
then calls the configured `StatementObserver`.

An abrupt process exit therefore leaves either exact completed progress or a
dirty row that requires database inspection. `RepairMigration` rejects
`ResumeFrom` while the unknown-outcome marker is present because replaying the
statement could duplicate committed SQL. A custom `MigrationFunc` is opaque to
Ptah and can only be recorded at function completion; use SQL-backed migrations
when statement-level crash progress is required.

Atlas-format down execution is the deliberate exception. It leaves the
revision row unchanged before and during the down body to reproduce Atlas's
measured bookkeeping. A non-transactional Atlas-format down can therefore
partially change the schema without leaving statement progress in the revision
table. Prefer the native revision format when crash-visible rollback progress
matters.

## Safety Features

- **Transaction Wrapping**: Each migration runs in its own transaction unless marked `no_transaction`
- **Rollback on Failure**: If a migration fails, the transaction is rolled back
- **Confirmation Prompts**: Down migrations require confirmation (unless `--confirm` is used)
- **Dry Run Mode**: Preview migrations without applying them
- **Migration Timeouts**: File-level directives and CLI defaults can cap lock waits and statement runtime for safer production rollouts
- **Baseline Guardrails**: Brownfield baselining refuses existing migration metadata by default and the CLI can verify against a replayed shadow database
- **Validation**: Migrations are validated before execution

## Limitations

- **Baseline Verification Without Shadow DB**: Entity drift checks cannot prove that migration files replay to the same schema; use `--shadow-db` for production adoption
- **Advanced Features**: Some advanced migration features like conditional migrations or complex rollback scenarios are not yet implemented

## Integration with Ptah

The migrator integrates seamlessly with Ptah's existing infrastructure:

- Uses Ptah's dbschema package for database connections
- Supports the same databases as Ptah (PostgreSQL, MySQL, MariaDB)
- Follows Ptah's transaction and error handling patterns
- Uses Ptah's SQL parsing utilities for statement splitting

## Future Enhancements

- Enhanced query support in executor interfaces
- Migration locking to prevent concurrent execution
- Migration dependency resolution
- Schema validation after migrations
- Migration performance metrics
- Web UI for migration management
