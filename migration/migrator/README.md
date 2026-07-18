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

```
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

Migration directory commands use `--dir-format=auto` by default. Auto mode
prefers Ptah paired files (`NNNNNNNNNN_description.up.sql` and
`NNNNNNNNNN_description.down.sql`) when they are present, and otherwise accepts
Atlas-style timestamp files such as `20220318104614_team_A.sql` or
`20240112070806.sql`, plus numeric migration names produced by Atlas importers
such as `1_initial.sql`, `2.sql`, `1_initial.up.sql`, `1_initial.down.sql`, and
`1.my.sql`. Use
`--dir-format=ptah` or `--dir-format=atlas` on `migrate-up`, `migrate-down`,
`migrate-status`, `migrate-hash`, and `migrate-validate` when detection should be
explicit. Ordinary Atlas and imported single SQL files are forward migrations.
Imported `.down.sql` files are paired with their matching `.up.sql` version for
rollback. Atlas txtar files can also embed a `down.sql` section that Ptah
executes during rollback:

```sql
-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
```

For Atlas txtar migrations, Ptah executes only the `migration.sql` section for
`migrate-up` and only the `down.sql` section for `migrate-down`. Other embedded
txtar files, such as `schema.sql`, are ignored by the migrator; ordinary SQL
comments that look like `-- keep this comment --` remain comments, not txtar
section boundaries. Ptah's txtar support is intentionally limited to Atlas SQL
migration containers and is not a general-purpose txtar parser.

Atlas-format SQL template migrations are rendered with Go `text/template`
before execution and linting. Root versioned files such as `1.sql` and `2.sql`
are executable migrations; shared template files in subdirectories can define
helpers such as `{{ define "shared/users" }}` and are not executed as standalone
migrations. The template data object exposes `.Env`; CLI commands set it with
`--atlas-env`, and programmatic callers can pass `WithAtlasTemplateData`.

If an Atlas migration does not provide `down.sql`, `migrate-down` returns a typed
error explaining that Atlas dynamic down-plan synthesis is not implemented yet.
This is distinct from transaction rollback on a failed migration: transaction
rollback undoes an in-progress failure, Ptah paired `.down.sql` files and Atlas
txtar `down.sql` sections revert already-applied migrations, and Atlas dynamic
`migrate down` would synthesize a downgrade plan from database/dev state.

`-- +ptah` directives inside `migration.sql` and `down.sql` are parsed per
section for timeout and validation purposes. The current `no_transaction` model
is still migration-level: if either direction opts out of transactions, Ptah
treats the migration as non-transactional.

### Migrate Up
Apply all pending migrations:
```bash
go run ./cmd migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations
```

With dry run:
```bash
go run ./cmd migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dry-run
```

Allow applying a migration whose version is below the current high-water mark:
```bash
go run ./cmd migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --exec-order non-linear
```

With a custom migration state table:
```bash
go run ./cmd migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --migrations-schema infra --migrations-table ptah_migrations
```

With an Atlas-style versioned migration directory:
```bash
go run ./cmd migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dir-format atlas
```

### Migrate Down
Roll back to a specific version:
```bash
go run ./cmd migrate-down --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --target 5
```

With confirmation skip (dangerous!):
```bash
go run ./cmd migrate-down --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --target 5 --confirm
```

With a custom migration state table:
```bash
go run ./cmd migrate-down --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --target 5 --migrations-schema infra --migrations-table ptah_migrations
```

### Migration Status
Check current migration status:
```bash
go run ./cmd migrate-status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations
```

Verbose output:
```bash
go run ./cmd migrate-status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --verbose
```

JSON output:
```bash
go run ./cmd migrate-status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --json
```

With a custom migration state table:
```bash
go run ./cmd migrate-status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --migrations-schema infra --migrations-table ptah_migrations
```

With an Atlas-style versioned migration directory:
```bash
go run ./cmd migrate-status --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --dir-format atlas
```

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
logs a warning for each skipped version and `migrate-status` continues to report it as
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
- **`Baseline(ctx, version)` / `BaselineWithOptions(ctx, opts)`**: Records provider migrations as already applied without executing their SQL bodies

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
already exist. The CLI `migrate-baseline` adds pre-flight schema verification:
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
continue until `RepairMigration` or the `migrate-repair` CLI resolves it.
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

PostgreSQL, MySQL, and MariaDB migrators acquire a session-level advisory lock
around the planning and apply window for `MigrateUp`, `MigrateDown`,
`MigrateDownTo`, and `MigrateTo`. This prevents concurrent runners from reading
the same pending migration set and applying it more than once.

By default the migrator waits until the lock is available. Use
`WithMigrationLockTimeout` or the CLI `--migration-lock-timeout` flag to bound
that wait. Timed-out callers receive a typed error that can be detected with
`migrator.IsMigrationLockTimeout`.

### Per-Migration Timeouts

Set CLI defaults for every pending migration:

```bash
go run ./cmd migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir /path/to/migrations --lock-timeout 3s --statement-timeout 30s --migration-lock-timeout 30s
```

Override those defaults in a specific migration file with top-of-file directives:

```sql
-- +ptah lock_timeout=3s
-- +ptah statement_timeout=30s

ALTER TABLE users ADD COLUMN email TEXT;
```

PostgreSQL runs `SET LOCAL lock_timeout` and `SET LOCAL statement_timeout` inside the migration transaction. MySQL and MariaDB run `SET SESSION innodb_lock_wait_timeout`; statement timeouts use MySQL `max_execution_time` and MariaDB `max_statement_time`.

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
later statement in the same migration. Migration timeouts are rejected for
`no_transaction` migrations because Ptah cannot safely apply writer/session
timeouts to raw autocommit statements.

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
