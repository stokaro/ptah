# Migration Generator

The migration generator package provides functionality to automatically generate both UP and DOWN migration files by comparing the desired database schema (from Go entities) with the current database state.

## Features

- **Automatic Schema Comparison**: Compares Go entity definitions with current database schema
- **Bidirectional Migrations**: Generates both UP and DOWN migration files
- **Multiple Database Support**: Works with PostgreSQL, MySQL, and MariaDB
- **Schema-Scoped Introspection**: Restricts PostgreSQL reads to selected schemas
- **Proper File Naming**: Uses timestamp-based naming convention for migration files
- **Embedded Field Support**: Handles embedded structs in Go entities

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/stokaro/ptah/migration/generator"
)

func main() {
    opts := generator.GenerateMigrationOptions{
        GoEntitiesDir: "./entities",           // Directory containing Go entities
        DatabaseURL:   "postgres://user:pass@localhost/db", // Database connection
        MigrationName: "add_user_table",       // Optional: defaults to "migration"
        OutputDir:     "./migrations",         // Directory to save migration files
        Schemas:       []string{"auth", "billing", "public"}, // Optional PostgreSQL schema allow-list
        ShadowDatabaseURL: "postgres://user:pass@localhost/shadow", // Optional pre-write verification DB
    }

    // Bound the initial database connection so a stuck host fails fast.
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    files, err := generator.GenerateMigration(ctx, opts)
    if err != nil {
        log.Fatal(err)
    }

    // Check if any migration was generated (nil means no changes detected)
    if files == nil {
        fmt.Println("No schema changes detected - no migration needed")
        return
    }

    fmt.Printf("Generated migration files:\n")
    fmt.Printf("UP:   %s\n", files.UpFile)
    fmt.Printf("DOWN: %s\n", files.DownFile)
    fmt.Printf("Version: %d\n", files.Version)
}
```

### Migration Process

The generator follows this process:

1. **Parse Go Entities**: Scans the specified directory for Go structs with schema annotations
2. **Read Current Database Schema**: Connects to the database and introspects the current schema
3. **Calculate Differences**: Compares the desired schema with the current database state
4. **Generate UP Migration**: Creates SQL statements to transform current schema to desired schema
5. **Generate DOWN Migration**: Creates SQL statements to reverse the changes (rollback)
6. **Shadow Verification (Optional)**: Replays prior migrations plus the candidate on a disposable database before files are written
7. **Save Files**: Writes both migration files with proper naming convention

### Shadow Database Verification

Set `ShadowDatabaseURL` to verify generated migrations before they are written:

```go
opts := generator.GenerateMigrationOptions{
    GoEntitiesDir:      "./entities",
    DatabaseURL:        "postgres://localhost:5432/app_dev",
    MigrationName:      "add_user_table",
    OutputDir:          "./migrations",
    ShadowDatabaseURL:  "postgres://localhost:5432/app_shadow",
}
```

The shadow database is treated as disposable. The generator drops all objects in
it, applies every existing migration from `OutputDir`, applies the candidate
migration, re-introspects the database, and compares the result against the Go
schema. If the replayed schema differs, generation aborts before writing files:

```text
shadow check failed: missing column users.email
```

Ptah also runs an `up -> down -> up` round-trip on the candidate migration and
aborts if either direction fails.

### File Naming Convention

Migration files follow the pattern:
```
<timestamp>_<migration_name>.<up|down>.sql
```

Examples:
- `1703123456_add_user_table.up.sql`
- `1703123456_add_user_table.down.sql`

### Supported Schema Changes

The generator can handle:

- **Table Operations**: CREATE, DROP, ALTER TABLE
- **Column Operations**: ADD COLUMN, DROP COLUMN, ALTER COLUMN
- **Index Operations**: CREATE INDEX, DROP INDEX
- **Enum Operations**: CREATE TYPE, DROP TYPE, ALTER TYPE (PostgreSQL)
- **Constraint Operations**: ADD/DROP foreign keys, unique constraints

### Example Generated Migration

**UP Migration** (`1703123456_add_user_table.up.sql`):
```sql
-- Migration generated from schema differences
-- Generated on: 2023-12-21T10:30:56Z
-- Direction: UP

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);
```

**DOWN Migration** (`1703123456_add_user_table.down.sql`):
```sql
-- Migration rollback
-- Generated on: 2023-12-21T10:30:56Z
-- Direction: DOWN

DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;
```

### Error Handling

The generator will return an error if:

- Database connection fails
- Go entity parsing fails
- File system operations fail
- SQL generation fails

**Note:** When no schema changes are detected, the generator returns `nil, nil` (not an error). This is considered a successful no-op operation.

### Integration with Migration System

The generated files are compatible with the ptah migration system and can be executed using:

```bash
go run ./cmd migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations --lock-timeout 3s --statement-timeout 30s
```

For PostgreSQL, MySQL, and MariaDB targets, generated migrations containing `ALTER TABLE` automatically include:

```sql
-- +ptah lock_timeout=3s
-- +ptah statement_timeout=30s
```

Review these defaults before production deployment and adjust them per migration when a longer rollout window is intentional.

## Configuration Options

### GenerateMigrationOptions

The `GenerateMigrationOptions` struct provides comprehensive configuration for migration generation:

```go
type GenerateMigrationOptions struct {
    // GoEntitiesDir is the directory to scan for Go entities
    GoEntitiesDir string

    // GoEntitiesFS is the filesystem to use for reading entities (optional, defaults to os.DirFS)
    // Useful for embedded filesystems or testing with virtual filesystems
    GoEntitiesFS fs.FS

    // DatabaseURL is the connection string for the database
    DatabaseURL string

    // DBConn is the database connection (optional, if not provided, a new connection will be created)
    // Useful for reusing existing connections or custom connection management
    DBConn *dbschema.DatabaseConnection

    // MigrationName is the name for the migration (optional, defaults to "migration")
    MigrationName string

    // OutputDir is the directory where migration files will be saved (always real filesystem)
    OutputDir string

    // AllowedOutputRoot constrains OutputDir when accepting user-supplied paths
    AllowedOutputRoot string

    // CompareOptions are the options to use when comparing schemas
    CompareOptions *config.CompareOptions

    // Schemas restricts database introspection to the listed schemas when the
    // connected dialect supports schema scoping.
    Schemas []string

    // ShadowDatabaseURL enables pre-write verification on a disposable database.
    ShadowDatabaseURL string
}
```

### Field Details

- `GoEntitiesDir`: Directory to scan for Go entities (required)
- `DatabaseURL`: Database connection string (required)
- `DBConn`: Existing database connection (optional; used instead of `DatabaseURL` when set)
- `MigrationName`: Name for the migration (optional, defaults to "migration")
- `OutputDir`: Directory where migration files will be saved (required)
- `CompareOptions`: Schema comparison options (optional)
- `Schemas`: PostgreSQL schema allow-list for database introspection (optional)
- `ShadowDatabaseURL`: Disposable database URL for pre-write migration replay and round-trip checks (optional)

### PostgreSQL Concurrent Indexes

When Ptah generates a new PostgreSQL index on an existing table whose
introspected row estimate is greater than zero, it emits `CREATE INDEX
CONCURRENTLY` and marks that migration file with `-- +ptah no_transaction`.
This avoids PostgreSQL's `CREATE INDEX CONCURRENTLY cannot run inside a
transaction block` failure and reduces write blocking on populated tables.

If a generated change set contains both ordinary transactional DDL and
concurrent index builds, Ptah writes separate migration file pairs: the
transactional migration first, then the `no_transaction` concurrent-index
migration at the next version. Rollbacks naturally run in the reverse order, so
the index is dropped before prerequisite schema changes are reverted.

Ptah only enables this policy when the live target capabilities include
PostgreSQL `CREATE INDEX CONCURRENTLY`. PostgreSQL-wire engines such as
YugabyteDB and CockroachDB keep regular `CREATE INDEX` output when their
capability preset disables the keyword.

### Database URL Examples

- PostgreSQL: `postgres://user:password@localhost:5432/database`
- MySQL: `mysql://user:password@localhost:3306/database`
- MariaDB: `mariadb://user:password@localhost:3306/database`

## Best Practices

1. **Review Generated SQL**: Always review the generated migration files before applying them
2. **Test Migrations**: Test both UP and DOWN migrations in a development environment
3. **Backup Data**: Always backup your database before running migrations in production
4. **Version Control**: Commit migration files to version control
5. **Sequential Application**: Apply migrations in the correct order based on timestamps

## Limitations

- **Data Loss Warning**: DOWN migrations may result in data loss (e.g., dropping columns/tables)
- **Complex Changes**: Some complex schema changes may require manual intervention
- **Database-Specific Features**: Some database-specific features may not be fully supported in reverse migrations
