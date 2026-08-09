// Package migrator provides database migration execution and management for the Ptah schema management system.
//
// This package implements the core migration engine that applies and rolls back database migrations
// with proper version tracking, transaction safety, and comprehensive error handling. It manages
// the migration history table and provides both programmatic and CLI interfaces for migration operations.
//
// # Overview
//
// The migrator package is responsible for executing database migrations in a controlled and safe manner.
// It maintains a migration history table to track applied migrations and provides functionality for
// applying migrations forward (up) or rolling them back (down) to previous versions.
//
// # Key Features
//
//   - Version-based migration tracking with timestamp support
//   - Configurable file, batch, and nontransactional execution modes
//   - Support for both up and down migrations
//   - Migration to specific versions (forward or backward)
//   - Comprehensive migration status reporting
//   - SQL file-based and programmatic migration support
//   - Cross-database compatibility (PostgreSQL, MySQL, MariaDB)
//
// # Core Components
//
// The package provides these main types:
//
//   - Migrator: Main migration engine that manages migration execution
//   - Migration: Represents a single database migration with up/down functions
//   - MigrationFunc: Function type for programmatic migrations
//   - MigrationProvider: Interface for providing migrations to the migrator
//   - RegisteredMigrationProvider: In-memory provider for programmatic migrations
//   - FSMigrationProvider: Filesystem-based provider that loads migrations from SQL files
//   - MigrationStatus: Represents the current state of migrations
//
// # Migration Structure
//
// Each migration consists of:
//
//   - Version: Unique integer identifier (typically timestamp-based)
//   - Description: Human-readable description of the migration
//   - Up: Function to apply the migration
//   - Down: Function to roll back the migration
//
// # Usage Example
//
// Basic migration setup and execution:
//
//	// Create database connection (supply a context to bound the initial Ping)
//	conn, err := dbschema.ConnectToDatabase(ctx, "postgres://user:pass@localhost/db")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer dbschema.CloseAndWarn(conn)
//
//	// Option 1: Create migrator from filesystem
//	m, err := migrator.NewFSMigrator(conn, os.DirFS("/path/to/migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Option 2: Create migrator with a registered SQL-file migration
//	migration, err := migrator.NewMigrationFromSQLFiles(
//		20240101120000,
//		"Create users table",
//		"001_create_users.up.sql",
//		"001_create_users.down.sql",
//		fsys,
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	provider := migrator.NewRegisteredMigrationProvider()
//	provider.Register(migration)
//	m := migrator.NewMigrator(conn, provider)
//
//	// Apply all pending migrations
//	if err := m.MigrateUp(context.Background()); err != nil {
//		log.Fatal(err)
//	}
//
// # Migration History Table
//
// The migrator automatically creates and manages a `schema_migrations` table:
//
//	CREATE TABLE schema_migrations (
//		version BIGINT PRIMARY KEY,
//		description TEXT NOT NULL,
//		applied_at TIMESTAMP NOT NULL,
//		state VARCHAR(32) NOT NULL DEFAULT 'applied',
//		applied INTEGER NOT NULL DEFAULT 1,
//		total INTEGER NOT NULL DEFAULT 1,
//		error TEXT NULL,
//		error_stmt TEXT NULL,
//		execution_time_ms BIGINT NOT NULL DEFAULT 0,
//		checksum VARCHAR(64) NOT NULL DEFAULT ''
//	);
//
// This table tracks which migrations have been applied and when. In
// nontransactional mode, failed or interrupted migrations leave a dirty
// revision row with statement progress and error details; later migration
// operations refuse to continue until the row is repaired. Applied rows also
// store a checksum of the up SQL so edited
// migration files are detected before new work starts. Use
// WithMigrationsTable(schema, table) to store migration history in a custom
// schema or table, for example an `infra.ptah_migrations` table in PostgreSQL.
//
// PostgreSQL, MySQL, MariaDB, and SQL Server migrations acquire a session-level
// advisory lock around the planning and apply window. Use
// WithMigrationLockName to coordinate on a custom lock name, and use
// WithMigrationLockTimeout to bound the wait for that lock. Callers can detect
// acquisition timeouts with IsMigrationLockTimeout. WithoutMigrationLock turns
// the lock off entirely for callers that serialize migration runs by some
// other means; MigrationLockName and MigrationLockSkipped report the resulting
// decision so a command can name what it acquires, or announce what it does
// not.
//
// # Migration Operations
//
// The migrator supports several migration operations:
//
//   - MigrateUp(): Apply all pending migrations
//   - MigrateDown(): Roll back to the previous version
//   - MigrateTo(version): Migrate to a specific version (up or down)
//   - GetCurrentVersion(): Get the current migration version
//   - GetAppliedMigrations(): List all applied migration versions
//   - GetPendingMigrations(): List all pending migration versions
//   - GetMigrationStatus(): Get comprehensive migration status information
//   - RepairMigration(opts): Clear a dirty migration after manual repair, or resume the remaining
//     statements of the direction that left it dirty -- up statements before recording the migration
//     applied, down statements before removing its revision
//
// # Migration Providers
//
// The migrator uses a provider pattern to supply migrations:
//
//   - NewFSMigrator(conn, fsys): Creates a migrator that loads migrations from a filesystem
//   - NewMigrator(conn, provider): Creates a migrator with a custom migration provider
//   - NewRegisteredMigrationProvider(): Creates an in-memory provider for programmatic migrations
//   - NewFSMigrationProvider(fsys): Creates a filesystem-based migration provider
//
// # Transaction Safety
//
// Up migrations default to one transaction per file. The global transaction
// mode can instead wrap the selected batch in one transaction (`all`) or run
// without migration transactions (`none`); explicit per-file `file` and `none`
// directives override global `file` or `none`. Down migrations resolve their
// direction-specific file mode independently and default to `file`.
//
// Transactional execution rolls back schema statements on failure. A
// nontransactional failure can leave earlier statements applied and records
// dirty progress that blocks later migration operations until repaired. Atlas
// revision-table mode intentionally keeps its own documented bookkeeping
// semantics.
//
// MySQL and MariaDB file transactions use an InnoDB revision-row witness on the
// same physical transaction as the migration body because their DDL can commit
// independently. These dialects reject transaction-control SQL, durable server
// settings outside that transaction, and unverified storage engines in file
// mode; batch mode is unsupported.
//
// # SQL File Support
//
// The package provides utilities for SQL file-based migrations:
//
//	// Read both directions and preserve their execution metadata.
//	migration, err := migrator.NewMigrationFromSQLFiles(
//		20240101120000,
//		"Add user preferences",
//		"migration.up.sql",
//		"migration.down.sql",
//		fsys,
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// # SQL Statement Splitting
//
// The migrator properly handles multi-statement SQL files:
//
//   - Uses AST-based parsing to split SQL statements
//   - Properly handles semicolons within string literals and comments
//   - Executes each statement separately for better MySQL compatibility
//   - Provides detailed error reporting for failed statements
//
// # Error Handling
//
// The migrator provides comprehensive error handling:
//
//   - Database connection errors
//   - SQL execution errors with statement context
//   - Transaction management errors
//   - Migration file reading errors
//   - Version tracking errors
//
// # Cross-Database Support
//
// The migrator works with all supported database platforms:
//
//   - PostgreSQL: Full support with proper timestamp handling
//   - MySQL: Compatible with MySQL-specific SQL syntax
//   - MariaDB: Full compatibility with MariaDB features
//
// # Integration with Ptah
//
// This package integrates with other Ptah components:
//
//   - ptah/dbschema: Uses database connections and schema operations
//   - ptah/migration/generator: Applies generated migration files
//   - ptah/core/sqlutil: Uses SQL parsing utilities for statement splitting
//   - ptah/cmd/migrate*: Provides CLI interfaces for migration operations
//
// # Performance Considerations
//
// The migrator is optimized for:
//
//   - Efficient migration execution with minimal overhead
//   - Fast version checking and status queries
//   - Atomic migration operations with proper transaction handling
//   - Memory-efficient SQL file processing
//
// # Thread Safety
//
// Migrator instances are not thread-safe and should not be used concurrently
// from multiple goroutines. However, the migration history table includes
// proper constraints to prevent concurrent migration execution conflicts.
package migrator
