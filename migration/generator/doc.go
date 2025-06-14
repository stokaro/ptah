// Package generator provides dynamic migration file generation for the Ptah schema management system.
//
// This package implements the core functionality for generating database migration files
// by comparing desired schema (from Go entity definitions) with the current database state.
// It produces both up and down migration files with proper SQL statements to synchronize
// schemas in both directions.
//
// # Overview
//
// The generator package bridges the gap between schema differences and executable migration
// files. It takes schema differences identified by the schemadiff package and converts them
// into timestamped migration files that can be applied by the migrator package.
//
// # Key Features
//
//   - Dynamic migration generation from schema differences
//   - Automatic up and down migration file creation
//   - Timestamped migration versioning
//   - Dialect-specific SQL generation
//   - Proper dependency ordering and safety checks
//   - Comprehensive error handling and validation
//
// # Migration Generation Process
//
// The migration generation follows this workflow:
//
//  1. Parse Go entities to extract desired schema
//  2. Connect to database and read current schema
//  3. Calculate differences between desired and current schemas
//  4. Generate up migration SQL from schema differences
//  5. Generate down migration SQL by reversing the differences
//  6. Create timestamped migration files with proper naming
//
// # Core Types
//
// The package provides these main types:
//
//   - GenerateMigrationOptions: Configuration for migration generation
//   - MigrationFiles: Information about generated migration files
//
// # Usage Example
//
// Basic migration generation:
//
//	opts := generator.GenerateMigrationOptions{
//		RootDir:       "./entities",
//		DatabaseURL:   "postgres://user:pass@localhost:5432/db",
//		MigrationName: "add_user_table",
//		OutputDir:     "./migrations",
//	}
//
//	files, err := generator.GenerateMigration(opts)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Generated migration files:\n")
//	fmt.Printf("Up:   %s\n", files.UpFile)
//	fmt.Printf("Down: %s\n", files.DownFile)
//
// # Migration File Structure
//
// Generated migration files follow this naming convention:
//
//   - Up migration: {timestamp}_{name}.up.sql
//   - Down migration: {timestamp}_{name}.down.sql
//
// Each file includes:
//
//   - Header comment with generation timestamp and direction
//   - Properly ordered SQL statements
//   - Semicolon-terminated statements for execution
//
// # Schema Difference Handling
//
// The generator handles various types of schema changes:
//
//   - Table creation and removal
//   - Column addition, modification, and removal
//   - Index creation and removal
//   - Enum type creation, modification, and removal
//   - Constraint addition and removal
//
// # Reverse Migration Logic
//
// Down migrations are generated by reversing the schema differences:
//
//   - Tables added become tables removed
//   - Columns added become columns removed
//   - Modifications are reversed (new -> old becomes old -> new)
//   - Proper dependency ordering is maintained
//
// # Safety Features
//
// The generator includes several safety mechanisms:
//
//   - Validation of schema differences before generation
//   - Proper SQL statement ordering to avoid constraint violations
//   - Error handling for invalid or dangerous operations
//   - Dry-run capabilities through the underlying planner
//
// # Integration with Ptah
//
// This package integrates with other Ptah components:
//
//   - ptah/core/goschema: Parses Go entities for desired schema
//   - ptah/dbschema: Connects to database and reads current schema
//   - ptah/migration/schemadiff: Calculates schema differences
//   - ptah/migration/planner: Generates SQL statements from differences
//   - ptah/migration/migrator: Applies generated migration files
//
// # Error Handling
//
// The generator provides comprehensive error handling:
//
//   - Database connection errors
//   - Schema parsing errors
//   - File system errors during migration file creation
//   - SQL generation errors from invalid schema differences
//
// # Performance Considerations
//
// The generator is optimized for:
//
//   - Efficient schema parsing and comparison
//   - Minimal database queries for schema reading
//   - Fast SQL generation through AST-based rendering
//   - Atomic file operations for migration creation
//
// # Thread Safety
//
// The generator functions are thread-safe and can be called concurrently
// from multiple goroutines. However, care should be taken when generating
// migrations for the same database simultaneously to avoid version conflicts.
package generator
