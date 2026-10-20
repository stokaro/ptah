// Package planner provides migration planning and SQL generation for the Ptah schema management system.
//
// This package implements the core functionality for converting schema differences into executable
// SQL statements. It serves as the bridge between schema comparison results and actual database
// migration execution, providing dialect-specific SQL generation with proper dependency ordering
// and safety considerations.
//
// # Overview
//
// The planner package takes schema differences identified by the schemadiff package and converts
// them into database-specific SQL statements that can be executed to synchronize schemas. It
// provides both AST-based and string-based SQL generation with support for multiple database
// dialects.
//
// # Architecture
//
// The selected runtime owns each dialect-specific implementation:
//
//	┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
//	│   SchemaDiff    │───▶│     planext.Planner      │───▶│   AST Nodes     │
//	│   (Changes)     │    │   (Runtime)      │    │  (SQL Logic)    │
//	└─────────────────┘    └──────────────────┘    └─────────────────┘
//	                                │
//	                                ▼
//	                       ┌──────────────────┐
//	                       │ Dialect-Specific │
//	                       │   Generators     │
//	                       └──────────────────┘
//
// # Key Features
//
//   - A planner for each target supplied by the selected runtime
//   - AST-based SQL generation for type safety and consistency
//   - Proper dependency ordering to avoid constraint violations
//   - Safety checks and warnings for destructive operations
//   - Support for complex schema changes including tables, columns, indexes, and enums
//
// # Core Interface
//
// The package provides a planext.Planner interface for extensible dialect support:
//
//	type planext.Planner interface {
//		GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) ([]ast.Node, error)
//	}
//
// The diff carries the schema operands. The caller passes its context and
// selected runtime separately. Each change entry carries its own operands
// (the difftypes Changes elements and Desired fields), and schema-wide
// vocabulary travels on the diff's Declared* carries, so a planner reads
// everything it plans from the diff. The desired schema is not this package's
// input at all: it is prepared and validated where it is supplied, in the
// schemadiff package, and the comparison puts on the diff what planning needs.
//
// Each implementation handles dialect-specific features, constraints, and SQL generation patterns.
//
// # Supported Dialects
//
// engine/builtin.New assembles the bundled dialects and their planners. This
// package links none of them. Runtime.Targets lists the selected targets, and
// Runtime.MigrationPlanner resolves a target or its alias within that runtime.
// Constructing another runtime does not change the selection. A missing target
// returns ptaherr.ErrUnsupportedDialect; a target without a planner returns
// ptaherr.ErrUnsupportedFeature.
//
// # Main Functions
//
// The package provides several convenience functions for SQL generation:
//
//   - GenerateSchemaDiffAST(): Generates AST nodes from schema differences
//   - GenerateSchemaDiffSQL(): Generates complete SQL string from schema differences
//   - GenerateSchemaDiffSQLStatements(): Generates individual SQL statements as string slice
//   - planext.Resolver.MigrationPlanner(): Selects a planner from the caller's runtime
//
// # Usage Example
//
// The three generation helpers sit at different levels of abstraction:
//
//	// High-level: individual SQL statements
//	statements, err := planner.GenerateSchemaDiffSQLStatements(ctx, runtime, diff, "postgres")
//
//	// Mid-level: one complete SQL string
//	sql, err := planner.GenerateSchemaDiffSQL(ctx, runtime, diff, "postgres")
//
//	// Low-level: AST nodes for custom processing
//	nodes, err := planner.GenerateSchemaDiffAST(ctx, runtime, diff, "postgres")
//
// Basic migration planning:
//
//	// Compare schemas to get differences
//	runtime, err := builtin.New()
//	if err != nil { return err }
//	diff, err := schemadiff.CompareWithDialect(ctx, generated, database, "postgres", runtime)
//	if err != nil { return err }
//
//	// Generate SQL statements for PostgreSQL
//	statements, err := planner.GenerateSchemaDiffSQLStatements(ctx, runtime, diff, "postgres")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Execute statements transactionally
//	tx, err := conn.SchemaWriter().BeginTransaction(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//	for _, stmt := range statements {
//		if err := tx.ExecuteSQL(ctx, stmt); err != nil {
//			tx.Rollback()
//			log.Fatal(err)
//		}
//	}
//	if err := tx.Commit(); err != nil {
//		log.Fatal(err)
//	}
//
// # Migration Order
//
// The planner generates SQL statements in a specific order to respect database dependencies:
//
//  1. Create new enum types (PostgreSQL requirement)
//  2. Modify existing enum types (add new values only)
//  3. Create new tables with all columns and constraints
//  4. Modify existing tables (add/modify/remove columns)
//  5. Add new indexes
//  6. Remove indexes (safe operations)
//  7. Remove tables (with CASCADE warnings)
//  8. Remove enum types (with dependency warnings)
//
// On MySQL and MariaDB, trigger changes form one block that runs after columns
// are added and modified and before columns are dropped, because the server
// accepts a trigger whose body uses a missing column and fails every write to
// the table afterwards. When the block both removes or replaces a trigger and
// creates one on a table, it runs between LOCK TABLES and UNLOCK TABLES, so no
// write lands between the statements. Such a plan has to run in order on one
// database session: on a connection pool, a statement after LOCK TABLES can
// wait behind the lock another connection holds.
//
// # AST-Based Generation
//
// The planner uses AST-based SQL generation for several benefits:
//
//   - Type safety and validation during SQL construction
//   - Consistent formatting across different dialects
//   - Easier testing and debugging of generated SQL
//   - Extensibility for new SQL constructs and dialects
//
// # Safety Features
//
// The planner includes several safety mechanisms:
//
//   - Destructive operations include warning comments
//   - DROP operations use IF EXISTS clauses when possible
//   - CASCADE options are explicitly noted for review
//   - Proper dependency ordering to avoid constraint violations
//
// # SQL Statement Splitting
//
// The planner properly handles multi-statement SQL generation:
//
//   - Uses AST-based parsing to split SQL statements
//   - Properly handles semicolons within string literals and comments
//   - Generates individual statements for better execution control
//   - Provides detailed error context for failed statements
//
// # Integration with Ptah
//
// This package integrates with other Ptah components:
//
//   - ptah/core/difftypes: Consumes schema difference data
//   - ptah/core/goschema: Uses generated schema information
//   - ptah/core/ast: Generates AST nodes for SQL representation
//   - ptah/engine/builtin: Converts AST nodes to dialect-specific SQL
//   - ptah/core/sqlutil: Uses SQL parsing utilities for statement handling
//   - ptah/migration/generator: Used in migration file generation
//
// # Error Handling
//
// Public helpers return errors for user-controlled and configuration-dependent
// failures, including unsupported dialects, renderer failures, and unsupported
// dialect features. Planning failures are *ptaherr.PlanError and rendering
// failures are *ptaherr.RenderError, both answering errors.As; an unsupported
// dialect satisfies errors.Is against ptaherr.ErrUnsupportedDialect and a
// refused diff satisfies errors.Is against ptaherr.ErrInvalidSchemaDiff. CLI
// callers should surface these errors directly instead of relying on panic
// recovery. The errors carry context:
//
//   - Validation of schema differences before SQL generation
//   - Detailed error messages with context information
//   - Explicit refusal of unsupported operations
//   - Proper error propagation for debugging
//
// # Performance Considerations
//
// The planner is optimized for:
//
//   - Efficient AST node generation and manipulation
//   - Fast SQL rendering through optimized visitor patterns
//   - Memory-efficient handling of large schema differences
//   - Minimal computational overhead for complex migrations
//
// # Extensibility
//
// A provider supplies a planext.Planner factory on its engine.Target. The application
// passes that provider to engine.New and keeps the returned runtime throughout
// comparison, planning, and rendering. Factories receive planext.Options, including the
// live capability set when available. There is no package-level registration or
// fallback to another runtime. See ExamplePlanningRuntime_unsupportedDialect and
// ExamplePlanningRuntime_targets for selection examples.
//
// # Thread Safety
//
// The planner functions are thread-safe and can be called concurrently
// from multiple goroutines. The generated AST nodes and SQL statements
// are immutable and safe for concurrent access.
package planner
