// Package planner provides the core migration planning functionality for the Ptah schema management system.
//
// This package serves as the central orchestrator for converting schema differences into executable
// SQL migration statements. It acts as a bridge between schema comparison results and database-specific
// SQL generation, providing a unified interface for migration planning across multiple database dialects.
//
// # Architecture Overview
//
// The planner package follows a registry pattern with dialect-specific implementations:
//
//	┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
//	│   SchemaDiff    │───▶│     Planner      │───▶│   AST Nodes     │
//	│   (Changes)     │    │   (Registry)     │    │  (SQL Logic)    │
//	└─────────────────┘    └──────────────────┘    └─────────────────┘
//	                                │
//	                                ▼
//	                       ┌──────────────────┐
//	                       │ Dialect-Specific │
//	                       │   Generators     │
//	                       │ (postgres/mysql/│
//	                       │  sqlite)         │
//	                       └──────────────────┘
//
// # Core Interface
//
// The Planner interface defines the contract for all dialect-specific migration generators:
//
//	type Planner interface {
//		GenerateMigrationAST(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node
//		GenerateMigrationASTChecked(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error)
//	}
//
// Each implementation handles dialect-specific features, constraints, and SQL generation patterns.
//
// # Supported Database Dialects
//
// Currently supported database platforms:
//   - PostgreSQL: Full support with ENUM types, SERIAL columns, and advanced constraints
//   - MySQL: Complete support with AUTO_INCREMENT, ENGINE specifications, and charset handling
//   - MariaDB: Served by the MySQL planner configured with the MariaDB
//     capability preset (capability.MariaDB1011), which unlocks
//     MariaDB-only SQL such as IF EXISTS guards on constraint drops
//   - SQLite: Conservative support for native CREATE TABLE, ADD COLUMN,
//     indexes, views, triggers, and drops; table rebuilds are reported for
//     structural ALTER operations SQLite cannot perform directly
//
// # Usage Patterns
//
// The package provides multiple levels of abstraction for different use cases:
//
//	// High-level: Get SQL statements directly
//	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, "postgres")
//
//	// Mid-level: Get complete SQL string
//	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, "postgres")
//
//	// Low-level: Get AST nodes for custom processing
//	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, "postgres")
//
// # Error Handling
//
// Public helpers return errors for user-controlled and configuration-dependent
// failures, including unsupported dialects, renderer failures, and unsupported
// dialect features. CLI callers should surface these errors directly instead of
// relying on panic recovery.
package planner

import (
	"errors"
	"sync"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/migration/planner/dialects/clickhouse"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/planner/dialects/sqlite"
	"github.com/stokaro/ptah/migration/planner/registry"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

var builtInPlannerRegistration struct {
	once sync.Once
	err  error
}

// Planner defines the interface for database-specific migration planning.
//
// Implementations of this interface are responsible for converting schema differences
// into Abstract Syntax Tree (AST) nodes that represent the SQL operations needed to
// migrate from the current database schema to the target schema.
//
// The interface is designed to be dialect-agnostic at the contract level while
// allowing implementations to handle database-specific features, constraints,
// and optimization strategies.
//
// # Implementation Requirements
//
// Implementations must:
//   - Generate AST nodes in dependency-aware order (e.g., create tables before foreign keys)
//   - Handle dialect-specific data types and constraints appropriately
//   - Provide safe migration paths that minimize data loss risks
//   - Support rollback scenarios where applicable
//
// # Parameters
//
//   - diff: Contains the differences between target and current schemas
//   - generated: The target schema derived from Go struct annotations
//
// # Return Value
//
// Returns a slice of AST nodes representing the SQL operations needed for migration.
// The nodes are ordered to respect database dependencies and constraints.
//
// # Example Implementation Pattern
//
//	func (p *PostgresPlanner) GenerateMigrationASTChecked(
//		diff *types.SchemaDiff,
//		generated *goschema.Database,
//	) ([]ast.Node, error) {
//		var nodes []ast.Node
//
//		// 1. Create enum types first (PostgreSQL-specific)
//		nodes = append(nodes, p.generateEnumCreations(diff, generated)...)
//
//		// 2. Create tables in dependency order
//		nodes = append(nodes, p.generateTableCreations(diff, generated)...)
//
//		// 3. Add indexes and constraints
//		nodes = append(nodes, p.generateIndexCreations(diff, generated)...)
//
//		return nodes, nil
//	}
type Planner = registry.Planner

// Options configures high-level planner helpers.
type Options = registry.Options

// Factory creates a planner for a dialect from construction options.
type Factory = registry.Factory

// Register registers a planner factory for a dialect. Third-party dialects can
// call this from init and then use the standard planner helpers.
func Register(dialect string, factory Factory) error {
	if err := ensureBuiltInPlannersRegistered(); err != nil {
		return err
	}
	return registry.Register(dialect, factory)
}

// RegisteredDialects returns the registered planner dialect names.
func RegisteredDialects() []string {
	if err := ensureBuiltInPlannersRegistered(); err != nil {
		return nil
	}
	return registry.RegisteredDialects()
}

// GetPlanner returns a dialect-specific migration planner for the given database dialect.
//
// This registry lookup creates and returns the appropriate planner
// implementation based on the specified database dialect. Each planner handles
// dialect-specific features, SQL syntax variations, and optimization
// strategies.
//
// # Supported Dialects
//
// The function supports the following database dialects:
//   - "postgres": Returns a PostgreSQL-specific planner with support for ENUM types,
//     SERIAL columns, and PostgreSQL-specific constraints
//   - "mysql": Returns a MySQL-specific planner with support for AUTO_INCREMENT,
//     ENGINE specifications, and MySQL-specific features, configured with the
//     capability.MySQL80 preset (no IF EXISTS guards — exactly-once drops)
//   - "mariadb": Returns the same MySQL planner configured with the
//     capability.MariaDB1011 preset, which additionally requests IF EXISTS
//     guards on constraint drops (issue #226)
//   - "sqlite": Returns a conservative SQLite planner for native DDL and
//     explicit errors for table rebuild operations
//
// # Parameters
//
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a Planner implementation specific to the requested dialect, or an
// error for unknown, unsupported, empty, or invalid dialect strings.
//
// # Usage Example
//
//	import "github.com/stokaro/ptah/core/platform"
//
//	// Get PostgreSQL planner
//	pgPlanner, err := planner.GetPlanner(platform.Postgres)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Get MySQL planner
//	mysqlPlanner, err := planner.GetPlanner(platform.MySQL)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Generate migration AST
//	nodes, err := pgPlanner.GenerateMigrationASTChecked(diff, generated)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// # Design Rationale
//
// The registry pattern is used here to:
//   - Provide a clean, consistent interface for planner creation
//   - Allow third-party extension for new database dialects
//   - Centralize dialect validation and error handling
//   - Enable dependency injection and testing scenarios
func GetPlanner(dialect string) (Planner, error) {
	return GetPlannerWithCapabilities(dialect, capability.ForDialect(dialect))
}

// GetPlannerWithCapabilities returns a dialect-specific migration planner for
// a concrete target capability set. Live database paths should pass
// DBInfo.Capabilities so planning uses the same server-version preset as
// readers and renderers. Offline callers should use GetPlanner.
func GetPlannerWithCapabilities(dialect string, caps capability.Capabilities) (Planner, error) {
	return GetPlannerWithOptions(dialect, Options{Capabilities: caps})
}

// GetPlannerWithOptions returns a dialect-specific migration planner with
// explicit high-level generation policy.
func GetPlannerWithOptions(dialect string, opts Options) (Planner, error) {
	if err := ensureBuiltInPlannersRegistered(); err != nil {
		return nil, err
	}
	return registry.Get(dialect, opts)
}

func ensureBuiltInPlannersRegistered() error {
	builtInPlannerRegistration.once.Do(func() {
		builtInPlannerRegistration.err = registerBuiltInPlanners()
	})
	return builtInPlannerRegistration.err
}

func registerBuiltInPlanners() error {
	for _, dialect := range []string{
		platform.Postgres,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Spanner,
	} {
		if err := registerPostgresFamilyPlanner(dialect); err != nil {
			return err
		}
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		if err := registerMySQLFamilyPlanner(dialect); err != nil {
			return err
		}
	}

	if err := registry.Register(platform.ClickHouse, func(Options) Planner {
		return clickhouse.New()
	}); err != nil {
		return err
	}
	return registry.Register(platform.SQLite, func(Options) Planner {
		return sqlite.New()
	})
}

func registerPostgresFamilyPlanner(dialect string) error {
	return registry.Register(dialect, func(opts Options) Planner {
		return postgres.NewWithCapabilities(opts.CapabilitiesFor(dialect)).
			WithConcurrentIndexNames(opts.ConcurrentIndexNames...)
	})
}

func registerMySQLFamilyPlanner(dialect string) error {
	return registry.Register(dialect, func(opts Options) Planner {
		return mysql.NewWithCapabilities(opts.CapabilitiesFor(dialect))
	})
}

// GenerateSchemaDiffAST generates AST nodes for schema differences using the specified dialect.
//
// This is a convenience function that combines planner creation and AST generation
// into a single call. It internally uses GetPlanner to obtain the appropriate
// dialect-specific planner and then calls GenerateMigrationAST on it.
//
// # Parameters
//
//   - diff: Schema differences identified by the schemadiff package
//   - generated: Target schema parsed from Go struct annotations
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a slice of AST nodes representing the SQL operations needed for migration.
// The nodes are ordered to respect database dependencies and constraints.
//
// # Usage Example
//
//	import "github.com/stokaro/ptah/core/platform"
//
//	// Generate AST nodes for PostgreSQL
//	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)
//	if err != nil {
//		return err
//	}
//
//	// Process nodes for custom validation or transformation
//	for _, node := range nodes {
//		// Custom processing logic
//	}
//
// # See Also
//
//   - GenerateSchemaDiffSQL: For complete SQL string generation
//   - GenerateSchemaDiffSQLStatements: For individual SQL statements
//   - GetPlanner: For direct planner access
func GenerateSchemaDiffAST(diff *types.SchemaDiff, generated *goschema.Database, dialect string) ([]ast.Node, error) {
	planner, err := GetPlannerWithCapabilities(dialect, capability.ForDialect(dialect))
	if err != nil {
		return nil, err
	}
	return planner.GenerateMigrationASTChecked(diff, generated)
}

// GenerateSchemaDiffASTWithCapabilities generates AST nodes for a concrete
// target capability set resolved from a live server version.
func GenerateSchemaDiffASTWithCapabilities(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) ([]ast.Node, error) {
	return GenerateSchemaDiffASTWithOptions(diff, generated, dialect, Options{Capabilities: caps})
}

// GenerateSchemaDiffASTWithOptions generates AST nodes with explicit planning
// options.
func GenerateSchemaDiffASTWithOptions(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	opts Options,
) ([]ast.Node, error) {
	planner, err := GetPlannerWithOptions(dialect, opts)
	if err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	if err != nil {
		return nil, wrapPlanError(dialect, err)
	}
	return nodes, nil
}

// NodeRequiresNoTransaction reports whether a single planned AST node must run
// outside the migrator's per-migration transaction.
func NodeRequiresNoTransaction(dialect string, node ast.Node) bool {
	if !platform.IsPostgresFamily(dialect) {
		return false
	}
	if index, ok := node.(*ast.IndexNode); ok {
		return index.Concurrently
	}
	alterType, ok := node.(*ast.AlterTypeNode)
	if !ok {
		return false
	}
	for _, op := range alterType.Operations {
		if _, ok := op.(*ast.AddEnumValueOperation); ok {
			return true
		}
	}
	return false
}

// RequiresNoTransaction reports whether the planned migration contains
// statements that must be applied outside the migrator's per-migration
// transaction. Keep this conservative: it should only return true for DDL that
// is known to be rejected or unusable in a PostgreSQL-family transaction.
func RequiresNoTransaction(dialect string, nodes []ast.Node) bool {
	for _, node := range nodes {
		if NodeRequiresNoTransaction(dialect, node) {
			return true
		}
	}
	return false
}

// GenerateSchemaDiffSQLStatements generates individual SQL statements for schema differences.
//
// This high-level convenience function provides the most commonly used output format:
// a slice of individual SQL statements that can be executed sequentially to perform
// the migration. It combines AST generation, SQL rendering, and statement splitting
// into a single operation.
//
// # Parameters
//
//   - diff: Schema differences identified by the schemadiff package
//   - generated: Target schema parsed from Go struct annotations
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a slice of individual SQL statements, each ending with a semicolon.
// The statements are ordered to respect database dependencies and can be executed
// sequentially to perform the migration.
//
// # Statement Processing
//
// The function performs the following processing steps:
//  1. Generate AST nodes using GenerateSchemaDiffAST
//  2. Render AST nodes to complete SQL using the renderer package
//  3. Split the SQL into individual statements using sqlutil.SplitSQLStatements
//  4. Return the statements as a string slice
//
// # Usage Example
//
//	import "github.com/stokaro/ptah/core/platform"
//
//	// Generate SQL statements for MySQL
//	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, platform.MySQL)
//	if err != nil {
//		return err
//	}
//
//	// Execute statements sequentially
//	for _, stmt := range statements {
//		if err := db.Exec(stmt); err != nil {
//			log.Fatalf("Failed to execute statement: %v", err)
//		}
//	}
//
// # See Also
//
//   - GenerateSchemaDiffSQL: For complete SQL string without splitting
//   - GenerateSchemaDiffAST: For AST nodes without rendering
func GenerateSchemaDiffSQLStatements(diff *types.SchemaDiff, generated *goschema.Database, dialect string) ([]string, error) {
	output, err := GenerateSchemaDiffSQLWithCapabilities(diff, generated, dialect, capability.ForDialect(dialect))
	if err != nil {
		return nil, err
	}
	statements := sqlutil.SplitSQLStatements(output)
	return statements, nil
}

// GenerateSchemaDiffSQLStatementsWithCapabilities generates individual SQL
// statements using a concrete server capability set.
func GenerateSchemaDiffSQLStatementsWithCapabilities(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) ([]string, error) {
	return GenerateSchemaDiffSQLStatementsWithOptions(diff, generated, dialect, Options{Capabilities: caps})
}

// GenerateSchemaDiffSQLStatementsWithOptions generates individual SQL
// statements using explicit planning options.
func GenerateSchemaDiffSQLStatementsWithOptions(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	opts Options,
) ([]string, error) {
	output, err := GenerateSchemaDiffSQLWithOptions(diff, generated, dialect, opts)
	if err != nil {
		return nil, err
	}
	statements := sqlutil.SplitSQLStatements(output)
	return statements, nil
}

// GenerateSchemaDiffSQL generates complete SQL for schema differences as a single string.
//
// This function provides a mid-level interface that generates a complete SQL script
// containing all the statements needed to perform the migration. The output is a
// single string with multiple SQL statements separated by semicolons and newlines.
//
// # Parameters
//
//   - diff: Schema differences identified by the schemadiff package
//   - generated: Target schema parsed from Go struct annotations
//   - dialect: Database dialect identifier (use constants from platform package)
//
// # Return Value
//
// Returns a complete SQL script as a single string. The script contains all
// statements needed for the migration, properly formatted and ordered.
//
// # SQL Generation Process
//
// The function performs the following steps:
//  1. Generate AST nodes using GenerateSchemaDiffAST
//  2. Render all AST nodes to SQL using the dialect-specific renderer
//  3. Return the complete SQL as a single string
//
// # Output Format
//
// The generated SQL includes:
//   - Proper statement termination with semicolons
//   - Appropriate line breaks and formatting
//   - Comments for complex operations (dialect-dependent)
//   - Dependency-ordered statements
//
// # Usage Example
//
//	import "github.com/stokaro/ptah/core/platform"
//
//	// Generate complete SQL script for PostgreSQL
//	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.Postgres)
//	if err != nil {
//		return err
//	}
//
//	// Write to migration file
//	if err := os.WriteFile("migration.sql", []byte(sql), 0644); err != nil {
//		log.Fatalf("Failed to write migration file: %v", err)
//	}
//
//	// Or execute as a single transaction
//	if _, err := db.Exec(sql); err != nil {
//		log.Fatalf("Migration failed: %v", err)
//	}
//
// # See Also
//
//   - GenerateSchemaDiffSQLStatements: For individual SQL statements
//   - GenerateSchemaDiffAST: For AST nodes without rendering
func GenerateSchemaDiffSQL(diff *types.SchemaDiff, generated *goschema.Database, dialect string) (string, error) {
	return GenerateSchemaDiffSQLWithCapabilities(diff, generated, dialect, capability.ForDialect(dialect))
}

// GenerateSchemaDiffSQLWithCapabilities generates complete SQL using a
// concrete server capability set.
func GenerateSchemaDiffSQLWithCapabilities(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) (string, error) {
	return GenerateSchemaDiffSQLWithOptions(diff, generated, dialect, Options{Capabilities: caps})
}

// GenerateSchemaDiffSQLWithOptions generates complete SQL using explicit
// planning options.
func GenerateSchemaDiffSQLWithOptions(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	opts Options,
) (string, error) {
	caps := opts.CapabilitiesFor(dialect)
	astNodes, err := GenerateSchemaDiffASTWithOptions(diff, generated, dialect, opts)
	if err != nil {
		return "", err
	}
	output, err := renderer.RenderSQLWithCapabilities(dialect, caps, astNodes...)
	if err != nil {
		return "", wrapRenderError(dialect, err)
	}
	return output, nil
}

func wrapPlanError(dialect string, err error) error {
	if err == nil {
		return nil
	}
	var planErr *ptaherr.PlanError
	if errors.As(err, &planErr) {
		return err
	}
	return &ptaherr.PlanError{
		Dialect: dialect,
		Err:     err,
		Message: err.Error(),
	}
}

func wrapRenderError(dialect string, err error) error {
	if err == nil {
		return nil
	}
	var renderErr *ptaherr.RenderError
	if errors.As(err, &renderErr) {
		return err
	}
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     err,
		Message: err.Error(),
	}
}
