// Package schemadiff provides comprehensive schema comparison and difference analysis for the Ptah schema management system.
//
// This package implements the core functionality for comparing database schemas and identifying
// differences between a desired schema (generated from Go entity definitions) and the current
// database state. It produces detailed difference reports that can be used for migration planning
// and schema synchronization.
//
// # Overview
//
// The schemadiff package serves as the bridge between schema generation and migration planning.
// It takes two schema representations - one from Go entity parsing and another from database
// introspection - and produces a comprehensive difference analysis that identifies all changes
// needed to synchronize the schemas.
//
// # Key Features
//
//   - Comprehensive schema comparison across all database objects
//   - Detailed difference analysis with change categorization
//   - Support for tables, columns, indexes, enums, and constraints
//   - Table-qualified index identity for dialects with table-scoped names
//   - Proper handling of schema modifications and additions/removals
//   - Integration with migration planning for SQL generation
//
// # Core Functionality
//
// Every comparison has the same shape -- a desired schema and a current schema
// in, a *difftypes.SchemaDiff out. The entry points differ in what the caller can
// supply and in what they report back:
//
//   - Compare: the desired schema against a database schema, under
//     config.DefaultCompareOptions
//   - CompareWithDialect: the same plus a target dialect, which selects the
//     dialect-specific normalization rules
//   - CompareWithOptions: full config.CompareOptions control, including ignored
//     extensions and an identifier-semantics override
//   - CompareSchemas: two in-memory desired-schema documents, where one is
//     treated as the current state
//   - CompareWithDatabaseInfo: caller-supplied database metadata
//   - CompareWithDatabase: resolves live catalog identifier equivalence from an
//     open connection before comparing
//   - CompareReportingUndecidedAdditions and
//     CompareWithDatabaseReportingUndecidedAdditions: the same comparisons,
//     also naming the desired objects the current state's coverage record left
//     undecidable
//
// # Comparison Categories
//
// The comparison covers every object kind difftypes.SchemaDiff carries, which is
// what difftypes.SchemaDiff.HasChanges reads:
//
//   - Tables: new, removed, and modified table structures
//   - Columns: added, removed, and modified column definitions
//   - Indexes: new and removed database indexes
//   - Enums: new, removed, and modified enum type definitions
//   - Constraints: primary keys, foreign keys, unique constraints, and check
//     constraints
//   - Views and materialized views, including refresh strategy
//   - Functions, procedures, and sequences
//   - Triggers
//   - Domains, composite types, and range types
//   - Synonyms
//   - Extensions and their installation schema
//   - Roles, granted privileges, and row-level security policies
//   - TimescaleDB hypertables and continuous aggregates
//   - SQL Server extended properties
//
// # Usage Example
//
// Basic schema comparison:
//
//	// Parse Go entities to get the desired schema
//	desired, err := goschema.ParseDir("./entities")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Connect to database and read current schema (supply a context)
//	conn, err := dbschema.ConnectToDatabase(ctx, "postgres://user:pass@localhost/db")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer dbschema.CloseAndWarn(conn)
//
//	current, err := conn.Reader().ReadSchemaContext(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Compare schemas
//	diff := schemadiff.Compare(desired, current)
//
//	// Check if there are any changes
//	if diff.HasChanges() {
//		fmt.Println("Schema differences detected:")
//		// Process differences...
//	}
//
// # Difference Types
//
// The comparison produces different types of changes:
//
//   - TablesAdded: Tables that exist in the desired schema but not in the current one
//   - TablesRemoved: Tables that exist in the current schema but not in the desired one
//   - TablesModified: Tables that exist in both but have structural differences
//   - EnumsAdded/EnumsRemoved/EnumsModified: Enum type changes
//   - IndexesAdded/IndexesRemoved: Index changes
//
// Index names are not globally unique in every supported database.
// SchemaDiff.IndexesAdded and SchemaDiff.IndexesRemoved therefore contain
// canonical IndexRef values with the raw index name and required owning table.
// IndexAdditions and IndexRemovals return copies for consumers that must mutate
// or retain the slices independently. Comparator output and setter input are
// sorted deterministically.
//
// # Table Modifications
//
// For modified tables, the comparison identifies:
//
//   - ColumnsAdded: New columns to be added
//   - ColumnsRemoved: Existing columns to be removed
//   - ColumnsModified: Existing columns with changed properties
//
// # Column Modifications
//
// For modified columns, the comparison tracks changes in:
//
//   - Data type changes
//   - Null/not null constraint changes
//   - Default value changes
//   - Primary key constraint changes
//   - Unique constraint changes
//   - Check constraint changes
//   - Foreign key constraint changes
//
// # Enum Modifications
//
// For modified enums, the comparison identifies:
//
//   - ValuesAdded: New enum values to be added
//   - ValuesRemoved: Existing enum values to be removed
//
// # Internal Architecture
//
// The package is organized with internal comparison modules:
//
//   - internal/compare: Core comparison logic for different schema objects
//   - internal/normalize: Schema normalization utilities
//   - types: Type definitions for difference structures
//
// # Integration with Ptah
//
// This package integrates with other Ptah components:
//
//   - ptah/core/schemamodel: Consumes the desired schema every reader produces
//   - ptah/catalog: Consumes database schema from introspection
//   - ptah/migration/planner: Provides difference data for migration planning
//   - ptah/migration/generator: Used in migration file generation
//
// # Performance Considerations
//
// The comparison algorithm is optimized for:
//
//   - Efficient schema traversal and comparison
//   - Memory-efficient difference storage
//   - Fast lookup operations for schema objects
//   - Minimal computational overhead for large schemas
//
// # Error Handling
//
// The comparison process is designed to be robust:
//
//   - Handles missing or malformed schema objects gracefully
//   - Provides detailed error context for debugging
//   - Continues comparison even when individual objects fail
//   - Produces partial results when possible
//
// # Thread Safety
//
// The comparison functions are thread-safe and can be called concurrently
// from multiple goroutines. The returned difference structures are immutable
// and safe for concurrent access.
//
// # Scope
//
// The comparison reports what differs. It does not order the result: statement
// ordering, dependency handling, and destructive-operation policy belong to
// ptah/migration/planner, which consumes the report.
package schemadiff
