// Package schemadiff provides comprehensive schema comparison and difference analysis for the Ptah schema management system.
//
// This package implements the core functionality for comparing database schemas and identifying
// differences between a desired schema (authored as Go annotations, YAML, or any
// other source that produces a schemamodel.Database) and the current database
// state. It produces detailed difference reports that can be used for migration
// planning and schema synchronization.
//
// # Overview
//
// The schemadiff package serves as the bridge between schema authoring and migration planning.
// It takes two schema representations - a desired state and one from database
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
// in, a *difftypes.SchemaDiff out. The entry points differ in what the caller
// can supply and in what they report back:
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
// The comparison covers every object kind difftypes.SchemaDiff carries, which
// is what difftypes.SchemaDiff.HasChanges reads:
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
// # Difference Types
//
// The comparison produces different types of changes:
//
//   - TablesAdded: Tables that exist in the desired schema but not in the database
//   - TablesRemoved: Tables that exist in the database but not in the desired schema
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
// # Package Organization
//
// The comparison logic and the result model live in two subpackages:
//
//   - internal/compare: per-object-kind comparison logic
//   - difftypes: the public SchemaDiff model and its supplement lists
//
// # Integration with Ptah
//
// This package integrates with other Ptah components:
//
//   - ptah/core/goschema: Consumes generated schema from Go entities
//   - ptah/catalog: Consumes database schema from introspection
//   - ptah/migration/planner: Provides difference data for migration planning
//   - ptah/migration/generator: Used in migration file generation
//
// # Error Handling
//
// The pure entry points -- Compare, CompareWithDialect, CompareWithOptions,
// CompareSchemas, and CompareReportingUndecidedAdditions -- never return an
// error. CompareWithDatabaseInfo and the CompareWithDatabase variants can
// refuse a comparison: they validate the identifier-semantics snapshot (an
// invalid one is reported with an error satisfying
// errors.Is(err, ptaherr.ErrInvalidSchemaDiff)) and check the declaration
// against the target before comparing, returning an error rather than a diff
// that plans a statement the server would refuse.
//
// # Thread Safety
//
// The comparison functions are safe to call concurrently from multiple
// goroutines: each call reads its inputs and returns a fresh
// *difftypes.SchemaDiff. The returned value is an ordinary mutable struct --
// SetIndexAdditions and the other setters edit it in place -- so share one
// diff across goroutines only with external synchronization.
//
// # Scope
//
// The comparison reports what differs. It does not order the result: statement
// ordering, dependency handling, and destructive-operation policy belong to
// ptah/migration/planner, which consumes the report.
package schemadiff
