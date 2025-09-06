package schemadiff

import (
	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// Compare performs schema comparison between generated and database schemas using default options.
// This is a convenience function that uses default comparison options (ignores "plpgsql" extension).
// For custom configuration, use CompareWithOptions.
func Compare(generated *goschema.Database, database *types.DBSchema) *difftypes.SchemaDiff {
	return CompareWithOptions(generated, database, nil)
}

// CompareWithOptions performs schema comparison between generated and database schemas
// with custom configuration options.
//
// This function provides full control over the comparison process, allowing users to
// specify which extensions should be ignored, and other comparison behaviors.
//
// Parameters:
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - opts: Configuration options for comparison (can be nil for defaults)
//
// Returns a SchemaDiff containing all identified differences between the schemas.
//
// Example usage:
//
//	// Use default options (ignores "plpgsql")
//	diff := schemadiff.CompareWithOptions(generated, database, nil)
//
//	// Ignore specific extensions
//	opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
//	diff := schemadiff.CompareWithOptions(generated, database, opts)
//
//	// Don't ignore any extensions
//	opts := config.WithIgnoredExtensions()
//	diff := schemadiff.CompareWithOptions(generated, database, opts)
func CompareWithOptions(generated *goschema.Database, database *types.DBSchema, opts *config.CompareOptions) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}

	// Compare tables and their column structures
	compare.TablesAndColumns(generated, database, diff)

	// Compare enum type definitions and values
	compare.Enums(generated, database, diff)

	// Compare database index definitions
	compare.Indexes(generated, database, diff)

	// Compare PostgreSQL extensions with configuration options
	compare.Extensions(generated, database, diff, opts)

	// Compare PostgreSQL functions (PostgreSQL-specific feature)
	compare.Functions(generated, database, diff)

	// Compare RLS policies (PostgreSQL-specific feature)
	compare.RLSPolicies(generated, database, diff)

	// Compare RLS enabled tables (PostgreSQL-specific feature)
	compare.RLSEnabledTables(generated, database, diff)

	// Compare roles (PostgreSQL-specific feature)
	compare.Roles(generated, database, diff)

	// Compare table-level constraints (EXCLUDE, CHECK, UNIQUE, etc.)
	compare.Constraints(generated, database, diff)

	return diff
}
