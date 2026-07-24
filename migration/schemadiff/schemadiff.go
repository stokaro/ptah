package schemadiff

import (
	"strings"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
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

// CompareWithDialect performs schema comparison using default options plus the
// supplied target dialect. The dialect drives dialect-specific normalization,
// such as MySQL-family catalog spellings and referential-action folds (see
// config.CompareOptions.Dialect). Pass an empty dialect for dialect-neutral
// comparison (equivalent to Compare).
func CompareWithDialect(generated *goschema.Database, database *types.DBSchema, dialect string) *difftypes.SchemaDiff {
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	return CompareWithOptions(generated, database, opts)
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
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}

	diff := &difftypes.SchemaDiff{}
	generated, database = normalizeInlineEnumsForCompare(generated, database, opts)
	generated = normalizeGeneratedColumnsForCompare(generated, opts)

	// Compare tables and their column structures
	compare.TablesAndColumnsWithDialect(generated, database, diff, opts.Dialect)

	// Compare enum type definitions and values
	compare.Enums(generated, database, diff)

	// Compare database index definitions
	compare.IndexesWithDialect(generated, database, diff, opts.Dialect)

	// Compare PostgreSQL extensions with configuration options
	compare.Extensions(generated, database, diff, opts)

	// Compare PostgreSQL functions (PostgreSQL-specific feature)
	compare.Functions(generated, database, diff)

	// Compare PostgreSQL standalone sequences (PostgreSQL-specific feature)
	compare.Sequences(generated, database, diff)

	// Compare PostgreSQL user-defined types (domains, composites, ranges)
	compare.Domains(generated, database, diff)
	compare.CompositeTypes(generated, database, diff)
	compare.Ranges(generated, database, diff)

	// Compare views, materialized views, and triggers
	compare.ViewsWithDialect(generated, database, diff, opts.Dialect)
	compare.MaterializedViews(generated, database, diff)
	compare.Triggers(generated, database, diff)

	// Compare RLS policies (PostgreSQL-specific feature)
	compare.RLSPolicies(generated, database, diff)

	// Compare RLS enabled tables (PostgreSQL-specific feature)
	compare.RLSEnabledTables(generated, database, diff)

	// Compare roles (PostgreSQL-specific feature)
	compare.Roles(generated, database, diff)

	// Compare role privilege grants (PostgreSQL-specific feature)
	compare.Grants(generated, database, diff)

	// Compare table-level constraints (EXCLUDE, CHECK, UNIQUE, etc.)
	compare.Constraints(generated, database, diff, opts)

	return diff
}

func normalizeInlineEnumsForCompare(
	generated *goschema.Database,
	database *types.DBSchema,
	opts *config.CompareOptions,
) (*goschema.Database, *types.DBSchema) {
	if generated == nil || database == nil || opts == nil || !isInlineEnumDialect(opts.Dialect) {
		return generated, database
	}

	normalizedGenerated := *generated
	normalizedGenerated.Enums = nil
	normalizedGenerated.Fields = append([]goschema.Field(nil), generated.Fields...)
	for i := range normalizedGenerated.Fields {
		field := &normalizedGenerated.Fields[i]
		if len(field.Enum) > 0 {
			switch platform.NormalizeDialect(opts.Dialect) {
			case platform.MySQL, platform.MariaDB:
				field.Type = mysqlInlineEnumType(field.Enum)
			case platform.SQLite:
				field.Type = "TEXT"
				field.Check = sqliteInlineEnumCheck(*field)
			case platform.SQLServer:
				field.Type = "NVARCHAR(255)"
				field.Check = sqlServerInlineEnumCheck(*field)
			}
		}
	}

	normalizedDatabase := *database
	normalizedDatabase.Enums = nil

	return &normalizedGenerated, &normalizedDatabase
}

func normalizeGeneratedColumnsForCompare(
	generated *goschema.Database,
	opts *config.CompareOptions,
) *goschema.Database {
	if generated == nil || opts == nil {
		return generated
	}

	defaultKind := defaultGeneratedColumnKind(platform.NormalizeDialect(opts.Dialect))
	if defaultKind == "" {
		return generated
	}
	normalizedGenerated := *generated
	normalizedGenerated.Fields = append([]goschema.Field(nil), generated.Fields...)
	for i := range normalizedGenerated.Fields {
		field := &normalizedGenerated.Fields[i]
		if field.GeneratedExpression != "" && field.GeneratedKind == "" {
			field.GeneratedKind = defaultKind
		}
	}
	return &normalizedGenerated
}

func defaultGeneratedColumnKind(dialect string) string {
	switch dialect {
	case platform.Postgres:
		return "STORED"
	case platform.MySQL, platform.MariaDB, platform.SQLite:
		return "VIRTUAL"
	case platform.SQLServer:
		return "PERSISTED"
	default:
		return ""
	}
}

func isInlineEnumDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer:
		return true
	default:
		return false
	}
}

func sqliteInlineEnumCheck(field goschema.Field) string {
	return enumCheck(field)
}

func sqlServerInlineEnumCheck(field goschema.Field) string {
	quoted := make([]string, 0, len(field.Enum))
	for _, value := range field.Enum {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	enumCheck := "[" + strings.ReplaceAll(field.Name, "]", "]]") + "] IN (" + strings.Join(quoted, ", ") + ")"
	if field.Check != "" {
		return "(" + field.Check + ") AND " + enumCheck
	}
	return enumCheck
}

func enumCheck(field goschema.Field) string {
	quoted := make([]string, 0, len(field.Enum))
	for _, value := range field.Enum {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	enumCheck := field.Name + " IN (" + strings.Join(quoted, ", ") + ")"
	if field.Check != "" {
		return "(" + field.Check + ") AND " + enumCheck
	}
	return enumCheck
}

func mysqlInlineEnumType(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	return "enum(" + strings.Join(quoted, ",") + ")"
}
