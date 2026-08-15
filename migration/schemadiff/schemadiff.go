package schemadiff

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/reservedrole"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/internal/identifiervalidation"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
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

// CompareWithDatabaseInfo compares using caller-supplied database metadata.
// SQL Server callers should prefer CompareWithDatabase, which resolves the
// complete candidate identifier set under the live catalog collation. A
// non-zero identifier snapshot must be valid, cover every compared identifier,
// and admit no target identifier collisions.
func CompareWithDatabaseInfo(
	generated *goschema.Database,
	database *types.DBSchema,
	info types.DBInfo,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, error) {
	diff, _, err := compareWithDatabaseInfoReportingUndecidedAdditions(
		generated, database, info, opts,
	)
	return diff, err
}

func compareWithDatabaseInfoReportingUndecidedAdditions(
	generated *goschema.Database,
	database *types.DBSchema,
	info types.DBInfo,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, []coverage.Object, error) {
	merged := config.DefaultCompareOptions()
	if opts != nil {
		*merged = *opts
		merged.IgnoredExtensions = slices.Clone(opts.IgnoredExtensions)
	}
	merged.Dialect = info.Dialect
	// Resolved before any of the validations below can return, so a malformed
	// drop toggle is reported on every SQLite comparison rather than only the
	// ones that get far enough to classify a virtual table (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidateToggle(info.Dialect); err != nil {
		return nil, nil, err
	}
	// A reserved PostgreSQL role is in neither DBSchema.Roles nor
	// DBSchema.RolesOutOfScope, so comparing it would read it as absent and
	// plan a CREATE ROLE the server always refuses. Refuse the declaration
	// here instead, before anything is compared (stokaro/ptah#1312).
	if generated != nil {
		if err := reservedrole.ValidateDeclared(info.Dialect, generated.Roles); err != nil {
			return nil, nil, err
		}
		if err := schemaselection.ValidateDeclaredPostgresSystemSchemas(
			info.Dialect,
			generated.Schemas,
		); err != nil {
			return nil, nil, err
		}
	}
	// A SQLite virtual table cannot appear on the desired side of any
	// comparison, so its absence there is not deletion intent and its presence
	// there is a different kind of object. Refuse both before anything is
	// compared, rather than planning a DROP the operator never asked for
	// (stokaro/ptah#1028).
	//
	// The caller's diff policy travels with the comparison because both halves
	// of that guard predict statements, and a caller that skips `drop_table`
	// deletes the predicted DROP again before anything is rendered. See
	// [config.CompareOptions.SkipTableDrops].
	virtualPolicy := sqlitevirtual.Policy{
		SkipDropTable:  merged.SkipTableDrops,
		SkipDropColumn: merged.SkipColumnDrops,
		SkipDropIndex:  merged.SkipIndexDrops,
	}
	if err := sqlitevirtual.ValidateComparison(info.Dialect, generated, database, virtualPolicy); err != nil {
		return nil, nil, err
	}
	generated = fromschema.AssignDefaultForeignKeyNames(generated, info.Dialect)
	semantics := info.IdentifierSemantics.Normalize(info.Dialect)
	if !info.IdentifierSemantics.IsZero() &&
		!info.IdentifierSemantics.Equal(semantics) {
		return nil, nil, fmt.Errorf(
			"%w: invalid identifier semantics snapshot",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	names := collectIdentifierNames(generated, database, semantics.DefaultSchema)
	if err := identifiervalidation.ValidateCoverage(semantics, names); err != nil {
		return nil, nil, err
	}
	if err := identifiervalidation.ValidateTarget(
		generated,
		info.Dialect,
		semantics,
	); err != nil {
		return nil, nil, err
	}
	merged.IdentifierSemantics = &semantics
	diff, undecided := CompareReportingUndecidedAdditions(generated, database, merged)
	// The half of the SQLite virtual-table guard that only the comparator can
	// answer. A table both sides name and describe differently is rebuilt by
	// the SQLite planner -- drop, recreate, copy -- which destroys a module's
	// storage as surely as a drop, and whether that will happen is this diff's
	// answer rather than anything the pre-comparison check could compute
	// without a second copy of these rules (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidatePlannedChanges(
		info.Dialect, database, diff, virtualPolicy,
	); err != nil {
		return nil, nil, err
	}
	if err := compare.ValidateMySQLFunctionDefinerReplacements(
		generated,
		database,
		diff,
		info.Dialect,
		semantics,
	); err != nil {
		return nil, nil, err
	}
	return diff, undecided, nil
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
	diff, _ := CompareReportingUndecidedAdditions(generated, database, opts)
	return diff
}

// CompareReportingUndecidedAdditions performs the same comparison as
// [CompareWithOptions] and also reports what it could not decide.
//
// The second return names objects the DESIRED state declares that the CURRENT
// state's coverage record made undecidable -- the read never looked at that
// kind -- and whose creation Ptah renders without an IF NOT EXISTS guard, so
// planning it would fail the migration if the object were already there. They
// are absent from the diff's added lists, and a caller that reports a synced
// schema without mentioning them is telling an operator less than the truth
// (stokaro/ptah#1276).
//
// It is a second return rather than a field on [difftypes.SchemaDiff] because
// every slice field of that type is a category of change the planner renders
// SQL for, asserted by reflection over the struct (stokaro/ptah#1284). An
// undecided addition is the opposite: there is no statement to run, and a
// `migrate diff` that wrote a migration file holding none would be worse than
// the silence this replaces.
//
// The entries are sorted by kind and then name, so a diagnostic built from them
// is stable across runs over the same two states.
func CompareReportingUndecidedAdditions(
	generated *goschema.Database,
	database *types.DBSchema,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, []coverage.Object) {
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}
	if opts.Dialect != "" {
		generated = fromschema.AssignDefaultForeignKeyNames(generated, opts.Dialect)
	}

	diff := &difftypes.SchemaDiff{}
	identifierSemantics := identifier.ForDialect(opts.Dialect)
	if opts.IdentifierSemantics != nil {
		candidate := opts.IdentifierSemantics.Normalize(opts.Dialect)
		candidateNames := collectIdentifierNames(
			generated,
			database,
			candidate.DefaultSchema,
		)
		validSnapshot := opts.IdentifierSemantics.IsZero() ||
			opts.IdentifierSemantics.Equal(candidate)
		if validSnapshot &&
			identifiervalidation.ValidateCoverage(candidate, candidateNames) == nil &&
			identifiervalidation.ValidateTarget(generated, opts.Dialect, candidate) == nil {
			identifierSemantics = candidate
		}
		storedSemantics := identifierSemantics
		diff.IdentifierSemantics = &storedSemantics
	}
	generated, database = normalizeInlineEnumsForCompare(generated, database, opts)
	generated = normalizeGeneratedColumnsForCompare(generated, opts)

	// What each side declined to describe travels with that side rather than
	// with the options, so every caller that builds options from scratch still
	// gets it. Putting it on the options is how an earlier attempt lost it:
	// four surfaces resolve a desired state independently, and one of them
	// built its compare options from a zero value (stokaro/ptah#1276).
	cov := compare.CoverageOf(generated, database)

	// Compare tables and their column structures
	compare.TablesAndColumnsWithSemantics(
		generated,
		database,
		diff,
		opts.Dialect,
		identifierSemantics,
		cov,
	)

	// Compare enum type definitions and values. The semantics carry the
	// connection's default schema, without which an `enum` block's mandatory
	// `schema = schema.public` and the reader's blanked schema read as two
	// different types (stokaro/ptah#1276).
	compare.EnumsWithSemantics(generated, database, diff, identifierSemantics)

	// Compare database index definitions
	compare.IndexesWithSemantics(generated, database, diff, opts.Dialect, identifierSemantics)

	// Compare PostgreSQL extensions with configuration options
	compare.ExtensionsWithSemantics(generated, database, diff, opts, cov, identifierSemantics)

	// Compare PostgreSQL functions (PostgreSQL-specific feature)
	compare.FunctionsWithSemantics(generated, database, diff, opts.Dialect, identifierSemantics)

	// Compare PostgreSQL standalone sequences (PostgreSQL-specific feature)
	compare.SequencesWithSemantics(generated, database, diff, cov, identifierSemantics)

	// Compare PostgreSQL user-defined types (domains, composites, ranges)
	compare.DomainsWithSemantics(generated, database, diff, cov, identifierSemantics)
	compare.CompositeTypesWithSemantics(generated, database, diff, cov, identifierSemantics)
	compare.RangesWithSemantics(generated, database, diff, cov, identifierSemantics)

	// Compare views, materialized views, and triggers
	compare.ViewsWithSemantics(generated, database, diff, opts.Dialect, identifierSemantics)
	compare.MaterializedViewsWithSemantics(generated, database, diff, opts.Dialect, identifierSemantics)
	compare.TriggersWithSemantics(generated, database, diff, identifierSemantics)

	// Compare RLS policies (PostgreSQL-specific feature)
	compare.RLSPoliciesWithSemantics(generated, database, diff, identifierSemantics, cov)

	// Compare RLS enabled tables (PostgreSQL-specific feature)
	compare.RLSEnabledTablesWithSemantics(generated, database, diff, identifierSemantics)

	// Compare roles (PostgreSQL-specific feature)
	compare.Roles(generated, database, diff, cov)

	// Compare role privilege grants (PostgreSQL-specific feature)
	compare.GrantsWithSemantics(generated, database, diff, identifierSemantics)

	// Compare table-level constraints (EXCLUDE, CHECK, UNIQUE, etc.)
	compare.ConstraintsWithSemantics(generated, database, diff, opts, identifierSemantics)

	// Every comparator sorts its own lists after filtering them, but the
	// undecided additions arrive from several comparators, and the order inside
	// each one follows the map iteration that produced the planned list. A
	// diagnostic whose line order changes between two runs over the same inputs
	// is one nobody can diff, so they are ordered here.
	undecided := cov.UndecidedAdditions()
	slices.SortFunc(undecided, func(a, b coverage.Object) int {
		if a.Kind != b.Kind {
			return strings.Compare(string(a.Kind), string(b.Kind))
		}
		return strings.Compare(a.Name, b.Name)
	})

	return diff, undecided
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
