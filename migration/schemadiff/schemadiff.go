package schemadiff

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/clickhouserbac"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/convert/goschematodb"
	"go.5x5.cz/ptah/internal/crdbttl"
	"go.5x5.cz/ptah/internal/reservedrole"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/internal/systemschema"
	"go.5x5.cz/ptah/internal/timescale"
	"go.5x5.cz/ptah/migration/internal/identifiervalidation"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// Compare performs schema comparison between the desired and current schemas using default options.
// This is a convenience function that uses default comparison options (ignores "plpgsql" extension).
// For custom configuration, use CompareWithOptions.
func Compare(desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	return CompareWithOptions(desired, current, nil)
}

// CompareWithDialect performs schema comparison using default options plus the
// supplied target dialect. The dialect drives dialect-specific normalization,
// such as MySQL-family catalog spellings and referential-action folds (see
// config.CompareOptions.Dialect). Pass an empty dialect for dialect-neutral
// comparison (equivalent to Compare).
func CompareWithDialect(desired *schemamodel.Database, current *catalog.Database, dialect string) *difftypes.SchemaDiff {
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	return CompareWithOptions(desired, current, opts)
}

// CompareSchemas diffs two in-memory desired-schema documents. Both sides are
// desired-schema documents: current names the side treated as the existing
// state, and the diff plans what would turn it into desired. The current side
// goes through the same conversion the file-to-file schema diff uses before
// the comparison runs under the supplied dialect. For a current state read
// from a live database, use CompareWithDatabase instead.
func CompareSchemas(desired, current *schemamodel.Database, dialect string) *difftypes.SchemaDiff {
	return CompareWithDialect(desired, goschematodb.ToCatalog(current, dialect), dialect)
}

// CompareWithDatabaseInfo compares using caller-supplied database metadata.
// SQL Server callers should prefer CompareWithDatabase, which resolves the
// complete candidate identifier set under the live catalog collation. A
// non-zero identifier snapshot must be valid, cover every compared identifier,
// and admit no target identifier collisions.
func CompareWithDatabaseInfo(
	desired *schemamodel.Database,
	current *catalog.Database,
	info catalog.ServerInfo,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, error) {
	diff, _, err := compareWithDatabaseInfoReportingUndecidedAdditions(
		desired, current, info, opts,
	)
	return diff, err
}

func compareWithDatabaseInfoReportingUndecidedAdditions(
	desired *schemamodel.Database,
	current *catalog.Database,
	info catalog.ServerInfo,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, []coverage.Object, error) {
	merged := config.DefaultCompareOptions()
	if opts != nil {
		*merged = *opts
		merged.IgnoredExtensions = slices.Clone(opts.IgnoredExtensions)
	}
	merged.Dialect = info.Dialect
	// Projected here as well as in the funnel below, because the refusals
	// between this line and that one read the desired state directly. A role
	// scoped away from this target must not be checked against this target's
	// reserved names: it is not being declared here at all. The projection is
	// idempotent, so applying it twice is the same schema.
	desired = schemamodel.ScopeToDialect(desired, info.Dialect)
	// Resolved before any of the validations below can return, so a malformed
	// drop toggle is reported on every SQLite comparison rather than only the
	// ones that get far enough to classify a virtual table (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidateToggle(info.Dialect); err != nil {
		return nil, nil, err
	}
	// A reserved PostgreSQL role is in neither catalog.Database.Roles nor
	// catalog.Database.RolesOutOfScope, so comparing it would read it as absent and
	// plan a CREATE ROLE the server always refuses. Refuse the declaration
	// here instead, before anything is compared (stokaro/ptah#1312).
	if err := validateDeclaredBeforeComparison(desired, current, info); err != nil {
		return nil, nil, err
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
	if err := sqlitevirtual.ValidateComparison(info.Dialect, desired, current, virtualPolicy); err != nil {
		return nil, nil, err
	}
	desired = fromschema.AssignDefaultForeignKeyNames(desired, info.Dialect)
	semantics := info.IdentifierSemantics.Normalize(info.Dialect)
	if !info.IdentifierSemantics.IsZero() &&
		!info.IdentifierSemantics.Equal(semantics) {
		return nil, nil, fmt.Errorf(
			"%w: invalid identifier semantics snapshot",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	names := collectIdentifierNames(desired, current, semantics.DefaultSchema)
	if err := identifiervalidation.ValidateCoverage(semantics, names); err != nil {
		return nil, nil, err
	}
	if err := identifiervalidation.ValidateTarget(
		desired,
		info.Dialect,
		semantics,
	); err != nil {
		return nil, nil, err
	}
	merged.IdentifierSemantics = &semantics
	diff, undecided := CompareReportingUndecidedAdditions(desired, current, merged)
	// The half of the SQLite virtual-table guard that only the comparator can
	// answer. A table both sides name and describe differently is rebuilt by
	// the SQLite planner -- drop, recreate, copy -- which destroys a module's
	// storage as surely as a drop, and whether that will happen is this diff's
	// answer rather than anything the pre-comparison check could compute
	// without a second copy of these rules (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidatePlannedChanges(
		info.Dialect, current, diff, virtualPolicy,
	); err != nil {
		return nil, nil, err
	}
	if err := compare.ValidateMySQLFunctionDefinerReplacements(
		desired,
		current,
		diff,
		info.Dialect,
		semantics,
	); err != nil {
		return nil, nil, err
	}
	return diff, undecided, nil
}

// CompareWithOptions performs schema comparison between the desired and current
// schemas with custom configuration options.
//
// This function provides full control over the comparison process, allowing users to
// specify which extensions should be ignored, and other comparison behaviors.
//
// Parameters:
//   - desired: the schema an authoring source declared
//   - current: the schema a live database reported
//   - opts: Configuration options for comparison (can be nil for defaults)
//
// Returns a SchemaDiff containing all identified differences between the schemas.
//
// Example usage:
//
//	// Use default options (ignores "plpgsql")
//	diff := schemadiff.CompareWithOptions(desired, current, nil)
//
//	// Ignore specific extensions
//	opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
//	diff := schemadiff.CompareWithOptions(desired, current, opts)
//
//	// Don't ignore any extensions
//	opts := config.WithIgnoredExtensions()
//	diff := schemadiff.CompareWithOptions(desired, current, opts)
func CompareWithOptions(desired *schemamodel.Database, current *catalog.Database, opts *config.CompareOptions) *difftypes.SchemaDiff {
	diff, _ := CompareReportingUndecidedAdditions(desired, current, opts)
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
	desired *schemamodel.Database,
	current *catalog.Database,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, []coverage.Object) {
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}
	if opts.Dialect != "" {
		// The declared scope resolves first, so every later step -- coverage,
		// identifier validation, each per-kind comparison -- sees the desired
		// state this target actually has. An object scoped away from this
		// dialect is absent rather than reported as added, which is what makes
		// a multi-dialect schema converge: before this, `schema apply` created
		// nothing for such an object, exited 0, and the very next comparison
		// asked for it again, forever.
		// Both sides move together. Projecting only the desired state leaves
		// the database still holding a scoped-away object, which reads as
		// present in the target and absent from the declaration -- the shape of
		// a drop. See suppressScopedAway.
		omitted := schemamodel.OmissionsForDialect(desired, opts.Dialect)
		desired = schemamodel.ScopeToDialect(desired, opts.Dialect)
		desired = fromschema.AssignDefaultForeignKeyNames(desired, opts.Dialect)
		current = suppressScopedAway(current, omitted)
	}

	diff := &difftypes.SchemaDiff{}
	identifierSemantics := identifier.ForDialect(opts.Dialect)
	if opts.IdentifierSemantics != nil {
		candidate := opts.IdentifierSemantics.Normalize(opts.Dialect)
		candidateNames := collectIdentifierNames(
			desired,
			current,
			candidate.DefaultSchema,
		)
		validSnapshot := opts.IdentifierSemantics.IsZero() ||
			opts.IdentifierSemantics.Equal(candidate)
		if validSnapshot &&
			identifiervalidation.ValidateCoverage(candidate, candidateNames) == nil &&
			identifiervalidation.ValidateTarget(desired, opts.Dialect, candidate) == nil {
			identifierSemantics = candidate
		}
		storedSemantics := identifierSemantics
		diff.IdentifierSemantics = &storedSemantics
	}
	desired, current = normalizeInlineEnumsForCompare(desired, current, opts)
	desired = normalizeGeneratedColumnsForCompare(desired, opts)

	// What each side declined to describe travels with that side rather than
	// with the options, so every caller that builds options from scratch still
	// gets it. Putting it on the options is how an earlier attempt lost it:
	// four surfaces resolve a desired state independently, and one of them
	// built its compare options from a zero value (stokaro/ptah#1276).
	cov := compare.CoverageOf(desired, current)

	// Compare tables and their column structures
	compare.TablesAndColumnsWithGeneratedExpressions(
		desired,
		current,
		diff,
		opts.Dialect,
		identifierSemantics,
		cov,
		opts.GeneratedExpressions,
	)

	// Compare enum type definitions and values. The semantics carry the
	// connection's default schema, without which an `enum` block's mandatory
	// `schema = schema.public` and the reader's blanked schema read as two
	// different types (stokaro/ptah#1276).
	compare.EnumsWithSemantics(desired, current, diff, identifierSemantics)

	// Compare database index definitions
	compare.IndexesWithSemantics(
		desired, current, diff, opts.Dialect, identifierSemantics, opts.IndexExpressions)

	// Compare PostgreSQL extensions with configuration options
	compare.ExtensionsWithSemantics(desired, current, diff, opts, cov, identifierSemantics)

	// Compare PostgreSQL functions (PostgreSQL-specific feature)
	compare.FunctionsWithSemantics(desired, current, diff, opts.Dialect, identifierSemantics)

	// Compare PostgreSQL standalone sequences (PostgreSQL-specific feature)
	compare.SequencesWithSemantics(desired, current, diff, cov, identifierSemantics)

	// Compare PostgreSQL user-defined types (domains, composites, ranges)
	compare.DomainsWithSemantics(desired, current, diff, cov, identifierSemantics, opts.DomainExpressions)
	compare.CompositeTypesWithSemantics(desired, current, diff, cov, identifierSemantics)
	compare.RangesWithSemantics(desired, current, diff, cov, identifierSemantics)

	// Compare views, materialized views, and triggers
	compare.ViewsWithSemantics(desired, current, diff, opts.Dialect, identifierSemantics)
	compare.Synonyms(desired, current, diff, cov)

	// Compare TimescaleDB hypertables (PostgreSQL with the extension)
	compare.Hypertables(desired, current, diff, cov)
	compare.ContinuousAggregates(
		desired, current, diff, cov, opts.ContinuousAggregateBodies, identifierSemantics)

	// Compare SQL Server extended properties (schema, table and column scope)
	compare.ExtendedProperties(desired, current, diff, cov)
	compare.MaterializedViewsWithSemantics(desired, current, diff, opts.Dialect, identifierSemantics)
	compare.TriggersWithSemantics(desired, current, diff, identifierSemantics)

	// Compare RLS policies (PostgreSQL-specific feature)
	compare.RLSPoliciesWithSemantics(
		desired, current, diff, identifierSemantics, cov, opts.PolicyExpressions)

	// Compare RLS enabled tables (PostgreSQL-specific feature)
	compare.RLSEnabledTablesWithSemantics(desired, current, diff, identifierSemantics)

	// Compare roles (PostgreSQL-specific feature)
	compare.Roles(desired, current, diff, cov)

	// Compare role privilege grants (PostgreSQL-specific feature)
	compare.GrantsWithSemantics(desired, current, diff, identifierSemantics)

	// Compare table-level constraints (EXCLUDE, CHECK, UNIQUE, etc.)
	compare.ConstraintsWithSemantics(desired, current, diff, opts, identifierSemantics)

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
	desired *schemamodel.Database,
	current *catalog.Database,
	opts *config.CompareOptions,
) (*schemamodel.Database, *catalog.Database) {
	if desired == nil || current == nil || opts == nil || !isInlineEnumDialect(opts.Dialect) {
		return desired, current
	}

	normalizedGenerated := *desired
	normalizedGenerated.Enums = nil
	normalizedGenerated.Fields = append([]schemamodel.Field(nil), desired.Fields...)
	for i := range normalizedGenerated.Fields {
		field := &normalizedGenerated.Fields[i]
		resolveDeclaredEnumValues(field, desired.Enums)
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
			case platform.Oracle:
				field.Type = "VARCHAR2(255)"
				field.Check = oracleInlineEnumCheck(*field)
			}
		}
	}

	normalizedDatabase := *current
	normalizedDatabase.Enums = nil

	return &normalizedGenerated, &normalizedDatabase
}

func normalizeGeneratedColumnsForCompare(
	desired *schemamodel.Database,
	opts *config.CompareOptions,
) *schemamodel.Database {
	if desired == nil || opts == nil {
		return desired
	}

	defaultKind := defaultGeneratedColumnKind(platform.NormalizeDialect(opts.Dialect))
	if defaultKind == "" {
		return desired
	}
	normalizedGenerated := *desired
	normalizedGenerated.Fields = append([]schemamodel.Field(nil), desired.Fields...)
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

// resolveDeclaredEnumValues fills in the values for the other spelling of an
// enum column.
//
// A column can name its values two ways: inline on the field, or by naming an
// enum declared elsewhere. The renderer reads the second -- handleEnumTypes
// finds the enum by the column's type -- and this normalization read only the
// first, so a schema written with `//ptah:schema:enum` plus `type="status_kind"`
// rendered as the target's inline model and compared as the enum's own name.
// Nothing converged: measured on SQLite, the plan rebuilt the table into one
// whose only difference from the original was none, on every apply.
//
// Filling the values here rather than teaching every arm about the second
// spelling keeps the arms about what a dialect writes, which is what they are
// for.
func resolveDeclaredEnumValues(field *schemamodel.Field, enums []schemamodel.Enum) {
	if len(field.Enum) > 0 {
		return
	}
	for _, enum := range enums {
		if enum.Name == field.Type {
			field.Enum = enum.Values
			return
		}
	}
}

func isInlineEnumDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle:
		return true
	default:
		return false
	}
}

func sqliteInlineEnumCheck(field schemamodel.Field) string {
	return enumCheck(field)
}

func sqlServerInlineEnumCheck(field schemamodel.Field) string {
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

// oracleInlineEnumCheck spells the column the way the Oracle renderer spells the
// declaration beside it.
//
// Oracle refuses a CHECK whose spelling disagrees with the column it constrains,
// so the two have to be decided by one rule: sqlident.Ident is what the
// renderer's escapeIdentifier calls, and it is what fromschema.applyInlineEnumModel
// already uses for the same expression on the rendering side.
func oracleInlineEnumCheck(field schemamodel.Field) string {
	quoted := make([]string, 0, len(field.Enum))
	for _, value := range field.Enum {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	enumCheck := sqlident.Ident(platform.Oracle, field.Name) + " IN (" + strings.Join(quoted, ", ") + ")"
	if field.Check != "" {
		return "(" + field.Check + ") AND " + enumCheck
	}
	return enumCheck
}

func enumCheck(field schemamodel.Field) string {
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

// rowTTLTables projects a declaration's tables into the pairs
// internal/crdbttl validates.
func rowTTLTables(desired *schemamodel.Database) []crdbttl.TableTTL {
	tables := make([]crdbttl.TableTTL, 0, len(desired.Tables))
	for _, table := range desired.Tables {
		tables = append(tables, crdbttl.TableTTL{Name: table.Name, RowTTL: table.RowTTL})
	}
	return tables
}

// validateDeclaredBeforeComparison applies every refusal a declaration must
// meet before anything is compared, and returns nil when there is nothing to
// validate.
//
// They live together in one function rather than inline because each is a
// separate claim about a separate feature, and the list grows: keeping them
// here means the comparison entry point reads as its own sequence of steps
// rather than as one long guarded block.
func validateDeclaredBeforeComparison(
	desired *schemamodel.Database,
	current *catalog.Database,
	info catalog.ServerInfo,
) error {
	if desired == nil {
		return nil
	}
	if err := reservedrole.ValidateDeclared(info.Dialect, desired.Roles); err != nil {
		return err
	}
	// The same ClickHouse refusals the renderer applies, at the other entry
	// point a declaration reaches before a server does. The empty default
	// database matches the renderer's, so one set of declarations cannot be
	// accepted by a comparison and refused by a render (stokaro/ptah#1025).
	if err := clickhouserbac.ValidateDeclared(
		info.Dialect, desired.Roles, desired.Grants, "",
	); err != nil {
		return err
	}
	// A live partial revoke narrows a managed role's effective privileges
	// in a way no declaration can express, so the comparison would find
	// nothing to plan and report convergence. Refuse instead, here, where
	// an error can still travel.
	if err := clickhouserbac.ValidateLive(info.Dialect, desired, current); err != nil {
		return err
	}
	// A declared relation whose name a continuous aggregate already occupies.
	// The server would answer `relation ... already exists` halfway through
	// the script; this says which object it is (stokaro/ptah#1026).
	if err := timescale.ValidateLive(info.Dialect, desired, current); err != nil {
		return err
	}
	// The row-level TTL refusals, at the same seam and for the same reason:
	// a declaration this comparison accepts and the renderer refuses would
	// be a plan that fails halfway. info.Capabilities is the live target's,
	// so the dialect gate here answers for the server actually connected
	// rather than for a preset (stokaro/ptah#1027).
	if err := crdbttl.ValidateDeclared(
		info.Dialect, info.Capabilities, crdbttl.DeclaredIn(rowTTLTables(desired)),
	); err != nil {
		return err
	}
	if err := systemschema.ValidateDeclaredPostgresSystemSchemas(
		info.Dialect,
		desired.Schemas,
	); err != nil {
		return err
	}
	return nil
}
