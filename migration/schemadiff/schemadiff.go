package schemadiff

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/clickhouserbac"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/crdbttl"
	"ptah.run/internal/foreignkeyscope"
	"ptah.run/internal/reservedrole"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/systemschema"
	"ptah.run/internal/timescale"
	"ptah.run/migration/internal/identifiervalidation"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// Compare performs schema comparison between a desired schema and an
// introspected database schema using default options
// (config.DefaultCompareOptions, which ignores the "plpgsql" extension).
//
// The comparison never returns an error and reads no database: current is
// whatever snapshot the caller supplies. It is dialect-neutral -- no dialect
// scoping and no dialect-specific normalization runs -- so prefer
// CompareWithDialect when the target dialect is known, or CompareWithDatabase
// when a live connection can also answer identifier semantics. The output
// does not vary between runs and does not depend on the order the inputs list
// their objects, so two comparisons of the same two states produce the same
// diff. For custom configuration, use CompareWithOptions.
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
	return CompareWithDialect(desired, goschematodb.ToDBSchema(current, dialect), dialect)
}

// CompareWithDatabaseInfo compares using caller-supplied database metadata.
// SQL Server callers should prefer CompareWithDatabase, which resolves the
// complete candidate identifier set under the live catalog collation.
//
// info.Dialect selects the comparison dialect, overriding opts.Dialect. A
// non-zero info.IdentifierSemantics snapshot must be valid, cover every
// compared identifier, and admit no target identifier collisions; a snapshot
// failing any of those checks is refused with an error satisfying
// errors.Is(err, ptaherr.ErrInvalidSchemaDiff), where CompareWithOptions
// would silently fall back to the dialect's offline rules.
//
// Unlike the pure entry points, this variant also validates the declaration
// against the target before comparing, and returns an error instead of a diff
// that plans a statement the server would refuse. Which rules apply is
// dialect-specific and grows with the dialects; a reserved PostgreSQL role
// name and SQLite's virtual-table rules are two of them. One of those checks
// is a promise in its own right: PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP is
// resolved on every SQLite comparison, so a malformed value is reported
// whether or not this comparison reaches a virtual table.
func CompareWithDatabaseInfo(
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, error) {
	diff, _, err := compareWithDatabaseInfoReportingUndecidedAdditions(
		desired, database, info, opts,
	)
	return diff, err
}

func compareWithDatabaseInfoReportingUndecidedAdditions(
	desired *schemamodel.Database,
	database *catalog.Database,
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
	// A reserved PostgreSQL role is in neither Database.Roles nor
	// Database.RolesOutOfScope, so comparing it would read it as absent and
	// plan a CREATE ROLE the server always refuses. Refuse the declaration
	// here instead, before anything is compared (stokaro/ptah#1312).
	if err := validateDeclaredBeforeComparison(desired, database, info); err != nil {
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
	if err := sqlitevirtual.ValidateComparison(info.Dialect, desired, database, virtualPolicy); err != nil {
		return nil, nil, err
	}
	desired = schemaprep.AssignDefaultForeignKeyNames(desired, info.Dialect)
	semantics := info.IdentifierSemantics.Normalize(info.Dialect)
	if !info.IdentifierSemantics.IsZero() &&
		!info.IdentifierSemantics.Equal(semantics) {
		return nil, nil, fmt.Errorf(
			"%w: invalid identifier semantics snapshot",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	names := collectIdentifierNames(desired, database, semantics.DefaultSchema)
	if err := identifiervalidation.ValidateCoverage(semantics, names); err != nil {
		return nil, nil, err
	}
	if err := ValidateDesiredSchema(desired, info); err != nil {
		return nil, nil, err
	}
	if err := ValidateRolePasswordComparison(desired, database, info.Dialect); err != nil {
		return nil, nil, err
	}
	merged.IdentifierSemantics = &semantics
	// A connection to a whole MySQL-family server compares its databases as
	// well as what is in them. Only the connection says it is one: a server
	// read and a read of one database both describe tables, and only the
	// first describes the databases (stokaro/ptah#3789).
	merged.ServerSchemas = merged.ServerSchemas || info.WholeServer
	if merged.ServerSchemas && isMySQLFamilyComparison(info.Dialect) {
		if err := compare.RequireServerDatabases(desired); err != nil {
			return nil, nil, err
		}
	}
	diff, undecided := compareReportingUndecidedAdditions(desired, database, merged, info.Capabilities, info.DefaultIntSize)
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
		desired,
		database,
		diff,
		info.Dialect,
		semantics,
	); err != nil {
		return nil, nil, err
	}
	return diff, undecided, nil
}

// CompareWithOptions performs schema comparison between a desired schema and an
// introspected database schema with custom configuration options.
//
// A nil opts selects config.DefaultCompareOptions (which ignores the "plpgsql"
// extension). The comparison never returns an error and reads no database.
//
// Setting opts.Dialect selects more than normalization rules. The desired
// state is first projected to that target -- an object whose declaration is
// scoped to other dialects is absent rather than reported as added -- and the
// matching scoped-away objects are suppressed on the current side too, so a
// multi-dialect declaration converges instead of re-planning (or dropping) the
// same objects forever. Default foreign-key names are also assigned under the
// dialect, the way the renderer would assign them. All of that preparation
// works on a copy: neither argument is mutated, here or on any other entry
// point in this package.
//
// A non-nil opts.IdentifierSemantics is honored only when it is a valid
// resolved snapshot that covers every compared identifier and admits no
// target identifier collisions. A snapshot failing any of those checks is
// silently discarded here and the dialect's conservative offline rules apply;
// use CompareWithDatabaseInfo to have an invalid snapshot refused with an
// error instead.
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
	database *catalog.Database,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, []coverage.Object) {
	return compareReportingUndecidedAdditions(desired, database, opts, nil, 0)
}

// compareReportingUndecidedAdditions is [CompareReportingUndecidedAdditions]
// with the target's capabilities, which decide what the comparison can expect
// a read to report. Nil takes the dialect's default preset, which is all an
// offline comparison has; a live one passes what the server's version
// resolved to, because two releases of one engine can answer differently.
// defaultIntSize is [catalog.ServerInfo.DefaultIntSize], 0 where no connection
// read it.
func compareReportingUndecidedAdditions(
	desired *schemamodel.Database,
	database *catalog.Database,
	opts *config.CompareOptions,
	caps capability.Capabilities,
	defaultIntSize int,
) (*difftypes.SchemaDiff, []coverage.Object) {
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}
	if len(caps) == 0 {
		caps = comparisonCapabilities(opts.Dialect)
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
		desired = schemaprep.AssignDefaultForeignKeyNames(desired, opts.Dialect)
		// A UNIQUE constraint is a unique index on YDB, which is what the
		// reader reports for one a plan applied.
		desired = schemaprep.UniqueConstraintsAsIndexesFor(desired, opts.Dialect, caps)
		// A YDB privilege spelled as GRANT does is the permission name the
		// reader reports.
		desired = schemaprep.YDBPermissionNamesFor(desired, opts.Dialect)
		database = suppressScopedAway(database, omitted)
	}

	diff := &difftypes.SchemaDiff{}
	identifierSemantics := identifier.ForDialect(opts.Dialect)
	if opts.IdentifierSemantics != nil {
		candidate := opts.IdentifierSemantics.Normalize(opts.Dialect)
		candidateNames := collectIdentifierNames(
			desired,
			database,
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
	desired, database = normalizeInlineEnumsForCompare(desired, database, opts)
	desired = normalizeGeneratedColumnsForCompare(desired, opts)
	desired = compare.AdoptUndescribedChangefeeds(desired, database, opts.Dialect, identifierSemantics)
	desired = compare.AdoptHeldColumnFamilies(desired, database, opts.Dialect, identifierSemantics)
	desired = compare.AdoptUndescribedRowDeletionPolicies(desired, database, opts.Dialect, identifierSemantics)

	// What each side declined to describe travels with that side rather than
	// with the options, so every caller that builds options from scratch still
	// gets it. Putting it on the options is how an earlier attempt lost it:
	// four surfaces resolve a desired state independently, and one of them
	// built its compare options from a zero value (stokaro/ptah#1276).
	cov := compare.CoverageOf(desired, database)

	// Compare the databases of a whole MySQL-family server, before what is in
	// them; see [config.CompareOptions.ServerSchemas].
	if opts.ServerSchemas && isMySQLFamilyComparison(opts.Dialect) {
		compare.ServerSchemas(diff, desired, database.Schemas, identifierSemantics)
	}

	// Compare tables and their column structures
	compare.TablesAndColumnsWithServerSpellings(
		desired,
		database,
		diff,
		opts.Dialect,
		identifierSemantics,
		cov,
		compare.ServerSpellings{
			Generated:      opts.GeneratedExpressions,
			Columns:        opts.ColumnSpellings,
			DefaultIntSize: defaultIntSize,
		},
		caps,
	)

	// Compare enum type definitions and values. The semantics carry the
	// connection's default schema, without which an `enum` block's mandatory
	// `schema = schema.public` and the reader's blanked schema read as two
	// different types (stokaro/ptah#1276).
	compare.EnumsWithSemantics(desired, database, diff, identifierSemantics)

	// Compare database index definitions
	compare.IndexesWithSemantics(
		desired, database, diff, opts.Dialect, identifierSemantics, opts.IndexExpressions, caps,
	)

	// Compare PostgreSQL extensions with configuration options
	compare.ExtensionsWithSemantics(desired, database, diff, opts, cov, identifierSemantics)

	// Compare PostgreSQL functions (PostgreSQL-specific feature)
	compare.FunctionsWithSemantics(desired, database, diff, opts.Dialect, identifierSemantics, opts.RoutineArguments)

	// Compare PostgreSQL standalone sequences (PostgreSQL-specific feature)
	compare.SequencesWithSemantics(desired, database, diff, cov, identifierSemantics)

	// Compare PostgreSQL user-defined types (domains, composites, ranges)
	compare.DomainsWithSemantics(desired, database, diff, cov, identifierSemantics, opts.DomainExpressions)
	compare.CompositeTypesWithSemantics(desired, database, diff, cov, identifierSemantics)
	compare.RangesWithSemantics(desired, database, diff, cov, identifierSemantics)

	// Compare views, materialized views, and triggers
	compare.ViewsWithSemantics(desired, database, diff, opts.Dialect, identifierSemantics, opts.ViewBodies)
	compare.Synonyms(desired, database, diff, cov)
	compare.Topics(desired, database, diff, cov)

	// Compare YDB coordination nodes
	compare.CoordinationNodes(desired, database, diff, cov)

	// Compare TimescaleDB hypertables (PostgreSQL with the extension)
	compare.Hypertables(desired, database, diff, cov)
	compare.ContinuousAggregates(
		desired, database, diff, cov, opts.ContinuousAggregateBodies, identifierSemantics,
	)

	// Compare SQL Server extended properties (schema, table and column scope)
	compare.ExtendedProperties(desired, database, diff, cov)
	compare.MaterializedViewsWithSemantics(desired, database, diff, opts.Dialect, identifierSemantics, opts.ViewBodies)
	compare.TriggersWithSemanticsAndConditions(desired, database, diff, identifierSemantics, opts.TriggerConditions, caps)

	// Compare RLS policies (PostgreSQL-specific feature)
	compare.RLSPoliciesWithSemantics(
		desired, database, diff, identifierSemantics, opts.Dialect, cov, opts.PolicyExpressions,
	)

	// Compare RLS enabled tables (PostgreSQL-specific feature)
	compare.RLSEnabledTablesWithSemantics(desired, database, diff, identifierSemantics, opts.Dialect)

	// Compare roles (PostgreSQL-specific feature)
	compare.Roles(desired, database, diff, cov)

	// Compare the membership of each role in another, where a planner plans
	// one.
	compare.RoleMemberships(desired, database, diff, caps)

	// Compare role privilege grants (PostgreSQL-specific feature)
	compare.GrantsWithSemantics(desired, database, diff, identifierSemantics)

	// Compare default privileges (PostgreSQL-specific feature)
	compare.DefaultPrivilegesWithSemantics(desired, database, diff, identifierSemantics, cov)

	// Compare table-level constraints (EXCLUDE, CHECK, UNIQUE, etc.)
	compare.ConstraintsWithSemantics(desired, database, diff, opts, identifierSemantics)
	compare.ConstraintComments(desired, database, diff, opts, caps, identifierSemantics)

	// The declaration of every table those constraints name, for a target that
	// rebuilds a table to change one. It is filled here rather than beside the
	// other carries because it reads the constraint lists, which the call above
	// is what fills (stokaro/ptah#2315).
	diff.DeclaredConstraintHosts = difftypes.ConstraintHostDeclarationsOf(
		desired, diff.ConstraintsAdded, diff.ConstraintsRemoved, identifierSemantics,
	)
	// What the read of the database declined to describe, for a target that
	// rebuilds a table and must not drop a setting nobody compared.
	diff.CurrentNotDescribed = database.NotDescribed
	// Where the read happened, for the statements YDB takes only with an
	// absolute path, and every grant it reported, for a plan that recreates a
	// table and must give the table its grants back.
	diff.CurrentDatabasePath = database.DatabasePath
	diff.CurrentGrants = compare.CurrentGrants(database)

	// Comments on the objects that take theirs through a statement of its
	// own, compared only where the target stores and reports them.
	compare.ObjectComments(desired, database, diff, opts, caps, identifierSemantics)

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

// comparisonCapabilities is the capability set a comparison without a live
// server assumes: the dialect's default preset, and PostgreSQL's where the
// dialect is not named, which is what every comparator here takes an unnamed
// dialect to mean.
func comparisonCapabilities(dialect string) capability.Capabilities {
	if dialect == "" {
		return capability.ForDialect(platform.Postgres)
	}
	return capability.ForDialect(dialect)
}

func normalizeInlineEnumsForCompare(
	desired *schemamodel.Database,
	database *catalog.Database,
	opts *config.CompareOptions,
) (*schemamodel.Database, *catalog.Database) {
	if desired == nil || database == nil || opts == nil || !isInlineEnumDialect(opts.Dialect) {
		return desired, database
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

	normalizedDatabase := *database
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
	case platform.YDB:
		// YDB has no enum, inline or standalone, and the renderer and the
		// planner refuse one; it is classed with the standalone dialects to
		// agree with schemaprep.EmitsStandaloneEnumDefinitions, which keeps
		// the enum a declaration of its own for that refusal to meet.
		return false
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
// renderer's escapeIdentifier calls, and it is what modelast.applyInlineEnumModel
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
	database *catalog.Database,
	info catalog.ServerInfo,
) error {
	if desired == nil {
		return nil
	}
	// The server's own default schema, where a bare table name lands: a
	// search path set to another schema makes `accounts` and
	// `public.accounts` two tables, and the static default would call them one.
	defaultSchema := info.IdentifierSemantics.Normalize(info.Dialect).DefaultSchema
	if err := schemaprep.ValidateTableSpellings(desired, defaultSchema); err != nil {
		return err
	}
	if err := reservedrole.ValidateDeclared(info.Dialect, desired.Roles); err != nil {
		return err
	}
	// On a target where the grant option belongs to the grantee at the whole
	// object, two declarations disagreeing on it for the same role and object
	// would plan a REVOKE and a GRANT that undo each other forever. Refuse the
	// pair here, before anything is compared (stokaro/ptah-operator#481).
	if err := schemamodel.ValidateGrantOptionConsistency(desired, info.Dialect); err != nil {
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
	if err := clickhouserbac.ValidateLive(info.Dialect, desired, database); err != nil {
		return err
	}
	// A declared relation whose name a continuous aggregate already occupies.
	// The server would answer `relation ... already exists` halfway through
	// the script; this says which object it is (stokaro/ptah#1026).
	if err := timescale.ValidateLive(info.Dialect, desired, database); err != nil {
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
	return validateKeysIntoComparedSchemas(desired, database, info.Dialect)
}

// validateKeysIntoComparedSchemas refuses a foreign key into a schema the
// desired schema holds nothing of, when the database it is compared with holds
// that schema.
//
// The renderer writes such a key as declared, because a description of one
// schema cannot hold a table of another. A comparison is the exception: when
// the database read covers the other schema, the desired schema saying nothing
// of it asks for everything there to be dropped, the referenced table
// included, and the plan would drop a table while adding a key into it. So the
// key is refused as the renderer refuses a reference to a table the
// description does not hold. Measured on PostgreSQL 18.6: `schema apply` of a
// file declaring only public's tables, against a URL covering every schema,
// planned `ALTER TABLE "crm"."customers" DROP CONSTRAINT IF EXISTS
// "customers_pkey"`, and the server refused it with SQLSTATE 2BP01
// (stokaro/ptah#3906).
func validateKeysIntoComparedSchemas(desired *schemamodel.Database, database *catalog.Database, dialect string) error {
	if database == nil {
		return nil
	}
	held := make(map[string]bool, len(database.Schemas)+len(database.Tables))
	for _, schema := range database.Schemas {
		held[schema.Name] = true
	}
	for _, table := range database.Tables {
		held[strings.TrimSpace(table.Schema)] = true
	}
	for _, reference := range foreignkeyscope.References(*desired) {
		schema, outside := foreignkeyscope.Outside(*desired, dialect, reference.Table)
		if outside && held[schema] {
			return &ptaherr.RenderError{
				Dialect: dialect,
				Err:     ptaherr.ErrInvalidSchemaDiff,
				Message: fmt.Sprintf(
					"invalid foreign key: %s references unknown table %q: the database compared holds schema %q "+
						"and the desired schema declares nothing of it",
					reference.Declaration, reference.Table, schema,
				),
			}
		}
	}
	return nil
}

// ValidateDesiredSchema refuses a desired schema this target cannot be planned
// against, before anything is compared.
//
// Two rules are asked, and both need the whole declaration rather than a set of
// changes. Identifier validation answers questions between declarations -- an
// index name used twice, a foreign key whose referenced columns are not unique,
// two names one collation folds together -- and the renderer's validation
// answers whether this target can host what the document declares at all. A
// plan reads only the diff, so neither can run there: a conflict between two
// unchanged tables is invisible to a change set that names neither
// (stokaro/ptah#2315).
//
// It is exported because the comparison is reachable through variants that
// return no error, and a surface that takes one of those has to make the same
// refusal itself. Giving both ends one predicate is what keeps them from
// drifting apart: the alternative is two lists of rules that agree when the
// second is written and stop agreeing when the first is extended.
//
// Render and plan share this validation, which is what stokaro/ptah#1717 asks
// for -- `schema render` reaches it through the renderer directly, and the plan
// pipeline reaches it through the comparison that feeds the planner.
func ValidateDesiredSchema(desired *schemamodel.Database, info catalog.ServerInfo) error {
	if desired == nil {
		return nil
	}
	// Both projections are idempotent, so a caller that already applied them
	// gets the same schema back. A declaration this dialect was not given is
	// not part of its desired state, and a foreign key without a name is one
	// the plan would name the same way.
	scoped := schemaprep.AssignDefaultForeignKeyNames(
		schemamodel.ScopeToDialect(desired, info.Dialect),
		info.Dialect,
	)
	if err := identifiervalidation.ValidateTarget(
		scoped,
		info.Dialect,
		info.IdentifierSemantics.Normalize(info.Dialect),
	); err != nil {
		return err
	}
	caps := info.Capabilities
	if len(caps) == 0 {
		caps = capability.ForDialect(info.Dialect)
	}
	// A column does not carry the schema of the user type it names, only the
	// declaration does (stokaro/ptah#1138).
	return renderer.ValidateSchemaWithCapabilities(
		schemaprep.QualifyDeclaredUserTypes(scoped, info.Dialect),
		info.Dialect,
		caps,
	)
}

// isMySQLFamilyComparison reports whether dialect names MySQL or MariaDB, the
// dialects whose schemas are databases.
func isMySQLFamilyComparison(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}
