package schemadiff

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaproperties"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/clickhouserbac"
	"ptah.run/internal/foreignkeyscope"
	"ptah.run/internal/reservedrole"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/systemschema"
	"ptah.run/internal/timescale"
	"ptah.run/migration/internal/identifiervalidation"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// CompareWithOptions compares schema snapshots using the selected services and
// options. A nil options value selects config.DefaultCompareOptions. Inputs are
// not mutated. Invalid identifier snapshots and incomplete knowledge are errors.
// Nil schema inputs return ptaherr.ErrInvalidSchemaDiff; an empty schema must
// be supplied explicitly and retain the source's knowledge limits.
func CompareWithOptions(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, opts *config.CompareOptions, runtime schemapreparation.Runtime) (*difftypes.SchemaDiff, error) {
	return completeComparison(CompareReportingUndecidedAdditions(ctx, desired, current, opts, runtime))
}

// CompareReportingUndecidedAdditions returns the established changes and all
// undecided common or feature state. Diagnostics are separate from executable
// changes. A caller rendering a partial diff must also report its knowledge
// limits. Service errors and cancellation discard the complete result.
// Nil schemas are missing inputs, not empty declarations or catalogs.
func CompareReportingUndecidedAdditions(
	ctx context.Context,
	desired *schemamodel.Database,
	database *catalog.Database,
	opts *config.CompareOptions,
	runtime schemapreparation.Runtime,
) (*difftypes.SchemaDiff, Diagnostics, error) {
	return compareReportingUndecidedAdditions(ctx, desired, database, opts, nil, 0, runtime)
}

// compareReportingUndecidedAdditions is [CompareReportingUndecidedAdditions]
// with the target's capabilities, which decide what the comparison can expect
// a read to report. Nil takes the dialect's default preset, which is all an
// offline comparison has; a live one passes what the server's version
// resolved to, because two releases of one engine can answer differently.
// defaultIntSize is [catalog.ServerInfo.DefaultIntSize], 0 where no connection
// read it.
func compareReportingUndecidedAdditions(
	ctx context.Context,
	desired *schemamodel.Database,
	database *catalog.Database,
	opts *config.CompareOptions,
	caps capability.Capabilities,
	defaultIntSize int,
	runtime schemapreparation.Runtime,
) (*difftypes.SchemaDiff, Diagnostics, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, Diagnostics{}, err
	}
	if desired == nil || database == nil {
		return nil, Diagnostics{}, fmt.Errorf("%w: comparison requires desired and observed schemas", ptaherr.ErrInvalidSchemaDiff)
	}
	opts, selected, err := selectedComparisonOptions(opts, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	if len(caps) == 0 {
		caps = comparisonCapabilities(opts.Dialect)
	}
	if opts.Dialect != "" {
		var err error
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
		desired, database, err = scopeComparison(desired, database, selected)
		if err != nil {
			return nil, Diagnostics{}, err
		}
		desired, err = decodeSourceProperties(ctx, desired, selected.Name(), runtime)
		if err != nil {
			return nil, Diagnostics{}, err
		}
		desired = schemaprep.AssignDefaultForeignKeyNames(desired, opts.Dialect)
		// A UNIQUE constraint is a unique index on YDB, which is what the
		// reader reports for one a plan applied.
		desired = schemaprep.UniqueConstraintsAsIndexesFor(desired, opts.Dialect, caps)
		// A YDB privilege spelled as GRANT does is the permission name the
		// reader reports.
		desired = schemaprep.YDBPermissionNamesFor(desired, opts.Dialect)
	}

	diff := &difftypes.SchemaDiff{}
	identity, err := comparisonTableIdentities(desired, database, opts)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	desired, database = identity.desired, identity.current
	identifierSemantics := identity.semantics
	if opts.IdentifierSemantics != nil {
		stored := identifierSemantics.Clone()
		diff.IdentifierSemantics = &stored
	}
	desired, database = normalizeInlineEnumsForCompare(desired, database, opts)
	desired = normalizeGeneratedColumnsForCompare(desired, opts)
	desired, err = compare.AdoptTransferConsumers(desired, database, opts.Dialect, identifierSemantics)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	desired = compare.AdoptUndescribedColumnTables(desired, database, opts.Dialect, identifierSemantics)
	desired = compare.AdoptHeldColumnFamilies(desired, database, opts.Dialect, identifierSemantics)
	desired = compare.AdoptUndescribedRowDeletionPolicies(desired, database, opts.Dialect, identifierSemantics)
	prepared, err := prepareComparisonTables(ctx, desired, database, opts.Dialect, identifierSemantics, caps, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	desired, diff.TablePreparation = prepared.desired, prepared.capture

	featureResult, err := compareFeatures(ctx, desired, database, opts.Dialect, identifierSemantics, caps, prepared.parents, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	desired, err = effectiveFeatureState(desired, featureResult.Desired, opts.Dialect, identifierSemantics)
	if err != nil {
		return nil, Diagnostics{}, err
	}

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
	compare.TablesAndColumnsWithTableContext(
		desired,
		database,
		diff,
		opts.Dialect,
		identifierSemantics,
		cov,
		compare.TableContext{
			Prepared:       prepared.bySubject,
			Generated:      opts.GeneratedExpressions,
			Columns:        opts.ColumnSpellings,
			DefaultIntSize: defaultIntSize,
		},
		caps,
	)

	if err := attachFeatureChanges(diff, desired, database, comparisonChanges(featureResult), opts.Dialect, identifierSemantics); err != nil {
		return nil, Diagnostics{}, err
	}

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
	compare.Replications(desired, database, diff, cov)

	compare.Secrets(desired, database, diff, cov)
	compare.ExternalObjects(desired, database, diff, cov)

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
	diff.DeclaredConstraintHosts = constraintHostDeclarations(desired, diff, identifierSemantics)
	diff.ObservedConstraintHosts = constraintHostObservations(database, diff.DeclaredConstraintHosts, opts.Dialect, identifierSemantics)
	// What the read of the database declined to describe, for a target that
	// rebuilds a table and must not drop a setting nobody compared.
	diff.CurrentNotDescribed = database.NotDescribed
	// Where the read happened, for the statements YDB takes only with an
	// absolute path, every grant it reported, for a plan that recreates a
	// table and must give the table its grants back, and every YDB table's
	// settings, for the same plan to keep the ones nobody declared.
	diff.CurrentDatabasePath = database.DatabasePath
	diff.CurrentGrants = compare.CurrentGrants(database)
	diff.CurrentYDBSettings = compare.CurrentYDBSettings(database)

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

	if err := ctx.Err(); err != nil {
		return nil, Diagnostics{}, err
	}
	return diff, Diagnostics{Common: undecided, Features: featureResult.Undecided}, nil
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
// Identifier validation and selected target validation need the whole
// declaration rather than a set of changes. Identifier validation checks an
// index name used twice, a foreign key whose referenced columns are not unique,
// two names one collation folds together. The selected validator answers
// whether this target can host what the document declares. A
// plan reads only the diff, so neither can run there: a conflict between two
// unchanged tables is invisible to a change set that names neither
// (stokaro/ptah#2315).
//
// Adapters that compare documents without database-aware comparison can call
// this validation explicitly. It uses the same selected service as comparison.
//
// The built-in validator shares checks with schema rendering. The comparison
// invokes the selected service before the diff reaches the planner, so a target
// refusal prevents planning even when no changed object exposes the conflict.
func ValidateDesiredSchema(ctx context.Context, service schemavalidation.Runtime, desired *schemamodel.Database, info catalog.ServerInfo) error {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return err
	}
	if desired == nil {
		return fmt.Errorf("%w: cannot validate a nil database schema", ptaherr.ErrInvalidSchemaDiff)
	}
	// Both projections are idempotent, so a caller that already applied them
	// gets the same schema back. A declaration this dialect was not given is
	// not part of its desired state, and a foreign key without a name is one
	// the plan would name the same way.
	selected, err := service.ResolveTarget(info.Dialect)
	if err != nil {
		return err
	}
	info.Dialect = selected.Name()
	scoped, err := schemamodel.ScopeToTarget(desired, selected)
	if err != nil {
		return err
	}
	scoped = schemaprep.AssignDefaultForeignKeyNames(scoped, info.Dialect)
	if err := identifiervalidation.ValidateTarget(
		scoped,
		info.Dialect,
		info.IdentifierSemantics.Normalize(info.Dialect),
	); err != nil {
		return &RefusalError{cause: err}
	}
	caps := info.Capabilities
	if len(caps) == 0 {
		caps = capability.ForDialect(info.Dialect)
	}
	// A column does not carry the schema of the user type it names, only the
	// declaration does (stokaro/ptah#1138).
	target := selected.Name()
	result, err := schemavalidation.Validate(ctx, service, schemavalidation.Request{
		Target: target, Capabilities: caps, Identifiers: info.IdentifierSemantics.Normalize(info.Dialect),
		Schema: schemaprep.QualifyDeclaredUserTypes(scoped, info.Dialect),
	})
	if err != nil {
		return err
	}
	return result.Err(target)
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

// decodeSourceProperties lowers the selected target's table and index source
// properties into owned facets, after scoping and before any comparison step
// reads the desired schema.
func decodeSourceProperties(ctx context.Context, desired *schemamodel.Database, target string, runtime schemaproperties.Runtime) (*schemamodel.Database, error) {
	desired, err := schemaproperties.DecodeTables(ctx, desired, target, runtime)
	if err != nil {
		return nil, err
	}
	return schemaproperties.DecodeIndexes(ctx, desired, target, runtime)
}
