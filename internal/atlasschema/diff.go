package atlasschema

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/clickhouserbac"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/convert/goschematodb"
	"go.5x5.cz/ptah/internal/crdbttl"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/internal/servertarget"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/internal/systemschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

type DiffOptions struct {
	FromURLs []string
	ToURLs   []string
	DevURL   string
	Exclude  []string
	// Schemas restricts both comparison sides to the named schema scopes.
	Schemas []string
	// Include restricts both comparison sides to resources matched by
	// Atlas-style include selectors.
	Include []string
	Policy  DiffPolicy
	// ProjectEnv expands env:// desired-state references in FromURLs and
	// ToURLs.
	ProjectEnv atlassource.ProjectEnv
	// ConnectTimeout bounds opening database-backed sources (including the
	// dev database) and reading their initial connection metadata. A zero
	// value leaves the caller's context deadline unchanged.
	ConnectTimeout time.Duration
	// Diagnostics receives non-fatal notices, such as unmatched --exclude
	// selectors and undecidable additions. It never receives plan output, so
	// the bytes on standard output stay unchanged.
	Diagnostics io.Writer
	// ServerVersion pins the server the plan targets, as `--server-version`
	// spells it. Empty plans against the dialect's newest preset, which is
	// what this command did before the field existed.
	//
	// The string is resolved here rather than by the caller because the
	// dialect is not known until the sources are classified: `schema diff`
	// takes its dialect from --dev-url or from a source URL, not from a flag.
	ServerVersion string
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [go.5x5.cz/ptah/internal/atlassource.ResolveOptions].
	IgnoreUnknownHCLNames bool
	// ValidateSchema applies a caller-selected policy to both fully resolved
	// authored states before comparison. Nil accepts every modeled object.
	ValidateSchema func(*schemamodel.Database) error
	// ValidateInspectedSchema replaces ValidateSchema for live database and
	// replayed migration-directory states.
	ValidateInspectedSchema func(*schemamodel.Database) error
	// ValidateLiveObject applies a caller-selected policy to supplemental
	// catalog objects in live database and replayed migration-directory sources.
	// Nil performs no supplemental catalog reads.
	ValidateLiveObject func(LiveSchemaObject) error
	// ValidateMigrationSource applies a caller-selected policy to each stable
	// migration-directory snapshot before dev-database replay.
	ValidateMigrationSource func(fs.FS) error
	// ValidateLocalSchemaSource applies a caller-selected policy to each local
	// schema path before parsing or dev-database work.
	ValidateLocalSchemaSource func(string) error
}

// DiffReportingChanges computes the Atlas schema diff between two
// desired-state sources, returning both the rendered statements and the
// structural comparison they were planned from.
//
// Either side accepts local schema files, one database URL, one migration
// directory (replayed on --dev-url), or one env:// reference. The SQL dialect
// is pinned by --dev-url first, then by --from and --to database sources;
// local files alone still require --dev-url.
//
// The structural value is the comparison AFTER the caller's DiffPolicy has been
// applied, which is the one the statements were generated from. Returning the
// comparison from before it would describe a change the statements do not make.
func DiffReportingChanges(ctx context.Context, opts DiffOptions) (atlasreport.SchemaDiff, *difftypes.SchemaDiff, error) {
	prepared, err := prepareDiffSources(opts)
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	fromSet := prepared.from
	toSet := prepared.to
	dialect := prepared.dialect

	// The first thing after the dialect is known, and deliberately ahead of
	// source resolution. Everything below can return before the comparison is
	// reached -- a source that will not load, a selection matching neither
	// side -- and resolution is not free: a migration-directory source replays
	// into the dev database. A malformed drop toggle must not survive any of
	// that unreported, nor let that work start. See stokaro/ptah#1028.
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}

	// Resolved here for the same reason, and equally early: a version naming
	// no server is the caller's typo, and refusing it before a
	// migration-directory source is replayed into the dev database is the
	// difference between a fast diagnostic and one that arrives after the
	// expensive part.
	target, err := servertarget.Resolve(dialect, opts.ServerVersion)
	if err != nil {
		// The flag name is the caller's to add: this package is reached from a
		// native verb and an Atlas-shaped one, which spell it differently.
		return atlasreport.SchemaDiff{}, nil, fmt.Errorf("invalid server version: %w", err)
	}
	if target.Note != "" && opts.Diagnostics != nil {
		fmt.Fprintf(opts.Diagnostics, "Warning: %s.\n", target.Note)
	}

	// Both sides are desired states here, so --dev-url is the only URL that can
	// limit the run to one schema.
	schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(opts.DevURL, "", "")
	resolveOpts := atlassource.ResolveOptions{
		Dialect:         dialect,
		DialectFlag:     prepared.dialectFlag,
		DevURL:          opts.DevURL,
		SchemaScope:     schemaScope,
		SchemaScopeFlag: schemaScopeFlag,
		// Both sides introspect exactly the schemas --schema asked for. Without
		// this the read is scoped to the connection default and the scope
		// projection below filters a universe that never contained the
		// requested schema, so the diff answers "synced" for a database it
		// never looked at.
		Schemas:                   opts.Schemas,
		ConnectTimeout:            opts.ConnectTimeout,
		IgnoreUnknownHCLNames:     opts.IgnoreUnknownHCLNames,
		ReportIgnored:             opts.Diagnostics,
		Vars:                      opts.Vars,
		ValidateSchema:            opts.ValidateSchema,
		ValidateInspectedSchema:   opts.ValidateInspectedSchema,
		ValidateInspectedDatabase: LiveDatabaseValidator(opts.ValidateLiveObject),
		ValidateMigrationSource:   opts.ValidateMigrationSource,
		ValidateLocalSchemaSource: opts.ValidateLocalSchemaSource,
	}
	fromState, toState, err := resolveDiffSources(ctx, fromSet, toSet, resolveOpts)
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	if err := validateDiffSystemSchemaStates(fromState, toState, dialect); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}

	defaultSchema, realmRelative := diffPatternScope(dialect, fromState, toState)
	scope := atlasfilter.Scope{
		Schemas:       opts.Schemas,
		Include:       opts.Include,
		Exclude:       opts.Exclude,
		DefaultSchema: defaultSchema,
	}
	scope.RealmRelativePatterns = realmRelative
	fromSide, toSide := scopeDiffStates(fromState, toState, scope, dialect)
	if fromSide.err != nil {
		return atlasreport.SchemaDiff{}, nil, fromSide.err
	}
	from, fromReport, fromErr := fromSide.schema, fromSide.report, fromSide.selectionErr
	if toSide.err != nil {
		return atlasreport.SchemaDiff{}, nil, toSide.err
	}
	to, toReport, toErr := toSide.schema, toSide.report, toSide.selectionErr
	applyExtensionSupportCoverage(to, fromSide.selection, toSide.selection)
	// One empty side is how a create or a drop looks. A selection that matched
	// neither side cannot answer the requested comparison, so fail instead of
	// reporting a false synced result to CI.
	if emptySelection(fromErr) && emptySelection(toErr) {
		return atlasreport.SchemaDiff{}, nil, fromErr
	}
	compareOpts := config.DefaultCompareOptions()
	compareOpts.Dialect = dialect
	// Same split as the empty --include selection above: diff previews rather
	// than executes, so it keeps its exit status and says on stderr that a
	// selector protected nothing.
	reportUnmatchedExclude(opts.Diagnostics, atlasfilter.UnmatchedAcrossStates(fromReport, toReport))

	// Same refusal the native comparison seam makes, applied here because this
	// surface reaches the comparator through the variant that returns no error.
	// A SQLite virtual table on the --from side is an object --to cannot
	// declare, so comparing them plans a DROP nobody asked for
	// (stokaro/ptah#1028).
	//
	// It gets the same diff policy that filters the diff below, because the
	// refusal is a claim about a statement and `skip drop_table` deletes that
	// statement before it is rendered.
	virtualPolicy := sqlitevirtual.Policy{SkipDropTable: opts.Policy.SkipDropTable}
	if err := sqlitevirtual.ValidateComparison(dialect, to, fromSide.database, virtualPolicy); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}

	if err := validateClickHouseRBAC(dialect, to, fromSide.database); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	if err := validateRowTTL(dialect, to); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	// The target validation the erroring comparator makes, applied here for
	// the same reason the refusals above are: this surface reaches the
	// comparator through the variant that returns no error, so a desired
	// schema this target cannot host would otherwise reach the planner
	// (stokaro/ptah#2315).
	if err := validateDesiredDiffComparison(
		to, fromSide.database, dialect, target.Capabilities,
	); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}

	// The comparison reports what the --from document's coverage record made
	// undecidable alongside what it decided. The list is empty for every --from
	// that is a database, because only a document declares limits about itself.
	compared, undecided := schemadiff.CompareReportingUndecidedAdditions(to, fromSide.database, compareOpts)
	ReportUndecidedAdditions(opts.Diagnostics, undecided, "--from", "--to")
	// Same second half the native seam applies, applied here for the same
	// reason the refusal above is: this surface reaches the comparator through
	// the variant that returns no error. A table both sides name and describe
	// differently is rebuilt by the SQLite planner, which destroys a module's
	// storage as surely as a drop (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidatePlannedChanges(
		dialect, fromSide.database, compared, virtualPolicy,
	); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}

	diff := applyDiffPolicy(compared, opts.Policy)
	var statements []string
	if diff.HasChanges() {
		statements, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, dialect, planner.Options{

			Capabilities:         target.Capabilities,
			ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
			ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
			ConcurrentIndexRefs: declaredConcurrentIndexRefs(
				opts.Policy, diff, to, fromSide.database, dialect, target.Capabilities,
			),
		})

		if err != nil {
			return atlasreport.SchemaDiff{}, nil, fmt.Errorf("generate schema diff SQL: %w", err)
		}
	}
	return atlasreport.NewSchemaDiff(from, to, statements), diff, nil
}

// Diff is DiffReportingChanges for the callers that render statements and
// nothing else.
//
// The two are one implementation on purpose. The rendered statements and the
// structural comparison are two readings of a single run, and a second
// comparison to produce the second reading is a second answer that can disagree
// with the first (stokaro/ptah#1229).
func Diff(ctx context.Context, opts DiffOptions) (atlasreport.SchemaDiff, error) {
	report, _, err := DiffReportingChanges(ctx, opts)
	return report, err
}

// validateDesiredDiffComparison collects the error-capable validation needed
// before this adapter calls the deliberately non-erroring pure comparator.
func validateDesiredDiffComparison(
	desired *schemamodel.Database,
	current *catalog.Database,
	dialect string,
	capabilities capability.Capabilities,
) error {
	if err := schemadiff.ValidateDesiredSchema(desired, catalog.ServerInfo{
		Dialect:      dialect,
		Capabilities: capabilities,
	}); err != nil {
		return err
	}
	return schemadiff.ValidateRolePasswordComparison(desired, current, dialect)
}

func validateDiffSystemSchemaStates(
	fromState, toState atlassource.State,
	dialect string,
) error {
	if err := validateDiffSystemSchemaState(fromState, dialect, "--from"); err != nil {
		return err
	}
	if err := validateDiffSystemSchemaState(toState, dialect, "--to"); err != nil {
		return err
	}
	return nil
}

func validateDiffSystemSchemaState(state atlassource.State, dialect, flag string) error {
	// Database and migration-directory states are introspected snapshots. Their
	// schema lists describe server namespaces; a migration directory's authored
	// SQL has already been executed and validated by replay before this point.
	if state.DB != nil {
		for _, schema := range state.DB.Schemas {
			if !systemschema.IsPostgresFamilySystemSchema(dialect, schema.Name) {
				continue
			}
			return fmt.Errorf("validate %s database schema: %w", flag, &ptaherr.PlanError{
				Err: ptaherr.ErrInvalidSchemaDiff,
				Message: fmt.Sprintf(
					"observed server-owned PostgreSQL schema %q cannot be compared safely; its catalog objects are not migration-managed state",
					schema.Name,
				),
			})
		}
		return nil
	}
	if err := systemschema.ValidateDeclaredPostgresSystemSchemas(
		dialect,
		state.Schema.Schemas,
	); err != nil {
		return fmt.Errorf("validate %s schema: %w", flag, err)
	}
	return nil
}

func resolveDiffSources(
	ctx context.Context,
	fromSet, toSet atlassource.Set,
	opts atlassource.ResolveOptions,
) (fromState, toState atlassource.State, err error) {
	// Strict live-catalog validation must finish before either side can replay
	// and reset the dev database. Full/default mode supplies a nil callback and
	// retains the established left-to-right resolution order.
	fromState, fromResolved, err := preResolveLiveDiffSource(ctx, fromSet, opts)
	if err != nil {
		return atlassource.State{}, atlassource.State{}, err
	}
	toState, toResolved, err := preResolveLiveDiffSource(ctx, toSet, opts)
	if err != nil {
		return atlassource.State{}, atlassource.State{}, err
	}

	if !fromResolved {
		fromState, err = resolveDiffSource(ctx, fromSet, opts)
		if err != nil {
			return atlassource.State{}, atlassource.State{}, err
		}
	}
	if !toResolved {
		toState, err = resolveDiffSource(ctx, toSet, opts)
		if err != nil {
			return atlassource.State{}, atlassource.State{}, err
		}
	}
	return fromState, toState, nil
}

func preResolveLiveDiffSource(
	ctx context.Context,
	set atlassource.Set,
	opts atlassource.ResolveOptions,
) (atlassource.State, bool, error) {
	if opts.ValidateInspectedDatabase == nil || set.Kind != atlassource.KindDatabase {
		return atlassource.State{}, false, nil
	}
	state, err := resolveDiffSource(ctx, set, opts)
	return state, true, err
}

func resolveDiffSource(
	ctx context.Context,
	set atlassource.Set,
	opts atlassource.ResolveOptions,
) (atlassource.State, error) {
	state, err := set.Resolve(ctx, opts)
	if err != nil {
		return atlassource.State{}, fmt.Errorf("load %s schema: %w", set.Flag, err)
	}
	return state, nil
}

type preparedDiffSources struct {
	from        atlassource.Set
	to          atlassource.Set
	dialect     string
	dialectFlag string
}

func prepareDiffSources(opts DiffOptions) (preparedDiffSources, error) {
	fromSet, err := atlassource.ClassifySet("--from", opts.FromURLs, opts.ProjectEnv)
	if err != nil {
		return preparedDiffSources{}, err
	}
	toSet, err := atlassource.ClassifySet("--to", opts.ToURLs, opts.ProjectEnv)
	if err != nil {
		return preparedDiffSources{}, err
	}
	if err := fromSet.EnsureDevDatabase(opts.DevURL); err != nil {
		return preparedDiffSources{}, err
	}
	if err := toSet.EnsureDevDatabase(opts.DevURL); err != nil {
		return preparedDiffSources{}, err
	}
	// Validate both local sides before resolving either one. In particular, a
	// refused --to must not be preceded by opening a database-backed --from.
	if err := fromSet.ValidateLocalSchemaSources(opts.ValidateLocalSchemaSource); err != nil {
		return preparedDiffSources{}, fmt.Errorf("load --from schema: %w", err)
	}
	if err := toSet.ValidateLocalSchemaSources(opts.ValidateLocalSchemaSource); err != nil {
		return preparedDiffSources{}, fmt.Errorf("load --to schema: %w", err)
	}
	fromSet, err = fromSet.PrepareMigrationSource(opts.ValidateMigrationSource)
	if err != nil {
		return preparedDiffSources{}, fmt.Errorf("load --from schema: %w", err)
	}
	toSet, err = toSet.PrepareMigrationSource(opts.ValidateMigrationSource)
	if err != nil {
		return preparedDiffSources{}, fmt.Errorf("load --to schema: %w", err)
	}
	dialect, dialectFlag, err := atlassource.PinDialect(opts.DevURL, fromSet, toSet)
	if err != nil {
		return preparedDiffSources{}, err
	}
	if dialect == "" {
		return preparedDiffSources{}, fmt.Errorf("--dev-url is required for local schema file diffing")
	}
	return preparedDiffSources{
		from:        fromSet,
		to:          toSet,
		dialect:     dialect,
		dialectFlag: dialectFlag,
	}, nil
}

type scopedDiffState struct {
	schema       *schemamodel.Database
	database     *catalog.Database
	report       atlasfilter.ExcludeReport
	selection    atlasfilter.SelectionReport
	selectionErr error
	err          error
}

// scopeDiffStates projects both original comparison sides and repeats both
// projections in extension-support mode when either side matched a
// non-extension resource. The second pass is deliberately pair-wide: a match
// can exist on only one side while an unselected extension already exists on
// both, and filtering the other side would manufacture a change.
func scopeDiffStates(
	fromState, toState atlassource.State,
	scope atlasfilter.Scope,
	dialect string,
) (fromSide, toSide scopedDiffState) {
	fromSide = scopeDiffState(fromState, scope, "--from schema", dialect)
	toSide = scopeDiffState(toState, scope, "--to schema", dialect)
	if fromSide.err != nil || toSide.err != nil {
		return fromSide, toSide
	}
	supportScope, changed := extensionSupportScope(scope, fromSide.selection, toSide.selection)
	if !changed {
		return fromSide, toSide
	}
	return scopeDiffState(fromState, supportScope, "--from schema", dialect),
		scopeDiffState(toState, supportScope, "--to schema", dialect)
}

// scopeDiffState projects one resolved comparison side without asking either
// of its representations to answer questions it cannot answer. Generated
// schema owns desired SQL and cross-scope dependency validation. For a
// database-backed source, catalog state owns selector match truth, exclusion
// reporting, and the current-side comparison. The generated projection keeps
// the desired dependency closure and every selectable identity.
func scopeDiffState(
	state atlassource.State,
	scope atlasfilter.Scope,
	side,
	dialect string,
) scopedDiffState {
	desired, generatedReports, generatedErr := scopeGeneratedSide(state.Schema, scope, side)
	if generatedErr != nil && !emptySelection(generatedErr) {
		return scopedDiffState{report: generatedReports.Exclude, err: generatedErr}
	}
	if state.DB == nil {
		return scopedDiffState{
			schema:       desired,
			database:     goschematodb.ToDBSchema(desired, dialect),
			report:       generatedReports.Exclude,
			selection:    generatedReports.Selection,
			selectionErr: generatedErr,
		}
	}

	filteredDatabase, databaseReports, databaseErr := scopeDatabaseSide(state.DB, scope, side)
	if databaseErr != nil && !emptySelection(databaseErr) {
		return scopedDiffState{report: databaseReports.Exclude, err: databaseErr}
	}
	if !scope.Positive() {
		return scopedDiffState{
			schema:       desired,
			database:     filteredDatabase,
			report:       databaseReports.Exclude,
			selection:    databaseReports.Selection,
			selectionErr: databaseErr,
		}
	}

	// An authoritative miss must not be turned back into a match by a lossy
	// conversion. Positive matches need no catalog compensation because every
	// independently selectable identity survives conversion.
	if emptySelection(databaseErr) {
		desired = dbschematogo.ConvertDBSchemaToGoSchema(filteredDatabase)
	}

	return scopedDiffState{
		schema:       desired,
		database:     filteredDatabase,
		report:       databaseReports.Exclude,
		selection:    databaseReports.Selection,
		selectionErr: databaseErr,
	}
}

// diffPatternScope resolves both halves of an exclude pattern's scope for one
// diff: the schema that owns unqualified objects, and whether the run describes
// a whole realm.
//
// Both sides must share one default, so a database-backed side pins it
// (--from first), and local-file-only diffs fall back to the dialect default.
// The realm answer comes from the SAME side, deliberately: a diff that counted
// a pattern against one state's schema while taking the other state's idea of
// what the run describes would refuse a pattern for a scope neither side has.
func diffPatternScope(dialect string, fromState, toState atlassource.State) (defaultSchema string, realmRelative bool) {
	if fromState.DefaultSchema != "" {
		return fromState.DefaultSchema, fromState.RealmScoped
	}
	if toState.DefaultSchema != "" {
		return toState.DefaultSchema, toState.RealmScoped
	}
	// Neither side connected to anything, so nothing named a scope. The
	// dialect's own default owns unqualified objects, and a pattern stays
	// relative to it -- which is what a run over two local files has always
	// done.
	return dialectDefaultSchema(dialect), false
}

// validateClickHouseRBAC applies the ClickHouse role and grant refusals to a
// `schema diff`, and returns nil for every other dialect.
//
// It is here for the reason sqlitevirtual.ValidateComparison is: this surface
// reaches the comparator through the variant that returns no error, so a
// refusal this seam does not make is one nothing makes. Rendering the diff's
// SQL does reach internal/clickhouserbac, which covers a diff that plans
// something — but a diff that plans nothing renders nothing, and without this
// call `ptah-compat schema diff` answered exit 0 with no changes for a
// declaration every other surface refuses (stokaro/ptah#1025).
//
// The empty default database matches the native seam's, so one set of
// declarations cannot be accepted by one surface and refused by the other.
func validateClickHouseRBAC(dialect string, to *schemamodel.Database, from *catalog.Database) error {
	if to == nil {
		return nil
	}
	if err := clickhouserbac.ValidateDeclared(dialect, to.Roles, to.Grants, ""); err != nil {
		return err
	}
	return clickhouserbac.ValidateLive(dialect, to, from)
}

// validateRowTTL applies the CockroachDB row-level TTL refusals to a
// `schema diff`, for the reason validateClickHouseRBAC exists: this surface
// reaches the comparator through the variant that returns no error, so a
// refusal it does not make is one nothing makes on this path
// (stokaro/ptah#1027).
//
// The capability set is resolved from the dialect rather than from a live
// connection, because either side of a `schema diff` may be a document and
// there may be no server at all. That resolves to the dialect's newest preset,
// which is the same answer `schema render` gives an offline declaration.
func validateRowTTL(dialect string, to *schemamodel.Database) error {
	if to == nil {
		return nil
	}
	tables := make([]crdbttl.TableTTL, 0, len(to.Tables))
	for _, table := range to.Tables {
		tables = append(tables, crdbttl.TableTTL{Name: table.Name, RowTTL: table.RowTTL})
	}
	return crdbttl.ValidateDeclared(dialect, capability.ForDialect(dialect), crdbttl.DeclaredIn(tables))
}
