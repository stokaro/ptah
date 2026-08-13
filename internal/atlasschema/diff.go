package atlasschema

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"time"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
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
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [go.5x5.cz/ptah/internal/atlassource.ResolveOptions].
	IgnoreUnknownHCLNames bool
	// ValidateSchema applies a caller-selected policy to both fully resolved
	// authored states before comparison. Nil accepts every modeled object.
	ValidateSchema func(*goschema.Database) error
	// ValidateInspectedSchema replaces ValidateSchema for live database and
	// replayed migration-directory states.
	ValidateInspectedSchema func(*goschema.Database) error
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

// Diff computes the Atlas schema diff between two desired-state sources.
// Either side accepts local schema files, one database URL, one migration
// directory (replayed on --dev-url), or one env:// reference. The SQL dialect
// is pinned by --dev-url first, then by --from and --to database sources;
// local files alone still require --dev-url.
func Diff(ctx context.Context, opts DiffOptions) (atlasreport.SchemaDiff, error) {
	prepared, err := prepareDiffSources(opts)
	if err != nil {
		return atlasreport.SchemaDiff{}, err
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
		return atlasreport.SchemaDiff{}, err
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
		Vars:                      opts.Vars,
		ValidateSchema:            opts.ValidateSchema,
		ValidateInspectedSchema:   opts.ValidateInspectedSchema,
		ValidateInspectedDatabase: LiveDatabaseValidator(opts.ValidateLiveObject),
		ValidateMigrationSource:   opts.ValidateMigrationSource,
		ValidateLocalSchemaSource: opts.ValidateLocalSchemaSource,
	}
	fromState, toState, err := resolveDiffSources(ctx, fromSet, toSet, resolveOpts)
	if err != nil {
		return atlasreport.SchemaDiff{}, err
	}
	if err := validateDiffSystemSchemaStates(fromState, toState, dialect); err != nil {
		return atlasreport.SchemaDiff{}, err
	}

	scope := atlasfilter.Scope{
		Schemas:       opts.Schemas,
		Include:       opts.Include,
		Exclude:       opts.Exclude,
		DefaultSchema: diffDefaultSchema(dialect, fromState, toState),
	}
	fromSide, toSide := scopeDiffStates(fromState, toState, scope, dialect)
	if fromSide.err != nil {
		return atlasreport.SchemaDiff{}, fromSide.err
	}
	from, fromReport, fromErr := fromSide.schema, fromSide.report, fromSide.selectionErr
	if toSide.err != nil {
		return atlasreport.SchemaDiff{}, toSide.err
	}
	to, toReport, toErr := toSide.schema, toSide.report, toSide.selectionErr
	applyExtensionSupportCoverage(to, fromSide.selection, toSide.selection)
	// One empty side is how a create or a drop looks. A selection that matched
	// neither side cannot answer the requested comparison, so fail instead of
	// reporting a false synced result to CI.
	if emptySelection(fromErr) && emptySelection(toErr) {
		return atlasreport.SchemaDiff{}, fromErr
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
	if err := sqlitevirtual.ValidateComparison(dialect, to, fromSide.database); err != nil {
		return atlasreport.SchemaDiff{}, err
	}

	// The comparison reports what the --from document's coverage record made
	// undecidable alongside what it decided. The list is empty for every --from
	// that is a database, because only a document declares limits about itself.
	compared, undecided := schemadiff.CompareReportingUndecidedAdditions(to, fromSide.database, compareOpts)
	ReportUndecidedAdditions(opts.Diagnostics, undecided, "--from", "--to")

	diff := applyDiffPolicy(compared, opts.Policy)
	var statements []string
	if diff.HasChanges() {
		statements, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, to, dialect, planner.Options{
			ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
			ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
		})
		if err != nil {
			return atlasreport.SchemaDiff{}, fmt.Errorf("generate schema diff SQL: %w", err)
		}
	}
	return atlasreport.NewSchemaDiff(from, to, statements), nil
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
			if !schemaselection.IsPostgresFamilySystemSchema(dialect, schema.Name) {
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
	if err := schemaselection.ValidateDeclaredPostgresSystemSchemas(
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
	schema       *goschema.Database
	database     *types.DBSchema
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
	side string,
	dialect string,
) scopedDiffState {
	generated, generatedReports, generatedErr := scopeGeneratedSide(state.Schema, scope, side)
	if generatedErr != nil && !emptySelection(generatedErr) {
		return scopedDiffState{report: generatedReports.Exclude, err: generatedErr}
	}
	if state.DB == nil {
		return scopedDiffState{
			schema:       generated,
			database:     schemafile.ToDBSchema(generated, dialect),
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
			schema:       generated,
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
		generated = dbschematogo.ConvertDBSchemaToGoSchema(filteredDatabase)
	}

	return scopedDiffState{
		schema:       generated,
		database:     filteredDatabase,
		report:       databaseReports.Exclude,
		selection:    databaseReports.Selection,
		selectionErr: databaseErr,
	}
}

// diffDefaultSchema resolves the schema that owns unqualified objects for one
// diff. Both sides must share one default, so a database-backed side pins it
// (--from first), and local-file-only diffs fall back to the dialect default.
func diffDefaultSchema(dialect string, fromState, toState atlassource.State) string {
	if fromState.DefaultSchema != "" {
		return fromState.DefaultSchema
	}
	if toState.DefaultSchema != "" {
		return toState.DefaultSchema
	}
	return dialectDefaultSchema(dialect)
}
