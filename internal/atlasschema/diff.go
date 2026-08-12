package atlasschema

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"time"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemafile"
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

	scope := atlasfilter.Scope{
		Schemas:       opts.Schemas,
		Include:       opts.Include,
		Exclude:       opts.Exclude,
		DefaultSchema: diffDefaultSchema(dialect, fromState, toState),
	}
	fromSide := scopeDiffState(fromState, scope, "--from schema", dialect)
	if fromSide.err != nil {
		return atlasreport.SchemaDiff{}, fromSide.err
	}
	from, fromReport, fromErr := fromSide.schema, fromSide.report, fromSide.selectionErr
	toSide := scopeDiffState(toState, scope, "--to schema", dialect)
	if toSide.err != nil {
		return atlasreport.SchemaDiff{}, toSide.err
	}
	to, toReport, toErr := toSide.schema, toSide.report, toSide.selectionErr
	// One empty side is how a create or a drop looks. A selection that matched
	// neither side cannot answer the requested comparison, so fail instead of
	// reporting a false synced result to CI.
	if emptySelection(fromErr) && emptySelection(toErr) {
		return atlasreport.SchemaDiff{}, fromErr
	}
	compareOpts := config.DefaultCompareOptions()
	compareOpts.Dialect = dialect
	if toState.DB != nil {
		if err := validateDesiredExtensionSchemas(
			fromSide.database,
			toSide.database,
			scope.DefaultSchema,
			compareOpts,
		); err != nil {
			return atlasreport.SchemaDiff{}, err
		}
	}
	// Same split as the empty --include selection above: diff previews rather
	// than executes, so it keeps its exit status and says on stderr that a
	// selector protected nothing.
	reportUnmatchedExclude(opts.Diagnostics, atlasfilter.UnmatchedAcrossStates(fromReport, toReport))

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
	selectionErr error
	err          error
}

// scopeDiffState projects one resolved comparison side without asking either
// of its representations to answer questions it cannot answer. Generated
// schema owns desired SQL and cross-scope dependency validation. For a
// database-backed source, catalog state owns selector match truth, exclusion
// reporting, and the current-side comparison. Catalog compensation is merged
// only after the generated projection validates, and only for identities the
// conversion cannot represent during selection, so it cannot reintroduce
// unrelated support objects or replace generated dependency closure.
func scopeDiffState(
	state atlassource.State,
	scope atlasfilter.Scope,
	side string,
	dialect string,
) scopedDiffState {
	generated, generatedReport, generatedErr := scopeGeneratedSide(state.Schema, scope, side)
	if generatedErr != nil && !emptySelection(generatedErr) {
		return scopedDiffState{report: generatedReport, err: generatedErr}
	}
	if state.DB == nil {
		return scopedDiffState{
			schema:       generated,
			database:     schemafile.ToDBSchema(generated, dialect),
			report:       generatedReport,
			selectionErr: generatedErr,
		}
	}

	filteredDatabase, databaseReport, databaseErr := scopeDatabaseSide(state.DB, scope, side)
	if databaseErr != nil && !emptySelection(databaseErr) {
		return scopedDiffState{report: databaseReport, err: databaseErr}
	}
	if !scope.Positive() {
		return scopedDiffState{
			schema:       generated,
			database:     filteredDatabase,
			report:       databaseReport,
			selectionErr: databaseErr,
		}
	}

	// A database match can be invisible after conversion, as with an extension
	// installed outside the connection's default schema. Restore only the
	// extension identities missing from the already-validated generated
	// projection. Merging the whole catalog projection would also restore
	// support objects retained from lossy type metadata, even when the selector
	// did not name them. Conversely, an authoritative miss must not be turned
	// back into a match by a lossy conversion.
	if emptySelection(databaseErr) {
		generated = dbschematogo.ConvertDBSchemaToGoSchema(filteredDatabase)
	} else if compensation := catalogIdentityCompensation(generated, filteredDatabase); compensation != nil {
		merged, err := goschema.Merge(generated, compensation)
		if err != nil {
			return scopedDiffState{
				report: databaseReport,
				err:    fmt.Errorf("merge scoped %s representations: %w", side, err),
			}
		}
		merged.NotDescribed = generated.NotDescribed
		generated = merged
	}

	return scopedDiffState{
		schema:       generated,
		database:     filteredDatabase,
		report:       databaseReport,
		selectionErr: databaseErr,
	}
}

// catalogIdentityCompensation returns only identities whose qualified catalog
// selector cannot survive the DBSchema-to-goschema conversion. Extensions are
// database-scoped in goschema, so conversion drops their installation schema;
// a qualified selector can therefore match the catalog while missing the
// generated projection. Other selected catalog objects are deliberately not
// copied here: the generated projection remains authoritative for their
// dependency closure and desired SQL.
func catalogIdentityCompensation(
	generated *goschema.Database,
	filtered *types.DBSchema,
) *goschema.Database {
	missing := make([]types.DBExtension, 0, len(filtered.Extensions))
	for _, extension := range filtered.Extensions {
		found := false
		for _, present := range generated.Extensions {
			if present.Name == extension.Name {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, extension)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return dbschematogo.ConvertDBSchemaToGoSchema(&types.DBSchema{Extensions: missing})
}

// validateDesiredExtensionSchemas refuses a live desired placement that the
// generated model cannot render faithfully. Extension creation in the default
// schema and schema-independent drops remain representable. A non-default
// create or a placement change would otherwise render an unqualified CREATE
// EXTENSION, or report two different placements as synced.
func validateDesiredExtensionSchemas(
	from, to *types.DBSchema,
	defaultSchema string,
	compareOpts *config.CompareOptions,
) error {
	current := make(map[string]string, len(from.Extensions))
	for _, extension := range from.Extensions {
		if compareOpts.IsExtensionIgnored(extension.Name) {
			continue
		}
		current[extension.Name] = effectiveExtensionSchema(extension.Schema, defaultSchema)
	}
	for _, extension := range to.Extensions {
		if compareOpts.IsExtensionIgnored(extension.Name) {
			continue
		}
		desiredSchema := effectiveExtensionSchema(extension.Schema, defaultSchema)
		currentSchema, exists := current[extension.Name]
		switch {
		case exists && currentSchema != desiredSchema:
			return fmt.Errorf(
				"cannot move extension %q from schema %q to schema %q: schema diff cannot represent PostgreSQL extension installation schemas",
				extension.Name,
				currentSchema,
				desiredSchema,
			)
		case !exists && desiredSchema != defaultSchema:
			return fmt.Errorf(
				"cannot create extension %q in schema %q: schema diff cannot represent PostgreSQL extension installation schemas",
				extension.Name,
				desiredSchema,
			)
		}
	}
	return nil
}

func effectiveExtensionSchema(schema, defaultSchema string) string {
	if schema == "" {
		return defaultSchema
	}
	return schema
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
