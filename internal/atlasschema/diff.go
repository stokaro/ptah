package atlasschema

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/clickhouserbac"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/crdbttl"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/servertarget"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/systemschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
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
	// spells them; see [ptah.run/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [ptah.run/internal/atlassource.ResolveOptions].
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

	// DevServerDisposable is the operator's declaration that the server
	// DevURL names is the run's own; see
	// [ptah.run/internal/migrationreplay.Options.DevServerDisposable].
	DevServerDisposable bool
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
		Dialect:             dialect,
		DialectFlag:         prepared.dialectFlag,
		DevURL:              opts.DevURL,
		DevServerDisposable: opts.DevServerDisposable,
		SchemaScope:         schemaScope,
		SchemaScopeFlag:     schemaScopeFlag,
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
	var (
		report  atlasreport.SchemaDiff
		changes *difftypes.SchemaDiff
	)
	err = withResolvedDiffSources(ctx, fromSet, toSet, resolveOpts,
		func(fromState, toState atlassource.State, conn *dbschema.DatabaseConnection) error {
			var diffErr error
			report, changes, diffErr = diffResolvedStates(ctx, conn, fromState, toState, dialect, target.Capabilities, opts)
			return diffErr
		})
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	return report, changes, nil
}

// diffResolvedStates is the part of a schema diff that runs once both sides
// are resolved: scoping, the refusals, the comparison and the plan.
//
// conn is the connection the --from side was read on, while it is still open,
// or nil when the --from side is not a database or a replayed migration
// directory. With it, the comparison asks that server how it spells each
// expression the --to side declares; without it, the two are compared as
// text. See [withResolvedDiffSources] for when it is held.
func diffResolvedStates(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	fromState, toState atlassource.State,
	dialect string,
	capabilities capability.Capabilities,
	opts DiffOptions,
) (atlasreport.SchemaDiff, *difftypes.SchemaDiff, error) {
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
	// A comparison on a held connection runs the SQLite virtual-table guard
	// itself, and the guard reads the drop policy from here: without it, a
	// project that skips `drop_table` is refused for a DROP the policy deletes
	// (stokaro/ptah#1028). schema apply sets the same field.
	compareOpts.SkipTableDrops = opts.Policy.SkipDropTable
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
		to, fromSide.database, dialect, capabilities,
	); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}

	// The comparison reports what the --from document's coverage record made
	// undecidable alongside what it decided. The list is empty for every --from
	// that is a database, because only a document declares limits about itself.
	compared, undecided, err := compareDiffSides(ctx, conn, to, fromSide.database, compareOpts)
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
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

			Capabilities:         capabilities,
			ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
			OnlineAlter:          opts.Policy.OnlineAlter,
			ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
			ConcurrentIndexRefs: declaredConcurrentIndexRefs(
				opts.Policy, diff, to, fromSide.database, dialect, capabilities,
			),
		})

		if err != nil {
			return atlasreport.SchemaDiff{}, nil, fmt.Errorf("generate schema diff SQL: %w", err)
		}
	}
	return atlasreport.NewSchemaDiff(from, to, statements), diff, nil
}

// compareDiffSides runs the comparison on conn when there is one, and offline
// otherwise.
//
// Without a connection, an expression the server rewrites -- a CHECK, a policy
// clause, an index predicate, a column default -- is compared as the text each
// side carries, and a --from database or replayed directory holds the server's
// text while a --to file holds the author's. Compared offline, a diff between a
// migration directory and the schema file it was written from drops and
// re-creates every one of them (stokaro/ptah#3651).
func compareDiffSides(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	current *catalog.Database,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, []coverage.Object, error) {
	if conn == nil {
		compared, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, current, opts)
		return compared, undecided, nil
	}
	compared, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(ctx, conn, desired, current, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("compare schemas: %w", err)
	}
	return compared, undecided, nil
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

// withResolvedDiffSources resolves both sides and calls use with them. When
// the comparison has a server to ask, use runs while that server's connection
// is still open, and receives it.
//
// That is when --from is a database or a migration directory and --to is
// neither. The --from state is then what the server stored, and --to is what
// an author wrote, so the two spell a rewritten expression differently. The
// connection is the one --from was read on: its own database, or the session
// the directory was replayed on, before the replay's cleanup drops the schema.
// A docker:// dev server the replay provisioned serves the comparison too,
// because the comparison runs inside the replay.
//
// --to is resolved inside that scope, after --from, which is the order
// [resolveDiffSources] keeps. It cannot need the held server: a --to that is a
// database or a directory takes the other branch. When both sides are
// databases or directories, both hold the server's spelling and nothing is
// held; when --from is a file, there is no server behind it to ask
// (stokaro/ptah#3658).
func withResolvedDiffSources(
	ctx context.Context,
	fromSet, toSet atlassource.Set,
	opts atlassource.ResolveOptions,
	use func(fromState, toState atlassource.State, conn *dbschema.DatabaseConnection) error,
) error {
	if materializesFrom(fromSet, toSet, opts.DevURL) {
		return withMaterializedFromSource(ctx, fromSet, toSet, opts, use)
	}
	if !holdsServerForComparison(fromSet, toSet) {
		fromState, toState, err := resolveDiffSources(ctx, fromSet, toSet, opts)
		if err != nil {
			return err
		}
		return use(fromState, toState, nil)
	}
	var useErr error
	err := fromSet.ResolveHolding(ctx, opts, func(fromState atlassource.State, conn *dbschema.DatabaseConnection) error {
		toState, err := resolveDiffSource(ctx, toSet, opts)
		if err != nil {
			useErr = err
			return nil
		}
		useErr = use(fromState, toState, conn)
		return nil
	})
	if err != nil {
		return fmt.Errorf("load %s schema: %w", fromSet.Flag, err)
	}
	return useErr
}

// materializesFrom reports whether a --from declaration -- a schema file, an
// external schema program or a remote schema -- is created on the dev database
// and read back before it is compared: when --to is a database or a migration
// directory, and a dev database is given.
//
// Read as written, such a --from is the author's text on the current side of
// the comparison, and a --to read from a server holds the server's spelling of
// every expression it rewrites. A file compared with the database its own SQL
// built planned its defaults, CHECKs and policies again (stokaro/ptah#3658).
// Materialized, the --from side is what the server holds too, and the two
// compare like with like. Atlas CE materializes a --from that is not a
// database the same way.
//
// A --to declaration keeps the --from declaration as written: two documents
// spell an expression the same way when they declare the same schema, and the
// comparison already folds key columns on both. Without a dev database there
// is nowhere to create the --from side, and it is compared as written.
func materializesFrom(fromSet, toSet atlassource.Set, devURL string) bool {
	return !readsServer(fromSet.Kind) && readsServer(toSet.Kind) && strings.TrimSpace(devURL) != ""
}

// withMaterializedFromSource is [withResolvedDiffSources] for a --from that
// [materializesFrom] creates on the dev database.
//
// The declaration is loaded and validated before the dev database is touched,
// so a source that does not load fails without a reset. What the dev database
// holds after the materialization is read back as the --from state, and the
// dev database is released before --to is resolved: a --to directory replays
// on the same dev database.
//
// The read-back keeps the document's coverage record. A document that records
// `ptah:not-described` for a kind makes no claim about it, and a catalog read
// would turn the dev database's answer for that kind into one.
func withMaterializedFromSource(
	ctx context.Context,
	fromSet, toSet atlassource.Set,
	opts atlassource.ResolveOptions,
	use func(fromState, toState atlassource.State, conn *dbschema.DatabaseConnection) error,
) error {
	declared, err := resolveDiffSource(ctx, fromSet, opts)
	if err != nil {
		return err
	}
	// The strict live-catalog validation of a --to database finishes before the
	// dev database is reset, as [resolveDiffSources] keeps it for a replay.
	toState, toResolved, err := preResolveLiveDiffSource(ctx, toSet, opts)
	if err != nil {
		return err
	}
	fromState, err := materializedState(ctx, fromSet, declared, opts)
	if err != nil {
		return fmt.Errorf("load %s schema: %w", fromSet.Flag, err)
	}
	if !toResolved {
		toState, err = resolveDiffSource(ctx, toSet, opts)
		if err != nil {
			return err
		}
	}
	return use(fromState, toState, nil)
}

// materializedState creates a declared state on the dev database, reads it
// back and returns what was read, with the declaration's coverage record.
func materializedState(
	ctx context.Context,
	set atlassource.Set,
	declared atlassource.State,
	opts atlassource.ResolveOptions,
) (atlassource.State, error) {
	devURL, releaseDev, err := devdocker.Resolve(ctx, opts.DevURL, devdocker.Options{
		DeclaredDisposable: opts.DevServerDisposable,
	})
	if err != nil {
		return atlassource.State{}, err
	}
	defer releaseDev()
	devConn, err := connectInspectSource(ctx, devURL, opts.ConnectTimeout)
	if err != nil {
		return atlassource.State{}, fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(devConn)

	var state atlassource.State
	err = withMaterializedDevSchema(ctx, devConn, declared.Schema, opts.ReportIgnored,
		func(materialized *dbschema.DatabaseConnection) error {
			read, err := set.DevState(ctx, materialized, opts)
			if err != nil {
				return err
			}
			read.DB.NotDescribed = declared.Schema.NotDescribed
			read.Schema.NotDescribed = declared.Schema.NotDescribed
			if opts.ValidateInspectedSchema != nil {
				if err := opts.ValidateInspectedSchema(read.Schema); err != nil {
					return err
				}
			}
			state = read
			return nil
		})
	if err != nil {
		return atlassource.State{}, err
	}
	return state, nil
}

// resetsDevDatabase reports whether a diff resets the dev database: it
// replays a migration directory there, or materializes a --from declaration
// there. Only then can a source that aliases the dev database lose its state.
func resetsDevDatabase(fromSet, toSet atlassource.Set, devURL string) bool {
	if strings.TrimSpace(devURL) == "" {
		return false
	}
	return fromSet.Kind == atlassource.KindMigrationDir ||
		toSet.Kind == atlassource.KindMigrationDir ||
		materializesFrom(fromSet, toSet, devURL)
}

// holdsServerForComparison reports whether --from has a server behind it and
// --to does not; see [withResolvedDiffSources].
func holdsServerForComparison(fromSet, toSet atlassource.Set) bool {
	return readsServer(fromSet.Kind) && !readsServer(toSet.Kind)
}

// readsServer reports whether a source kind's state is read from a server,
// and therefore carries the server's spelling of what it holds.
func readsServer(kind atlassource.Kind) bool {
	return kind == atlassource.KindDatabase || kind == atlassource.KindMigrationDir
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
	// A diff that resets the dev database refuses a database source the dev
	// URL also names, before anything is opened. Measured on master with
	// `--from file://migrations --to $DB --dev-url $DB`: the replay's reset
	// dropped every table in the --to database, and the diff then planned to
	// create them again. Atlas CE refuses the same argv with `connected
	// database is not clean`.
	if resetsDevDatabase(fromSet, toSet, opts.DevURL) {
		if err := fromSet.EnsureDevIsolation(opts.DevURL); err != nil {
			return preparedDiffSources{}, err
		}
		if err := toSet.EnsureDevIsolation(opts.DevURL); err != nil {
			return preparedDiffSources{}, err
		}
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
		desired = dbschematogo.ConvertDBSchemaToGoSchema(filteredDatabase, "")
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
