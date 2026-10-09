package atlasschema

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clickhouserbac"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/devlock"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/servertarget"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/systemschema"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

type DiffOptions struct {
	// Runtime selects feature services and codecs for this operation. It is required.
	Runtime  engine.SchemaRuntime
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
	// OmitNullBackfill plans SET NOT NULL without filling the column's NULL
	// rows from its declared default first; see
	// [ptah.run/migration/planner.Options]. The caller selects it from its
	// compatibility policy.
	OmitNullBackfill bool
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

	// CheckDevDatabase, when set, judges the dev database once both sides are
	// classified and every local source has passed validation, and before any
	// database is opened or any source resolved. An error from it ends the
	// diff. It receives both sides because whether a diff uses the dev
	// database at all depends on what they are.
	CheckDevDatabase func(ctx context.Context, from, to atlassource.Set) error
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
	if err := schemaext.RequireRuntime(ctx, opts.Runtime); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
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

	if opts.CheckDevDatabase != nil {
		if err := opts.CheckDevDatabase(ctx, fromSet, toSet); err != nil {
			return atlasreport.SchemaDiff{}, nil, err
		}
	}

	// Both sides are desired states here, so --dev-url is the only URL that can
	// limit the run to one schema.
	schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(opts.DevURL, "", "")
	resolveOpts := atlassource.ResolveOptions{
		Runtime:     opts.Runtime,
		DatabaseURL: sourceDatabaseURL(opts.DevURL, fromSet, toSet),
		Dialect:     dialect,
		DialectFlag: prepared.dialectFlag,
		DevURL:      opts.DevURL,
		// A database on either side is compared live with the dev database
		// before the dev database is reset for the other side.
		Protected:           append(fromSet.DevProtected(), toSet.DevProtected()...),
		DevServerDisposable: opts.DevServerDisposable,
		SchemaScope:         schemaScope,
		SchemaScopeFlag:     schemaScopeFlag,
		// Both sides introspect exactly the schemas --schema asked for. Without
		// this the read is scoped to the connection default and the scope
		// projection below filters a universe that never contained the
		// requested schema, so the diff answers "synced" for a database it
		// never looked at.
		Schemas: opts.Schemas,
		// Two databases are compared as they are, a MySQL-family server as
		// the whole server, and so is a database beside a dev server, which
		// compares whole servers too (stokaro/ptah#3885). Beside a dev
		// database, a file or a directory is one database, and so is the
		// database side.
		ServerScope: (fromSet.Kind == atlassource.KindDatabase && toSet.Kind == atlassource.KindDatabase) ||
			isDevServer(opts.DevURL),
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
	// Two documents compared as written hold no connection, so the server
	// behind --dev-url is asked once for what it is.
	var documentCaps capability.Capabilities
	if opts.ServerVersion == "" && !materializesFrom(fromSet, toSet, opts.DevURL) &&
		!holdsServerForComparison(fromSet, toSet) {
		documentCaps = devServerCapabilities(ctx, dialect, opts.DevURL, opts.ConnectTimeout)
	}
	err = withResolvedDiffSources(ctx, opts.Runtime, fromSet, toSet, resolveOpts,
		func(fromState, toState atlassource.State, conn *dbschema.DatabaseConnection) error {
			// A side read from the dev database holds that database's own
			// extensions, which neither source declared, and the starting
			// point a docker block gave it.
			fromState, toState = fromState.WithoutEnvironment(toState.ExtensionNames()),
				toState.WithoutEnvironment(fromState.ExtensionNames())
			originalFrom := fromState
			var err error
			fromState, err = fromState.WithoutStartingPoint(ctx, toState, dialect, opts.Runtime)
			if err != nil {
				return err
			}
			toState, err = toState.WithoutStartingPoint(ctx, originalFrom, dialect, opts.Runtime)
			if err != nil {
				return err
			}
			var diffErr error
			report, changes, diffErr = diffResolvedStates(ctx, conn, fromState, toState, dialect,
				diffCapabilities(target, opts.ServerVersion, conn, documentCaps), devServerSidesOf(opts.DevURL, fromSet, toSet), opts)
			return diffErr
		})
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	return report, changes, nil
}

// diffCapabilities is the capability set a schema diff plans with.
//
// A --server-version pins it. Without one, a diff that holds a connection --
// the --from or --to database, or the dev database a file or directory is
// replayed on -- plans for that server, as schema apply and migrate diff plan
// for the server they connect to. Measured on PostgreSQL 18.6, planning for
// the dialect default refused a NOT ENFORCED CHECK as unavailable on the
// server that holds it, and reported a NOT NULL rename as unavailable too
// (stokaro/ptah#3910, stokaro/ptah#3936). Only a diff with no connection at
// all keeps the dialect default. Two documents compared as written hold no
// connection, and plan for the dev server instead; see
// [devServerCapabilities], which answers documentCaps.
func diffCapabilities(
	target servertarget.Target,
	version string,
	conn *dbschema.DatabaseConnection,
	documentCaps capability.Capabilities,
) capability.Capabilities {
	switch {
	case version != "":
		return target.Capabilities
	case conn != nil:
		return conn.Info().Capabilities
	case documentCaps != nil:
		return documentCaps
	default:
		return target.Capabilities
	}
}

// devServerCapabilities answers what the server behind a dev URL can do, for a
// diff of two documents that never opens it otherwise, or nil where the dialect
// default stands.
//
// A docker:// URL names its server in the image tag, `docker://postgres/18`,
// which is read without starting the container; a tag no release line
// matches, such as `latest`, answers nil. Any other URL is opened and asked,
// as the comparison of a file with a database asks the database. Two
// documents are compared as written, and the dev database is not otherwise
// opened, so one that cannot be opened within devVersionTimeout answers nil:
// the comparison it would have informed runs as it does with no dev database,
// and a declaration the default cannot plan is refused by name there.
func devServerCapabilities(ctx context.Context, dialect, devURL string, timeout time.Duration) capability.Capabilities {
	devURL = strings.TrimSpace(devURL)
	if devURL == "" {
		return nil
	}
	if devdocker.IsURL(devURL) {
		spec, err := devdocker.Parse(devURL)
		if err != nil {
			return nil
		}
		_, tag, tagged := strings.Cut(spec.Image, ":")
		resolution := capability.ResolveServerVersion(dialect, tag)
		if !tagged || !resolution.Recognized {
			return nil
		}
		return resolution.Capabilities
	}
	if atlasurl.NamesAFile(devURL) {
		// A SQLite file has no server to ask: the engine is the one compiled
		// into Ptah, which the dialect's default preset describes. Opening the
		// file to ask creates it, so without this `--dev-url sqlite://dev.db`
		// on a diff of two files leaves an empty dev.db in the working
		// directory.
		return nil
	}
	if timeout <= 0 || timeout > devVersionTimeout {
		timeout = devVersionTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := dbschema.ConnectToServer(ctx, devURL)
	if err != nil {
		slog.Debug("could not open --dev-url for its server version; planning for the dialect default",
			"dialect", dialect, "error", err)
		return nil
	}
	defer func() { _ = conn.Close() }()
	return conn.Info().Capabilities
}

// devVersionTimeout bounds the one connection a diff of two documents makes to
// learn the dev server's version, which nothing else in that diff waits for.
const devVersionTimeout = 5 * time.Second

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
	sides devServerSides,
	opts DiffOptions,
) (atlasreport.SchemaDiff, *difftypes.SchemaDiff, error) {
	// The dev-server scope comes first: it refuses what CE refuses on a dev
	// server, in CE's words, and narrows a one-schema document to the one
	// database. The one-database rule then reads the narrowed sides.
	fromState, toState, err := scopeOnDevServer(fromState, toState, sides)
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	if err := validateResolvedDiffScope(fromState, toState, dialect); err != nil {
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
	fromSide, toSide := scopeDiffStates(ctx, fromState, toState, scope, dialect, opts.Runtime)
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

	if err := setYDBDiffRoot(dialect, opts.DevURL, fromSide.database); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	compareOpts := config.DefaultCompareOptions()
	compareOpts.Dialect = dialect
	// Two whole MySQL-family servers are compared database by database too,
	// as the pinned community binary v1.3.0 compares them: measured on MySQL
	// 8.4.11 and MariaDB 11.8.9, `schema diff` between two servers plans
	// CREATE DATABASE and DROP DATABASE (stokaro/ptah#3789).
	compareOpts.ServerSchemas = comparesTwoServers(fromState, toState)
	// A comparison on a held connection runs the SQLite virtual-table guard
	// itself, and the guard reads the drop policy from here: without it, a
	// project that skips `drop_table` is refused for a DROP the policy deletes
	// (stokaro/ptah#1028). schema apply sets the same field.
	compareOpts.SkipTableDrops = opts.Policy.SkipDropTable
	// Same split as the empty --include selection above: diff previews rather
	// than executes, so it keeps its exit status and says on stderr that a
	// selector protected nothing.
	reportUnmatchedExclude(opts.Diagnostics, atlasfilter.UnmatchedAcrossStates(fromReport, toReport))

	// Offline comparisons have no live target metadata, so this adapter applies
	// the target validation that the connected comparison performs.
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
	// Without a held connection, validate using the selected target's offline
	// facts before the declaration reaches planning (stokaro/ptah#2315).
	// A held connection validates the same
	// schema with the identifier semantics that server resolves. Run here
	// instead, the validation would use the offline rules, and on SQL Server
	// those keep no two names apart: every table with two columns would be
	// refused (stokaro/ptah#4122).
	if conn == nil {
		if err := validateDesiredDiffComparison(
			ctx, opts.Runtime, to, fromSide.database, dialect, capabilities,
		); err != nil {
			return atlasreport.SchemaDiff{}, nil, err
		}
	}

	// Both documents and live reads may carry knowledge limits. Preserve them
	// beside the established changes; silence cannot establish agreement.
	rotated, err := withSecretRotation(to, opts.Policy)
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	compared, undecided, err := compareDiffSides(ctx, conn, rotated, fromSide.database, compareOpts, opts.Runtime)
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	if opts.Diagnostics == nil && !undecided.Empty() {
		return atlasreport.SchemaDiff{}, nil, undecided.Err()
	}
	undecidednote.Report(opts.Diagnostics, undecided, "--from", "--to")
	// Apply the planned-change half of target validation to offline comparisons
	// as well. A table both sides name and describe
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
		statements, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(
			ctx, opts.Runtime,
			diff, dialect, planner.Options{

				Capabilities:         capabilities,
				ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
				OnlineAlter:          opts.Policy.OnlineAlter,
				ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
				ConcurrentIndexRefs: declaredConcurrentIndexRefs(
					opts.Policy, diff, to, fromSide.database, dialect, capabilities,
				),
				OmitNullBackfill:    opts.OmitNullBackfill,
				AllowTableRebuild:   opts.Policy.AllowTableRebuild,
				TableRebuildRequest: opts.Policy.TableRebuildRequest,
			},
		)

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
	runtime schemadiff.DatabaseRuntime,
) (*difftypes.SchemaDiff, schemadiff.Diagnostics, error) {
	if conn == nil {
		return schemadiff.CompareReportingUndecidedAdditions(ctx, desired, current, opts, runtime)
	}
	compared, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(ctx, conn, desired, current, opts, runtime)
	if err != nil {
		return nil, schemadiff.Diagnostics{}, fmt.Errorf("compare schemas: %w", err)
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

// validateDesiredDiffComparison checks the target declaration before the
// document comparator, which does not otherwise validate target capabilities.
func validateDesiredDiffComparison(
	ctx context.Context,
	service schemavalidation.Runtime,
	desired *schemamodel.Database,
	current *catalog.Database,
	dialect string,
	capabilities capability.Capabilities,
) error {
	if err := schemadiff.ValidateDesiredSchema(ctx, service, desired, catalog.ServerInfo{
		Dialect:      dialect,
		Capabilities: capabilities,
	}); err != nil {
		return err
	}
	selected, err := service.ResolveTarget(dialect)
	if err != nil {
		return err
	}
	return schemadiff.ValidateRolePasswordComparison(desired, current, selected)
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
	service renderer.SchemaService,
	fromSet, toSet atlassource.Set,
	opts atlassource.ResolveOptions,
	use func(fromState, toState atlassource.State, conn *dbschema.DatabaseConnection) error,
) error {
	if materializesFrom(fromSet, toSet, opts.DevURL) {
		return withMaterializedFromSource(ctx, service, fromSet, toSet, opts, use)
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
	service renderer.SchemaService,
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
	fromState, err := materializedState(ctx, service, fromSet, declared, opts)
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
	service renderer.SchemaService,
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
	// A dev server takes the declaration database by database, and the state
	// read back from it is the whole server.
	devConn, err := connectInspectSource(ctx, devURL, opts.ConnectTimeout, dbschema.ConnectToServer)
	if err != nil {
		return atlassource.State{}, fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(devConn)
	// The materialization resets the dev database before it creates the
	// declaration there.
	if err := devlock.EnsureDistinct(ctx, devConn, opts.Protected...); err != nil {
		return atlassource.State{}, err
	}

	var state atlassource.State
	err = withMaterializedDevSchema(ctx, service, devConn, declared.Schema, opts.ReportIgnored,
		func(materialized *dbschema.DatabaseConnection, baseline devclean.Baseline) error {
			read, err := set.DevState(ctx, materialized, opts)
			if err != nil {
				return err
			}
			read.EnvironmentExtensions = baseline.Extensions()
			read.EnvironmentState = baseline.StartingPoint()
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
	ctx context.Context,
	fromState, toState atlassource.State,
	scope atlasfilter.Scope,
	dialect string,
	runtime goschematodb.Runtime,
) (fromSide, toSide scopedDiffState) {
	fromSide = scopeDiffState(ctx, fromState, scope, "--from schema", dialect, runtime)
	toSide = scopeDiffState(ctx, toState, scope, "--to schema", dialect, runtime)
	if fromSide.err != nil || toSide.err != nil {
		return fromSide, toSide
	}
	supportScope, changed := extensionSupportScope(scope, fromSide.selection, toSide.selection)
	if !changed {
		return fromSide, toSide
	}
	return scopeDiffState(ctx, fromState, supportScope, "--from schema", dialect, runtime),
		scopeDiffState(ctx, toState, supportScope, "--to schema", dialect, runtime)
}

// scopeDiffState projects one resolved comparison side without asking either
// of its representations to answer questions it cannot answer. Generated
// schema owns desired SQL and cross-scope dependency validation. For a
// database-backed source, catalog state owns selector match truth, exclusion
// reporting, and the current-side comparison. The generated projection keeps
// the desired dependency closure and every selectable identity.
func scopeDiffState(
	ctx context.Context,
	state atlassource.State,
	scope atlasfilter.Scope,
	side,
	dialect string,
	runtime goschematodb.Runtime,
) scopedDiffState {
	desired, generatedReports, generatedErr := scopeGeneratedSide(state.Schema, scope, side)
	if generatedErr != nil && !emptySelection(generatedErr) {
		return scopedDiffState{report: generatedReports.Exclude, err: generatedErr}
	}
	if state.DB == nil {
		database, err := goschematodb.ToDBSchema(ctx, desired, dialect, runtime)
		if err != nil {
			return scopedDiffState{err: err}
		}
		return scopedDiffState{
			schema:       desired,
			database:     database,
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
		var err error
		desired, err = dbschematogo.ConvertDBSchemaToGoSchema(ctx, filteredDatabase, dialect, runtime)
		if err != nil {
			return scopedDiffState{err: err}
		}
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

// comparesTwoServers reports whether both sides of a diff were read from a
// whole MySQL or MariaDB server, a connection that selected no database.
func comparesTwoServers(fromState, toState atlassource.State) bool {
	return fromState.WholeServer && toState.WholeServer
}

// A live side identifies the database the other side describes. When both
// sides are files, only an explicit dev URL supplies that context.
func sourceDatabaseURL(devURL string, sets ...atlassource.Set) string {
	for _, set := range sets {
		if set.Kind == atlassource.KindDatabase && len(set.Sources) == 1 {
			return set.Sources[0].Raw
		}
	}
	return devURL
}

// Two file sources carry no live catalog root. The explicit dev URL still
// supplies the path needed to render database permissions; retain it only
// as comparison context, never as part of the portable desired schema.
func setYDBDiffRoot(dialect, devURL string, current *catalog.Database) error {
	if dialect != platform.YDB || current.DatabasePath != "" || devURL == "" {
		return nil
	}
	root, err := schemafile.YDBSourceRoot(devURL)
	if err != nil {
		return fmt.Errorf("YDB diff requires a valid database or Docker dev URL")
	}
	current.DatabasePath = root
	return nil
}

// validateResolvedDiffScope applies scope refusals after dev-server projection.
func validateResolvedDiffScope(fromState, toState atlassource.State, dialect string) error {
	if err := refuseOutsideComparedDatabase(dialect, fromState, toState); err != nil {
		return err
	}
	if err := refuseServerScopeMismatch(stateSide(fromState), stateSide(toState)); err != nil {
		return err
	}
	if err := validateDiffSystemSchemaStates(fromState, toState, dialect); err != nil {
		return err
	}

	return nil
}
