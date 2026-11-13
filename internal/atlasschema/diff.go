package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"time"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlaschange"
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
	"ptah.run/internal/pgsystemschema"
	"ptah.run/internal/planstyle"
	"ptah.run/internal/readscope"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/servertarget"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/internal/tableref"
	"ptah.run/internal/undecidednote"
	"ptah.run/internal/unmodelednote"
	"ptah.run/internal/valuecontext"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

type DiffOptions struct {
	// Runtime selects feature services and codecs for this operation. It is required.
	Runtime engine.SchemaRuntime
	// ChangeComments writes the comment that names each statement's change, the
	// Atlas contract of this verb: `-- Create "users" table`. See
	// [ptah.run/internal/atlaschange].
	ChangeComments bool
	// Shape is the statement shape of the plan; see [planext.Options.Shape].
	Shape    planext.StatementShape
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
	// RefuseUnrequestedSchemas holds an HCL desired state to Schemas; see
	// [ptah.run/internal/schemafile.Options.RequestedSchemas].
	RefuseUnrequestedSchemas bool
	// OmitNullBackfill plans SET NOT NULL without filling the column's NULL
	// rows from its declared default first; see
	// [ptah.run/migration/planext.Options]. The caller selects it from its
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

	// ToReplaysMigrations says the database ToURLs names holds a migration
	// directory the caller replayed there, as a derived rollback replays one
	// on its dev database. Its grants are the ones the migrations make, so it
	// is compared as a --to migration directory is: a grant only the --from
	// database holds is removed only from a role the replay manages. Read as
	// a whole answer, every grant of the --from database the replay does not
	// make reads as a removal, such as the grants a YDB database root gives
	// its built-in groups, which no dev realm holds.
	ToReplaysMigrations bool
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
	ctx = readscope.Without(ctx, atlasfilter.KindsLeftOut(opts.Runtime.Selections(), opts.Exclude, opts.Include)...)
	prepared, err := prepareDiffSources(ctx, opts)
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
		Protected:                append(fromSet.DevProtected(), toSet.DevProtected()...),
		DevServerDisposable:      opts.DevServerDisposable,
		SchemaScope:              schemaScope,
		SchemaScopeFlag:          schemaScopeFlag,
		RequestedSchemas:         opts.Schemas,
		RefuseUnrequestedSchemas: opts.RefuseUnrequestedSchemas,
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
		// database side. With no dev URL at all, a --from server beside a file
		// is the whole server, as `schema compare` reads it: the comparison
		// runs on that server's connection, which compares its databases.
		ServerScope: (fromSet.Kind == atlassource.KindDatabase && toSet.Kind == atlassource.KindDatabase) ||
			isDevServer(opts.DevURL) || readsFromServerWithoutDev(opts.DevURL, fromSet),
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
			fromState, toState, err = moveToFromContext(ctx, opts.Runtime, dialect, fromState, toState)
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

// moveToFromContext moves the desired values of a --to side read from a
// database to the context of the database the --from side was read from: the
// --from database, or the dev database a --from file or directory was created
// on. An executable provider binds every value it reads to the database it
// read (ADR 0046), and refuses a comparison that holds values bound to two
// databases. The values move as a rehearsal moves the target's (ADR 0086);
// where [valuecontext.Move] moves nothing, or either side is no read, the
// states come back as they are. A --to file is bound to the database the diff
// names already; see [sourceDatabaseURL]. The --to state is the diff's own, so
// its values are replaced in place.
//
// When a side that moved was read in a dev realm, both sides leave out the
// permissions on the database root, as a rehearsal's baseline does; see
// [devclean.WithoutDatabaseGrants].
func moveToFromContext(
	ctx context.Context, runtime engine.SchemaRuntime, dialect string, fromState, toState atlassource.State,
) (from, to atlassource.State, err error) {
	if fromState.Address == "" || toState.Address == "" || toState.Schema == nil {
		return fromState, toState, nil
	}
	moved, err := valuecontext.Move(ctx, runtime, dialect, toState.Address, fromState.Address, toState.Schema)
	if err != nil {
		return fromState, toState, fmt.Errorf("bind the --to schema to the --from database: %w", err)
	}
	if !moved {
		return fromState, toState, nil
	}
	realm, err := readsADevRealm(ctx, runtime, dialect, fromState.Address, toState.Address)
	if err != nil {
		return fromState, toState, err
	}
	if realm {
		fromState.DB, toState.Schema = devclean.WithoutDatabaseGrants(fromState.DB, toState.Schema)
	}
	return fromState, toState, nil
}

// readsADevRealm reports whether any of addresses names a dev realm: a context
// whose database is not the root its authored references are read against, as
// [namesDevRealm] reads a connection's.
func readsADevRealm(ctx context.Context, contexts schemaext.DatabaseContextResolver, dialect string, addresses ...string) (bool, error) {
	for _, address := range addresses {
		bound, err := contexts.ResolveDatabaseContext(ctx, schemaext.DatabaseContextRequest{Target: dialect, Address: address})
		if err != nil {
			return false, err
		}
		if !bound.IsZero() && bound.Database != bound.SourceDatabase {
			return true, nil
		}
	}
	return false, nil
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
	case documentCaps.Len() != 0:
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
// ErrDevURLRequired is returned by a diff whose sources are all local files and
// that has no dev database to load them into, so no source names the dialect.
var ErrDevURLRequired = errors.New("--dev-url is required for local schema file diffing")

func devServerCapabilities(ctx context.Context, dialect, devURL string, timeout time.Duration) capability.Capabilities {
	devURL = strings.TrimSpace(devURL)
	if devURL == "" {
		return capability.Capabilities{}
	}
	if devdocker.IsURL(devURL) {
		spec, err := devdocker.Parse(devURL)
		if err != nil {
			return capability.Capabilities{}
		}
		_, tag, tagged := strings.Cut(spec.Image, ":")
		resolution := capability.ResolveServerVersion(dialect, tag)
		if !tagged || !resolution.Recognized {
			return capability.Capabilities{}
		}
		return resolution.Capabilities
	}
	if atlasurl.NamesAFile(devURL) {
		// A SQLite file has no server to ask: the engine is the one compiled
		// into Ptah, which the dialect's default preset describes. Opening the
		// file to ask creates it, so without this `--dev-url sqlite://dev.db`
		// on a diff of two files leaves an empty dev.db in the working
		// directory.
		return capability.Capabilities{}
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
		return capability.Capabilities{}
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
	fromState = rebaseDevViewBodies(fromState, toState, dialect)
	toState = rebaseDevViewBodies(toState, fromState, dialect)
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
		Selections:    opts.Runtime.Selections(),
	}
	scope.RealmRelativePatterns = realmRelative
	scope.Bindings, err = scopeBindings(ctx, opts.Runtime, dialect, scope, generatedSide(fromState.Schema), databaseSide(fromState.DB),
		generatedSide(toState.Schema), databaseSide(toState.DB))
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
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
	fromSide = withDocumentBaseline(fromState, fromSide, dialect)
	// One empty side is how a create or a drop looks. A selection that matched
	// neither side cannot answer the requested comparison, so fail instead of
	// reporting a false synced result to CI.
	if emptySelection(fromErr) && emptySelection(toErr) {
		return atlasreport.SchemaDiff{}, nil, fromErr
	}

	if err := setDiffRoot(ctx, dialect, opts.DevURL, fromSide.database, opts.Runtime); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	// A comparison on a held connection runs the SQLite virtual-table guard
	// itself, and the guard reads the drop policy from here; see
	// [DiffPolicy.CompareOptions]. schema apply sets the same fields.
	compareOpts := opts.Policy.CompareOptions()
	compareOpts.Dialect = dialect
	// Two whole MySQL-family servers are compared database by database too,
	// as the pinned community binary v1.3.0 compares them: measured on MySQL
	// 8.4.11 and MariaDB 11.8.9, `schema diff` between two servers plans
	// CREATE DATABASE and DROP DATABASE (stokaro/ptah#3789).
	compareOpts.ServerSchemas = comparesTwoServers(fromState, toState)
	compareOpts.DesiredGrantsComplete = desiredGrantsComplete(toState, opts)
	compareOpts.DesiredReadGrants = desiredReadGrants(toState, opts)
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
	// refusal is a claim about a statement and a skipped drop deletes that
	// statement before it is rendered.
	virtualPolicy := opts.Policy.virtualTablePolicy()
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
	withSecretRotation(compareOpts, opts.Policy)
	if conn == nil {
		compareOpts.IdentifierSemantics = offlineDiffSemantics(toState)
	}
	compared, undecided, err := compareDiffSides(ctx, conn, to, fromSide.database, compareOpts, opts.Runtime)
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	if opts.Diagnostics == nil && !undecided.Empty() {
		return atlasreport.SchemaDiff{}, nil, undecided.Err()
	}
	undecidednote.Report(ctx, opts.Diagnostics, undecided, "--from", "--to")
	// Neither side's objects of a kind Ptah does not model reach the comparison,
	// so a report that the two match says nothing about them.
	unmodelednote.Report(ctx, opts.Diagnostics, "--from", fromState.DB)
	unmodelednote.Report(ctx, opts.Diagnostics, "--to", toState.DB)
	// Apply the planned-change half of target validation to offline comparisons
	// as well. A table both sides name and describe
	// differently is rebuilt by the SQLite planner, which destroys a module's
	// storage as surely as a drop (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidatePlannedChanges(
		dialect, fromSide.database, compared, virtualPolicy,
	); err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}

	diff, omitted := ApplyDiffPolicy(compared, opts.Policy, dialect)
	if !diff.HasChanges() {
		ReportOmitted(opts.Diagnostics, omitted)
	}
	changes, err := plannedDiffChanges(ctx, opts, compared, diff, dialect, planext.Options{
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
		SkipChangeKinds:     opts.Policy.SkipChangeKinds,
		Statements:          planstyle.Statements,
		Shape:               opts.Shape,
	})
	if err != nil {
		return atlasreport.SchemaDiff{}, nil, err
	}
	return atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{
		Dialect: dialect, From: from, To: to, Changes: changes,
		Limits: diffComparisonLimits(fromState, toState, fromSide.database, toSide.database),
	}), diff, nil
}

// diffComparisonLimits names the source coverage a SQL diff cannot infer from
// an empty plan. A whole-server MySQL-family read sees the databases visible
// to its account; a refused role read cannot compare grants even when neither
// side declares a role. Neither limitation is a schema change.
func diffComparisonLimits(fromState, toState atlassource.State, from, to *catalog.Database) []atlasreport.SchemaDiffLimit {
	var limits []atlasreport.SchemaDiffLimit
	for _, side := range []struct {
		name  string
		state atlassource.State
		read  *catalog.Database
	}{
		{name: "--from", state: fromState, read: from},
		{name: "--to", state: toState, read: to},
	} {
		if side.state.DB == nil || side.read == nil {
			continue
		}
		if _, limited := side.read.NotDescribed.Limit(coverage.Role); limited {
			limits = append(limits, atlasreport.SchemaDiffLimit{
				Side: side.name, Kind: "roles and grants", Reason: "the source did not describe this kind",
			})
		}
		if side.state.WholeServer {
			limits = append(limits, atlasreport.SchemaDiffLimit{
				Side: side.name, Kind: "database list", Reason: "only databases visible to the connected account were read",
			})
		}
	}
	return limits
}

// A one-database catalog read records the database on each view, and MySQL and
// MariaDB qualify relations in its stored body with that database. A migration
// directory replayed on a dev database therefore reads the same view under a
// different name from the target. Rebind the stored body and leave the view
// unqualified for comparison; the replay and its cleanup stay in the dev
// database.
func rebaseDevViewBodies(state, database atlassource.State, dialect string) atlassource.State {
	if !described(dialect).Schemas.StoredViewBodyNamesDatabase ||
		state.Kind == atlassource.KindDatabase ||
		state.DB == nil || state.Schema == nil || state.WholeServer ||
		database.Kind != atlassource.KindDatabase || database.WholeServer ||
		state.DefaultSchema == "" || database.DefaultSchema == "" ||
		state.DefaultSchema == database.DefaultSchema || len(state.DB.Views) == 0 {
		return state
	}
	previous, target := state.DefaultSchema, database.DefaultSchema
	observed := *state.DB
	observed.Views = slices.Clone(state.DB.Views)
	for i := range observed.Views {
		observed.Views[i].Body = rewriteSchemaNames(observed.Views[i].Body, dialect, previous, target)
		observed.Views[i].Schema = ""
	}
	declared := *state.Schema
	declared.Views = slices.Clone(state.Schema.Views)
	for i := range declared.Views {
		declared.Views[i].Body = rewriteSchemaNames(declared.Views[i].Body, dialect, previous, target)
		if ref, ok := tableref.Parse(declared.Views[i].Name); ok && ref.Qualified && ref.Schema == previous {
			declared.Views[i].Name = tableref.Canonical("", ref.Name)
		}
	}
	state.DB, state.Schema = &observed, &declared
	state.DefaultSchema = target
	return state
}

// plannedDiffChanges is the statements of diff's plan, each carrying the comment
// that names its change where the verb writes one.
func plannedDiffChanges(
	ctx context.Context,
	opts DiffOptions,
	compared, diff *difftypes.SchemaDiff,
	dialect string,
	planOptions planext.Options,
) ([]atlasreport.SchemaChange, error) {
	if !diff.HasChanges() {
		return nil, nil
	}
	// diff is compared with the diff policy applied, and says whether there is
	// anything to plan. The plan is made from compared with the kinds the
	// policy skips, as schema apply makes it: the planner removes the same
	// changes again, and the statements open with a comment for each.
	plan, err := planner.GenerateSchemaDiffTextPlan(ctx, opts.Runtime, compared, dialect, planOptions)
	if err != nil {
		return nil, fmt.Errorf("generate schema diff SQL: %w", err)
	}
	statements, err := plan.Statements()
	if err != nil {
		return nil, fmt.Errorf("generate schema diff SQL: %w", err)
	}
	changes := make([]atlasreport.SchemaChange, 0, len(statements))
	for _, statement := range statements {
		changes = append(changes, atlasreport.SchemaChange{Cmd: statement})
	}
	if opts.ChangeComments {
		if err := nameDiffChanges(changes, plan, dialect); err != nil {
			return nil, fmt.Errorf("generate schema diff SQL: %w", err)
		}
	}
	return changes, nil
}

// nameDiffChanges writes the comment that names each change on the first statement
// its node renders, as a migration file does.
func nameDiffChanges(changes []atlasreport.SchemaChange, plan planner.RenderedPlan, dialect string) error {
	planned, err := plan.PlannedStatements()
	if err != nil {
		return err
	}
	names := atlaschange.For(dialect).Forward(plan.Request.Nodes)
	heading := -1
	at := 0
	for _, piece := range planned {
		if strings.TrimSpace(sqlutil.StripCommentsForDialect(piece.SQL, dialect)) == "" {
			continue
		}
		if at < len(changes) && piece.Node >= 0 && piece.Node != heading {
			changes[at].Comment = names[piece.Node]
			heading = piece.Node
		}
		at++
	}
	return nil
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

// offlineDiffSemantics is the identifier rules a diff without a held
// connection pairs names by: those of the server the --to database was read
// from, and the dialect's offline rules when --to is not a database.
//
// A --to database is read on a connection the diff does not hold for the
// comparison, and a URL that pins a schema -- a MySQL database, a PostgreSQL
// search_path -- reports its objects without that schema. Paired by the
// offline rules, which know nothing of the pinned schema, a --from file naming
// `f3a.t1` and the database's `t1` were two tables: measured on MySQL 8.4,
// MariaDB 11.4 and PostgreSQL 18, `schema diff --from <file> --to <database>`
// planned CREATE TABLE `t1` and DROP TABLE `f3a`.`t1` for a table both sides
// hold.
func offlineDiffSemantics(to atlassource.State) *identifier.Semantics {
	if to.IdentifierSemantics == nil {
		return nil
	}
	return new(to.IdentifierSemantics.Clone())
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
			if !pgsystemschema.IsPostgresFamilySystemSchema(dialect, schema.Name) {
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
	if err := pgsystemschema.ValidateDeclaredPostgresSystemSchemas(
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
// It is also when both sides are databases. Each then holds a server's
// spelling, but not necessarily the same one: PostgreSQL does not print a
// CHECK back in a form that reads back to itself. A history that wrote
// `x::text = ANY ((ARRAY['a'::varchar])::text[])`, which is how the server
// prints `x IN ('a')`, stores `x::text = ANY (ARRAY[('a'::varchar)::text])`,
// so the database the history built and the one the declaration built
// disagree as text about one constraint. Asked on the --from server, the --to
// spelling is rewritten the way the --from one was, which is what schema
// apply already does for the same two databases.
//
// --to is resolved inside that scope, after --from, which is the order
// [resolveDiffSources] keeps. It cannot need the held server: a --to
// directory takes the other branch, and so does a --from directory, whose
// replay resets the dev database a --to database may name. When --from is a
// file, there is no server behind it to ask (stokaro/ptah#3658).
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
// --to does not, or whether both sides are databases; see
// [withResolvedDiffSources].
func holdsServerForComparison(fromSet, toSet atlassource.Set) bool {
	if fromSet.Kind == atlassource.KindDatabase && toSet.Kind == atlassource.KindDatabase {
		return true
	}
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

func prepareDiffSources(ctx context.Context, opts DiffOptions) (preparedDiffSources, error) {
	fromSet, err := atlassource.ClassifySet("--from", opts.FromURLs, opts.ProjectEnv, dbschema.NativeSources(ctx))
	if err != nil {
		return preparedDiffSources{}, err
	}
	toSet, err := atlassource.ClassifySet("--to", opts.ToURLs, opts.ProjectEnv, dbschema.NativeSources(ctx))
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
		if err := fromSet.EnsureDevIsolation(ctx, opts.DevURL); err != nil {
			return preparedDiffSources{}, err
		}
		if err := toSet.EnsureDevIsolation(ctx, opts.DevURL); err != nil {
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
	dialect, dialectFlag, err := atlassource.PinDialect(ctx, opts.DevURL, fromSet, toSet)
	if err != nil {
		return preparedDiffSources{}, err
	}
	if dialect == "" {
		return preparedDiffSources{}, ErrDevURLRequired
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
// non-extension resource, and with the --to side's requirements when either
// side kept an object for a requirement alone (see [requirementScope]). The
// second pass is deliberately pair-wide: a match can exist on only one side
// while an unselected extension already exists on both, and filtering the
// other side would manufacture a change.
func scopeDiffStates(
	ctx context.Context,
	fromState, toState atlassource.State,
	scope atlasfilter.Scope,
	dialect string,
	runtime atlassource.StateRuntime,
) (fromSide, toSide scopedDiffState) {
	fromSide = scopeDiffState(ctx, fromState, scope, "--from schema", dialect, runtime)
	toSide = scopeDiffState(ctx, toState, scope, "--to schema", dialect, runtime)
	if fromSide.err != nil || toSide.err != nil {
		return fromSide, toSide
	}
	supportScope, supportChanged := extensionSupportScope(scope, fromSide.selection, toSide.selection)
	pairScope, requirementsChanged := requirementScope(supportScope, toSide.selection, fromSide.selection)
	if !supportChanged && !requirementsChanged {
		return fromSide, toSide
	}
	return scopeDiffState(ctx, fromState, pairScope, "--from schema", dialect, runtime),
		scopeDiffState(ctx, toState, pairScope, "--to schema", dialect, runtime)
}

// withDocumentBaseline returns the --from side with what a server gives every
// database it builds, when that side is a document: the current side stands
// for the database a server builds from the document, default schema
// included. See [goschematodb.WithDefaultSchemaBaseline]. A side read from a
// server already holds it.
func withDocumentBaseline(state atlassource.State, side scopedDiffState, dialect string) scopedDiffState {
	if state.DB == nil {
		side.database = goschematodb.WithDefaultSchemaBaseline(side.database, side.schema, dialect)
	}
	return side
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
	runtime atlassource.StateRuntime,
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

// readsFromServerWithoutDev reports a --from database with no dev URL beside
// it, which a MySQL-family URL naming no database reaches as the whole server.
//
// A --to server beside a --from file is left out. The comparison is driven by
// the --from side, and a file there is compared as text rather than read on a
// server, so its databases would not be compared with the server's: measured
// on MySQL 8.4, such a diff dropped the tables of a database the server lacks
// and kept the database. A dev server is what reads that file as a server.
func readsFromServerWithoutDev(devURL string, from atlassource.Set) bool {
	return strings.TrimSpace(devURL) == "" && from.Kind == atlassource.KindDatabase
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
//
// The engines this applies to name objects by absolute paths under a database
// root, and the root is the one their root reader reads from the dev URL; see
// [schemafile.RootReader]. A dev URL that names none is refused in the
// reader's words. An engine an executable provider serves has no root reader:
// its provider interprets the dev URL (schemaext.DatabaseContext), and the root
// is the one authored references are read against, SourceDatabase.
func setDiffRoot(ctx context.Context, dialect, devURL string, current *catalog.Database, contexts schemaext.DatabaseContextResolver) error {
	if current.DatabasePath != "" || devURL == "" {
		return nil
	}
	reader, err := schemafile.RootReader(dialect)
	if err != nil {
		return err
	}
	if reader == nil {
		if contexts == nil || platform.NormalizeDialect(dialect) != "" {
			return nil
		}
		bound, err := contexts.ResolveDatabaseContext(ctx, schemaext.DatabaseContextRequest{Target: dialect, Address: devURL})
		if err != nil {
			return err
		}
		current.DatabasePath = bound.SourceDatabase
		return nil
	}
	root, err := schemafile.SourceRoot(dialect, devURL)
	if err != nil {
		return reader.DiffRefusal()
	}
	current.DatabasePath = root
	return nil
}

// desiredGrantsComplete reports whether the --to side of a diff holds every
// grant on what it describes. A --to database is a whole answer: a grant it
// does not hold is one it does not have. Read as a declaration, a grant to a
// role --to does not name, or to PUBLIC, reads as synced in this direction
// and as a GRANT in the other. A database the caller filled by replaying a
// migration directory is not one; see [DiffOptions.ToReplaysMigrations].
func desiredGrantsComplete(toState atlassource.State, opts DiffOptions) bool {
	return toState.Kind == atlassource.KindDatabase && !opts.ToReplaysMigrations
}

// desiredReadGrants is the grants of the read behind a desired state that holds
// every grant on what it describes.
func desiredReadGrants(toState atlassource.State, opts DiffOptions) []catalog.Grant {
	if !desiredGrantsComplete(toState, opts) {
		return nil
	}
	return toState.DB.Grants
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
