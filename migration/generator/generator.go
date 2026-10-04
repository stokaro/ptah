package generator

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sync"
	"time"

	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/devclean"
	"ptah.run/internal/fsnapshot"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/schemadiff"
)

// GenerateMigrationOptions contains options for migration generation
type GenerateMigrationOptions struct {
	// GoEntitiesDir is the directory to scan for Go entities
	GoEntitiesDir string
	// GoEntitiesFS is the filesystem to use for reading entities (optional, defaults to os.DirFS)
	GoEntitiesFS fs.FS
	// Generated is a pre-parsed desired schema (optional). When set, it is used
	// directly and GoEntitiesDir/GoEntitiesFS are ignored, letting a caller supply
	// a composite schema merged from several sources.
	Generated *schemamodel.Database
	// DatabaseURL is the connection string for the database
	DatabaseURL string
	// ConnectTimeout bounds the attempt to open DatabaseURL, and nothing after
	// it. Zero leaves the connect bounded only by the caller's context.
	//
	// It is a field rather than a deadline on the context the caller passes,
	// because that context governs the whole generation -- planning, rendering,
	// and publishing the files. A connect budget spent as a run budget expires
	// somewhere later, and reports whatever step happened to notice:
	//
	//	error creating migration files: context deadline exceeded
	//
	// which named file publication for a run whose connect took milliseconds
	// (stokaro/ptah#1749).
	ConnectTimeout time.Duration
	// DBConn is the database connection (optional, if not provided, a new connection will be created)
	DBConn *dbschema.DatabaseConnection
	// MigrationName is the name for the migration (optional, defaults to "migration")
	MigrationName string
	// OutputDir is the directory where migration files will be saved (always real filesystem)
	OutputDir string
	// AllowedOutputRoot constrains OutputDir when set. Embedders that accept
	// user-supplied output paths should set this to the project/workspace root.
	AllowedOutputRoot string
	// CompareOptions are the options to use when comparing schemas
	CompareOptions *config.CompareOptions
	// Schemas restricts database introspection to the listed schemas when the
	// connected dialect supports schema scoping.
	Schemas []string
	// RevisionTables names the migrator's bookkeeping tables in the database
	// the plan reads: the revision table and, for Ptah's native format, the
	// operation log beside it. They are left out of the comparison, so a plan
	// never drops the record of the migrations already applied. Names compare
	// without regard to case. Empty leaves out only what the dialect's reader
	// leaves out by default, Ptah's native tables under their default names; a
	// custom revision table or the Atlas-format one is then compared as an
	// ordinary table.
	RevisionTables []string
	// EnvironmentExtensions names extensions the database held before the
	// current state was built and that the desired schema does not own: a dev
	// database's own, which the replay that built the current state kept. The
	// current state leaves out each one the desired schema does not declare,
	// so the plan does not drop it, and keeps one it declares, so the two
	// match. Nil compares every extension the database holds.
	EnvironmentExtensions []string
	// CheckDestructive refuses to generate destructive up migrations unless
	// AllowDestructive is set.
	CheckDestructive bool
	// AllowDestructive permits destructive up migrations when CheckDestructive is set.
	AllowDestructive bool
	// ReportFormat optionally writes a safety report next to generated files.
	// Supported values: "", "html", "json".
	ReportFormat string
	// ShadowDatabaseURL enables pre-write verification on an ephemeral database
	// whose live database realm must be distinct from the target connection.
	// The verification runs in [ptah.run/migration/shadow]: it drops all
	// objects in this database, replays existing migrations from OutputDir,
	// applies the candidate migration, re-introspects the result, and aborts if
	// it differs from the Go schema.
	ShadowDatabaseURL string
	// PriorMigrationsFS supplies the immutable, already-authorized migration
	// history used by shadow verification or another replay-based caller. When
	// set, publication also refuses if OutputDir no longer matches this snapshot,
	// so generating a fresh checksum cannot legitimize history that changed
	// after the caller's integrity decision.
	PriorMigrationsFS fs.FS
	// DiffPolicy controls which changes the planner emits: destructive change
	// kinds to skip and whether to create new indexes concurrently. The zero
	// value applies no policy. Skipping a destructive change omits it from the
	// plan (with a comment in its place), so it never trips the CheckDestructive
	// gate.
	DiffPolicy DiffPolicy
	// SchemaQualifier, when non-empty, rewrites every object named by the
	// generated up and down statements to this custom schema qualifier, so the
	// files can be applied to a schema other than the one they were planned
	// against. The plan must stay scoped to a single schema, and only dialects
	// with schema-qualified object names are supported.
	SchemaQualifier string
	// OnUndecided, when set, receives the declared objects the comparison
	// withheld: the read of the database did not describe their kind, so
	// nothing checked whether they exist, and no generated statement creates
	// them. It is called at most once per planning run, after the comparison
	// and before planning returns, with the objects sorted by kind and then
	// name, and it is not called when nothing was withheld. A run that
	// generates no files after OnUndecided was called has not shown that the
	// database matches the desired schema.
	OnUndecided func([]coverage.Object)
}

// reportUndecided hands undecided to [GenerateMigrationOptions.OnUndecided],
// when there is something to hand and somewhere to hand it.
func (opts GenerateMigrationOptions) reportUndecided(undecided []coverage.Object) {
	if len(undecided) > 0 && opts.OnUndecided != nil {
		opts.OnUndecided(undecided)
	}
}

// DiffPolicy is the generator-level view of the project diff policy.
type DiffPolicy struct {
	// SkipChangeKinds lists destructive change kinds to omit from generated
	// migrations. Currently honored by the PostgreSQL-family planner.
	SkipChangeKinds []diffpolicy.ChangeKind
	// OnlineAlter asks the server to apply the generated migration without
	// blocking the writes already running against its tables, in whichever
	// grammar the target has. A target without one generates as if this were
	// off.
	OnlineAlter bool
	// ConcurrentIndex requests CREATE INDEX CONCURRENTLY for every newly added
	// index, superseding the populated-table heuristic. It remains gated on the
	// target's CreateIndexConcurrently capability.
	ConcurrentIndex bool
	// ConcurrentIndexDrop requests DROP INDEX CONCURRENTLY for every standalone
	// index removal, gated on the target's DropIndexConcurrently capability. An
	// index that is dropped and recreated under the same identity is a
	// redefinition, not a standalone removal, and keeps the blocking drop the
	// planner already pairs with the rebuild.
	//
	// It does NOT govern the down direction: the rollback of a concurrent index
	// build is always emitted concurrently where the target supports it,
	// because a blocking drop there would undo the whole point of having built
	// the index without a lock.
	ConcurrentIndexDrop bool
	// AllowTableRebuild plans a change the target cannot make in place as an
	// explicit table rebuild, in both directions of the migration. See
	// [planner.Options.AllowTableRebuild].
	AllowTableRebuild bool
}

// MigrationFilePair represents one generated up/down migration file pair.
type MigrationFilePair struct {
	UpFile        string // Path to the up migration file
	DownFile      string // Path to the down migration file
	ReportFile    string // Path to the safety report file, when requested
	Version       int64  // Migration version (timestamp)
	NoTransaction bool   // Whether either direction requires +ptah no_transaction
}

// MigrationFiles represents the generated migration files. Files is the
// authoritative ordered list of migration pairs and their published paths.
type MigrationFiles struct {
	Files []MigrationFilePair // All generated migration file pairs, in apply order
}

// MigrationPlan is a fully validated migration that has not been written to
// disk yet. WriteFiles publishes the planned migration once.
type MigrationPlan struct {
	mu        sync.Mutex
	outputDir string
	// dir is the migration directory, bound while the plan was built and held
	// open until the plan's one publication attempt returns. It is what makes
	// the plan a claim on a filesystem object rather than on a pathname:
	// publication verifies and writes through this one handle, so a directory
	// replaced -- or removed and recreated -- between the two exported calls
	// cannot receive the batch (stokaro/ptah#1118).
	//
	// It is nil once the handles have been released, which is what marks the
	// plan spent. WriteFilesContext releases them on the way out whether the
	// publication succeeded or failed, so the window in which the plan holds
	// the directory ends at a point the caller controls rather than at the next
	// garbage collection. A plan that is never published at all is released by
	// Close, which is the same release point named explicitly; relying on
	// os.Root's finalizer instead leaves the directory held for as long as the
	// collector takes to get there.
	dir *atlasmigrate.MigrationWriter
	// plannedContents is what dir held when the plan was built, and nothing
	// else. Identity lives in dir, which is a handle, rather than beside the
	// contents as a detached fs.FileInfo the operating system is free to
	// reissue to a replacement.
	plannedContents fsnapshot.Snapshot
	// authorizedPriorMigrations is the migration-only snapshot whose SQL was
	// authorized for replay. A zero value with the boolean false means the
	// caller did not supply a prior snapshot.
	authorizedPriorMigrations    fsnapshot.Snapshot
	hasAuthorizedPriorMigrations bool
	reportFormat                 string
	specs                        []generatedMigrationSpec
	written                      bool
	// closed records that the plan was released by Close rather than by a
	// publication attempt. Both leave dir nil, and a caller that publishes
	// afterwards deserves to be told which of the two happened.
	closed bool
}

// GenerateMigration generates both up and down migration files by comparing
// the desired schema (from Go entities) with the current database state. It is
// the convenience composition of [PlanMigration] and
// [MigrationPlan.WriteFilesContext]; when the schemas already match it returns
// nil files with a nil error rather than publishing anything.
//
// The context bounds connection, planning, lock acquisition, and publication.
func GenerateMigration(ctx context.Context, opts GenerateMigrationOptions) (*MigrationFiles, error) {
	plan, err := PlanMigration(ctx, opts)
	if err != nil || plan == nil {
		return nil, err
	}
	return plan.WriteFilesContext(ctx)
}

// PlanMigration performs schema loading, live introspection, diff planning,
// safety checks, and optional shadow verification without writing migration
// artifacts. Call WriteFiles only after any surrounding database cleanup or
// other pre-publication work succeeds.
//
// A nil plan with a nil error means the comparison found no changes: there is
// nothing to publish and nothing to release. Otherwise the returned plan holds
// the migration directory open until it is published or closed, so a caller
// that may abandon it should defer plan.Close next to this call --
// [MigrationPlan.Close] is a no-op on a published plan.
//
// A shadow-verification failure is returned as a *shadow.VerificationError,
// inspectable with errors.As, carrying the stage and the deterministically
// ordered mismatch list. When the URL or connection selects SQLite, a
// malformed PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP value is refused up front
// rather than resolved to the documented default, so a typo cannot lie
// dormant until a run reaches the branch that would have read it; non-SQLite
// plans do not consult the variable at all.
func PlanMigration(ctx context.Context, opts GenerateMigrationOptions) (*MigrationPlan, error) {
	opts, err := normalizeGenerateMigrationOptions(opts)
	if err != nil {
		return nil, err
	}
	authorizedPriorMigrations, err := captureAuthorizedPriorMigrations(opts.PriorMigrationsFS)
	if err != nil {
		return nil, err
	}

	// 1. Determine the desired schema: use a pre-merged one when provided (for a
	// composite desired-state assembled from several sources), otherwise parse the
	// Go entities directory.
	desired, err := resolveDesiredSchema(opts)
	if err != nil {
		return nil, err
	}

	// 2. Connect to database and read current schema
	conn, ownedConnection, err := resolvePlanDatabaseConnection(ctx, opts)
	if err != nil {
		return nil, err
	}
	if ownedConnection {
		defer dbschema.CloseAndWarn(conn)
	}

	dbSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, opts.Schemas)
	if err != nil {
		return nil, fmt.Errorf("error reading database schema: %w", err)
	}
	dbSchema = devclean.WithoutKeptExtensions(dbSchema, opts.EnvironmentExtensions, declaredExtensions(desired))
	dbSchema = revisiontable.Without(dbSchema, opts.RevisionTables)
	if err := recoverMigrationPublication(ctx, opts.AllowedOutputRoot, opts.OutputDir); err != nil {
		return nil, err
	}
	// Bind the migration directory here, at planning time, and hold it. Every
	// later step of this plan -- the version scan below, the pre-publication
	// verification, and the publication itself -- addresses that one handle.
	writer, err := bindPlannedMigrationDir(opts.AllowedOutputRoot, opts.OutputDir)
	if err != nil {
		return nil, err
	}
	// Planning can still fail, or find nothing to do, on any of the paths
	// below. Only a plan that is handed back keeps the handles.
	planned := false
	defer func() {
		if !planned {
			_ = writer.Close()
		}
	}()
	plannedContents, err := captureMigrationDirectoryContents(writer)
	if err != nil {
		return nil, fmt.Errorf("capture migration directory before planning: %w", err)
	}

	// 3. Calculate the diff between desired and current schema using live
	// dialect and catalog identifier metadata.
	info := conn.Info()
	diff, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(
		ctx,
		conn,
		desired,
		dbSchema,
		compareOptionsWithDiffPolicy(opts.CompareOptions, opts.DiffPolicy),
	)
	if err != nil {
		return nil, fmt.Errorf("error comparing generated and database schemas: %w", err)
	}
	opts.reportUndecided(undecided)

	// Check if there are any changes
	if !diff.HasChanges() {
		// No changes detected - this is a successful no-op operation
		return nil, nil
	}

	// 4. Generate migration version (timestamp). The scan for a free version
	// reads the bound handle rather than the pathname, so the names this plan
	// avoids colliding with are the ones in the directory it will publish into.
	version := migrationfile.NextVersion()
	version, err = nextAvailableMigrationVersion(writer, version, opts.MigrationName)
	if err != nil {
		return nil, fmt.Errorf("error reading migration directory: %w", err)
	}
	slog.Debug("Generated migration version", "version", version)

	qualifier, err := atlasmigrate.ParseQualifier(opts.SchemaQualifier)
	if err != nil {
		return nil, err
	}
	qualifier = qualifier.WithErrorLabel("--qualifier")
	if err := qualifier.ValidateScope(info.Dialect, opts.Schemas); err != nil {
		return nil, err
	}

	specs, assessments, err := planMigrationFiles(ctx, conn, diff, desired, dbSchema, version, opts, qualifier)
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, nil
	}
	if err := checkDestructiveAllowed(opts, assessments); err != nil {
		return nil, err
	}

	if err := verifyPlannedShadowMigration(ctx, opts, conn, info, diff, specs, desired); err != nil {
		return nil, err
	}

	planned = true
	return &MigrationPlan{
		outputDir:                    opts.OutputDir,
		dir:                          writer,
		plannedContents:              plannedContents,
		authorizedPriorMigrations:    authorizedPriorMigrations,
		hasAuthorizedPriorMigrations: opts.PriorMigrationsFS != nil,
		reportFormat:                 opts.ReportFormat,
		specs:                        specs,
	}, nil
}

// declaredExtensions is the set of extensions a desired schema declares.
func declaredExtensions(desired *schemamodel.Database) map[string]bool {
	names := make(map[string]bool)
	if desired == nil {
		return names
	}
	for _, extension := range desired.Extensions {
		names[extension.Name] = true
	}
	return names
}
