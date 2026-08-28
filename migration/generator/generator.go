package generator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/concurrentindex"
	"go.5x5.cz/ptah/internal/constraintscope"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/migrationversion"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/internal/txrequire"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/migrationfile"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/safety"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/shadow"
)

var (
	// ErrMigrationDirectoryChanged reports that migration artifacts changed
	// after the history used for planning or replay was captured and before
	// publication.
	ErrMigrationDirectoryChanged = errors.New(
		"migration directory changed before publication",
	)
	// ErrMigrationPlanInUse reports concurrent publication through one plan.
	ErrMigrationPlanInUse = errors.New("migration plan is already being written")
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
	// The verification runs in [go.5x5.cz/ptah/migration/shadow]: it drops all
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
}

// DiffPolicy is the generator-level view of the project diff policy.
type DiffPolicy struct {
	// SkipChangeKinds lists destructive change kinds to omit from generated
	// migrations. Currently honored by the PostgreSQL-family planner.
	SkipChangeKinds []diffpolicy.ChangeKind
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
	// else. It used to carry a filesystem identity beside the contents; identity
	// now lives in dir, which is a handle rather than a detached fs.FileInfo the
	// operating system is free to reissue to a replacement.
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

// EmptyMigrationOptions contains options for skeleton migration creation.
type EmptyMigrationOptions struct {
	// MigrationName is the descriptive migration name used in filenames and headers.
	MigrationName string
	// OutputDir is the directory where migration files will be saved.
	OutputDir string
	// AllowedOutputRoot constrains OutputDir when set.
	AllowedOutputRoot string
	// DirFormat selects the generated migration file layout. Empty generates
	// Ptah paired up/down files.
	DirFormat migrationfile.DirFormat
}

// GenerateEmptyMigration creates skeleton migration files for manual SQL
// authoring.
//
// The whole creation runs through one rooted migration-directory handle, bound
// before anything is read or written: the directory is materialized, the
// version scanned, the files created and atlas.sum committed through that one
// handle rather than through the pathname it was selected by
// (stokaro/ptah#1118). When AllowedOutputRoot is set the handle is opened
// through it, so the transaction stays inside that root even if the directory
// or one of its ancestors is replaced after the path was validated.
func GenerateEmptyMigration(opts EmptyMigrationOptions) (*MigrationFiles, error) {
	name := strings.TrimSpace(opts.MigrationName)
	if strings.TrimSpace(opts.OutputDir) == "" {
		return nil, fmt.Errorf("output directory is required")
	}
	dirFormat, err := migrationfile.ParseDirFormat(string(opts.DirFormat))
	if err != nil {
		return nil, err
	}

	outputDir, err := pathguard.ResolveWithinRoot(opts.OutputDir, opts.AllowedOutputRoot)
	if err != nil {
		return nil, fmt.Errorf("error validating output directory: %w", err)
	}
	// The Atlas layout derives its own file name from the migration name and
	// accepts an empty one, so only the paired layout validates it here -- and
	// it validates before the directory is bound, so a rejected name never
	// creates a directory.
	if dirFormat != migrationfile.DirFormatAtlas {
		if err := validateEmptyMigrationName(name); err != nil {
			return nil, err
		}
	} else if err := checkAtlasEmptyMigrationNameReadable(name); err != nil {
		return nil, err
	}

	root, err := openOutputRoot(opts.AllowedOutputRoot)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closeOutputRoot(root) }()

	return writeEmptyMigration(root, outputDir, name, dirFormat)
}

// atlasVersionClock reads the wall clock an Atlas-layout version is stamped
// from. It is a variable so a test can freeze the second and then state the
// exact version a scan must choose; production never assigns it.
var atlasVersionClock = func() time.Time { return time.Now().UTC() }

func nextAtlasMigrationVersion() int64 {
	version, err := strconv.ParseInt(atlasVersionClock().Format("20060102150405"), 10, 64)
	if err != nil {
		return migrationfile.NextVersion()
	}
	return version
}

// nextAvailableAtlasMigrationVersion answers the version question for a
// directory named by pathname, for readers that are not inside a writer
// transaction. A writer holding a rooted handle asks nextAvailableAtlasVersion
// over the names it listed through that handle instead, so it never resolves
// the directory a second time.
func nextAvailableAtlasMigrationVersion(outputDir string, version int64) int64 {
	next, err := nextAvailableAtlasVersion(migrationDirFileNames(outputDir), version)
	if err != nil {
		return 0
	}
	return next
}

// nextAvailableAtlasVersion returns a version that outranks every migration in
// names. It is the CHECKPOINT rule: a checkpoint whose version does not sort
// above the history it squashes would be replayed on top of that history by a
// fresh database (stokaro/ptah#954), so here the bump past the newest migration
// is the point.
//
// It is deliberately NOT the rule `migrate new` uses -- see
// firstFreeAtlasVersion. The bump is also why this one can run out of versions:
// a directory whose newest migration is at [migrationversion.AtlasMax] has
// nothing above it, and `migrate import --dir-format flyway` puts a repeatable
// migration exactly there. Before stokaro/ptah#938 that computed
// math.MinInt64 and wrote it.
//
// The bump asks [migrationversion.Advance] rather than adding one, so it steps
// to the next real second instead of the next integer. Beside a
// `29991231235959_future.sql` the raw increment wrote a checkpoint at
// 29991231235960 -- sixty seconds past the minute, an instant time.Parse
// refuses -- and a second checkpoint then wrote 29991231235961 on top of it.
func nextAvailableAtlasVersion(names []string, version int64) (int64, error) {
	if latest := latestAtlasVersionIn(names); latest >= version {
		next, err := migrationversion.Advance(latest, migrationfile.DirFormatAtlas)
		if err != nil {
			return 0, err
		}
		version = next
	}
	taken := nameSet(names)
	for taken[atlasEmptyMigrationFileName(version, "")] {
		next, err := migrationversion.Advance(version, migrationfile.DirFormatAtlas)
		if err != nil {
			return 0, err
		}
		version = next
	}
	return version, nil
}

// firstFreeAtlasVersion returns the first version at or after version that no
// migration in names already occupies. It is the rule `migrate new` writes an
// Atlas-layout migration with, and it does NOT bump past the newest migration.
//
// Two measurements settle that. The pinned community binary v1.3.0 stamps the
// current UTC second and nothing else: into a directory holding
// `29991231235959_future.sql` it writes today's version, sorting BELOW the
// migration already there. And `migrate diff` in this binary has done the same
// since stokaro/ptah#1218 (see atlasmigrate.nextMigrationVersionFS), as does
// `migrate new` for the five external layouts (atlasmigrate.WriteSkeletonMigration).
// Bumping here made `migrate new` the one verb in the binary stamping a
// different shape: on that directory it wrote `29991231235960`, a version that
// is not a time anyone can parse back, while `migrate diff` a second later
// wrote the ordinary UTC stamp (stokaro/ptah#938).
//
// The collision step asks [migrationversion.Writable] rather than testing the
// bounds itself, so two migrations created inside the same second at :59 land
// on the next real second instead of on a sixtieth one.
func firstFreeAtlasVersion(names []string, version int64) (int64, error) {
	taken := atlasVersionsIn(names)
	for {
		free, err := migrationversion.Writable(version, migrationfile.DirFormatAtlas)
		if err != nil {
			return 0, err
		}
		version = free
		if _, ok := taken[version]; !ok {
			return version, nil
		}
		version++
	}
}

// atlasVersionsIn returns the versions names already occupy. A version is taken
// by ANY migration at it, whatever its description, which is what the reader
// orders by and what atlasmigrate.nextMigrationVersionFS already checks.
func atlasVersionsIn(names []string) map[int64]struct{} {
	taken := make(map[int64]struct{}, len(names))
	for _, name := range names {
		migrationFile, err := migrationfile.ParseAtlasFileName(name)
		if err != nil {
			continue
		}
		taken[migrationFile.Version] = struct{}{}
	}
	return taken
}

func latestAtlasVersionIn(names []string) int64 {
	var latest int64
	for _, name := range names {
		migrationFile, err := migrationfile.ParseAtlasFileName(name)
		if err != nil {
			continue
		}
		if migrationFile.Version > latest {
			latest = migrationFile.Version
		}
	}
	return latest
}

// atlasEmptyMigrationFileName composes the Atlas-layout file name `migrate new`
// writes, and it carries the caller's name unchanged.
//
// It used to rewrite the name first: spaces became hyphens and every character
// outside [-_0-9A-Za-z] was dropped, so `migrate new "add users table"` wrote
// `<version>_add-users-table.sql` and `migrate new "add_users.sql"` wrote
// `<version>_add_userssql.sql`. The Atlas layout is not ours to rename in: the
// pinned community binary v1.3.0 composes `<version>_<name>.sql` from the name
// verbatim, so the same two commands write `<version>_add users table.sql` and
// `<version>_add_users.sql.sql` there (measured 2026-08-08). The file name is
// covered by atlas.sum, so a rewritten name is also a different checksum for the
// same command (stokaro/ptah#1235 findings 8.6 and 8.7).
//
// Two rules still apply and are deliberate, not leftovers:
//
//   - [GenerateEmptyMigration] trims leading and trailing whitespace off the
//     name for every layout. That binary keeps it -- `migrate new "  padded  "`
//     writes `<version>_  padded  .sql` -- but a file name with trailing spaces
//     does not survive every filesystem this tool writes into, and no finding in
//     that register asks for one.
//   - A path separator is refused rather than stripped, on the compatibility
//     surface by checkAtlasMigrationName and here by the rooted writer, which
//     cannot open a path outside the directory it holds.
//
// [atlasCheckpointNameStem] keeps the old rewriting for `migrate checkpoint`.
// That verb has no measured counterpart on the pinned binary, so there is
// nothing to move towards; both writers now open the file through a rooted
// handle.
func atlasEmptyMigrationFileName(version int64, name string) string {
	if name == "" {
		return fmt.Sprintf("%d.sql", version)
	}
	return fmt.Sprintf("%d_%s.sql", version, name)
}

// atlasCheckpointNameStem maps a checkpoint description onto a conservative file
// name stem: spaces to hyphens, everything outside [-_0-9A-Za-z] dropped.
//
// This is what every Atlas-layout name used to go through. `migrate new` no
// longer does, because that binary was measured writing the name verbatim there.
// `migrate checkpoint` keeps it: the register that prompted the change records
// no cell for the verb, so there is nothing measured to move towards.
//
// Containment is no longer the reason. [WriteAtlasCheckpointFileWithOptions]
// now creates the file through a rooted directory handle, which refuses a name
// carrying a separator rather than following it out of the directory
// (stokaro/ptah#1118), so the stem rewriting is a naming convention and not a
// boundary. Removing it would change file names for a verb with no measured
// counterpart, which is why it stays.
func atlasCheckpointNameStem(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "-")
	var b strings.Builder
	for _, r := range name {
		if isAtlasMigrationNameChar(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isAtlasMigrationNameChar(r rune) bool {
	return r == '-' || r == '_' ||
		('0' <= r && r <= '9') ||
		('A' <= r && r <= 'Z') ||
		('a' <= r && r <= 'z')
}

// checkAtlasEmptyMigrationNameReadable refuses a migration name whose composed
// file name this tool would read back as something other than the new up
// migration it just wrote.
//
// Writing the name verbatim means it can now reach the Atlas file-name grammar,
// and one suffix collides with it: [migrationfile.ParseAtlasFileName]
// classifies `<version>_x.down.sql` as the down half of a pair, because Atlas
// importers emit that spelling for golang-migrate directories. So `migrate new
// "x.down"` would write a file `migrate status` then refuses with `Atlas
// migration version <version> has down migration but no up migration`, exit 1 --
// one verb producing a directory another verb cannot read.
//
// The check is a round trip rather than a list of banned suffixes, so a later
// change to that grammar cannot reopen the hole silently.
//
// This refusal is stricter than the pinned community binary v1.3.0, which
// writes `<version>_x.down.sql` at exit 0 and reads it back as a pending
// migration at exit 0. Ptah refusing to WRITE it does not fix Ptah's reader:
// that binary can still hand us such a directory, and `migrate status` still
// exits 1 on it. That reader gap is a separate divergence and is reported, not
// closed, by the change that introduced this guard.
func checkAtlasEmptyMigrationNameReadable(name string) error {
	// The version is a stand-in: the grammar this probes keys on the name's
	// suffix, not on the digits in front of it, and the message quotes the shape
	// rather than a version no caller asked for.
	const probeVersion = 1
	fileName := atlasEmptyMigrationFileName(probeVersion, name)
	parsed, err := migrationfile.ParseAtlasFileName(fileName)
	if err == nil && parsed.Version == probeVersion && parsed.Direction == "up" {
		return nil
	}
	shape := "<version>" + strings.TrimPrefix(fileName, strconv.FormatInt(probeVersion, 10))
	return fmt.Errorf(
		"migration name %q composes the file name %s, which this tool does not read back as a new migration",
		name,
		shape,
	)
}

func validateEmptyMigrationName(name string) error {
	if name == "" {
		return fmt.Errorf("migration name is required")
	}

	fileName := migrationfile.FileName(1, name, "up")
	if strings.HasPrefix(fileName, "0000000001_.") {
		return fmt.Errorf("migration name must contain letters, digits, or underscores")
	}

	return nil
}

func emptyMigrationSQL(name, generatedAt, direction string) string {
	return fmt.Sprintf(`-- Migration: %s
-- Generated on: %s
-- Direction: %s

-- Add your migration SQL here.
`, name, generatedAt, direction)
}

// GenerateMigration generates both up and down migration files by comparing
// the desired schema (from Go entities) with the current database state.
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
	diff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		desired,
		dbSchema,
		compareOptionsWithDiffPolicy(opts.CompareOptions, opts.DiffPolicy),
	)
	if err != nil {
		return nil, fmt.Errorf("error comparing generated and database schemas: %w", err)
	}

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

	specs, assessments, err := planGeneratedMigrationSpecs(diff, desired, dbSchema, info, version, opts.MigrationName, opts.DiffPolicy, qualifier)
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

func verifyPlannedShadowMigration(
	ctx context.Context,
	opts GenerateMigrationOptions,
	conn *dbschema.DatabaseConnection,
	info catalog.ServerInfo,
	diff *difftypes.SchemaDiff,
	specs []generatedMigrationSpec,
	desired *schemamodel.Database,
) error {
	if opts.ShadowDatabaseURL == "" {
		return nil
	}
	return shadow.VerifyMigration(ctx, shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: opts.ShadowDatabaseURL,
		TargetConnection:  conn,
		MigrationsDir:     opts.OutputDir,
		MigrationsFS:      opts.PriorMigrationsFS,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
		IdentifierSemantics: cloneIdentifierSemanticsValue(
			diff.IdentifierSemantics,
		),
		Candidates:  shadowCandidatesFromSpecs(specs),
		Generated:   desired,
		CompareOpts: opts.CompareOptions,
		Schemas:     opts.Schemas,
	})
}

func captureAuthorizedPriorMigrations(fsys fs.FS) (fsnapshot.Snapshot, error) {
	if fsys == nil {
		return fsnapshot.Snapshot{}, nil
	}
	snapshot, err := migrationsnapshot.Capture(fsys)
	if err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("capture authorized prior migrations: %w", err)
	}
	return snapshot, nil
}

func resolvePlanDatabaseConnection(
	ctx context.Context,
	opts GenerateMigrationOptions,
) (*dbschema.DatabaseConnection, bool, error) {
	if opts.DBConn != nil {
		return opts.DBConn, false, nil
	}
	// The connect budget is spent here and released here. Deferring the cancel
	// in this function rather than in the caller is what keeps it off the rest
	// of the generation, which is the whole distinction the field exists to
	// draw.
	connectCtx, cancelConnect := connectContextFor(ctx, opts)
	defer cancelConnect()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.DatabaseURL)
	if err != nil {
		return nil, false, fmt.Errorf("error connecting to database: %w", err)
	}
	return conn, true, nil
}

// connectContextFor derives the context the connect runs under.
//
// Split from its caller so the derivation can be checked without a server and
// without a clock: a test asks whether the returned context carries a deadline,
// which is decided when it is built. Asserting that a small budget *fires*
// would depend on timer granularity -- on Windows `time.Now()` can return the
// same instant either side of a nanosecond budget, so the deadline is scheduled
// rather than already past and a local connect finishes first
// (stokaro/ptah#1749).
func connectContextFor(
	ctx context.Context,
	opts GenerateMigrationOptions,
) (context.Context, context.CancelFunc) {
	return connectContext(ctx, opts.ConnectTimeout)
}

// connectContext bounds a connect attempt by the configured budget, and leaves
// the context unbounded when no budget was set. The cancel function is returned
// in both cases, so every caller releases the context the same way.
func connectContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// resolveDesiredSchema answers what the migration should bring the database to:
// a pre-merged schema when the caller assembled one from several sources, and
// otherwise the Go entities directory, parsed through a filesystem rooted at its
// parent so the scan cannot walk out of the directory the caller named.
func resolveDesiredSchema(opts GenerateMigrationOptions) (*schemamodel.Database, error) {
	if opts.Generated != nil {
		return opts.Generated, nil
	}
	entitiesFS := opts.GoEntitiesFS
	entitiesDir := opts.GoEntitiesDir
	if entitiesFS == nil {
		absPath, err := filepath.Abs(opts.GoEntitiesDir)
		if err != nil {
			return nil, fmt.Errorf("error resolving root directory path: %w", err)
		}
		entitiesFS = os.DirFS(filepath.Dir(absPath))
		entitiesDir = filepath.Base(absPath)
	}
	desired, err := goschema.ParseFS(entitiesFS, entitiesDir)
	if err != nil {
		return nil, fmt.Errorf("error parsing Go entities: %w", err)
	}
	return desired, nil
}

// recoverMigrationPublication resolves an interrupted publication left by an
// earlier run, before this one starts planning. It resolves the directory by
// pathname because it is the start of its own transaction rather than a step
// inside this one; the levels above it are still created through the confining
// root, so a recovery run cannot materialize directories outside it.
func recoverMigrationPublication(
	ctx context.Context,
	allowedOutputRoot, outputDir string,
) error {
	root, err := openOutputRoot(allowedOutputRoot)
	if err != nil {
		return err
	}
	return errors.Join(
		recoverMigrationPublicationWithin(ctx, root, outputDir),
		closeOutputRoot(root),
	)
}

func recoverMigrationPublicationWithin(
	ctx context.Context,
	root *pathguard.OpenedDirectory,
	outputDir string,
) error {
	if err := atlasmigrate.EnsureMigrationParent(root, outputDir); err != nil {
		return err
	}
	if err := atlasmigrate.RecoverPendingPublication(ctx, outputDir); err != nil {
		return fmt.Errorf("recover migration publication before planning: %w", err)
	}
	return nil
}

// Close releases the plan's claim on the migration directory without
// publishing anything. It is what a caller that decides not to publish should
// use, and it is safe to defer next to PlanMigration: publishing already
// releases the handles, so a Close after WriteFilesContext does nothing.
// Closing twice is likewise a no-op, and Close never reports an error, so it
// composes with defer.
//
// A plan holds the directory open from the moment it is built. Without this
// call an abandoned plan keeps holding it until the garbage collector runs a
// finalizer, and on Windows an open directory handle blocks removing or
// renaming that directory -- so the release point has to be one the caller
// chooses rather than one the runtime chooses (stokaro/ptah#1549).
func (p *MigrationPlan) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.release()
}

// WriteFiles publishes the migration artifacts represented by the plan. A plan
// is single-use after a successful publication.
func (p *MigrationPlan) WriteFiles() (*MigrationFiles, error) {
	return p.WriteFilesContext(context.Background())
}

// WriteFilesContext publishes the migration artifacts represented by the plan.
// The context bounds waiting for the migration-directory publication lock.
//
// The plan already holds the migration directory. This call does not reopen it:
// under the lock it revalidates the handle it was given, compares the contents
// against what planning recorded, and publishes through that same handle.
//
// One call is one use of the plan. Whatever this call returns, it releases the
// migration directory handles before returning, so a failed publication ends
// the plan's hold on the directory at a moment the caller can observe instead
// of at the next garbage collection. A plan whose attempt already happened is
// reported rather than retried: its recorded contents and its chosen version
// both describe a directory as it was before the attempt, so the honest retry
// is a fresh PlanMigration.
//
// It used to reopen the directory by pathname here and decide whether it was
// still the planned one by comparing an fs.FileInfo captured before any handle
// existed. That comparison is only as good as the operating system's promise
// not to reissue an identifier, and it makes no such promise: measured on ext4,
// a directory removed and recreated at the same pathname took its inode number
// back in 20 of 20 cycles, so the guard stayed silent on exactly the
// substitution an attacker performs most easily (stokaro/ptah#1118).
func (p *MigrationPlan) WriteFilesContext(ctx context.Context) (*MigrationFiles, error) {
	if p == nil {
		return nil, fmt.Errorf("migration plan is nil")
	}
	if !p.mu.TryLock() {
		return nil, ErrMigrationPlanInUse
	}
	defer p.mu.Unlock()
	if p.written {
		return nil, fmt.Errorf("migration plan has already been written")
	}
	if p.closed {
		return nil, fmt.Errorf("migration plan was closed")
	}
	if p.dir == nil {
		return nil, fmt.Errorf("migration plan was released by a failed publication")
	}
	// The plan is single-use, so the handles have no reader left once this call
	// returns -- on the failure paths as much as on the successful one.
	defer p.release()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var files *MigrationFiles
	err := atlasmigrate.WithMigrationDirectoryLock(ctx, p.outputDir, 0, func(context.Context) error {
		published, publishErr := p.publishLocked(ctx)
		files = published
		return publishErr
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// release closes the migration directory handles the plan holds and marks the
// plan spent. It is idempotent and runs with p.mu held.
//
// Deterministic release is the point. os.Root closes its descriptor from a
// finalizer, so an unreleased handle survives until the next collection; on
// Windows that is also the window in which nothing else can rename or remove
// the migration directory, and a failed publication would otherwise leave that
// window open with no event that ends it (stokaro/ptah#1118).
func (p *MigrationPlan) release() {
	if p.dir == nil {
		return
	}
	_ = p.dir.Close()
	p.dir = nil
}

func (p *MigrationPlan) publishLocked(ctx context.Context) (*MigrationFiles, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := p.dir.Revalidate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMigrationDirectoryChanged, err)
	}
	currentContents, err := captureMigrationDirectoryContents(p.dir)
	if err != nil {
		return nil, fmt.Errorf("capture migration directory before publication: %w", err)
	}
	// The contents check is the concurrency half of the guard: another writer
	// that added a migration while this plan was outstanding. Which filesystem
	// object is being committed to was settled by Revalidate above.
	if !p.plannedContents.Equal(currentContents) {
		return nil, ErrMigrationDirectoryChanged
	}
	if err := p.verifyAuthorizedPriorMigrations(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	notifyMigrationPublicationVerified()
	files, err := publishPlannedMigration(ctx, p.dir, p.reportFormat, p.specs)
	if err != nil {
		return nil, fmt.Errorf("error creating migration files: %w", err)
	}
	p.written = true
	return files, nil
}

func (p *MigrationPlan) verifyAuthorizedPriorMigrations() error {
	if !p.hasAuthorizedPriorMigrations {
		return nil
	}
	var currentMigrations fsnapshot.Snapshot
	if p.dir.Exists() {
		fsys, err := p.dir.FS()
		if err != nil {
			return fmt.Errorf("open migration directory before publication: %w", err)
		}
		currentMigrations, err = migrationsnapshot.Capture(fsys)
		if err != nil {
			return fmt.Errorf("capture migration directory before publication: %w", err)
		}
	}
	if !p.authorizedPriorMigrations.Equal(currentMigrations) {
		return ErrMigrationDirectoryChanged
	}
	return nil
}

// captureMigrationDirectoryContents reads the migration directory through the
// bound handle, so what the publication compares is the object it is about to
// commit to rather than whatever the pathname resolves to at comparison time.
//
// A directory the writer bound as absent reads as the empty snapshot. It cannot
// have appeared since -- Revalidate refuses that before this runs -- so the two
// captures either both describe the bound object or both describe nothing.
func captureMigrationDirectoryContents(
	writer *atlasmigrate.MigrationWriter,
) (fsnapshot.Snapshot, error) {
	if !writer.Exists() {
		return fsnapshot.Snapshot{}, nil
	}
	fsys, err := writer.FS()
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	snapshot, err := migrationsnapshot.CaptureStable(fsys)
	if err != nil {
		return fsnapshot.Snapshot{}, explainMigrationDirectoryRead(writer, err)
	}
	return snapshot, nil
}

// explainMigrationDirectoryRead names the entries that make a migration
// directory unreadable through the handle the run bound, when that is what went
// wrong. Any other failure is returned unchanged.
//
// The reachable cause is a migration file that is a symbolic link out of the
// migration directory -- a shared migration linked in from elsewhere. Reading
// the directory by pathname followed such a link, and reading it through the
// bound handle does not, so this is a refusal rather than an accident: every
// read, checksum and publication of the directory goes through the object the
// run opened, and a file whose bytes live outside it cannot be part of a
// directory Ptah is willing to seal (stokaro/ptah#1118). A link that resolves
// inside the migration directory stays supported and never reaches this.
//
// The diagnosis runs only after a failed capture, so the successful path pays
// nothing for it.
func explainMigrationDirectoryRead(writer *atlasmigrate.MigrationWriter, cause error) error {
	escaping := escapingMigrationEntries(writer)
	if len(escaping) == 0 {
		return cause
	}
	return fmt.Errorf(
		"migration directory %s: symbolic links resolving outside it: %s;"+
			" a migration file linked in from another directory is refused because"+
			" the whole directory is read, checksummed and published through the"+
			" directory itself: %w",
		writer.Path(),
		strings.Join(escaping, ", "),
		cause,
	)
}

// escapingMigrationEntries lists the migration directory's symbolic links that
// do not resolve inside it, asked through the bound handle: the link itself is
// visible as an entry, and a stat that cannot follow it is the escape.
func escapingMigrationEntries(writer *atlasmigrate.MigrationWriter) []string {
	entries, err := writer.Entries()
	if err != nil {
		return nil
	}
	fsys, err := writer.FS()
	if err != nil {
		return nil
	}
	var escaping []string
	for _, entry := range entries {
		if entry.Type()&fs.ModeSymlink == 0 {
			continue
		}
		if _, statErr := fs.Stat(fsys, entry.Name()); statErr != nil {
			escaping = append(escaping, entry.Name())
		}
	}
	return escaping
}

func normalizeGenerateMigrationOptions(opts GenerateMigrationOptions) (GenerateMigrationOptions, error) {
	if opts.MigrationName == "" {
		opts.MigrationName = "migration"
	}
	dialect := ""
	if opts.DBConn != nil {
		dialect = opts.DBConn.Info().Dialect
	} else if resolvedDialect, dialectErr := atlasurl.DialectFromURL(opts.DatabaseURL); dialectErr == nil {
		dialect = resolvedDialect
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return opts, err
	}
	outputDir, err := pathguard.ResolveWithinRoot(opts.OutputDir, opts.AllowedOutputRoot)
	if err != nil {
		return opts, fmt.Errorf("error validating output directory: %w", err)
	}
	opts.OutputDir = outputDir
	return opts, nil
}

func checkDestructiveAllowed(opts GenerateMigrationOptions, assessments []safety.StatementAssessment) error {
	if opts.CheckDestructive && safety.HasDestructiveAssessment(assessments) && !opts.AllowDestructive {
		return fmt.Errorf("destructive migration statements require AllowDestructive")
	}
	return nil
}

// compareOptionsWithDiffPolicy tells the comparison what
// [planGeneratedMigrationSpecs] will do to its answer.
//
// The SQLite virtual-table guard runs inside the comparison and refuses on the
// statements it predicts, while the skip filter that deletes those statements
// runs afterwards, here. Without this the comparison refused a plan the policy
// had already emptied (stokaro/ptah#1028). The caller's options are copied
// rather than written through: GenerateMigrationOptions is a value the caller
// may reuse for another run.
//
// Every skip kind the guard can read is forwarded, not only the table drop.
// `drop_column` empties a table diff's ColumnsRemoved, which is one of the
// fields the rebuild predicate reads, and `drop_index` empties the standalone
// index removals the guard counts the owning table for; forwarding one of the
// three left the other two refusing plans this function had already emptied.
// diffpolicy.DropEnum is the one kind deliberately not forwarded: SQLite has no
// enum type and the guard reads no enum field, and the census in
// internal/sqlitevirtual fails if a new kind appears unclassified.
func compareOptionsWithDiffPolicy(
	opts *config.CompareOptions,
	policy DiffPolicy,
) *config.CompareOptions {
	merged := config.DefaultCompareOptions()
	if opts != nil {
		*merged = *opts
		merged.IgnoredExtensions = slices.Clone(opts.IgnoredExtensions)
	}
	merged.SkipTableDrops = slices.Contains(policy.SkipChangeKinds, diffpolicy.DropTable)
	merged.SkipColumnDrops = slices.Contains(policy.SkipChangeKinds, diffpolicy.DropColumn)
	merged.SkipIndexDrops = slices.Contains(policy.SkipChangeKinds, diffpolicy.DropIndex)
	return merged
}

type generatedMigrationSpec struct {
	Version       int64
	Name          string
	UpSQL         string
	DownSQL       string
	Assessments   []safety.StatementAssessment
	NoTransaction bool
}

func planGeneratedMigrationSpecs(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
	version int64,
	migrationName string,
	policy DiffPolicy,
	qualifier atlasmigrate.Qualifier,
) ([]generatedMigrationSpec, []safety.StatementAssessment, error) {
	// Apply the diff policy once, up front, BEFORE any concurrent-index split.
	// The split separates an index redefinition's added and removed entries into
	// different sub-diffs; if the skip filter ran per sub-diff after the split,
	// it would mistake the orphaned removal for a genuine standalone drop and
	// skip it, silently discarding the redefinition. Filtering here keeps the
	// added/removed pair together, and downstream planning runs with an empty
	// planner-level skip. The omitted changes are surfaced as leading comments.
	var skipped []diffpolicy.SkippedChange
	if skipSet := diffpolicy.NewSkipSet(policy.SkipChangeKinds...); !skipSet.Empty() {
		diff, skipped = diffpolicy.ApplyForDialect(diff, skipSet, info.Dialect)
	}

	bidirectional, err := PlanBidirectionalSchemaDiff(BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: dbSchema,
		Dialect:       info.Dialect,
		Capabilities:  info.Capabilities,
		Policy:        bidirectionalPlanPolicy(policy),
	})
	if err != nil {
		return nil, nil, err
	}
	upNodes := bidirectional.Forward.Nodes
	if len(upNodes) == 0 {
		return nil, nil, nil
	}
	requiresNoTransaction := bidirectional.Forward.RequiresNoTransaction
	if !requiresNoTransaction {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Plan:      bidirectional,
			Qualifier: qualifier,
			Version:   version,
			Name:      migrationName,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return withSkipComments([]generatedMigrationSpec{spec}, skipped), assessments, nil
	}

	nodeGroups := splitNoTransactionNodes(info.Dialect, upNodes)
	if len(nodeGroups.transactional) == 0 {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Plan:      bidirectional,
			Qualifier: qualifier,
			Version:   version,
			Name:      migrationName,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return withSkipComments([]generatedMigrationSpec{spec}, skipped), assessments, nil
	}
	if containsUnsplittableNoTransactionNode(nodeGroups.noTransaction) {
		return nil, nil, txrequire.UnsplittableMixError(nodeGroups.noTransaction)
	}

	// Two splits, composed, and the order of the files they produce is the
	// order the statements have to run in. Enum values are pulled out first and
	// LEAD, because `ALTER TYPE ... ADD VALUE` must be committed before any
	// statement that uses the value -- PostgreSQL answers 55P04 otherwise. The
	// concurrent indexes come out of what is left and FOLLOW, because an index
	// is built after the table it indexes.
	//
	// A plan with no enum additions produces exactly the two files it produced
	// before: the leading group has no changes and is skipped.
	enumGroups := splitEnumValueAdditionDiff(diff)
	indexGroups := splitConcurrentIndexDiff(
		enumGroups.transactional,
		bidirectional.Forward.ConcurrentIndexRefs,
		bidirectional.Forward.ConcurrentIndexDropRefs,
	)
	ordered := []struct {
		diff   *difftypes.SchemaDiff
		suffix string
	}{
		{enumGroups.noTransaction, "_enum_values"},
		{indexGroups.transactional, "_transactional"},
		{indexGroups.noTransaction, "_concurrent_indexes"},
	}

	specs := make([]generatedMigrationSpec, 0, len(ordered))
	allAssessments := make([]safety.StatementAssessment, 0)
	for _, group := range ordered {
		if !group.diff.HasChanges() {
			continue
		}
		plan, err := bidirectionalSubplan(bidirectional, group.diff)
		if err != nil {
			return nil, nil, err
		}
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Plan:      plan,
			Qualifier: qualifier,
			Version:   version,
			Name:      migrationName + group.suffix,
		})
		if err != nil {
			return nil, nil, err
		}
		if spec.UpSQL == "" {
			continue
		}
		specs = append(specs, spec)
		allAssessments = append(allAssessments, assessments...)
		version++
	}
	return withSkipComments(specs, skipped), allAssessments, nil
}

func bidirectionalPlanPolicy(policy DiffPolicy) BidirectionalPlanPolicy {
	createMode := ConcurrentIndexAutomatic
	if policy.ConcurrentIndex {
		createMode = ConcurrentIndexAll
	}
	dropMode := ConcurrentIndexDisabled
	if policy.ConcurrentIndexDrop {
		dropMode = ConcurrentIndexAll
	}
	return BidirectionalPlanPolicy{Create: createMode, Drop: dropMode}
}

// withSkipComments prepends the diff-policy omission comments to the first
// generated spec so the audit trail is visible in the migration. When every
// change was skipped there is no spec to attach to and the comments are dropped
// along with the (empty) migration.
func withSkipComments(specs []generatedMigrationSpec, skipped []diffpolicy.SkippedChange) []generatedMigrationSpec {
	if len(specs) == 0 || len(skipped) == 0 {
		return specs
	}
	var block strings.Builder
	for _, change := range skipped {
		block.WriteString("-- ")
		block.WriteString(change.Comment())
		block.WriteByte('\n')
	}
	specs[0].UpSQL = block.String() + specs[0].UpSQL
	return specs
}

type generatedMigrationSpecOptions struct {
	Plan      *BidirectionalSchemaPlan
	Version   int64
	Name      string
	Qualifier atlasmigrate.Qualifier
}

func buildGeneratedMigrationSpec(opts generatedMigrationSpecOptions) (generatedMigrationSpec, []safety.StatementAssessment, error) {
	if opts.Plan == nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("bidirectional migration plan is nil")
	}
	upNodes := opts.Plan.Forward.Nodes
	if err := opts.Qualifier.ApplyToPlan(opts.Plan.Dialect, opts.Plan.DesiredSchema, upNodes); err != nil {
		return generatedMigrationSpec{}, nil, err
	}
	assessments, err := safety.AssessRenderedWithCapabilities(
		upNodes,
		opts.Plan.Dialect,
		opts.Plan.Capabilities,
	)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error assessing migration safety: %w", err)
	}
	upDirectiveOpts := generatedDirectiveOptions{skipTimeouts: opts.Plan.Forward.RequiresNoTransaction}
	upSQL, err := renderGeneratedMigrationSQL(
		upNodes,
		opts.Plan.Dialect,
		opts.Plan.Capabilities,
		"UP",
		upDirectiveOpts,
	)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating up migration SQL: %w", err)
	}
	if upSQL == "" {
		return generatedMigrationSpec{}, assessments, nil
	}
	if opts.Plan.Forward.RequiresNoTransaction {
		upSQL = withNoTransactionDirective(upSQL)
	}

	downSQL, err := renderGeneratedDownMigrationSQL(opts.Plan, opts.Qualifier)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if opts.Plan.Reverse.RequiresNoTransaction {
		downSQL = withNoTransactionDirective(downSQL)
	}

	return generatedMigrationSpec{
		Version:       opts.Version,
		Name:          opts.Name,
		UpSQL:         upSQL,
		DownSQL:       downSQL,
		Assessments:   assessments,
		NoTransaction: opts.Plan.Forward.RequiresNoTransaction || opts.Plan.Reverse.RequiresNoTransaction,
	}, assessments, nil
}

func bidirectionalSubplan(
	full *BidirectionalSchemaPlan,
	diff *difftypes.SchemaDiff,
) (*BidirectionalSchemaPlan, error) {
	createRefs := selectIndexRefOccurrences(
		diff.IndexAdditions(),
		indexRefSet(full.Forward.ConcurrentIndexRefs),
	)
	dropRefs := selectIndexRefOccurrences(
		diff.IndexRemovals(),
		indexRefSet(full.Forward.ConcurrentIndexDropRefs),
	)
	return planBidirectionalSchemaDiffWithRefs(BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: full.DesiredSchema,
		CurrentSchema: full.CurrentSchema,
		Dialect:       full.Dialect,
		Capabilities:  full.Capabilities,
		Policy:        full.Policy,
	}, full.Dialect, full.Capabilities, createRefs, dropRefs)
}

func renderGeneratedDownMigrationSQL(
	plan *BidirectionalSchemaPlan,
	qualifier atlasmigrate.Qualifier,
) (string, error) {
	priorSchema := dbschematogo.ConvertDBSchemaToGoSchema(plan.CurrentSchema)
	nodes := plan.Reverse.Nodes
	if err := qualifier.ApplyToPlan(plan.Dialect, priorSchema, nodes); err != nil {
		return "", err
	}
	output, err := renderer.RenderSQLWithCapabilities(plan.Dialect, plan.Capabilities, nodes...)
	if err != nil {
		return "", err
	}
	statements := sqlutil.SplitSQLStatementsForDialect(output, plan.Dialect)
	if len(statements) == 0 {
		return fmt.Sprintf(
			"-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n-- No rollback operations needed\n",
			time.Now().Format(time.RFC3339),
		), nil
	}
	header := fmt.Sprintf(
		"-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n",
		time.Now().Format(time.RFC3339),
	)
	directives := generatedDirectiveOptions{skipTimeouts: plan.Reverse.RequiresNoTransaction}
	return withGeneratedTimeoutDirectivesForOptions(
		header+strings.Join(statements, ";\n")+";",
		plan.Dialect,
		directives,
	), nil
}

func renderGeneratedMigrationSQL(
	nodes []ast.Node,
	dialect string,
	caps capability.Capabilities,
	direction string,
	directiveOpts generatedDirectiveOptions,
) (string, error) {
	rawSQL, err := renderer.RenderSQLWithCapabilities(dialect, caps, nodes...)
	if err != nil {
		return "", err
	}
	statements := sqlutil.SplitSQLStatementsForDialect(rawSQL, dialect)
	if len(statements) == 0 || !hasActualSQLStatements(statements) {
		return "", nil
	}
	header := fmt.Sprintf("-- Migration generated from schema differences\n-- Generated on: %s\n-- Direction: %s\n\n",
		time.Now().Format(time.RFC3339), direction)
	return withGeneratedTimeoutDirectivesForOptions(header+strings.Join(statements, ";\n")+";", dialect, directiveOpts), nil
}

type splitMigrationNodes struct {
	transactional []ast.Node
	noTransaction []ast.Node
}

func splitNoTransactionNodes(dialect string, nodes []ast.Node) splitMigrationNodes {
	txNodes := make([]ast.Node, 0, len(nodes))
	noTxNodes := make([]ast.Node, 0)
	for _, node := range nodes {
		if planner.NodeRequiresNoTransaction(dialect, node) {
			noTxNodes = append(noTxNodes, node)
			continue
		}
		txNodes = append(txNodes, node)
	}
	return splitMigrationNodes{transactional: txNodes, noTransaction: noTxNodes}
}

// containsUnsplittableNoTransactionNode reports whether any statement that must
// leave the transactional file is one the generator has no ordered place for.
func containsUnsplittableNoTransactionNode(nodes []ast.Node) bool {
	for _, node := range nodes {
		if txrequire.Kind(node) == txrequire.KindUnsplittable {
			return true
		}
	}
	return false
}

// concurrentIndexRefsForPolicy resolves which newly added indexes are built
// concurrently. When the diff policy requests it, every newly added index is
// concurrent (still gated on dialect and the CreateIndexConcurrently
// capability); otherwise the populated-table heuristic applies.
//
// A partitioned parent is refused rather than published: PostgreSQL rejects
// CREATE INDEX CONCURRENTLY on relkind 'p' with SQLSTATE 0A000, so the request
// cannot be honored and silently downgrading it would give a project that asked
// for a non-blocking build a blocking one without saying so. The heuristic path
// downgrades instead -- see [concurrentIndexRefsForPopulatedTables].
func concurrentIndexRefsForPolicy(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
	policy DiffPolicy,
) ([]difftypes.IndexRef, error) {
	if !policy.ConcurrentIndex {
		return concurrentIndexRefsForPopulatedTables(diff, dbSchema, info), nil
	}
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil, nil
	}
	refs := diff.IndexAdditions()
	if err := refusePartitionedConcurrentIndexRefs(refs, dbSchema, concurrentIndexCreatePolicy); err != nil {
		return nil, err
	}
	return refs, nil
}

// concurrentIndexPolicyKind names the half of the concurrent-index policy a
// refusal came from, so the diagnostic can quote the configuration key the
// operator would change.
type concurrentIndexPolicyKind struct {
	statement string
	configKey string
}

var (
	concurrentIndexCreatePolicy = concurrentIndexPolicyKind{
		statement: "CREATE INDEX CONCURRENTLY",
		configKey: "diff.concurrent_index.create",
	}
	concurrentIndexDropPolicy = concurrentIndexPolicyKind{
		statement: "DROP INDEX CONCURRENTLY",
		configKey: "diff.concurrent_index.drop",
	}
)

// refusePartitionedConcurrentIndexRefs fails generation before any migration
// file is written when an explicitly requested concurrent index statement names
// a PostgreSQL partitioned parent.
//
// PostgreSQL answers both statements with SQLSTATE 0A000 on relkind 'p'
// ("cannot create index on partitioned table ... concurrently", "cannot drop
// partitioned index ... concurrently"), and it answers at execution time -- so
// without this the plan is written, hashed, and committed, and the failure
// arrives against a production database instead of against the developer who
// generated it.
func refusePartitionedConcurrentIndexRefs(
	refs []difftypes.IndexRef,
	dbSchema *catalog.Database,
	kind concurrentIndexPolicyKind,
) error {
	tables := concurrentindex.IndexTableFacts(dbSchema)
	var offending []string
	for _, ref := range refs {
		facts, known := tables.Lookup(ref.TableName)
		if !known || !facts.Partitioned {
			continue
		}
		offending = append(offending, fmt.Sprintf("%q on %q", ref.Name, ref.TableName))
	}
	if len(offending) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s requested by %s cannot be generated for partitioned table(s): %s; "+
			"PostgreSQL refuses a concurrent index statement on a partitioned parent (SQLSTATE 0A000). "+
			"Unset %s to generate the plain, transactional statement, or manage the index per partition "+
			"(CREATE INDEX ... ON ONLY the parent, CREATE INDEX CONCURRENTLY on each partition, then ALTER INDEX ... ATTACH PARTITION)",
		kind.statement,
		kind.configKey,
		strings.Join(offending, ", "),
		kind.configKey,
	)
}

// concurrentIndexDropRefsForPolicy resolves which index removals are dropped
// concurrently in the UP direction. Unlike builds there is no populated-table
// heuristic: a concurrent drop happens only when the project asks for one, so
// the default output is byte-identical to before this policy existed.
//
// A removal that is also an addition under the same identity is a redefinition
// whose drop the planner pairs with the rebuild; it is excluded here so the
// pair is never split across a transactional and a non-transactional file.
//
// A UNIQUE constraint's backing index is excluded for a different reason: it is
// not dropped as an index at all (the planner spells it
// ALTER TABLE ... DROP CONSTRAINT), and PostgreSQL has no concurrent form of
// that statement. Routing it into the no-transaction file would also strand the
// marker, which the no-transaction diff does not carry.
func concurrentIndexDropRefsForPolicy(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
	policy DiffPolicy,
) ([]difftypes.IndexRef, error) {
	if !policy.ConcurrentIndexDrop {
		return nil, nil
	}
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.DropIndexConcurrently) {
		return nil, nil
	}
	// Match the planner's own redefinition test (indexscope conflict semantics),
	// not plain struct equality: two refs differing only in identifier case are
	// the same index on a case-insensitive target, and treating them as distinct
	// here would route a rebuild's drop into the wrong migration file.
	additions := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(info.Dialect),
		diff.IndexAdditions(),
	)
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()
	var refs []difftypes.IndexRef
	for _, ref := range diff.IndexRemovals() {
		if additions.Contains(ref) {
			continue
		}
		if _, ownedByConstraint := constraintBacked[ref]; ownedByConstraint {
			continue
		}
		refs = append(refs, ref)
	}
	if err := refusePartitionedConcurrentIndexRefs(refs, dbSchema, concurrentIndexDropPolicy); err != nil {
		return nil, err
	}
	return refs, nil
}

// concurrentIndexRefsForPopulatedTables is the default heuristic: build an
// index concurrently when the table it targets already holds rows.
//
// A partitioned parent is excluded rather than refused. Nothing asked for a
// concurrent build here, PostgreSQL supports no concurrent form for relkind
// 'p', and the plain CREATE INDEX the exclusion selects is legal SQL that says
// what it does -- lint reports it as PG101 exactly like any other blocking
// build. Refusing instead would leave a project with a partitioned table unable
// to generate an index migration at all.
func concurrentIndexRefsForPopulatedTables(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	info catalog.ServerInfo,
) []difftypes.IndexRef {
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil
	}
	tables := concurrentindex.IndexTableFacts(dbSchema)
	var refs []difftypes.IndexRef
	for _, ref := range diff.IndexAdditions() {
		facts, known := tables.Lookup(ref.TableName)
		if !known || facts.Partitioned || !facts.Populated {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

type splitSchemaDiffs struct {
	transactional *difftypes.SchemaDiff
	noTransaction *difftypes.SchemaDiff
}

func splitConcurrentIndexDiff(
	diff *difftypes.SchemaDiff,
	concurrentIndexRefs,
	concurrentIndexDropRefs []difftypes.IndexRef,
) splitSchemaDiffs {
	txDiff := cloneSchemaDiff(diff)
	noTxDiff := &difftypes.SchemaDiff{
		IdentifierSemantics: cloneIdentifierSemantics(diff.IdentifierSemantics),
	}
	addTx, addNoTx := partitionIndexRefs(diff.IndexAdditions(), concurrentIndexRefs)
	txDiff.SetIndexAdditions(addTx)
	noTxDiff.SetIndexAdditions(addNoTx)

	// Only rewrite the removal lists when something actually moves. SetIndexRemovals
	// re-sorts, so calling it unconditionally would reorder the drops in every
	// existing split migration for no reason.
	if len(concurrentIndexDropRefs) > 0 {
		dropTx, dropNoTx := partitionIndexRefs(diff.IndexRemovals(), concurrentIndexDropRefs)
		txDiff.SetIndexRemovals(dropTx)
		noTxDiff.SetIndexRemovals(dropNoTx)
	}
	return splitSchemaDiffs{transactional: txDiff, noTransaction: noTxDiff}
}

// partitionIndexRefs splits refs into the ones that stay in the transactional
// migration and the ones that move to the no_transaction migration, preserving
// the input order within each group so file contents stay deterministic.
func partitionIndexRefs(refs, selected []difftypes.IndexRef) (transactional, noTransaction []difftypes.IndexRef) {
	set := indexRefSet(selected)
	for _, ref := range refs {
		if _, ok := set[ref]; ok {
			noTransaction = append(noTransaction, ref)
			continue
		}
		transactional = append(transactional, ref)
	}
	return transactional, noTransaction
}

func indexRefSet(values []difftypes.IndexRef) map[difftypes.IndexRef]struct{} {
	out := make(map[difftypes.IndexRef]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func cloneSchemaDiff(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	clone := *diff
	clone.IdentifierSemantics = cloneIdentifierSemantics(diff.IdentifierSemantics)
	clone.TablesAdded = slices.Clone(diff.TablesAdded)
	clone.TablesRemoved = slices.Clone(diff.TablesRemoved)
	clone.TablesModified = slices.Clone(diff.TablesModified)
	clone.EnumsAdded = slices.Clone(diff.EnumsAdded)
	clone.EnumsRemoved = slices.Clone(diff.EnumsRemoved)
	clone.EnumsModified = slices.Clone(diff.EnumsModified)
	clone.IndexesAdded = slices.Clone(diff.IndexesAdded)
	clone.IndexesRemoved = slices.Clone(diff.IndexesRemoved)
	clone.ConstraintBackedIndexRemovals = slices.Clone(diff.ConstraintBackedIndexRemovals)
	clone.ExtensionsAdded = slices.Clone(diff.ExtensionsAdded)
	clone.ExtensionsRemoved = slices.Clone(diff.ExtensionsRemoved)
	clone.ExtensionsModified = slices.Clone(diff.ExtensionsModified)
	clone.FunctionsAdded = slices.Clone(diff.FunctionsAdded)
	clone.FunctionsRemoved = slices.Clone(diff.FunctionsRemoved)
	clone.FunctionsModified = slices.Clone(diff.FunctionsModified)
	clone.SequencesAdded = slices.Clone(diff.SequencesAdded)
	clone.SequencesRemoved = slices.Clone(diff.SequencesRemoved)
	clone.SequencesModified = slices.Clone(diff.SequencesModified)
	clone.DomainsAdded = slices.Clone(diff.DomainsAdded)
	clone.DomainsRemoved = slices.Clone(diff.DomainsRemoved)
	clone.DomainsModified = slices.Clone(diff.DomainsModified)
	clone.CompositeTypesAdded = slices.Clone(diff.CompositeTypesAdded)
	clone.CompositeTypesRemoved = slices.Clone(diff.CompositeTypesRemoved)
	clone.CompositeTypesModified = slices.Clone(diff.CompositeTypesModified)
	clone.RangesAdded = slices.Clone(diff.RangesAdded)
	clone.RangesRemoved = slices.Clone(diff.RangesRemoved)
	clone.RangesModified = slices.Clone(diff.RangesModified)
	clone.ViewsAdded = slices.Clone(diff.ViewsAdded)
	clone.ViewsRemoved = slices.Clone(diff.ViewsRemoved)
	clone.ViewsModified = slices.Clone(diff.ViewsModified)
	clone.SynonymsAdded = slices.Clone(diff.SynonymsAdded)
	clone.SynonymsRemoved = slices.Clone(diff.SynonymsRemoved)
	clone.SynonymsModified = slices.Clone(diff.SynonymsModified)
	clone.HypertablesAdded = slices.Clone(diff.HypertablesAdded)
	clone.HypertablesRemoved = slices.Clone(diff.HypertablesRemoved)
	clone.HypertablesModified = slices.Clone(diff.HypertablesModified)
	clone.ContinuousAggregatesAdded = slices.Clone(diff.ContinuousAggregatesAdded)
	clone.ContinuousAggregatesRemoved = slices.Clone(diff.ContinuousAggregatesRemoved)
	clone.ContinuousAggregatesModified = slices.Clone(diff.ContinuousAggregatesModified)
	clone.ExtendedPropertiesAdded = slices.Clone(diff.ExtendedPropertiesAdded)
	clone.ExtendedPropertiesRemoved = slices.Clone(diff.ExtendedPropertiesRemoved)
	clone.ExtendedPropertiesModified = slices.Clone(diff.ExtendedPropertiesModified)
	clone.MaterializedViewsAdded = slices.Clone(diff.MaterializedViewsAdded)
	clone.MaterializedViewsRemoved = slices.Clone(diff.MaterializedViewsRemoved)
	clone.MaterializedViewsModified = slices.Clone(diff.MaterializedViewsModified)
	clone.TriggersAdded = slices.Clone(diff.TriggersAdded)
	clone.TriggersRemoved = slices.Clone(diff.TriggersRemoved)
	clone.TriggersModified = slices.Clone(diff.TriggersModified)
	clone.RLSPoliciesAdded = slices.Clone(diff.RLSPoliciesAdded)
	clone.RLSPoliciesRemoved = slices.Clone(diff.RLSPoliciesRemoved)
	clone.RLSPoliciesModified = slices.Clone(diff.RLSPoliciesModified)
	clone.RLSEnabledTablesAdded = slices.Clone(diff.RLSEnabledTablesAdded)
	clone.RLSEnabledTablesRemoved = slices.Clone(diff.RLSEnabledTablesRemoved)
	clone.RolesAdded = slices.Clone(diff.RolesAdded)
	clone.RolesRemoved = slices.Clone(diff.RolesRemoved)
	clone.RolesModified = slices.Clone(diff.RolesModified)
	clone.GrantsAdded = slices.Clone(diff.GrantsAdded)
	clone.GrantsRemoved = slices.Clone(diff.GrantsRemoved)
	clone.GrantOptionsAdded = slices.Clone(diff.GrantOptionsAdded)
	clone.GrantOptionsRevoked = slices.Clone(diff.GrantOptionsRevoked)
	clone.ConstraintsAdded = slices.Clone(diff.ConstraintsAdded)
	clone.ConstraintsAddedWithTables = slices.Clone(diff.ConstraintsAddedWithTables)
	clone.ConstraintsRemoved = slices.Clone(diff.ConstraintsRemoved)
	clone.ConstraintsRemovedWithTables = slices.Clone(diff.ConstraintsRemovedWithTables)
	clone.ForeignKeysRemovedWithTables = cloneForeignKeyRemovalInfos(diff.ForeignKeysRemovedWithTables)
	return &clone
}

func cloneForeignKeyRemovalInfos(values []difftypes.ForeignKeyRemovalInfo) []difftypes.ForeignKeyRemovalInfo {
	cloned := slices.Clone(values)
	for position := range cloned {
		cloned[position].Columns = slices.Clone(cloned[position].Columns)
		cloned[position].ForeignColumns = slices.Clone(cloned[position].ForeignColumns)
	}
	return cloned
}

func cloneIdentifierSemantics(
	semantics *identifier.Semantics,
) *identifier.Semantics {
	if semantics == nil {
		return nil
	}
	return new(semantics.Clone())
}

func cloneIdentifierSemanticsValue(
	semantics *identifier.Semantics,
) identifier.Semantics {
	if semantics == nil {
		return identifier.Semantics{}
	}
	return semantics.Clone()
}

func renderSafetyReport(
	upFile, format string,
	assessments []safety.StatementAssessment,
) (string, []byte, error) {
	var contents bytes.Buffer
	var reportFile string
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "html":
		reportFile = strings.TrimSuffix(upFile, ".up.sql") + ".safety.html"
		if err := safety.RenderHTML(&contents, assessments); err != nil {
			return "", nil, err
		}
	case "json":
		reportFile = strings.TrimSuffix(upFile, ".up.sql") + ".safety.json"
		if err := safety.RenderJSON(&contents, assessments); err != nil {
			return "", nil, err
		}
	default:
		return "", nil, fmt.Errorf("unsupported safety report format %q", format)
	}
	return reportFile, contents.Bytes(), nil
}

// hasActualSQLStatements checks if the statements contain actual SQL operations (not just comments)
func hasActualSQLStatements(statements []string) bool {
	for _, stmt := range statements {
		// Strip comments and check if there's any actual SQL content
		stripped := strings.TrimSpace(sqlutil.StripComments(stmt))
		if stripped != "" {
			return true
		}
	}
	return false
}

// generateUpMigrationSQL generates the SQL for the up migration.
func generateUpMigrationSQL(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateUpMigrationSQLWithOptions(diff, desired, dialect, generatedDirectiveOptions{}, capsOverride...)
}

type generatedDirectiveOptions struct {
	skipTimeouts bool
}

func generateUpMigrationSQLWithOptions(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	caps := capability.ForDialect(dialect)
	if len(capsOverride) > 0 {
		caps = capsOverride[0]
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		diff, desired, dialect,
		planner.Options{Capabilities: caps},
	)
	if err != nil {
		return "", fmt.Errorf("error generating up migration SQL: %w", err)
	}

	if len(statements) == 0 || !hasActualSQLStatements(statements) {
		// No actual SQL statements generated - this is a successful no-op operation
		return "", nil
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration generated from schema differences\n-- Generated on: %s\n-- Direction: UP\n\n",
		time.Now().Format(time.RFC3339))

	return withGeneratedTimeoutDirectivesForOptions(header+strings.Join(statements, ";\n")+";", dialect, directiveOpts), nil
}

// generateDownMigrationSQL generates the SQL for the down migration by reversing the diff.
func generateDownMigrationSQL(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateDownMigrationSQLWithOptions(diff, desired, dbSchema, dialect, generatedDirectiveOptions{}, capsOverride...)
}

func generateDownMigrationSQLWithOptions(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	opts := downMigrationOptions{directives: directiveOpts}
	if len(capsOverride) > 0 {
		opts.capabilities = capsOverride[0]
	}
	return generateDownMigrationSQLQualified(diff, desired, dbSchema, dialect, opts)
}

// downMigrationOptions carries the down-direction planning inputs that vary per
// caller. A nil capabilities set means "the dialect default preset".
type downMigrationOptions struct {
	directives   generatedDirectiveOptions
	qualifier    atlasmigrate.Qualifier
	capabilities capability.Capabilities
	// concurrentIndexRefs and concurrentIndexDropRefs are expressed in DOWN
	// direction terms: they name indexes the down file builds and drops, which
	// are the mirror image of the up file's own two sets.
	concurrentIndexRefs     []difftypes.IndexRef
	concurrentIndexDropRefs []difftypes.IndexRef
}

func generateDownMigrationSQLQualified(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	opts downMigrationOptions,
) (string, error) {
	directiveOpts := opts.directives
	// For down migrations, we need to use the current database schema as the "generated" schema
	// since we're reverting back to the current state
	dbAsGoSchema := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	// Create a reverse diff to generate down migration. We pass the original
	// generated schema to resolve table names for RLS policies, and the
	// introspected database schema so the reversed constraint additions can
	// rebuild the full prior body from the pre-change DB state — that is exactly
	// the definition the down must restore.
	reverseDiff := reverseSchemaDiffWithSchemaForDialect(diff, desired, dbSchema, dialect)
	if normalized := platform.NormalizeDialect(dialect); normalized == platform.MySQL || normalized == platform.MariaDB {
		forwardNodes, err := planner.GenerateSchemaDiffASTWithOptions(
			diff,
			desired,
			dialect,
			planner.Options{Capabilities: opts.capabilities},
		)
		if err != nil {
			return "", fmt.Errorf("error planning forward migration: %w", err)
		}
		if err := addMySQLFamilyForeignKeyBackingIndexRemovals(
			reverseDiff,
			diff,
			dbSchema,
			dialect,
			forwardNodes,
		); err != nil {
			return "", err
		}
	}

	plannerOpts := planner.Options{
		Capabilities:            opts.capabilities,
		ConcurrentIndexRefs:     opts.concurrentIndexRefs,
		ConcurrentIndexDropRefs: opts.concurrentIndexDropRefs,
	}
	statements, err := planDownMigrationStatements(reverseDiff, dbAsGoSchema, dialect, plannerOpts, opts.qualifier)
	if err != nil {
		return "", err
	}

	if len(statements) == 0 {
		// If no statements generated, create a simple comment
		header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n-- No rollback operations needed\n",
			time.Now().Format(time.RFC3339))
		return header, nil
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n",
		time.Now().Format(time.RFC3339))

	return withGeneratedTimeoutDirectivesForOptions(header+strings.Join(statements, ";\n")+";", dialect, directiveOpts), nil
}

// planDownMigrationStatements renders the reversed diff into ordered down
// statements. Without a qualifier it is the historical direct-render path;
// with one, the plan is generated as AST first so the qualifier rewrite runs
// before rendering, mirroring the up direction.
func planDownMigrationStatements(
	reverseDiff *difftypes.SchemaDiff,
	dbAsGoSchema *schemamodel.Database,
	dialect string,
	plannerOpts planner.Options,
	qualifier atlasmigrate.Qualifier,
) ([]string, error) {
	if qualifier.IsZero() {
		statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(reverseDiff, dbAsGoSchema, dialect, plannerOpts)
		if err != nil {
			return nil, fmt.Errorf("error generating down migration SQL: %w", err)
		}
		return statements, nil
	}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(reverseDiff, dbAsGoSchema, dialect, plannerOpts)
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if err := qualifier.ApplyToPlan(dialect, dbAsGoSchema, nodes); err != nil {
		return nil, err
	}
	output, err := renderer.RenderSQLWithCapabilities(dialect, plannerOpts.CapabilitiesFor(dialect), nodes...)
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	return sqlutil.SplitSQLStatementsForDialect(output, dialect), nil
}

func withGeneratedTimeoutDirectivesForOptions(sql, dialect string, opts generatedDirectiveOptions) string {
	if opts.skipTimeouts {
		return sql
	}
	return withGeneratedTimeoutDirectives(sql, dialect)
}

func withGeneratedTimeoutDirectives(sql, dialect string) string {
	if !containsAlterTable(sql) || !supportsGeneratedTimeoutDirectives(dialect) {
		return sql
	}

	directives := "-- +ptah lock_timeout=3s\n-- +ptah statement_timeout=30s\n"
	separator := "\n\n"
	if before, after, ok := strings.Cut(sql, separator); ok {
		return before + "\n" + directives + "\n" + after
	}
	return directives + sql
}

func containsAlterTable(sql string) bool {
	stripped := sqlutil.StripComments(sql)
	return strings.Contains(strings.ToUpper(stripped), "ALTER TABLE")
}

func supportsGeneratedTimeoutDirectives(dialect string) bool {
	normalized := platform.NormalizeDialect(dialect)
	return slices.Contains([]string{platform.Postgres, platform.MySQL, platform.MariaDB}, normalized)
}

// reverseSchemaDiff creates a reverse diff for generating down migrations
//
// Deprecated: Use reverseSchemaDiffWithSchema for proper RLS policy table name resolution
func reverseSchemaDiff(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	return reverseSchemaDiffWithSchema(diff, nil, nil)
}

// reverseSchemaDiffWithSchema creates a reverse diff for generating down migrations with schema context.
//
// schema is the generated (target) Go schema, used to resolve table names for
// RLS policies. dbSchema is the introspected (pre-change) database schema, used
// to rebuild prior FK/PK/CHECK/UNIQUE definitions for reversed constraint
// additions; it may be nil when callers only have the generated schema (the
// reversed additions then fall back to the name-only path).
//
// # Every field of SchemaDiff is accounted for here
//
// This function builds a fresh SchemaDiff literal, and a literal that
// enumerates fields has no compiler check for the ones it forgets. Nine fields
// -- views, materialized views and triggers, added/removed/modified -- were
// once simply absent, so every down migration silently dropped those whole
// categories: an up that created a view rolled back to "No rollback operations
// needed" and left the view in place (issue #1287). Three dispositions are
// available, and every field must have exactly one:
//
//   - Exchanged. Added and removed swap where both sides carry the same kind of
//     value and the reverse operation is the inverse of the forward one:
//     tables, enums, indexes, extensions, functions, sequences, domains,
//     composite types, ranges, views, materialized views, triggers, RLS
//     policies, RLS enablement, roles, grants, grant options and constraints.
//   - Carried. A Modified entry is not the inverse of itself. The planner
//     re-renders a modified object from the schema it is handed, and the down
//     direction is handed the pre-change database schema, so carrying the entry
//     across is what restores the prior definition. Only the recorded
//     "old -> new" description is flipped, plus any recorded prior state (a
//     view's PreviousBody) that names a side rather than a change.
//   - Derived. IdentifierSemantics is cloned rather than reversed: it describes
//     the catalog the diff was measured against, which does not have a
//     direction. The table-qualified constraint collections are rebuilt from
//     the pre-change database schema by reverseConstraintAdditions and
//     reverseConstraintRemovals rather than swapped, because a down migration
//     must restore the prior body, not the new one.
//     ForeignKeysRemovedWithTables is supplemental input for forward removal
//     ordering and is likewise rebuilt from the reversed FK additions rather
//     than itself creating a reverse operation. ConstraintBackedIndexRemovals
//     is derived too, and it redirects rather than reverses: it names the subset
//     of the index removals whose object is really a UNIQUE constraint, so
//     reverseIndexRemovals turns exactly that subset into constraint additions
//     rebuilt from the introspected constraint, and leaves the rest as index
//     additions.
//
// No field is deliberately dropped, and none is unreachable in the down
// direction. TestReverseSchemaDiff_AccountsForEverySchemaDiffField enforces
// that by reflection: it zeroes one field of a fully populated diff at a time
// and fails when doing so leaves the reverse plan unchanged.
func reverseSchemaDiffWithSchema(diff *difftypes.SchemaDiff, schema *schemamodel.Database, dbSchema *catalog.Database) *difftypes.SchemaDiff {
	return reverseSchemaDiffWithSchemaForDialect(diff, schema, dbSchema, "")
}

// reverseProceduresRemoved names which of the added routines are procedures, so
// the rollback drops each with the verb its object takes.
//
// reverseRoutinesOfKind splits the additions a forward diff recorded into the
// removals a rollback plans, by the kind each one carries.
//
// The kind used to be looked up in the desired schema, because the forward diff
// recorded an addition by name only. It travels with the change now
// (stokaro/ptah#2315), so a rollback no longer depends on the declaration still
// being there -- and the signature comes with it, which is what makes the DROP
// addressable when the name is overloaded (stokaro/ptah#2296).
//
// A routine whose kind is unset is left with the functions, because a DROP
// FUNCTION that is refused is a louder failure than a DROP PROCEDURE aimed at a
// function (stokaro/ptah#1722).
func reverseRoutinesOfKind(added difftypes.FunctionChanges, isKind func(difftypes.RoutineChange) bool) difftypes.FunctionChanges {
	if len(added) == 0 {
		return nil
	}
	var removals difftypes.FunctionChanges
	for _, routine := range added {
		if !isKind(routine) {
			continue
		}
		// The declaration's parameters are the signature a rollback drops by:
		// the routine being dropped is the one this addition created, so what
		// it was created with is what identifies it.
		routine.Signature = routine.Parameters
		removals = append(removals, routine)
	}
	return removals
}

// reverseFunctionsRemoved is the additions that were plain functions.
func reverseFunctionsRemoved(added difftypes.FunctionChanges) difftypes.FunctionChanges {
	return reverseRoutinesOfKind(added, func(routine difftypes.RoutineChange) bool {
		return !routine.IsProcedure()
	})
}

// reverseProceduresRemoved is the additions that were procedures.
func reverseProceduresRemoved(added difftypes.FunctionChanges) difftypes.FunctionChanges {
	return reverseRoutinesOfKind(added, difftypes.RoutineChange.IsProcedure)
}

func reverseSchemaDiffWithSchemaForDialect(
	diff *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
) *difftypes.SchemaDiff {
	// The identity the two producers agree on. The reversed diff carries the
	// same rules the forward one was compared under, so a down migration pairs
	// its drops exactly as the up migration it undoes did.
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	// The pre-change database as a desired schema. This is the schema the DOWN
	// plan is rendered against -- see generateDownMigrationSQLQualified, which
	// hands the planner exactly this -- so it is where a reversed modification
	// finds the definition it must restore (stokaro/ptah#2315).
	// nil is a real input here: callers that reverse a diff without a database
	// read pass one, and the conversion dereferences it.
	var prior *schemamodel.Database
	if dbSchema != nil {
		prior = dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)
	}
	reversed := &difftypes.SchemaDiff{
		IdentifierSemantics: cloneIdentifierSemantics(diff.IdentifierSemantics),

		// Reverse table operations
		TablesAdded:    diff.TablesRemoved,                                // Tables to remove become tables to add
		TablesRemoved:  deporder.TableDropOrder(diff.TablesAdded, schema), // Tables to add become tables to remove
		TablesModified: reverseTableDiffs(diff.TablesModified),

		// Reverse enum operations
		EnumsAdded:    diff.EnumsRemoved, // Enums to remove become enums to add
		EnumsRemoved:  diff.EnumsAdded,   // Enums to add become enums to remove
		EnumsModified: reverseEnumDiffs(diff.EnumsModified),

		// Reverse extension operations
		ExtensionsAdded:    diff.ExtensionsRemoved, // Extensions to remove become extensions to add
		ExtensionsRemoved:  diff.ExtensionsAdded,   // Extensions to add become extensions to remove
		ExtensionsModified: reverseExtensionDiffs(diff.ExtensionsModified),

		// Reverse function operations. A removed routine of either kind comes
		// back as an addition carrying its own kind, so the two removal lists
		// merge into one addition list without losing which was which.
		FunctionsAdded:    append(slices.Clone(diff.FunctionsRemoved), diff.ProceduresRemoved...),
		FunctionsRemoved:  reverseFunctionsRemoved(diff.FunctionsAdded),
		FunctionsModified: reverseFunctionDiffs(diff.FunctionsModified, prior),
		// A removed procedure comes back as an addition, and the planner reads
		// its kind off the declaration -- which is why the reverse of a removal
		// needs no kind of its own. The reverse of an ADDITION does: nothing
		// here knows whether the added routine was a procedure, so
		// reverseProceduresRemoved asks the desired schema.
		ProceduresRemoved: reverseProceduresRemoved(diff.FunctionsAdded),

		// Reverse sequence operations
		// Reverse user-defined type operations
		DomainsAdded:           diff.DomainsRemoved,
		DomainsRemoved:         diff.DomainsAdded,
		DomainsModified:        reverseDomainDiffs(diff.DomainsModified, schema, prior, semantics),
		CompositeTypesAdded:    diff.CompositeTypesRemoved,
		CompositeTypesRemoved:  diff.CompositeTypesAdded,
		CompositeTypesModified: reverseCompositeTypeDiffs(diff.CompositeTypesModified, schema, prior, semantics),
		RangesAdded:            diff.RangesRemoved,
		RangesRemoved:          diff.RangesAdded,
		RangesModified:         reverseRangeDiffs(diff.RangesModified, schema, prior, semantics),

		SequencesAdded:    diff.SequencesRemoved, // Sequences to remove become sequences to add
		SequencesRemoved:  diff.SequencesAdded,   // Sequences to add become sequences to remove
		SequencesModified: reverseSequenceDiffs(diff.SequencesModified, prior, semantics),

		// Reverse view, materialized view and trigger operations.
		//
		// Each side carries the same kind of value (view names, materialized
		// view names, table-qualified trigger refs) and DROP is the inverse of
		// CREATE for all three, so the plain swap is the correct reversal.
		//
		// The Modified entries are carried across rather than swapped: the
		// planner re-renders a modified object from the schema it is handed,
		// which in the down direction is the pre-change database schema, so the
		// entry itself is what selects the prior definition. A view carries the
		// body it will be replacing as well, and THAT is a side rather than a
		// change, so it is exchanged for the up migration's target body -- the
		// state the database is actually in when the rollback runs.
		ViewsAdded:    diff.ViewsRemoved, // Views to remove become views to add
		ViewsRemoved:  diff.ViewsAdded,   // Views to add become views to remove
		ViewsModified: reverseViewDiffs(diff.ViewsModified, schema, prior, semantics),

		// A synonym is an alias with no body, so reversing it needs no schema
		// side: the down direction drops what the up direction created and
		// recreates what it dropped. A retarget is the one entry that carries
		// state, and swapping its two targets is the whole reversal -- the
		// planner drops and recreates either way.
		SynonymsAdded:    diff.SynonymsRemoved,
		SynonymsRemoved:  diff.SynonymsAdded,
		SynonymsModified: reverseSynonymDiffs(diff.SynonymsModified, prior),

		// A hypertable reverses like a synonym in the diff and unlike one in
		// the plan. The swap is the same -- what the up direction partitioned,
		// the down direction stops declaring -- but the DOWN plan is a refusal
		// rather than a statement: TimescaleDB has no drop_hypertable, so a
		// rollback of a create_hypertable is a table the operator has to drop
		// and recreate. The swap belongs here anyway, because refusing is what
		// the planner does with a removal and refusing loudly is the point.
		HypertablesAdded:    slices.Clone(diff.HypertablesRemoved),
		HypertablesRemoved:  slices.Clone(diff.HypertablesAdded),
		HypertablesModified: reverseHypertableDiffs(diff.HypertablesModified),

		// A continuous aggregate reverses like a view and unlike the
		// hypertable above it: both directions are statements the server
		// accepts. What the up direction created, the down direction drops
		// with DROP MATERIALIZED VIEW; what it dropped, the down direction
		// creates from the body the pre-change read carried, which is why the
		// down plan's desired schema is the introspected one.
		ContinuousAggregatesAdded:    slices.Clone(diff.ContinuousAggregatesRemoved),
		ContinuousAggregatesRemoved:  slices.Clone(diff.ContinuousAggregatesAdded),
		ContinuousAggregatesModified: reverseContinuousAggregateDiffs(diff.ContinuousAggregatesModified, prior, semantics),

		// An extended property is a name, an address and a value, and the
		// reversal needs no schema side because all three are already in the
		// diff: the down direction drops what the up direction added, adds
		// back what it dropped with the value the removal carried, and swaps
		// the two values of a modification.
		ExtendedPropertiesAdded:    slices.Clone(diff.ExtendedPropertiesRemoved),
		ExtendedPropertiesRemoved:  slices.Clone(diff.ExtendedPropertiesAdded),
		ExtendedPropertiesModified: reverseExtendedPropertyDiffs(diff.ExtendedPropertiesModified),

		MaterializedViewsAdded:    diff.MaterializedViewsRemoved, // Materialized views to remove become materialized views to add
		MaterializedViewsRemoved:  diff.MaterializedViewsAdded,   // Materialized views to add become materialized views to remove
		MaterializedViewsModified: reverseMaterializedViewDiffs(diff.MaterializedViewsModified, prior, semantics),

		// Exchanged, but not carried across untouched. An addition renders from
		// its operand and a removal from its names, so the two directions want
		// opposite things: the reversed addition needs the definition the
		// pre-change database held, and the reversed removal needs none
		// (stokaro/ptah#2315).
		TriggersAdded:    triggerAdditionsFromRemovals(diff.TriggersRemoved, prior, semantics),
		TriggersRemoved:  triggerRemovalsFromAdditions(diff.TriggersAdded),
		TriggersModified: reverseTriggerDiffs(diff.TriggersModified, prior, semantics),

		// Reverse RLS policy operations. Both directions carry the owning
		// table, so reversing is a swap and no name-to-table resolution is
		// needed -- the resolution that used to happen here keyed a map by
		// policy name and lost one of two policies that shared one.
		RLSPoliciesAdded:    slices.Clone(diff.RLSPoliciesRemoved), // Policies to remove become policies to add
		RLSPoliciesRemoved:  slices.Clone(diff.RLSPoliciesAdded),   // Policies to add become policies to remove
		RLSPoliciesModified: reverseRLSPolicyDiffs(diff.RLSPoliciesModified),

		// Reverse RLS table enablement operations
		RLSEnabledTablesAdded:   diff.RLSEnabledTablesRemoved, // Tables to disable RLS become tables to enable RLS
		RLSEnabledTablesRemoved: diff.RLSEnabledTablesAdded,   // Tables to enable RLS become tables to disable RLS

		// Reverse role operations
		RolesAdded:          diff.RolesRemoved, // Roles to remove become roles to add
		RolesRemoved:        diff.RolesAdded,   // Roles to add become roles to remove
		RolesModified:       reverseRoleDiffs(diff.RolesModified, prior),
		GrantsAdded:         diff.GrantsRemoved,       // Grants to remove become grants to add
		GrantsRemoved:       diff.GrantsAdded,         // Grants to add become grants to revoke
		GrantOptionsAdded:   diff.GrantOptionsRevoked, // Revoked grant options become grant-option additions
		GrantOptionsRevoked: diff.GrantOptionsAdded,   // Grant-option additions become grant-option revocations

		// Reverse constraint operations. A modified constraint is expressed by
		// the comparator as remove + add of the SAME name (e.g. an on_delete
		// change on a field-level FK, issue #189). Swapping the two slices makes
		// the down migration drop the new definition and re-add the old one.
		// reverseConstraintAdditions restores the prior table-qualified body
		// from the introspected schema for the constraint types whose down
		// add-path needs more than a name.
		//
		// ConstraintsAddedWithTables carries the table-qualified prior body so
		// the down add-path can fan a shared constraint name out to every real
		// host table. Without it the down add-path falls back to name-only
		// resolution, which can emit one ADD for a single host while per-host
		// DROP also resolves only one host; the 2nd host's re-add then collides
		// with its still-present old constraint (Postgres 42710, MySQL 1826)
		// and the rollback aborts half-applied.
		ConstraintsAdded:             diff.ConstraintsRemoved,
		ConstraintsRemoved:           diff.ConstraintsAdded,
		ConstraintsRemovedWithTables: reverseConstraintRemovals(diff, schema, semantics),
		ForeignKeysRemovedWithTables: reverseForeignKeyRemovals(diff, schema, dialect),
		ConstraintsAddedWithTables:   reverseConstraintAdditions(diff, dbSchema, semantics),
	}
	// A re-created table brings its own primary key and field-level foreign keys
	// back with it, so listing those a second time as constraint additions is
	// how a rollback of a DROP TABLE became unexecutable. This runs before the
	// index-removal restorations are appended purely for readability: those are
	// UNIQUE constraints, which the rule deliberately never drops.
	dropReverseConstraintsRestoredByTableCreation(reversed, diff.ConstraintsRemovedWithTables, dbSchema)
	indexAdditions, constraintRestorations := reverseIndexRemovals(diff, dbSchema)
	reversed.SetIndexAdditions(indexAdditions)
	reversed.SetIndexRemovals(diff.IndexAdditions())
	for _, restored := range constraintRestorations {
		reversed.ConstraintsAdded = append(reversed.ConstraintsAdded, restored.Name)
		reversed.ConstraintsAddedWithTables = append(reversed.ConstraintsAddedWithTables, restored)
	}
	return reversed
}

func reverseExtensionDiffs(diffs []difftypes.ExtensionDiff) []difftypes.ExtensionDiff {
	if diffs == nil {
		return nil
	}
	reversed := make([]difftypes.ExtensionDiff, len(diffs))
	for i, diff := range diffs {
		reversed[i] = difftypes.ExtensionDiff{
			Name:       diff.Name,
			FromSchema: diff.ToSchema,
			ToSchema:   diff.FromSchema,
			// The version pair is swapped for the reason the schema pair is:
			// the rollback moves the extension back to where it was. Dropping
			// it made a version-only change reverse to a diff describing no
			// change at all, so the down file said "No rollback operations
			// needed" and the extension stayed upgraded (stokaro/ptah#2418).
			FromVersion: diff.ToVersion,
			ToVersion:   diff.FromVersion,
			// Carried, not swapped: whether the extension may be moved is a
			// property of the extension, not a direction. Dropping it made
			// every reversed diff claim false, and the planner refuses a move
			// of a non-relocatable extension -- so a legal move reversed into
			// a FAILING down migration.
			Relocatable: diff.Relocatable,
		}
	}
	return reversed
}

// reverseIndexRemovals splits the up direction's index removals by what the
// object each one names actually is, because the down direction has to put that
// object back and only one of the two spellings does.
//
// An ordinary index removal reverses into an index addition, which the down
// path resolves from the introspected schema. A removal the comparator marked
// as constraint-backed (ConstraintBackedIndexRemovals) does not: the object is
// a UNIQUE constraint whose index carries the constraint's name, the up
// direction dropped it with ALTER TABLE ... DROP CONSTRAINT, and the statement
// that restores it is ALTER TABLE ... ADD CONSTRAINT ... UNIQUE. Reversing it
// into an index addition is wrong twice over: the down path builds its target
// from ConvertDBSchemaToGoSchema, which deliberately omits a constraint-backed
// index (it is the constraint's, not an index of its own), so the addition has
// no definition to resolve and down generation fails outright with
// `added index users.uq_users_email at position 0 is missing or ambiguous in
// the target schema` -- and where it did resolve it would rebuild a plain
// unique index in place of the constraint, leaving the rollback's catalog
// different from the one the migration started against.
//
// The prior body comes from the introspected constraint, the same source
// reverseConstraintAdditions restores a removed UNIQUE constraint from, so a
// covering INCLUDE list and NULLS [NOT] DISTINCT survive the round trip.
//
// A marked removal with no introspected constraint to rebuild from -- a nil
// dbSchema, or a hand-built diff -- stays an index addition rather than
// disappearing. That is the loud failure above, which is the right outcome: a
// down migration that silently omits the uniqueness protection it is supposed
// to restore is worse than one that refuses to be generated.
func reverseIndexRemovals(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
) (additions []difftypes.IndexRef, restored []difftypes.ConstraintAdditionInfo) {
	removals := diff.IndexRemovals()
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()
	if len(constraintBacked) == 0 {
		return removals, nil
	}
	uniqueConstraints := introspectedUniqueConstraintsByHost(dbSchema)
	for _, ref := range removals {
		if _, ownedByConstraint := constraintBacked[ref]; !ownedByConstraint {
			additions = append(additions, ref)
			continue
		}
		dbConstraint, hasBody := uniqueConstraints[tableMemberKey{table: ref.TableName, member: ref.Name}]
		columns := dbConstraint.ColumnNamesOrDefault()
		if !hasBody || len(columns) == 0 {
			additions = append(additions, ref)
			continue
		}
		restored = append(restored, difftypes.ConstraintAdditionInfo{
			Name:           ref.Name,
			TableName:      ref.TableName,
			Type:           "UNIQUE",
			Columns:        slices.Clone(columns),
			IncludeColumns: slices.Clone(dbConstraint.IncludeColumns),
			NullsDistinct:  cloneBoolPtr(dbConstraint.NullsDistinct),
		})
	}
	return additions, restored
}

// introspectedUniqueConstraintsByHost keys the pre-change UNIQUE constraints by
// the host table and name an IndexRef names, which is the identity the
// comparator marked the removal under.
func introspectedUniqueConstraintsByHost(
	dbSchema *catalog.Database,
) map[tableMemberKey]catalog.Constraint {
	constraints := make(map[tableMemberKey]catalog.Constraint)
	if dbSchema == nil {
		return constraints
	}
	for _, constraint := range dbSchema.Constraints {
		if constraint.Type != "UNIQUE" {
			continue
		}
		constraints[tableMemberKey{table: constraint.QualifiedTableName(), member: constraint.Name}] = constraint
	}
	return constraints
}

func generatedTableByStructName(tables []schemamodel.Table, structName string) *schemamodel.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func generatedTableReference(tables []schemamodel.Table, structName, tableName string) *schemamodel.Table {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return generatedTableByStructName(tables, structName)
	}
	for i := range tables {
		if tables[i].QualifiedName() == tableName {
			return &tables[i]
		}
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return nil
	}
	for i := range tables {
		if tables[i].StructName == structName && tables[i].Name == ref.Name {
			return &tables[i]
		}
	}

	var match *schemamodel.Table
	for i := range tables {
		if tables[i].Name != ref.Name {
			continue
		}
		if match != nil {
			return nil
		}
		match = &tables[i]
	}
	return match
}

// reverseConstraintAdditions builds the table-qualified additions for the down
// migration. In the down direction the constraints to add back are the ones the
// up migration REMOVED (diff.ConstraintsRemovedWithTables) — restoring their
// prior definition. The prior body is read from the introspected (pre-change)
// database schema, which is the authoritative source for what the down must
// restore.
//
// Carrying the full per-host body here lets both dialect planners' add-paths
// (which already prefer ConstraintsAddedWithTables) emit one correct ALTER TABLE
// per real host table. This is what makes the down of a multi-host mixin FK
// modify apply cleanly: a name-only down re-adds only one host (and drops only
// one host), so the others collide on re-add (issue #197 DOWN path). When
// dbSchema is nil, the names still flow through ConstraintsAdded and the
// planners fall back to the name-only field scan.
func reverseConstraintAdditions(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	semantics identifier.Semantics,
) []difftypes.ConstraintAdditionInfo {
	if dbSchema == nil || len(diff.ConstraintsRemovedWithTables) == 0 {
		return nil
	}

	// Index the introspected constraints by (table, name) so each reversed
	// addition restores the body from the exact host it was removed from. A
	// mixin-shared FK name legitimately repeats across host tables, so a
	// name-only key would collapse them onto one host.
	dbConstraintByTableName := make(map[tableMemberKey]catalog.Constraint)
	for _, c := range dbSchema.Constraints {
		if c.Type != "FOREIGN KEY" && c.Type != "PRIMARY KEY" && c.Type != "CHECK" && c.Type != "UNIQUE" {
			continue
		}
		dbConstraintByTableName[tableMemberKey{table: c.QualifiedTableName(), member: c.Name}] = c
	}

	var infos []difftypes.ConstraintAdditionInfo
	for _, removed := range diff.ConstraintsRemovedWithTables {
		if removed.TableName == "" {
			continue
		}
		dbConstraint, ok := dbConstraintByTableName[tableMemberKey{table: removed.TableName, member: removed.Name}]
		if !ok {
			// No introspected body to restore (e.g. the constraint was a
			// pure-removal not present pre-change, or a type this helper does not
			// reconstruct). The name still rides in ConstraintsAdded for the
			// name-only fallback.
			continue
		}
		switch removed.Type {
		case "FOREIGN KEY":
			infos = append(infos, foreignKeyAdditionFromDBConstraint(removed.Name, removed.TableName, dbConstraint, semantics))
		case "PRIMARY KEY":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, difftypes.ConstraintAdditionInfo{
					Name:      removed.Name,
					TableName: removed.TableName,
					Identity:  constraintscope.Identity(semantics, removed.TableName, removed.Name),
					Type:      "PRIMARY KEY",
					Columns:   append([]string(nil), columns...),
				})
			}
		case "CHECK":
			if dbConstraint.CheckClause != nil && *dbConstraint.CheckClause != "" {
				infos = append(infos, difftypes.ConstraintAdditionInfo{
					Name:            removed.Name,
					TableName:       removed.TableName,
					Identity:        constraintscope.Identity(semantics, removed.TableName, removed.Name),
					Type:            "CHECK",
					CheckExpression: *dbConstraint.CheckClause,
				})
			}
		case "UNIQUE":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, difftypes.ConstraintAdditionInfo{
					Name:           removed.Name,
					TableName:      removed.TableName,
					Identity:       constraintscope.Identity(semantics, removed.TableName, removed.Name),
					Type:           "UNIQUE",
					Columns:        append([]string(nil), columns...),
					IncludeColumns: append([]string(nil), dbConstraint.IncludeColumns...),
					NullsDistinct:  cloneBoolPtr(dbConstraint.NullsDistinct),
				})
			}
		}
	}
	return infos
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

// foreignKeyAdditionFromDBConstraint builds a ConstraintAdditionInfo carrying the
// full FK body from an introspected database FOREIGN KEY constraint. The
// referential actions come straight from the pre-change DB, so the down
// migration restores exactly the prior ON DELETE / ON UPDATE behavior.
func foreignKeyAdditionFromDBConstraint(
	name, table string,
	dbFK catalog.Constraint,
	semantics identifier.Semantics,
) difftypes.ConstraintAdditionInfo {
	info := difftypes.ConstraintAdditionInfo{
		Name:      name,
		TableName: table,
		Identity:  constraintscope.Identity(semantics, table, name),
		Type:      "FOREIGN KEY",
		OnDelete:  derefString(dbFK.DeleteRule),
		OnUpdate:  derefString(dbFK.UpdateRule),
	}
	if columns := dbFK.ColumnNamesOrDefault(); len(columns) > 0 {
		info.Columns = uniqueStringsPreserveOrder(columns)
	}
	if dbFK.ForeignTable != nil {
		info.ForeignTable = *dbFK.ForeignTable
	}
	if foreignColumns := dbFK.ForeignColumnsOrDefault(); len(foreignColumns) > 0 {
		foreignColumns = uniqueStringsPreserveOrder(foreignColumns)
		info.ForeignColumn = foreignColumns[0]
		info.ForeignColumns = foreignColumns
	}
	return info
}

func uniqueStringsPreserveOrder(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// derefString returns the pointed-to string or "" when nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// reverseConstraintRemovals builds the table-qualified removal info for the
// down migration. In the down direction the constraints to remove are the ones
// the up migration ADDED (diff.ConstraintsAdded); their owning table and type
// are resolved from the generated schema, which is the source the up side
// synthesized them from. This lets dialect planners that need the table and a
// type-specific drop syntax (MySQL/MariaDB DROP FOREIGN KEY) emit a real drop in
// the down migration. When the schema is unavailable, the names still flow
// through ConstraintsRemoved; only the richer per-table info is omitted.
func reverseConstraintRemovals(
	diff *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.ConstraintRemovalInfo {
	if schema == nil {
		return nil
	}

	// Index explicit table-level constraints by name.
	tableConstraints := make(map[string]schemamodel.Constraint, len(schema.Constraints))
	for _, c := range schema.Constraints {
		tableConstraints[c.Name] = c
	}

	// Prefer the table-qualified additions the comparator recorded. A
	// field-level FK contributed by an embedded inline-relation mixin shares one
	// name across every host table, so resolving the table from a field's Go
	// struct name collapses every host onto the same (often non-table) name —
	// the down migration would then drop the constraint from the wrong table or
	// from a struct name that does not exist (issue #197). ConstraintsAddedWithTables
	// carries the concrete table for each addition, so the down side drops the
	// FK from exactly the table the up side added it to. Names present here are
	// recorded so the field-scan fallback below does not double-emit them.
	var infos []difftypes.ConstraintRemovalInfo
	seen := make(map[tableMemberKey]struct{})
	handled := make(map[string]struct{})
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			continue
		}
		infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{
			Name: add.Name, TableName: add.TableName, Type: add.Type,
			// Carried, not re-derived: this record IS the forward addition,
			// turned around. Deriving it again would be a second answer to a
			// question the comparator already answered.
			Identity: add.Identity,
		})
		handled[add.Name] = struct{}{}
	}
	infos = appendAddedTableForeignKeyRemovals(infos, seen, diff.TablesAdded, schema)
	infos = appendAddedColumnForeignKeyRemovals(infos, seen, diff.TablesModified, schema)

	// Index field-level constraint names to their owning table for the names
	// that did not arrive with table-qualified info.
	structToTable := make(map[string]string, len(schema.Tables))
	for _, t := range schema.Tables {
		structToTable[t.StructName] = t.Name
	}
	fkTables := make(map[string]string, len(schema.Fields))
	checkTables := make(map[string]string, len(schema.Fields))
	for _, f := range schema.Fields {
		tableName := structToTable[f.StructName]
		if tableName == "" {
			tableName = f.StructName
		}

		if f.Foreign != "" {
			name := f.ForeignKeyName
			if name == "" {
				name = fromschema.GenerateForeignKeyName(tableName, f.Name)
			}
			fkTables[name] = tableName
		}

		if f.Check != "" {
			name := f.CheckName
			if name == "" {
				name = tableName + "_" + f.Name + "_check"
			}
			checkTables[name] = tableName
		}
	}

	for _, name := range diff.ConstraintsAdded {
		if _, done := handled[name]; done {
			continue
		}
		switch {
		case tableConstraints[name].Name != "":
			c := tableConstraints[name]
			infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{Name: name, TableName: c.Table, Type: c.Type, Identity: constraintscope.Identity(semantics, c.Table, name)})
		case fkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{
				Name: name, TableName: fkTables[name], Type: "FOREIGN KEY",
			})
		case checkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{Name: name, TableName: checkTables[name], Type: "CHECK", Identity: constraintscope.Identity(semantics, checkTables[name], name)})
		}
	}
	return infos
}

func reverseForeignKeyRemovals(
	diff *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	dialect string,
) []difftypes.ForeignKeyRemovalInfo {
	if diff == nil || schema == nil {
		return nil
	}
	collector := newReverseForeignKeyRemovalCollector(diff, dialect)
	collector.addQualifiedAdditions(diff.ConstraintsAddedWithTables)
	collector.addFieldForeignKeys(schema)
	collector.addTableForeignKeys(schema)
	return collector.result()
}

type reverseForeignKeyRemovalCollector struct {
	diff        *difftypes.SchemaDiff
	semantics   identifier.Semantics
	removals    map[tableMemberKey]difftypes.ForeignKeyRemovalInfo
	addedNames  map[string]struct{}
	addedHosts  map[tableMemberKey]struct{}
	addedTables map[string]struct{}
}

func newReverseForeignKeyRemovalCollector(
	diff *difftypes.SchemaDiff,
	dialect string,
) *reverseForeignKeyRemovalCollector {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	collector := &reverseForeignKeyRemovalCollector{
		diff:        diff,
		semantics:   semantics,
		removals:    make(map[tableMemberKey]difftypes.ForeignKeyRemovalInfo),
		addedNames:  make(map[string]struct{}, len(diff.ConstraintsAdded)),
		addedHosts:  make(map[tableMemberKey]struct{}, len(diff.ConstraintsAddedWithTables)),
		addedTables: stringSet(diff.TablesAdded),
	}
	for _, name := range diff.ConstraintsAdded {
		collector.addedNames[semantics.IndexIdentityKey(name)] = struct{}{}
	}
	return collector
}

func (c *reverseForeignKeyRemovalCollector) addQualifiedAdditions(
	constraints []difftypes.ConstraintAdditionInfo,
) {
	for _, constraint := range constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		c.addedHosts[canonicalTableMemberKey(c.semantics, constraint.TableName, constraint.Name)] = struct{}{}
		foreignColumns := slices.Clone(constraint.ForeignColumns)
		if len(foreignColumns) == 0 && constraint.ForeignColumn != "" {
			foreignColumns = []string{constraint.ForeignColumn}
		}
		c.add(difftypes.ForeignKeyRemovalInfo{
			Name: constraint.Name, TableName: constraint.TableName,
			Columns: slices.Clone(constraint.Columns), ForeignTable: constraint.ForeignTable,
			ForeignColumns: foreignColumns,
			Identity:       constraintscope.Identity(c.semantics, constraint.TableName, constraint.Name),
		})
	}
}

func (c *reverseForeignKeyRemovalCollector) addFieldForeignKeys(schema *schemamodel.Database) {
	for _, field := range schema.Fields {
		if field.Foreign == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, field.StructName)
		if table == nil {
			continue
		}
		name := field.ForeignKeyName
		if name == "" {
			name = fromschema.GenerateForeignKeyName(table.Name, field.Name)
		}
		if !c.selectedFieldForeignKey(*table, name, field.Name) {
			continue
		}
		ref := fromschema.ParseForeignKeyReference(field.Foreign)
		if ref == nil {
			continue
		}
		c.add(difftypes.ForeignKeyRemovalInfo{
			Name: name, TableName: table.QualifiedName(), Columns: []string{field.Name},
			ForeignTable: ref.Table, ForeignColumns: slices.Clone(ref.ReferencedColumns()),
			Identity: constraintscope.Identity(c.semantics, table.QualifiedName(), name),
		})
	}
}

func (c *reverseForeignKeyRemovalCollector) addTableForeignKeys(schema *schemamodel.Database) {
	for _, constraint := range schema.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		table := generatedTableReference(schema.Tables, constraint.StructName, constraint.Table)
		if table == nil {
			continue
		}
		name := constraint.Name
		if name == "" {
			name = defaultForeignKeyConstraintName(table.Name, constraint.Columns)
		}
		tableName := table.QualifiedName()
		if constraint.Table != "" {
			tableName = constraint.Table
		}
		if !c.selectedTableForeignKey(*table, tableName, name) {
			continue
		}
		c.add(difftypes.ForeignKeyRemovalInfo{
			Name: name, TableName: tableName, Columns: slices.Clone(constraint.Columns),
			ForeignTable:   constraint.ForeignTable,
			ForeignColumns: slices.Clone(constraint.ForeignColumnsOrDefault()),
			Identity:       constraintscope.Identity(c.semantics, tableName, name),
		})
	}
}

func (c *reverseForeignKeyRemovalCollector) selectedFieldForeignKey(
	table schemamodel.Table,
	name,
	column string,
) bool {
	return c.namedOrHostedAddition(table.QualifiedName(), name) ||
		generatedTableInSet(table, c.addedTables) ||
		tableDiffAddsColumn(c.diff.TablesModified, table, column)
}

func (c *reverseForeignKeyRemovalCollector) selectedTableForeignKey(
	table schemamodel.Table,
	tableName,
	name string,
) bool {
	return c.namedOrHostedAddition(tableName, name) || generatedTableInSet(table, c.addedTables)
}

func (c *reverseForeignKeyRemovalCollector) namedOrHostedAddition(table, name string) bool {
	_, named := c.addedNames[c.semantics.IndexIdentityKey(name)]
	_, hosted := c.addedHosts[canonicalTableMemberKey(c.semantics, table, name)]
	return named || hosted
}

func (c *reverseForeignKeyRemovalCollector) add(info difftypes.ForeignKeyRemovalInfo) {
	if !completeForeignKeyRemovalInfo(info) {
		return
	}
	key := canonicalTableMemberKey(c.semantics, info.TableName, info.Name)
	c.removals[key] = info
}

func (c *reverseForeignKeyRemovalCollector) result() []difftypes.ForeignKeyRemovalInfo {
	result := make([]difftypes.ForeignKeyRemovalInfo, 0, len(c.removals))
	for _, removal := range c.removals {
		result = append(result, removal)
	}
	slices.SortFunc(result, func(a, b difftypes.ForeignKeyRemovalInfo) int {
		if order := strings.Compare(a.TableName, b.TableName); order != 0 {
			return order
		}
		return strings.Compare(a.Name, b.Name)
	})
	return result
}

func canonicalTableMemberKey(
	semantics identifier.Semantics,
	table,
	member string,
) tableMemberKey {
	return tableMemberKey{
		table:  semantics.QualifiedTableIdentityKey(table),
		member: semantics.IndexIdentityKey(member),
	}
}

func completeForeignKeyRemovalInfo(info difftypes.ForeignKeyRemovalInfo) bool {
	return info.Name != "" && info.TableName != "" && len(info.Columns) > 0 &&
		info.ForeignTable != "" && len(info.ForeignColumns) > 0
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func tableDiffAddsColumn(tableDiffs []difftypes.TableDiff, table schemamodel.Table, column string) bool {
	for _, tableDiff := range tableDiffs {
		if (tableDiff.TableName == table.Name || tableDiff.TableName == table.QualifiedName() ||
			tableDiff.TableName == table.StructName) && slices.Contains(tableDiff.ColumnsAdded.Names(), column) {
			return true
		}
	}
	return false
}

func appendAddedColumnForeignKeyRemovals(
	infos []difftypes.ConstraintRemovalInfo,
	seen map[tableMemberKey]struct{},
	tableDiffs []difftypes.TableDiff,
	schema *schemamodel.Database,
) []difftypes.ConstraintRemovalInfo {
	for _, tableDiff := range tableDiffs {
		if len(tableDiff.ColumnsAdded) == 0 {
			continue
		}
		table := generatedTableReference(schema.Tables, "", tableDiff.TableName)
		if table == nil {
			continue
		}
		for _, field := range schema.Fields {
			if field.StructName != table.StructName ||
				field.Foreign == "" ||
				!slices.Contains(tableDiff.ColumnsAdded.Names(), field.Name) {
				continue
			}
			name := field.ForeignKeyName
			if name == "" {
				name = fromschema.GenerateForeignKeyName(table.Name, field.Name)
			}
			info := difftypes.ConstraintRemovalInfo{
				Name: name, TableName: table.QualifiedName(), Type: "FOREIGN KEY",
			}
			infos = appendConstraintRemovalInfo(infos, seen, info)
		}
	}
	return infos
}

func appendAddedTableForeignKeyRemovals(
	infos []difftypes.ConstraintRemovalInfo,
	seen map[tableMemberKey]struct{},
	tableNames []string,
	schema *schemamodel.Database,
) []difftypes.ConstraintRemovalInfo {
	addedTables := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		addedTables[tableName] = struct{}{}
	}
	if len(addedTables) == 0 {
		return infos
	}

	for _, field := range schema.Fields {
		if field.Foreign == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, field.StructName)
		if table == nil || !generatedTableInSet(*table, addedTables) {
			continue
		}
		tableName := table.QualifiedName()
		name := field.ForeignKeyName
		if name == "" {
			name = fromschema.GenerateForeignKeyName(table.Name, field.Name)
		}
		info := difftypes.ConstraintRemovalInfo{Name: name, TableName: tableName, Type: "FOREIGN KEY"}
		infos = appendConstraintRemovalInfo(infos, seen, info)
	}

	for _, constraint := range schema.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		table := generatedTableReference(schema.Tables, constraint.StructName, constraint.Table)
		if table == nil || !generatedTableInSet(*table, addedTables) {
			continue
		}
		tableName := table.QualifiedName()
		if constraint.Table != "" {
			tableName = constraint.Table
		}
		name := constraint.Name
		if name == "" {
			name = defaultForeignKeyConstraintName(table.Name, constraint.Columns)
		}
		infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{
			Name: name, TableName: tableName, Type: "FOREIGN KEY",
		})
	}

	return infos
}

func generatedTableInSet(table schemamodel.Table, tableNames map[string]struct{}) bool {
	_, byName := tableNames[table.Name]
	_, byQualifiedName := tableNames[table.QualifiedName()]
	return byName || byQualifiedName
}

func appendConstraintRemovalInfo(
	infos []difftypes.ConstraintRemovalInfo,
	seen map[tableMemberKey]struct{},
	info difftypes.ConstraintRemovalInfo,
) []difftypes.ConstraintRemovalInfo {
	if info.Name == "" || info.TableName == "" {
		return infos
	}
	key := tableMemberKey{table: info.TableName, member: info.Name}
	if _, ok := seen[key]; ok {
		return infos
	}
	seen[key] = struct{}{}
	return append(infos, info)
}

type tableMemberKey struct {
	table  string
	member string
}

func defaultForeignKeyConstraintName(tableName string, columns []string) string {
	columnName := strings.Join(columns, "_")
	if columnName == "" {
		columnName = "foreign_key"
	}
	return fromschema.GenerateForeignKeyName(tableName, columnName)
}

// reverseTableDiffs reverses table modifications for down migrations
func reverseTableDiffs(tableDiffs []difftypes.TableDiff) []difftypes.TableDiff {
	reversed := make([]difftypes.TableDiff, len(tableDiffs))
	for i, tableDiff := range tableDiffs {
		reversed[i] = difftypes.TableDiff{
			TableName:       tableDiff.TableName,
			ColumnsAdded:    tableDiff.ColumnsRemoved, // Columns to remove become columns to add
			ColumnsRemoved:  tableDiff.ColumnsAdded,   // Columns to add become columns to remove
			ColumnsModified: reverseColumnDiffs(tableDiff.ColumnsModified),
			// The three Desired/Current pairs below carry BOTH sides for the
			// reason each of their doc comments gives, which is exactly so a
			// reversal can swap them. None of them was swapped, or carried at
			// all: a migration that changed a table's comment rolled back to
			// "No rollback operations needed" (stokaro/ptah#2418).
			CommentChange:           reverseCommentChange(tableDiff.CommentChange),
			RowTTLChange:            reverseRowTTLChange(tableDiff.RowTTLChange),
			RowDeletionPolicyChange: reverseRowDeletionPolicyChange(tableDiff.RowDeletionPolicyChange),
		}
	}
	return reversed
}

// reverseColumnDiffs reverses column modifications for down migrations
func reverseColumnDiffs(columnDiffs []difftypes.ColumnDiff) []difftypes.ColumnDiff {
	reversed := make([]difftypes.ColumnDiff, len(columnDiffs))
	for i, columnDiff := range columnDiffs {
		// For column changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range columnDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.ColumnDiff{
			ColumnName: columnDiff.ColumnName,
			Changes:    reversedChanges,
		}
	}
	return reversed
}

// reverseEnumDiffs reverses enum modifications for down migrations
func reverseEnumDiffs(enumDiffs []difftypes.EnumDiff) []difftypes.EnumDiff {
	reversed := make([]difftypes.EnumDiff, len(enumDiffs))
	for i, enumDiff := range enumDiffs {
		reversed[i] = difftypes.EnumDiff{
			EnumName:      enumDiff.EnumName,
			ValuesAdded:   enumDiff.ValuesRemoved, // Values to remove become values to add
			ValuesRemoved: enumDiff.ValuesAdded,   // Values to add become values to remove
		}
	}
	return reversed
}

// reverseFunctionDiffs reverses function modifications for down migrations
func reverseFunctionDiffs(
	functionDiffs []difftypes.FunctionDiff,
	prior *schemamodel.Database,
) []difftypes.FunctionDiff {
	reversed := make([]difftypes.FunctionDiff, len(functionDiffs))
	for i, functionDiff := range functionDiffs {
		// For function changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range functionDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.FunctionDiff{
			FunctionName: functionDiff.FunctionName,
			Changes:      reversedChanges,
			// The replacement renders from the operand, so reversing the change
			// map without reversing the operand would have the down direction
			// re-apply the body it is undoing (stokaro/ptah#2315).
			Desired: priorFunction(prior, functionDiff.FunctionName),
		}
	}
	return reversed
}

// reverseSequenceDiffs reverses sequence modifications for down migrations.
func reverseSequenceDiffs(
	sequenceDiffs []difftypes.SequenceDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.SequenceDiff {
	reversed := make([]difftypes.SequenceDiff, len(sequenceDiffs))
	for i, sequenceDiff := range sequenceDiffs {
		reversedChanges := make(map[string]string)
		for key, change := range sequenceDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.SequenceDiff{
			SequenceName: sequenceDiff.SequenceName,
			Changes:      reversedChanges,
			Desired:      priorSequence(prior, sequenceDiff.SequenceName, semantics),
		}
	}
	return reversed
}

func reverseChangeMap(changes map[string]string) map[string]string {
	reversed := make(map[string]string, len(changes))
	for key, change := range changes {
		parts := strings.Split(change, " -> ")
		if len(parts) == 2 {
			reversed[key] = parts[1] + " -> " + parts[0]
		} else {
			reversed[key] = change
		}
	}
	return reversed
}

// reverseDomainDiffs turns each modified domain around for the down direction.
//
// CurrentBaseType has to be re-derived rather than carried over: it names the
// shape the DROP will run against, and a down migration runs against the shape
// the up migration created. schema is the up direction's target, so that is
// where the down direction's from-side lives. A nil schema leaves it empty and
// the drop ordering falls back to declaration order.
func reverseDomainDiffs(
	domainDiffs []difftypes.DomainDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.DomainDiff {
	reversed := make([]difftypes.DomainDiff, len(domainDiffs))
	for i, domainDiff := range domainDiffs {
		reversed[i] = difftypes.DomainDiff{
			DomainName: domainDiff.DomainName,
			Changes:    reverseChangeMap(domainDiff.Changes),
			// The current-side base type is re-derived from the schema the UP
			// migration leaves behind, which is what the DOWN direction is
			// changing away from. CurrentCheckConstraints cannot be: see
			// nestedCoverageExempt.
			CurrentBaseType: targetDomainBaseType(schema, domainDiff.DomainName),
			// The recreate half renders from the operand, so reversing the
			// change map without reversing the operand would rebuild the very
			// definition the rollback is undoing (stokaro/ptah#2315).
			Desired: priorDomain(prior, domainDiff.DomainName, semantics),
		}
	}
	return reversed
}

// priorDomain is the domain the pre-change database held, or a zero one when it
// held none -- which is what withholds the drop.
func priorDomain(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.Domain {
	if prior == nil {
		return schemamodel.Domain{}
	}
	if domain := objectlookup.Qualified(prior.Domains, name, semantics); domain != nil {
		return *domain
	}
	return schemamodel.Domain{}
}

// reverseViewDiffs carries modified views into the down direction.
//
// The entry is carried across rather than swapped with anything: the planner
// renders a modified view from the schema it is given (the pre-change database
// schema, in the down direction), so the entry itself is what selects the prior
// definition.
//
// PreviousBody is different in kind: it names the body the view HAS when the
// statement runs, not a change. When the rollback runs, the database holds what
// the up migration wrote, which is the generated schema's body -- so that is
// what the reversed entry must carry. Getting this wrong is not cosmetic: the
// PostgreSQL planner reads it to decide whether CREATE OR REPLACE VIEW is legal
// for the rollback, and PostgreSQL refuses the replace for every column-list
// change except a trailing append.
//
// A nil schema (the deprecated reverseSchemaDiff entry point) leaves it empty,
// which planners read as "not known" and answer with drop-and-recreate. That is
// the safe direction: it always applies.
//
// Rollback is set for the same reason and is the other half of it. Where a
// planner can neither prove the replace legal nor prove it refused, the answer
// it should give differs by direction, and this is the only place that knows
// which direction is being built.
// reverseSynonymDiffs swaps each retarget so the down direction restores the
// target the database had before the up migration ran.
// reverseExtendedPropertyDiffs swaps the two values of every modified extended
// property, so the down direction restores what the database held.
func reverseExtendedPropertyDiffs(diffs []difftypes.ExtendedPropertyDiff) []difftypes.ExtendedPropertyDiff {
	if len(diffs) == 0 {
		return nil
	}
	reversed := make([]difftypes.ExtendedPropertyDiff, 0, len(diffs))
	for _, diff := range diffs {
		restored := diff
		restored.Value = diff.OldValue
		restored.OldValue = diff.Value
		reversed = append(reversed, restored)
	}
	return reversed
}

// reverseHypertableDiffs swaps the two sides of each partitioning change, so a
// rollback describes the partitioning the database had.
func reverseHypertableDiffs(changes []difftypes.HypertableDiff) []difftypes.HypertableDiff {
	reversed := make([]difftypes.HypertableDiff, 0, len(changes))
	for _, change := range changes {
		reversed = append(reversed, difftypes.HypertableDiff{
			Table:            change.Table,
			OldColumn:        change.NewColumn,
			NewColumn:        change.OldColumn,
			OldChunkInterval: change.NewChunkInterval,
			NewChunkInterval: change.OldChunkInterval,
		})
	}
	return reversed
}

// reverseContinuousAggregateDiffs swaps the two sides of each aggregate change,
// so a rollback restores the body and the option the database had.
func reverseContinuousAggregateDiffs(
	changes []difftypes.ContinuousAggregateDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.ContinuousAggregateDiff {
	reversed := make([]difftypes.ContinuousAggregateDiff, 0, len(changes))
	for _, change := range changes {
		reversed = append(reversed, difftypes.ContinuousAggregateDiff{
			Name:                change.Name,
			OldBody:             change.NewBody,
			NewBody:             change.OldBody,
			OldMaterializedOnly: change.NewMaterializedOnly,
			NewMaterializedOnly: change.OldMaterializedOnly,
			// Swapping the bodies without swapping the operand would have the
			// down direction drop the aggregate and create the very definition
			// it is undoing, since the operand is what the create renders from
			// (stokaro/ptah#2315).
			Desired: priorContinuousAggregate(prior, change.Name, semantics),
		})
	}
	return reversed
}

// priorContinuousAggregate is the aggregate the pre-change database held.
//
// The diff spells a qualified name the declaration produced, and the aggregate
// a read reports carries the schema the server puts it under, so the two are
// compared as qualified names on the connection's own identifier terms.
func priorContinuousAggregate(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.ContinuousAggregate {
	if prior == nil {
		return schemamodel.ContinuousAggregate{}
	}
	if aggregate := objectlookup.Qualified(prior.ContinuousAggregates, name, semantics); aggregate != nil {
		return *aggregate
	}
	return schemamodel.ContinuousAggregate{}
}

func reverseSynonymDiffs(
	diffs []difftypes.SynonymDiff,
	prior *schemamodel.Database,
) []difftypes.SynonymDiff {
	if len(diffs) == 0 {
		return nil
	}
	reversed := make([]difftypes.SynonymDiff, 0, len(diffs))
	for _, diff := range diffs {
		reversed = append(reversed, difftypes.SynonymDiff{
			SynonymName: diff.SynonymName,
			OldTarget:   diff.NewTarget,
			NewTarget:   diff.OldTarget,
			Desired:     priorSynonym(prior, diff.SynonymName),
		})
	}
	return reversed
}

// priorFunction is the function the pre-change database held.
//
// The name is compared exactly, which is the identity the comparison that
// produced the change already used: it pairs a declared routine with a reported
// one by the name the declaration carries.
func priorFunction(prior *schemamodel.Database, name string) schemamodel.Function {
	if prior == nil {
		return schemamodel.Function{}
	}
	for _, function := range prior.Functions {
		if function.Name == name {
			return function
		}
	}
	return schemamodel.Function{}
}

// priorSequence is the sequence the pre-change database held, resolved on the
// three identity tiers, because the diff spells a name the declaration produced
// and a read reports the schema the server puts the sequence under.
func priorSequence(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.Sequence {
	if prior == nil {
		return schemamodel.Sequence{}
	}
	if sequence := objectlookup.Qualified(prior.Sequences, name, semantics); sequence != nil {
		return *sequence
	}
	return schemamodel.Sequence{}
}

// priorSynonym is the synonym the pre-change database held.
//
// The qualified name is the key on both sides, which is the one the comparison
// that produced the change already used: it maps declared synonyms by
// QualifiedName and pairs them with the reported ones under the same key.
func priorSynonym(prior *schemamodel.Database, name string) schemamodel.Synonym {
	if prior == nil {
		return schemamodel.Synonym{}
	}
	for _, synonym := range prior.Synonyms {
		if synonym.QualifiedName() == name {
			return synonym
		}
	}
	return schemamodel.Synonym{}
}

func reverseViewDiffs(
	viewDiffs []difftypes.ViewDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.ViewDiff {
	reversed := make([]difftypes.ViewDiff, len(viewDiffs))
	for i, viewDiff := range viewDiffs {
		reversed[i] = difftypes.ViewDiff{
			ViewName:     viewDiff.ViewName,
			Changes:      reverseChangeMap(viewDiff.Changes),
			PreviousBody: generatedViewBody(schema, viewDiff.ViewName),
			// The view the database HAD, which is what this rollback restores.
			// The forward entry carries the declaration; reversing the change
			// map without reversing the operand would have the down direction
			// reapply the very body it is undoing (stokaro/ptah#2315).
			Desired:  priorView(prior, viewDiff.ViewName, semantics),
			Rollback: true,
		}
	}
	return reversed
}

// priorView is the view the pre-change database held, resolved on the terms the
// planner resolved it on when it was handed that schema directly: the diff
// spells a name the declaration used, and a database view carries the schema
// the server reports it under.
func priorView(prior *schemamodel.Database, name string, semantics identifier.Semantics) schemamodel.View {
	if prior == nil {
		return schemamodel.View{}
	}
	if view := objectlookup.View(prior.Views, name, semantics); view != nil {
		return *view
	}
	return schemamodel.View{}
}

// reverseRangeDiffs mirrors reverseDomainDiffs for range types.
func reverseRangeDiffs(
	rangeDiffs []difftypes.RangeDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.RangeDiff {
	reversed := make([]difftypes.RangeDiff, len(rangeDiffs))
	for i, rangeDiff := range rangeDiffs {
		reversed[i] = difftypes.RangeDiff{
			RangeName:      rangeDiff.RangeName,
			Changes:        reverseChangeMap(rangeDiff.Changes),
			CurrentSubtype: targetRangeSubtype(schema, rangeDiff.RangeName),
			Desired:        priorRange(prior, rangeDiff.RangeName, semantics),
		}
	}
	return reversed
}

// priorRange mirrors [priorDomain] for range types.
func priorRange(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.Range {
	if prior == nil {
		return schemamodel.Range{}
	}
	if rangeType := objectlookup.Qualified(prior.Ranges, name, semantics); rangeType != nil {
		return *rangeType
	}
	return schemamodel.Range{}
}

func generatedViewBody(schema *schemamodel.Database, viewName string) string {
	if schema == nil {
		return ""
	}
	for _, view := range schema.Views {
		if view.Name == viewName {
			return strings.TrimSpace(view.Body)
		}
	}
	return ""
}

func targetRangeSubtype(schema *schemamodel.Database, name string) string {
	if schema == nil {
		return ""
	}
	for _, rangeType := range schema.Ranges {
		if rangeType.QualifiedName() == name {
			return rangeType.Subtype
		}
	}
	return ""
}

// reverseMaterializedViewDiffs carries modified materialized views into the
// down direction, on the same terms as reverseViewDiffs. A materialized view
// has no in-place replace at all, so there is no prior body to record: both
// directions drop and recreate it.
func reverseMaterializedViewDiffs(
	viewDiffs []difftypes.MaterializedViewDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.MaterializedViewDiff {
	reversed := make([]difftypes.MaterializedViewDiff, len(viewDiffs))
	for i, viewDiff := range viewDiffs {
		reversed[i] = difftypes.MaterializedViewDiff{
			ViewName: viewDiff.ViewName,
			Changes:  reverseChangeMap(viewDiff.Changes),
			// A ClickHouse refresh schedule is a Desired/Current pair for the
			// reason the table's TTL is: the planner needs both sides to tell
			// an ALTER from a rebuild. Dropping it meant a rollback restored
			// the view without the schedule it had, so its rows would be right
			// once and never again (stokaro/ptah#2418).
			RefreshChange: reverseRefreshChange(viewDiff.RefreshChange),
			// The recreate half renders from the operand, so reversing the
			// change map without reversing the operand would rebuild the very
			// definition the rollback is undoing (stokaro/ptah#2315).
			Desired: priorMaterializedView(prior, viewDiff.ViewName, semantics),
		}
	}
	return reversed
}

// priorMaterializedView is the materialized view the pre-change database held,
// resolved the way [priorView] resolves a plain one: the diff spells a name the
// declaration used, and a database view carries the schema the server reports it
// under.
func priorMaterializedView(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.MaterializedView {
	if prior == nil {
		return schemamodel.MaterializedView{}
	}
	if view := objectlookup.MaterializedView(prior.MaterializedViews, name, semantics); view != nil {
		return *view
	}
	return schemamodel.MaterializedView{}
}

// reverseTriggerDiffs carries modified triggers into the down direction, on the
// same terms as reverseViewDiffs. TableName is part of the trigger's identity
// rather than a changed value, so it is preserved. PostgreSQL 17.10 accepts
// CREATE OR REPLACE TRIGGER even for a timing change, so a trigger needs no
// legality test of its own.
func reverseTriggerDiffs(
	triggerDiffs []difftypes.TriggerDiff,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.TriggerDiff {
	reversed := make([]difftypes.TriggerDiff, len(triggerDiffs))
	for i, triggerDiff := range triggerDiffs {
		reversed[i] = difftypes.TriggerDiff{
			TriggerName: triggerDiff.TriggerName,
			TableName:   triggerDiff.TableName,
			Changes:     reverseChangeMap(triggerDiff.Changes),
			// The replacement renders from the operand, so reversing the change
			// map without reversing the operand would have the down direction
			// re-apply the definition it is undoing.
			Desired: priorTrigger(prior, triggerDiff.TableName, triggerDiff.TriggerName, semantics),
		}
	}
	return reversed
}

// triggerAdditionsFromRemovals turns the forward direction's removals into the
// rollback's additions, giving each the definition the pre-change database held.
//
// A removal carries names only, which is all a DROP needs. The addition it
// becomes renders CREATE TRIGGER, so the operand has to be recovered here or
// the rollback drops a trigger it never puts back.
func triggerAdditionsFromRemovals(
	removals []difftypes.TriggerRef,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.TriggerRef {
	if len(removals) == 0 {
		return nil
	}
	additions := make([]difftypes.TriggerRef, len(removals))
	for i, ref := range removals {
		additions[i] = difftypes.TriggerRef{
			TriggerName: ref.TriggerName,
			TableName:   ref.TableName,
			Desired:     priorTrigger(prior, ref.TableName, ref.TriggerName, semantics),
		}
	}
	return additions
}

// triggerRemovalsFromAdditions turns the forward direction's additions into the
// rollback's removals, dropping the operand each one carried.
//
// A DROP is written from the two names. Carrying the declaration into a removal
// would leave the entry holding a definition nothing reads, which reads to the
// next person as though something did.
func triggerRemovalsFromAdditions(additions []difftypes.TriggerRef) []difftypes.TriggerRef {
	if len(additions) == 0 {
		return nil
	}
	removals := make([]difftypes.TriggerRef, len(additions))
	for i, ref := range additions {
		removals[i] = difftypes.TriggerRef{TriggerName: ref.TriggerName, TableName: ref.TableName}
	}
	return removals
}

// priorTrigger is the trigger the pre-change database held, resolved on the
// terms [objectlookup.Trigger] applies: the table half carries the schema, so
// the identity tiers are applied to it and the trigger's own name is folded by
// the same comparison rule.
func priorTrigger(
	prior *schemamodel.Database,
	tableName, triggerName string,
	semantics identifier.Semantics,
) schemamodel.Trigger {
	if prior == nil {
		return schemamodel.Trigger{}
	}
	if trigger := objectlookup.Trigger(prior.Triggers, tableName, triggerName, semantics); trigger != nil {
		return *trigger
	}
	return schemamodel.Trigger{}
}

// reverseCompositeTypeDiffs mirrors reverseDomainDiffs for composite types.
func reverseCompositeTypeDiffs(
	compositeDiffs []difftypes.CompositeTypeDiff,
	schema, prior *schemamodel.Database,
	semantics identifier.Semantics,
) []difftypes.CompositeTypeDiff {
	reversed := make([]difftypes.CompositeTypeDiff, len(compositeDiffs))
	for i, compositeDiff := range compositeDiffs {
		reversed[i] = difftypes.CompositeTypeDiff{
			TypeName:          compositeDiff.TypeName,
			Changes:           reverseChangeMap(compositeDiff.Changes),
			CurrentFieldTypes: targetCompositeFieldTypes(schema, compositeDiff.TypeName),
			// An added attribute is a removed one coming back, and the pair is
			// what lets the planner ALTER instead of rebuilding. Dropping both
			// sent every reversed composite down the drop-and-recreate path
			// (stokaro/ptah#2418).
			AttributesAdded:   restoredCompositeAttributes(prior, compositeDiff.TypeName, compositeDiff.AttributesRemoved),
			AttributesRemoved: compositeAttributeNames(compositeDiff.AttributesAdded),
			Desired:           priorCompositeType(prior, compositeDiff.TypeName, semantics),
		}
	}
	return reversed
}

// priorCompositeType mirrors [priorDomain] for composite types.
func priorCompositeType(
	prior *schemamodel.Database,
	name string,
	semantics identifier.Semantics,
) schemamodel.CompositeType {
	if prior == nil {
		return schemamodel.CompositeType{}
	}
	if composite := objectlookup.Qualified(prior.CompositeTypes, name, semantics); composite != nil {
		return *composite
	}
	return schemamodel.CompositeType{}
}

func targetDomainBaseType(schema *schemamodel.Database, name string) string {
	if schema == nil {
		return ""
	}
	for _, domain := range schema.Domains {
		if domain.QualifiedName() == name {
			return domain.BaseType
		}
	}
	return ""
}

func targetCompositeFieldTypes(schema *schemamodel.Database, name string) []string {
	if schema == nil {
		return nil
	}
	for _, composite := range schema.CompositeTypes {
		if composite.QualifiedName() != name {
			continue
		}
		fieldTypes := make([]string, len(composite.Fields))
		for i, field := range composite.Fields {
			fieldTypes[i] = field.Type
		}
		return fieldTypes
	}
	return nil
}

// reverseRLSPolicyDiffs reverses RLS policy modifications for down migrations
func reverseRLSPolicyDiffs(policyDiffs []difftypes.RLSPolicyDiff) []difftypes.RLSPolicyDiff {
	reversed := make([]difftypes.RLSPolicyDiff, len(policyDiffs))
	for i, policyDiff := range policyDiffs {
		// For policy changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range policyDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.RLSPolicyDiff{
			PolicyName: policyDiff.PolicyName,
			TableName:  policyDiff.TableName,
			Changes:    reversedChanges,
		}
	}
	return reversed
}

// reverseRoleDiffs reverses role modifications for down migrations
func reverseRoleDiffs(roleDiffs []difftypes.RoleDiff, prior *schemamodel.Database) []difftypes.RoleDiff {
	reversed := make([]difftypes.RoleDiff, len(roleDiffs))
	for i, roleDiff := range roleDiffs {
		// For role changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range roleDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.RoleDiff{
			RoleName: roleDiff.RoleName,
			Changes:  reversedChanges,
			// A password entry does not reverse: what it changed from is
			// unreadable, so the reversed change still says one is required.
			// The operand is what decides whether anything is written, and the
			// pre-change database holds no password -- which is the honest
			// answer, and the reason this rewrite is not optional. Carrying the
			// declaration's role through would have the down direction set the
			// NEW password (stokaro/ptah#2315).
			Desired: priorRole(prior, roleDiff.RoleName),
		}
	}
	return reversed
}

// priorRole is the role the pre-change database held.
//
// A role name is compared exactly, which is the identity the comparison that
// produced the change already used: a role lives outside any schema and is not
// resolved against a search path, so there is no qualified spelling of one and
// nothing for identifier semantics to fold.
func priorRole(prior *schemamodel.Database, name string) schemamodel.Role {
	if prior == nil {
		return schemamodel.Role{}
	}
	for _, role := range prior.Roles {
		if role.Name == name {
			return role
		}
	}
	return schemamodel.Role{}
}

// nextAvailableMigrationVersion answers the version question over the names
// listed through the writer handle the plan already holds, so the version it
// avoids colliding with comes from the directory the plan will publish into
// rather than from whatever the pathname resolves to while it is being chosen.
func nextAvailableMigrationVersion(
	writer *atlasmigrate.MigrationWriter,
	version int64,
	migrationName string,
) (int64, error) {
	names, err := migrationDirNames(writer)
	if err != nil {
		return 0, err
	}
	return nextAvailablePtahVersion(names, version, migrationName)
}

// nextAvailablePtahVersion keeps the paired layout's monotonic rule: the clock,
// bumped past the newest migration when the clock does not already outrank it.
// Nothing outside Ptah reads this layout, so no parity argument moves it off a
// version that only ever goes forwards.
//
// What did move is the arithmetic. The bump is bounded by what a ten-digit
// NNNNNNNNNN prefix can hold, because past it the writer produced an
// eleven-digit name -- `10000000000_addposts.up.sql` -- that
// [migrationfile.ParseFileName] refuses, so discovery dropped the file with
// no diagnostic and `migrations up` reported success without running it
// (stokaro/ptah#938).
func nextAvailablePtahVersion(names []string, version int64, migrationName string) (int64, error) {
	if latest := latestPtahVersionIn(names); latest >= version {
		next, err := migrationversion.Next(latest, migrationfile.DirFormatPtah)
		if err != nil {
			return 0, err
		}
		version = next
	}
	taken := nameSet(names)
	for {
		if err := migrationversion.Check(version, migrationfile.DirFormatPtah); err != nil {
			return 0, err
		}
		upName := migrationfile.FileName(version, migrationName, "up")
		downName := migrationfile.FileName(version, migrationName, "down")
		if !taken[upName] && !taken[downName] {
			return version, nil
		}
		version++
	}
}

func latestPtahVersionIn(names []string) int64 {
	var latest int64
	for _, name := range names {
		migrationFile, err := migrationfile.ParseFileName(name)
		if err != nil {
			continue
		}
		if migrationFile.Version > latest {
			latest = migrationFile.Version
		}
	}
	return latest
}

// migrationDirFileNames lists a migration directory by pathname. It is the
// reader-side counterpart of migrationDirNames, which lists the same thing
// through a bound writer handle; a directory that cannot be listed reads as
// empty, so a version scan over a missing directory starts from scratch.
func migrationDirFileNames(outputDir string) []string {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	return names
}

func nameSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}

func withNoTransactionDirective(sql string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}
	if directive, ok := migrationfile.ParseDirectives(sql)[migrationfile.DirectiveNoTransaction]; ok && directive == "true" {
		return sql
	}
	return "-- +ptah " + migrationfile.DirectiveNoTransaction + "\n" + sql
}

func renderMigrationArtifacts(
	outputDir, reportFormat string,
	specs []generatedMigrationSpec,
) ([]atlasmigrate.PublicationArtifact, []MigrationFilePair, error) {
	artifacts := make([]atlasmigrate.PublicationArtifact, 0, len(specs)*3)
	pairs := make([]MigrationFilePair, 0, len(specs))
	for _, spec := range specs {
		upName := migrationfile.FileName(spec.Version, spec.Name, "up")
		downName := migrationfile.FileName(spec.Version, spec.Name, "down")
		pair := MigrationFilePair{
			UpFile:        filepath.Join(outputDir, upName),
			DownFile:      filepath.Join(outputDir, downName),
			Version:       spec.Version,
			NoTransaction: spec.NoTransaction,
		}
		artifacts = append(
			artifacts,
			atlasmigrate.PublicationArtifact{Name: upName, Contents: []byte(spec.UpSQL)},
			atlasmigrate.PublicationArtifact{Name: downName, Contents: []byte(spec.DownSQL)},
		)
		if reportFormat != "" {
			reportName, reportContents, err := renderSafetyReport(
				upName,
				reportFormat,
				spec.Assessments,
			)
			if err != nil {
				return nil, nil, fmt.Errorf("error creating safety report: %w", err)
			}
			pair.ReportFile = filepath.Join(outputDir, reportName)
			artifacts = append(artifacts, atlasmigrate.PublicationArtifact{
				Name:     reportName,
				Contents: reportContents,
			})
		}
		pairs = append(pairs, pair)
	}
	return artifacts, pairs, nil
}

func shadowCandidatesFromSpecs(specs []generatedMigrationSpec) []shadow.Candidate {
	candidates := make([]shadow.Candidate, 0, len(specs))
	for _, spec := range specs {
		candidates = append(candidates, shadow.Candidate{
			Version: spec.Version,
			Name:    spec.Name,
			UpSQL:   spec.UpSQL,
			DownSQL: spec.DownSQL,
		})
	}
	return candidates
}

func migrationFilesFromPairs(pairs []MigrationFilePair) *MigrationFiles {
	if len(pairs) == 0 {
		return nil
	}
	return &MigrationFiles{
		Files: pairs,
	}
}

// reverseCommentChange swaps the two sides of a comment transition, so the
// rollback restores the comment the database had.
func reverseCommentChange(change *difftypes.CommentChange) *difftypes.CommentChange {
	if change == nil {
		return nil
	}
	return &difftypes.CommentChange{Current: change.Desired, Desired: change.Current}
}

// reverseRowTTLChange swaps the two sides of a row-level TTL transition.
func reverseRowTTLChange(change *difftypes.RowTTLChange) *difftypes.RowTTLChange {
	if change == nil {
		return nil
	}
	return &difftypes.RowTTLChange{Desired: change.Current, Current: change.Desired}
}

// reverseRowDeletionPolicyChange swaps the two sides of a row deletion policy
// transition.
func reverseRowDeletionPolicyChange(
	change *difftypes.RowDeletionPolicyChange,
) *difftypes.RowDeletionPolicyChange {
	if change == nil {
		return nil
	}
	return &difftypes.RowDeletionPolicyChange{Desired: change.Current, Current: change.Desired}
}

// reverseRefreshChange swaps the two sides of a materialized view's refresh
// schedule transition.
func reverseRefreshChange(change *difftypes.MatViewRefreshChange) *difftypes.MatViewRefreshChange {
	if change == nil {
		return nil
	}
	return &difftypes.MatViewRefreshChange{Desired: change.Current, Current: change.Desired}
}

// compositeAttributeNames is the removal shape of a set of added attributes: a
// DROP ATTRIBUTE takes a name, and nothing else of the attribute survives it.
func compositeAttributeNames(added []difftypes.CompositeAttribute) []string {
	if len(added) == 0 {
		return nil
	}
	names := make([]string, 0, len(added))
	for _, attribute := range added {
		names = append(names, attribute.Name)
	}
	return names
}

// restoredCompositeAttributes gives a removed attribute back its type, which a
// removal does not carry and an ADD ATTRIBUTE needs.
//
// The type comes from the PRE-CHANGE schema, because that is the composite the
// rollback is restoring. An attribute the prior schema does not describe is
// left out rather than added with an empty type, which would render
// `ADD ATTRIBUTE "x" ` and be refused.
func restoredCompositeAttributes(
	prior *schemamodel.Database,
	typeName string,
	removed []string,
) []difftypes.CompositeAttribute {
	if prior == nil || len(removed) == 0 {
		return nil
	}
	types := make(map[string]string)
	for _, composite := range prior.CompositeTypes {
		if composite.QualifiedName() != typeName {
			continue
		}
		for _, field := range composite.Fields {
			types[field.Name] = field.Type
		}
	}
	restored := make([]difftypes.CompositeAttribute, 0, len(removed))
	for _, name := range removed {
		fieldType, known := types[name]
		if !known {
			continue
		}
		restored = append(restored, difftypes.CompositeAttribute{Name: name, Type: fieldType})
	}
	return restored
}
