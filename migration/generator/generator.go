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

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/safety"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

var (
	// ErrMigrationDirectoryChanged reports that migration artifacts changed
	// between planning and publication.
	ErrMigrationDirectoryChanged = errors.New(
		"migration directory changed after migration planning",
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
	Generated *goschema.Database
	// DatabaseURL is the connection string for the database
	DatabaseURL string
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
	// The generator drops all objects in this database, replays existing
	// migrations from OutputDir, applies the candidate migration, re-introspects
	// the result, and aborts if it differs from the Go schema.
	ShadowDatabaseURL string
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
	NoTransaction bool   // Whether the pair is marked with +ptah no_transaction
}

// MigrationFiles represents the generated migration files.
type MigrationFiles struct {
	UpFile     string              // Path to the first up migration file
	DownFile   string              // Path to the first down migration file
	ReportFile string              // Path to the first safety report file, when requested
	Version    int64               // First migration version (timestamp)
	Files      []MigrationFilePair // All generated migration file pairs, in apply order
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
	// garbage collection. A plan that is never published at all still releases
	// them when it is collected, because os.Root closes its descriptor from a
	// finalizer.
	dir *atlasmigrate.MigrationWriter
	// plannedContents is what dir held when the plan was built, and nothing
	// else. It used to carry a filesystem identity beside the contents; identity
	// now lives in dir, which is a handle rather than a detached fs.FileInfo the
	// operating system is free to reissue to a replacement.
	plannedContents fsnapshot.Snapshot
	reportFormat    string
	specs           []generatedMigrationSpec
	written         bool
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
	DirFormat migrator.MigrationDirFormat
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
	dirFormat, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
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
	if dirFormat != migrator.MigrationDirFormatAtlas {
		if err := validateEmptyMigrationName(name); err != nil {
			return nil, err
		}
	}

	root, err := openOutputRoot(opts.AllowedOutputRoot)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closeOutputRoot(root) }()

	return writeEmptyMigration(root, outputDir, name, dirFormat)
}

func nextAtlasMigrationVersion() int64 {
	version, err := strconv.ParseInt(time.Now().UTC().Format("20060102150405"), 10, 64)
	if err != nil {
		return migrator.GetNextMigrationVersion()
	}
	return version
}

// nextAvailableAtlasMigrationVersion answers the version question for a
// directory named by pathname, for readers that are not inside a writer
// transaction. A writer holding a rooted handle asks nextAvailableAtlasVersion
// over the names it listed through that handle instead, so it never resolves
// the directory a second time.
func nextAvailableAtlasMigrationVersion(outputDir string, version int64) int64 {
	return nextAvailableAtlasVersion(migrationDirFileNames(outputDir), version)
}

func nextAvailableAtlasVersion(names []string, version int64) int64 {
	if latest := latestAtlasVersionIn(names); latest >= version {
		version = latest + 1
	}
	taken := nameSet(names)
	for taken[atlasEmptyMigrationFileName(version, "")] {
		version++
	}
	return version
}

func latestAtlasVersionIn(names []string) int64 {
	var latest int64
	for _, name := range names {
		migrationFile, err := migrator.ParseAtlasMigrationFileName(name)
		if err != nil {
			continue
		}
		if migrationFile.Version > latest {
			latest = migrationFile.Version
		}
	}
	return latest
}

func atlasEmptyMigrationFileName(version int64, name string) string {
	name = atlasEmptyMigrationName(name)
	if name == "" {
		return fmt.Sprintf("%d.sql", version)
	}
	return fmt.Sprintf("%d_%s.sql", version, name)
}

func atlasEmptyMigrationName(name string) string {
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

func validateEmptyMigrationName(name string) error {
	if name == "" {
		return fmt.Errorf("migration name is required")
	}

	fileName := migrator.GenerateMigrationFileName(1, name, "up")
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

	// 1. Determine the desired schema: use a pre-merged one when provided (for a
	// composite desired-state assembled from several sources), otherwise parse the
	// Go entities directory.
	generated, err := resolveDesiredSchema(opts)
	if err != nil {
		return nil, err
	}

	// 2. Connect to database and read current schema
	var conn *dbschema.DatabaseConnection

	if opts.DBConn != nil {
		conn = opts.DBConn
	} else {
		conn, err = dbschema.ConnectToDatabase(ctx, opts.DatabaseURL)
		if err != nil {
			return nil, fmt.Errorf("error connecting to database: %w", err)
		}
		defer dbschema.CloseAndWarn(conn)
	}

	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, opts.Schemas)
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
		generated,
		dbSchema,
		opts.CompareOptions,
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
	version := migrator.GetNextMigrationVersion()
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

	specs, assessments, err := planGeneratedMigrationSpecs(diff, generated, dbSchema, info, version, opts.MigrationName, opts.DiffPolicy, qualifier)
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, nil
	}
	if err := checkDestructiveAllowed(opts, assessments); err != nil {
		return nil, err
	}

	if opts.ShadowDatabaseURL != "" {
		if err := verifyShadowMigration(ctx, shadowMigrationOptions{
			DatabaseURL:      opts.ShadowDatabaseURL,
			TargetConnection: conn,
			MigrationsDir:    opts.OutputDir,
			Dialect:          info.Dialect,
			Capabilities:     info.Capabilities,
			IdentifierSemantics: cloneIdentifierSemanticsValue(
				diff.IdentifierSemantics,
			),
			Candidates:  shadowCandidatesFromSpecs(specs),
			Generated:   generated,
			CompareOpts: opts.CompareOptions,
			Schemas:     opts.Schemas,
		}); err != nil {
			return nil, err
		}
	}

	planned = true
	return &MigrationPlan{
		outputDir:       opts.OutputDir,
		dir:             writer,
		plannedContents: plannedContents,
		reportFormat:    opts.ReportFormat,
		specs:           specs,
	}, nil
}

// resolveDesiredSchema answers what the migration should bring the database to:
// a pre-merged schema when the caller assembled one from several sources, and
// otherwise the Go entities directory, parsed through a filesystem rooted at its
// parent so the scan cannot walk out of the directory the caller named.
func resolveDesiredSchema(opts GenerateMigrationOptions) (*goschema.Database, error) {
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
	generated, err := goschema.ParseFS(entitiesFS, entitiesDir)
	if err != nil {
		return nil, fmt.Errorf("error parsing Go entities: %w", err)
	}
	return generated, nil
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

type generatedMigrationSpec struct {
	Version       int64
	Name          string
	UpSQL         string
	DownSQL       string
	Assessments   []safety.StatementAssessment
	NoTransaction bool
}

func planGeneratedMigrationSpecs(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
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

	concurrentIndexRefs := concurrentIndexRefsForPolicy(diff, dbSchema, info, policy)
	concurrentIndexDropRefs := concurrentIndexDropRefsForPolicy(diff, info, policy)
	plannerOpts := planner.Options{
		Capabilities:            info.Capabilities,
		ConcurrentIndexRefs:     concurrentIndexRefs,
		ConcurrentIndexDropRefs: concurrentIndexDropRefs,
	}
	upNodes, err := planner.GenerateSchemaDiffASTWithOptions(diff, generated, info.Dialect, plannerOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("error generating up migration plan: %w", err)
	}
	if len(upNodes) == 0 {
		return nil, nil, nil
	}
	requiresNoTransaction := planner.RequiresNoTransaction(info.Dialect, upNodes)
	if !requiresNoTransaction {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:         diff,
			Qualifier:    qualifier,
			Generated:    generated,
			DBSchema:     dbSchema,
			Dialect:      info.Dialect,
			Capabilities: info.Capabilities,
			Version:      version,
			Name:         migrationName,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return withSkipComments([]generatedMigrationSpec{spec}, skipped), assessments, nil
	}

	nodeGroups := splitNoTransactionNodes(info.Dialect, upNodes)
	if len(nodeGroups.transactional) == 0 {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:                    diff,
			Qualifier:               qualifier,
			Generated:               generated,
			DBSchema:                dbSchema,
			Dialect:                 info.Dialect,
			Capabilities:            info.Capabilities,
			Version:                 version,
			Name:                    migrationName,
			ConcurrentIndexRefs:     concurrentIndexRefs,
			ConcurrentIndexDropRefs: concurrentIndexDropRefs,
			NoTransaction:           true,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return withSkipComments([]generatedMigrationSpec{spec}, skipped), assessments, nil
	}
	if !allNoTransactionNodesAreConcurrentIndexes(nodeGroups.noTransaction) {
		return nil, nil, fmt.Errorf("generated migration mixes transactional statements with non-transactional statements that cannot be split automatically")
	}

	diffGroups := splitConcurrentIndexDiff(diff, concurrentIndexRefs, concurrentIndexDropRefs)
	specs := make([]generatedMigrationSpec, 0, 2)
	allAssessments := make([]safety.StatementAssessment, 0)
	if diffGroups.transactional.HasChanges() {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:         diffGroups.transactional,
			Qualifier:    qualifier,
			Generated:    generated,
			DBSchema:     dbSchema,
			Dialect:      info.Dialect,
			Capabilities: info.Capabilities,
			Version:      version,
			Name:         migrationName + "_transactional",
		})
		if err != nil {
			return nil, nil, err
		}
		if spec.UpSQL != "" {
			specs = append(specs, spec)
			allAssessments = append(allAssessments, assessments...)
			version++
		}
	}
	if diffGroups.noTransaction.HasChanges() {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:                    diffGroups.noTransaction,
			Qualifier:               qualifier,
			Generated:               generated,
			DBSchema:                dbSchema,
			Dialect:                 info.Dialect,
			Capabilities:            info.Capabilities,
			Version:                 version,
			Name:                    migrationName + "_concurrent_indexes",
			ConcurrentIndexRefs:     concurrentIndexRefs,
			ConcurrentIndexDropRefs: concurrentIndexDropRefs,
			NoTransaction:           true,
		})
		if err != nil {
			return nil, nil, err
		}
		if spec.UpSQL != "" {
			specs = append(specs, spec)
			allAssessments = append(allAssessments, assessments...)
		}
	}
	return withSkipComments(specs, skipped), allAssessments, nil
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
	Diff                    *types.SchemaDiff
	Generated               *goschema.Database
	DBSchema                *dbschematypes.DBSchema
	Dialect                 string
	Capabilities            capability.Capabilities
	Version                 int64
	Name                    string
	ConcurrentIndexRefs     []types.IndexRef
	ConcurrentIndexDropRefs []types.IndexRef
	NoTransaction           bool
	Qualifier               atlasmigrate.Qualifier
}

func buildGeneratedMigrationSpec(opts generatedMigrationSpecOptions) (generatedMigrationSpec, []safety.StatementAssessment, error) {
	plannerOpts := planner.Options{
		Capabilities:            opts.Capabilities,
		ConcurrentIndexRefs:     opts.ConcurrentIndexRefs,
		ConcurrentIndexDropRefs: opts.ConcurrentIndexDropRefs,
	}
	upNodes, err := planner.GenerateSchemaDiffASTWithOptions(opts.Diff, opts.Generated, opts.Dialect, plannerOpts)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating up migration plan: %w", err)
	}
	if err := opts.Qualifier.ApplyToPlan(opts.Dialect, opts.Generated, upNodes); err != nil {
		return generatedMigrationSpec{}, nil, err
	}
	assessments, err := safety.AssessRenderedWithCapabilities(upNodes, opts.Dialect, opts.Capabilities)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error assessing migration safety: %w", err)
	}
	directiveOpts := generatedDirectiveOptions{skipTimeouts: opts.NoTransaction}
	upSQL, err := renderGeneratedMigrationSQL(upNodes, opts.Dialect, opts.Capabilities, "UP", directiveOpts)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating up migration SQL: %w", err)
	}
	if upSQL == "" {
		return generatedMigrationSpec{}, assessments, nil
	}
	if opts.NoTransaction {
		upSQL = withNoTransactionDirective(upSQL)
	}

	// opts.Diff is already diff-policy filtered by planGeneratedMigrationSpecs,
	// so the down migration reverses only what the up migration actually did: a
	// skipped destructive change is absent from the diff, so its inverse (e.g. a
	// CREATE TABLE that would collide with the kept table) is never emitted.
	// The rollback of a concurrent build must itself be non-blocking, so the
	// two ref sets swap with the direction: an index the up file BUILT
	// concurrently is DROPPED concurrently by the down file, and an index the
	// up file DROPPED concurrently is REBUILT concurrently.
	downOpts := downMigrationOptions{
		directives:              directiveOpts,
		qualifier:               opts.Qualifier,
		capabilities:            opts.Capabilities,
		concurrentIndexRefs:     opts.ConcurrentIndexDropRefs,
		concurrentIndexDropRefs: opts.ConcurrentIndexRefs,
	}
	downSQL, err := generateDownMigrationSQLQualified(opts.Diff, opts.Generated, opts.DBSchema, opts.Dialect, downOpts)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if opts.NoTransaction {
		downSQL = withNoTransactionDirective(downSQL)
	}

	return generatedMigrationSpec{
		Version:       opts.Version,
		Name:          opts.Name,
		UpSQL:         upSQL,
		DownSQL:       downSQL,
		Assessments:   assessments,
		NoTransaction: opts.NoTransaction,
	}, assessments, nil
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
	statements := sqlutil.SplitSQLStatements(rawSQL)
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

func allNoTransactionNodesAreConcurrentIndexes(nodes []ast.Node) bool {
	for _, node := range nodes {
		if !isConcurrentIndexNode(node) {
			return false
		}
	}
	return true
}

// isConcurrentIndexNode reports whether a node is one of the two concurrent
// index statements the generator knows how to split into its own
// no_transaction migration.
func isConcurrentIndexNode(node ast.Node) bool {
	switch typed := node.(type) {
	case *ast.IndexNode:
		return typed.Concurrently
	case *ast.DropIndexNode:
		return typed.Concurrently
	default:
		return false
	}
}

// concurrentIndexRefsForPolicy resolves which newly added indexes are built
// concurrently. When the diff policy requests it, every newly added index is
// concurrent (still gated on dialect and the CreateIndexConcurrently
// capability); otherwise the populated-table heuristic applies.
func concurrentIndexRefsForPolicy(
	diff *types.SchemaDiff,
	dbSchema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	policy DiffPolicy,
) []types.IndexRef {
	if !policy.ConcurrentIndex {
		return concurrentIndexRefsForPopulatedTables(diff, dbSchema, info)
	}
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil
	}
	return diff.IndexAdditions()
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
	diff *types.SchemaDiff,
	info dbschematypes.DBInfo,
	policy DiffPolicy,
) []types.IndexRef {
	if !policy.ConcurrentIndexDrop {
		return nil
	}
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.DropIndexConcurrently) {
		return nil
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
	var refs []types.IndexRef
	for _, ref := range diff.IndexRemovals() {
		if additions.Contains(ref) {
			continue
		}
		if _, ownedByConstraint := constraintBacked[ref]; ownedByConstraint {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

func concurrentIndexRefsForPopulatedTables(
	diff *types.SchemaDiff,
	dbSchema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
) []types.IndexRef {
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil
	}
	populatedTables := populatedTableSet(dbSchema)
	var refs []types.IndexRef
	for _, ref := range diff.IndexAdditions() {
		if _, ok := populatedTables[ref.TableName]; ok {
			refs = append(refs, ref)
		}
	}
	return refs
}

func populatedTableSet(dbSchema *dbschematypes.DBSchema) map[string]struct{} {
	out := make(map[string]struct{})
	if dbSchema == nil {
		return out
	}
	for _, table := range dbSchema.Tables {
		if table.EstimatedRows <= 0 {
			continue
		}
		out[table.QualifiedName()] = struct{}{}
		if table.Schema != "" {
			out[table.Name] = struct{}{}
		}
	}
	return out
}

type splitSchemaDiffs struct {
	transactional *types.SchemaDiff
	noTransaction *types.SchemaDiff
}

func splitConcurrentIndexDiff(
	diff *types.SchemaDiff,
	concurrentIndexRefs []types.IndexRef,
	concurrentIndexDropRefs []types.IndexRef,
) splitSchemaDiffs {
	txDiff := cloneSchemaDiff(diff)
	noTxDiff := &types.SchemaDiff{
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
func partitionIndexRefs(refs, selected []types.IndexRef) (transactional, noTransaction []types.IndexRef) {
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

func indexRefSet(values []types.IndexRef) map[types.IndexRef]struct{} {
	out := make(map[types.IndexRef]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func cloneSchemaDiff(diff *types.SchemaDiff) *types.SchemaDiff {
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
	clone.ViewsAdded = slices.Clone(diff.ViewsAdded)
	clone.ViewsRemoved = slices.Clone(diff.ViewsRemoved)
	clone.ViewsModified = slices.Clone(diff.ViewsModified)
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
	return &clone
}

func cloneIdentifierSemantics(
	semantics *identifier.Semantics,
) *identifier.Semantics {
	if semantics == nil {
		return nil
	}
	cloned := semantics.Clone()
	return &cloned
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
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateUpMigrationSQLWithOptions(diff, generated, dialect, generatedDirectiveOptions{}, capsOverride...)
}

type generatedDirectiveOptions struct {
	skipTimeouts bool
}

func generateUpMigrationSQLWithOptions(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	caps := capability.ForDialect(dialect)
	if len(capsOverride) > 0 {
		caps = capsOverride[0]
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(diff, generated, dialect, caps)
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
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateDownMigrationSQLWithOptions(diff, generated, dbSchema, dialect, generatedDirectiveOptions{}, capsOverride...)
}

func generateDownMigrationSQLWithOptions(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	opts := downMigrationOptions{directives: directiveOpts}
	if len(capsOverride) > 0 {
		opts.capabilities = capsOverride[0]
	}
	return generateDownMigrationSQLQualified(diff, generated, dbSchema, dialect, opts)
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
	concurrentIndexRefs     []types.IndexRef
	concurrentIndexDropRefs []types.IndexRef
}

func generateDownMigrationSQLQualified(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
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
	reverseDiff := reverseSchemaDiffWithSchema(diff, generated, dbSchema)
	addMySQLFamilyForeignKeyBackingIndexRemovals(reverseDiff, diff, dbSchema, dialect)

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
	reverseDiff *types.SchemaDiff,
	dbAsGoSchema *goschema.Database,
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
	return sqlutil.SplitSQLStatements(output), nil
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

func addMySQLFamilyForeignKeyBackingIndexRemovals(
	reverseDiff *types.SchemaDiff,
	upDiff *types.SchemaDiff,
	dbSchema *dbschematypes.DBSchema,
	dialect string,
) {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
	default:
		return
	}

	priorIndexes := dbIndexRefs(dbSchema)
	removals := reverseDiff.IndexRemovals()
	removed := indexRefSet(removals)
	for _, add := range upDiff.ConstraintsAddedWithTables {
		if add.TableName == "" || add.Name == "" || !strings.EqualFold(add.Type, "FOREIGN KEY") {
			continue
		}
		ref := types.IndexRef{Name: add.Name, TableName: add.TableName}
		if _, exists := priorIndexes[ref]; exists {
			continue
		}
		if _, exists := removed[ref]; exists {
			continue
		}
		removals = append(removals, ref)
		removed[ref] = struct{}{}
	}
	reverseDiff.SetIndexRemovals(removals)
}

func dbIndexRefs(dbSchema *dbschematypes.DBSchema) map[types.IndexRef]struct{} {
	out := make(map[types.IndexRef]struct{})
	if dbSchema == nil {
		return out
	}
	for _, index := range dbSchema.Indexes {
		out[types.IndexRef{Name: index.Name, TableName: index.TableName}] = struct{}{}
		if index.Schema != "" {
			out[types.IndexRef{
				Name:      index.Name,
				TableName: dbschematypes.QualifyTableName(index.Schema, index.TableName),
			}] = struct{}{}
		}
	}
	return out
}

// reverseSchemaDiff creates a reverse diff for generating down migrations
//
// Deprecated: Use reverseSchemaDiffWithSchema for proper RLS policy table name resolution
func reverseSchemaDiff(diff *types.SchemaDiff) *types.SchemaDiff {
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
//     must restore the prior body, not the new one. ConstraintBackedIndexRemovals
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
func reverseSchemaDiffWithSchema(diff *types.SchemaDiff, schema *goschema.Database, dbSchema *dbschematypes.DBSchema) *types.SchemaDiff {
	reversed := &types.SchemaDiff{
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
		ExtensionsAdded:   diff.ExtensionsRemoved, // Extensions to remove become extensions to add
		ExtensionsRemoved: diff.ExtensionsAdded,   // Extensions to add become extensions to remove

		// Reverse function operations
		FunctionsAdded:    diff.FunctionsRemoved, // Functions to remove become functions to add
		FunctionsRemoved:  diff.FunctionsAdded,   // Functions to add become functions to remove
		FunctionsModified: reverseFunctionDiffs(diff.FunctionsModified),

		// Reverse sequence operations
		// Reverse user-defined type operations
		DomainsAdded:           diff.DomainsRemoved,
		DomainsRemoved:         diff.DomainsAdded,
		DomainsModified:        reverseDomainDiffs(diff.DomainsModified, schema),
		CompositeTypesAdded:    diff.CompositeTypesRemoved,
		CompositeTypesRemoved:  diff.CompositeTypesAdded,
		CompositeTypesModified: reverseCompositeTypeDiffs(diff.CompositeTypesModified, schema),
		RangesAdded:            diff.RangesRemoved,
		RangesRemoved:          diff.RangesAdded,

		SequencesAdded:    diff.SequencesRemoved, // Sequences to remove become sequences to add
		SequencesRemoved:  diff.SequencesAdded,   // Sequences to add become sequences to remove
		SequencesModified: reverseSequenceDiffs(diff.SequencesModified),

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
		ViewsModified: reverseViewDiffs(diff.ViewsModified, schema),

		MaterializedViewsAdded:    diff.MaterializedViewsRemoved, // Materialized views to remove become materialized views to add
		MaterializedViewsRemoved:  diff.MaterializedViewsAdded,   // Materialized views to add become materialized views to remove
		MaterializedViewsModified: reverseMaterializedViewDiffs(diff.MaterializedViewsModified),

		TriggersAdded:    diff.TriggersRemoved, // Triggers to remove become triggers to add
		TriggersRemoved:  diff.TriggersAdded,   // Triggers to add become triggers to remove
		TriggersModified: reverseTriggerDiffs(diff.TriggersModified),

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
		RolesModified:       reverseRoleDiffs(diff.RolesModified),
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
		ConstraintsRemovedWithTables: reverseConstraintRemovals(diff, schema),
		ConstraintsAddedWithTables:   reverseConstraintAdditions(diff, dbSchema),
	}
	indexAdditions, constraintRestorations := reverseIndexRemovals(diff, dbSchema)
	reversed.SetIndexAdditions(indexAdditions)
	reversed.SetIndexRemovals(diff.IndexAdditions())
	for _, restored := range constraintRestorations {
		reversed.ConstraintsAdded = append(reversed.ConstraintsAdded, restored.Name)
		reversed.ConstraintsAddedWithTables = append(reversed.ConstraintsAddedWithTables, restored)
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
	diff *types.SchemaDiff,
	dbSchema *dbschematypes.DBSchema,
) (additions []types.IndexRef, restored []types.ConstraintAdditionInfo) {
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
		restored = append(restored, types.ConstraintAdditionInfo{
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
	dbSchema *dbschematypes.DBSchema,
) map[tableMemberKey]dbschematypes.DBConstraint {
	constraints := make(map[tableMemberKey]dbschematypes.DBConstraint)
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

func generatedTableByStructName(tables []goschema.Table, structName string) *goschema.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func generatedTableReference(tables []goschema.Table, structName, tableName string) *goschema.Table {
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

	var match *goschema.Table
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
func reverseConstraintAdditions(diff *types.SchemaDiff, dbSchema *dbschematypes.DBSchema) []types.ConstraintAdditionInfo {
	if dbSchema == nil || len(diff.ConstraintsRemovedWithTables) == 0 {
		return nil
	}

	// Index the introspected constraints by (table, name) so each reversed
	// addition restores the body from the exact host it was removed from. A
	// mixin-shared FK name legitimately repeats across host tables, so a
	// name-only key would collapse them onto one host.
	dbConstraintByTableName := make(map[tableMemberKey]dbschematypes.DBConstraint)
	for _, c := range dbSchema.Constraints {
		if c.Type != "FOREIGN KEY" && c.Type != "PRIMARY KEY" && c.Type != "CHECK" && c.Type != "UNIQUE" {
			continue
		}
		dbConstraintByTableName[tableMemberKey{table: c.QualifiedTableName(), member: c.Name}] = c
	}

	var infos []types.ConstraintAdditionInfo
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
			infos = append(infos, foreignKeyAdditionFromDBConstraint(removed.Name, removed.TableName, dbConstraint))
		case "PRIMARY KEY":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, types.ConstraintAdditionInfo{
					Name:      removed.Name,
					TableName: removed.TableName,
					Type:      "PRIMARY KEY",
					Columns:   append([]string(nil), columns...),
				})
			}
		case "CHECK":
			if dbConstraint.CheckClause != nil && *dbConstraint.CheckClause != "" {
				infos = append(infos, types.ConstraintAdditionInfo{
					Name:            removed.Name,
					TableName:       removed.TableName,
					Type:            "CHECK",
					CheckExpression: *dbConstraint.CheckClause,
				})
			}
		case "UNIQUE":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, types.ConstraintAdditionInfo{
					Name:           removed.Name,
					TableName:      removed.TableName,
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
	cloned := *value
	return &cloned
}

// foreignKeyAdditionFromDBConstraint builds a ConstraintAdditionInfo carrying the
// full FK body from an introspected database FOREIGN KEY constraint. The
// referential actions come straight from the pre-change DB, so the down
// migration restores exactly the prior ON DELETE / ON UPDATE behavior.
func foreignKeyAdditionFromDBConstraint(name, table string, dbFK dbschematypes.DBConstraint) types.ConstraintAdditionInfo {
	info := types.ConstraintAdditionInfo{
		Name:      name,
		TableName: table,
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
func reverseConstraintRemovals(diff *types.SchemaDiff, schema *goschema.Database) []types.ConstraintRemovalInfo {
	if schema == nil {
		return nil
	}

	// Index explicit table-level constraints by name.
	tableConstraints := make(map[string]goschema.Constraint, len(schema.Constraints))
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
	var infos []types.ConstraintRemovalInfo
	seen := make(map[tableMemberKey]struct{})
	handled := make(map[string]struct{})
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			continue
		}
		infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{
			Name:      add.Name,
			TableName: add.TableName,
			Type:      add.Type,
		})
		handled[add.Name] = struct{}{}
	}
	infos = appendAddedTableForeignKeyRemovals(infos, seen, diff.TablesAdded, schema)

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
			infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{Name: name, TableName: c.Table, Type: c.Type})
		case fkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{Name: name, TableName: fkTables[name], Type: "FOREIGN KEY"})
		case checkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{Name: name, TableName: checkTables[name], Type: "CHECK"})
		}
	}
	return infos
}

func appendAddedTableForeignKeyRemovals(
	infos []types.ConstraintRemovalInfo,
	seen map[tableMemberKey]struct{},
	tableNames []string,
	schema *goschema.Database,
) []types.ConstraintRemovalInfo {
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
		infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{
			Name:      name,
			TableName: tableName,
			Type:      "FOREIGN KEY",
		})
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
		infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{
			Name:      name,
			TableName: tableName,
			Type:      "FOREIGN KEY",
		})
	}

	return infos
}

func generatedTableInSet(table goschema.Table, tableNames map[string]struct{}) bool {
	_, byName := tableNames[table.Name]
	_, byQualifiedName := tableNames[table.QualifiedName()]
	return byName || byQualifiedName
}

func appendConstraintRemovalInfo(
	infos []types.ConstraintRemovalInfo,
	seen map[tableMemberKey]struct{},
	info types.ConstraintRemovalInfo,
) []types.ConstraintRemovalInfo {
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
func reverseTableDiffs(tableDiffs []types.TableDiff) []types.TableDiff {
	reversed := make([]types.TableDiff, len(tableDiffs))
	for i, tableDiff := range tableDiffs {
		reversed[i] = types.TableDiff{
			TableName:       tableDiff.TableName,
			ColumnsAdded:    tableDiff.ColumnsRemoved, // Columns to remove become columns to add
			ColumnsRemoved:  tableDiff.ColumnsAdded,   // Columns to add become columns to remove
			ColumnsModified: reverseColumnDiffs(tableDiff.ColumnsModified),
		}
	}
	return reversed
}

// reverseColumnDiffs reverses column modifications for down migrations
func reverseColumnDiffs(columnDiffs []types.ColumnDiff) []types.ColumnDiff {
	reversed := make([]types.ColumnDiff, len(columnDiffs))
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

		reversed[i] = types.ColumnDiff{
			ColumnName: columnDiff.ColumnName,
			Changes:    reversedChanges,
		}
	}
	return reversed
}

// reverseEnumDiffs reverses enum modifications for down migrations
func reverseEnumDiffs(enumDiffs []types.EnumDiff) []types.EnumDiff {
	reversed := make([]types.EnumDiff, len(enumDiffs))
	for i, enumDiff := range enumDiffs {
		reversed[i] = types.EnumDiff{
			EnumName:      enumDiff.EnumName,
			ValuesAdded:   enumDiff.ValuesRemoved, // Values to remove become values to add
			ValuesRemoved: enumDiff.ValuesAdded,   // Values to add become values to remove
		}
	}
	return reversed
}

// reverseFunctionDiffs reverses function modifications for down migrations
func reverseFunctionDiffs(functionDiffs []types.FunctionDiff) []types.FunctionDiff {
	reversed := make([]types.FunctionDiff, len(functionDiffs))
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

		reversed[i] = types.FunctionDiff{
			FunctionName: functionDiff.FunctionName,
			Changes:      reversedChanges,
		}
	}
	return reversed
}

// reverseSequenceDiffs reverses sequence modifications for down migrations.
func reverseSequenceDiffs(sequenceDiffs []types.SequenceDiff) []types.SequenceDiff {
	reversed := make([]types.SequenceDiff, len(sequenceDiffs))
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

		reversed[i] = types.SequenceDiff{
			SequenceName: sequenceDiff.SequenceName,
			Changes:      reversedChanges,
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
func reverseDomainDiffs(domainDiffs []types.DomainDiff, schema *goschema.Database) []types.DomainDiff {
	reversed := make([]types.DomainDiff, len(domainDiffs))
	for i, domainDiff := range domainDiffs {
		reversed[i] = types.DomainDiff{
			DomainName:      domainDiff.DomainName,
			Changes:         reverseChangeMap(domainDiff.Changes),
			CurrentBaseType: targetDomainBaseType(schema, domainDiff.DomainName),
		}
	}
	return reversed
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
func reverseViewDiffs(viewDiffs []types.ViewDiff, schema *goschema.Database) []types.ViewDiff {
	reversed := make([]types.ViewDiff, len(viewDiffs))
	for i, viewDiff := range viewDiffs {
		reversed[i] = types.ViewDiff{
			ViewName:     viewDiff.ViewName,
			Changes:      reverseChangeMap(viewDiff.Changes),
			PreviousBody: generatedViewBody(schema, viewDiff.ViewName),
			Rollback:     true,
		}
	}
	return reversed
}

func generatedViewBody(schema *goschema.Database, viewName string) string {
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

// reverseMaterializedViewDiffs carries modified materialized views into the
// down direction, on the same terms as reverseViewDiffs. A materialized view
// has no in-place replace at all, so there is no prior body to record: both
// directions drop and recreate it.
func reverseMaterializedViewDiffs(viewDiffs []types.MaterializedViewDiff) []types.MaterializedViewDiff {
	reversed := make([]types.MaterializedViewDiff, len(viewDiffs))
	for i, viewDiff := range viewDiffs {
		reversed[i] = types.MaterializedViewDiff{ViewName: viewDiff.ViewName, Changes: reverseChangeMap(viewDiff.Changes)}
	}
	return reversed
}

// reverseTriggerDiffs carries modified triggers into the down direction, on the
// same terms as reverseViewDiffs. TableName is part of the trigger's identity
// rather than a changed value, so it is preserved. PostgreSQL 17.10 accepts
// CREATE OR REPLACE TRIGGER even for a timing change, so a trigger needs no
// legality test of its own.
func reverseTriggerDiffs(triggerDiffs []types.TriggerDiff) []types.TriggerDiff {
	reversed := make([]types.TriggerDiff, len(triggerDiffs))
	for i, triggerDiff := range triggerDiffs {
		reversed[i] = types.TriggerDiff{
			TriggerName: triggerDiff.TriggerName,
			TableName:   triggerDiff.TableName,
			Changes:     reverseChangeMap(triggerDiff.Changes),
		}
	}
	return reversed
}

// reverseCompositeTypeDiffs mirrors reverseDomainDiffs for composite types.
func reverseCompositeTypeDiffs(compositeDiffs []types.CompositeTypeDiff, schema *goschema.Database) []types.CompositeTypeDiff {
	reversed := make([]types.CompositeTypeDiff, len(compositeDiffs))
	for i, compositeDiff := range compositeDiffs {
		reversed[i] = types.CompositeTypeDiff{
			TypeName:          compositeDiff.TypeName,
			Changes:           reverseChangeMap(compositeDiff.Changes),
			CurrentFieldTypes: targetCompositeFieldTypes(schema, compositeDiff.TypeName),
		}
	}
	return reversed
}

func targetDomainBaseType(schema *goschema.Database, name string) string {
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

func targetCompositeFieldTypes(schema *goschema.Database, name string) []string {
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
func reverseRLSPolicyDiffs(policyDiffs []types.RLSPolicyDiff) []types.RLSPolicyDiff {
	reversed := make([]types.RLSPolicyDiff, len(policyDiffs))
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

		reversed[i] = types.RLSPolicyDiff{
			PolicyName: policyDiff.PolicyName,
			TableName:  policyDiff.TableName,
			Changes:    reversedChanges,
		}
	}
	return reversed
}

// reverseRoleDiffs reverses role modifications for down migrations
func reverseRoleDiffs(roleDiffs []types.RoleDiff) []types.RoleDiff {
	reversed := make([]types.RoleDiff, len(roleDiffs))
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

		reversed[i] = types.RoleDiff{
			RoleName: roleDiff.RoleName,
			Changes:  reversedChanges,
		}
	}
	return reversed
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
	return nextAvailablePtahVersion(names, version, migrationName), nil
}

func nextAvailablePtahVersion(names []string, version int64, migrationName string) int64 {
	if latest := latestPtahVersionIn(names); latest >= version {
		version = latest + 1
	}
	taken := nameSet(names)
	for {
		upName := migrator.GenerateMigrationFileName(version, migrationName, "up")
		downName := migrator.GenerateMigrationFileName(version, migrationName, "down")
		if !taken[upName] && !taken[downName] {
			return version
		}
		version++
	}
}

func latestPtahVersionIn(names []string) int64 {
	var latest int64
	for _, name := range names {
		migrationFile, err := migrator.ParseMigrationFileName(name)
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func withNoTransactionDirective(sql string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}
	if directive, ok := migrator.ParseFileDirectives(sql)[migrator.DirectiveNoTransaction]; ok && directive == "true" {
		return sql
	}
	return "-- +ptah " + migrator.DirectiveNoTransaction + "\n" + sql
}

func renderMigrationArtifacts(
	outputDir, reportFormat string,
	specs []generatedMigrationSpec,
) ([]atlasmigrate.PublicationArtifact, []MigrationFilePair, error) {
	artifacts := make([]atlasmigrate.PublicationArtifact, 0, len(specs)*3)
	pairs := make([]MigrationFilePair, 0, len(specs))
	for _, spec := range specs {
		upName := migrator.GenerateMigrationFileName(spec.Version, spec.Name, "up")
		downName := migrator.GenerateMigrationFileName(spec.Version, spec.Name, "down")
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

func shadowCandidatesFromSpecs(specs []generatedMigrationSpec) []shadowCandidate {
	candidates := make([]shadowCandidate, 0, len(specs))
	for _, spec := range specs {
		candidates = append(candidates, shadowCandidate{
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
	first := pairs[0]
	return &MigrationFiles{
		UpFile:     first.UpFile,
		DownFile:   first.DownFile,
		ReportFile: first.ReportFile,
		Version:    first.Version,
		Files:      pairs,
	}
}

// ensureMigrationOutputDir creates the migration output directory, including
// every missing parent above it.
//
// The parents are the point. This used to os.Mkdir the leaf after requiring its
// parent to already exist, so `--dir file://a/b` with no `a` failed with
// `parent directory "…/a" is not available` and wrote nothing, where the pinned
// community binary v1.3.0 created `a`, `a/b`, the migration file and atlas.sum
// and exited 0 — measured on 2026-08-07 at two and at three missing levels
// (stokaro/ptah#1241 item 4). Ptah's OTHER writing verb already did this: the
// `migrate diff` writer creates parents through
// internal/atlasmigrate.ensureMigrationDirParent, so the two writers disagreed
// with each other as well as with that binary.
//
// What must NOT relax is a path component that exists and is not a directory.
// os.MkdirAll refuses that with ENOTDIR naming the offending component, and the
// pinned binary refuses it too (`--dir file://a/b` over a regular file `a`
// exits 1 with `stat a/b: not a directory`), so both stay exit 1.
func ensureMigrationOutputDir(outputDir string) error {
	info, err := os.Stat(outputDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%q exists and is not a directory", outputDir)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.MkdirAll(outputDir, 0755)
}

func writeNewMigrationFile(path, content string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	removeOnError = false
	return nil
}
