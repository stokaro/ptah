package atlasmigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/atlassource"
	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/migrationreplay"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
	"github.com/stokaro/ptah/internal/schemascope"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

const (
	revisionTableName = "atlas_schema_revisions"
	lockFileName      = ".ptah-migrate-diff.lock"
)

type DiffOptions struct {
	Dir                  string
	Directory            *PreparedDiffDirectory
	Desired              atlassource.Set
	SourceConnectTimeout time.Duration
	Name                 string
	Format               string
	Schemas              []string
	LockTimeout          time.Duration
	Policy               atlasschema.DiffPolicy
	Qualifier            Qualifier
	DryRun               bool
	// PreparePublication may edit the staged migration files before they are
	// durably published and included in atlas.sum. The callback runs while the
	// migration-directory lock is held.
	PreparePublication func([]string) error
}

// DiffDirectorySource is a rooted live view of a writable migration directory.
// VerifyPath must fail when the directory pathname no longer identifies the
// rooted filesystem object.
type DiffDirectorySource interface {
	FS() fs.FS
	VerifyPath() error
}

// PreparedDiffDirectory holds the immutable migration input and the rooted live
// directory used to reject changes before publication.
type PreparedDiffDirectory struct {
	snapshot           fsnapshot.Snapshot
	source             DiffDirectorySource
	publicationStarted atomic.Bool
}

type DiffResult struct {
	Synced bool
	SQL    string
	// MigrationPaths lists every migration file written by this diff run, in
	// apply order. A plan that mixes transactional statements with concurrent
	// index builds is split into two files (see BuildMigrationFileContents).
	MigrationPaths []string
	SumPath        string
}

type devSchemaReader func(
	conn *dbschema.DatabaseConnection,
	schemas []string,
	defaultSchema string,
) (*dbschematypes.DBSchema, error)

type replaySnapshotConsumer func(
	context.Context,
	*dbschema.DatabaseConnection,
	fs.FS,
	migrator.MigrationDirFormat,
	func(*dbschema.DatabaseConnection) error,
) error

type diffRuntime struct {
	readDevSchema        devSchemaReader
	withReplayedSnapshot replaySnapshotConsumer
}

type preparedDiff struct {
	opts    DiffOptions
	schemas []string
	format  string
}

// PrepareDiffDirectory recovers interrupted publication and captures one
// immutable migration snapshot while holding the directory lock. Callers must
// complete this preparation before opening the dev database and keep source
// open until GenerateDiff returns.
func PrepareDiffDirectory(
	ctx context.Context,
	dir string,
	source DiffDirectorySource,
	lockTimeout time.Duration,
) (prepared *PreparedDiffDirectory, resultErr error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("migrate diff requires migration directory")
	}
	if source == nil {
		return nil, errors.New("migrate diff requires rooted migration directory")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dirLock, err := acquireDirLock(ctx, dir, lockTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, dirLock.release())
	}()
	if err := source.VerifyPath(); err != nil {
		return nil, fmt.Errorf("verify migration directory identity: %w", err)
	}
	if err := recoverPendingPublication(dir); err != nil {
		return nil, fmt.Errorf("recover migration artifact publication: %w", err)
	}
	if err := source.VerifyPath(); err != nil {
		return nil, fmt.Errorf("verify migration directory identity: %w", err)
	}
	snapshot, err := migrationsnapshot.CaptureStable(source.FS())
	if err != nil {
		return nil, fmt.Errorf("capture migration directory: %w", err)
	}
	if err := verifyDirSum(snapshot); err != nil {
		return nil, err
	}
	return &PreparedDiffDirectory{snapshot: snapshot, source: source}, nil
}

func GenerateDiff(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts DiffOptions,
) (DiffResult, error) {
	return generateDiff(ctx, conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})
}

func generateDiff(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts DiffOptions,
	runtime diffRuntime,
) (result DiffResult, err error) {
	prepared, err := prepareDiff(ctx, conn, opts)
	if err != nil {
		return DiffResult{}, err
	}
	opts = prepared.opts
	schemas := prepared.schemas
	format := prepared.format

	dirLock, err := acquireDirLock(ctx, opts.Dir, opts.LockTimeout)
	if err != nil {
		return DiffResult{}, err
	}
	defer func() {
		err = errors.Join(err, dirLock.release())
	}()
	if err := opts.Directory.verifyIdentity(); err != nil {
		return DiffResult{}, err
	}
	if err := recoverPendingPublication(opts.Dir); err != nil {
		return DiffResult{}, fmt.Errorf("recover migration artifact publication: %w", err)
	}
	migrationSnapshot, err := opts.Directory.currentSnapshot()
	if err != nil {
		return DiffResult{}, err
	}
	devLock, err := acquireDevDatabaseLock(ctx, conn, opts.LockTimeout)
	if err != nil {
		return DiffResult{}, err
	}
	defer func() {
		err = errors.Join(err, devLock.release())
	}()
	desiredState, err := resolveDesiredState(ctx, conn, opts)
	if err != nil {
		return DiffResult{}, err
	}

	info := conn.Info()
	devDefaultSchema := info.Schema
	desiredDefaultSchema := desiredState.DefaultSchema
	if desiredDefaultSchema == "" {
		desiredDefaultSchema = devDefaultSchema
	}
	desired := schemascope.FilterGeneratedWithDefaultSchema(desiredState.Schema, schemas, desiredDefaultSchema)
	var diff *difftypes.SchemaDiff
	if err := runtime.withReplayedSnapshot(
		ctx,
		conn,
		migrationSnapshot,
		migrator.MigrationDirFormatAtlas,
		func(replayConn *dbschema.DatabaseConnection) error {
			current, err := runtime.readDevSchema(replayConn, schemas, devDefaultSchema)
			if err != nil {
				return err
			}
			diff, err = schemadiff.CompareWithDatabase(ctx, replayConn, desired, current, nil)
			if err != nil {
				return fmt.Errorf("compare dev database schema: %w", err)
			}
			return nil
		},
	); err != nil {
		return DiffResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DiffResult{}, err
	}
	diff = atlasschema.ApplyDiffPolicy(diff, opts.Policy)
	if !diff.HasChanges() {
		return DiffResult{Synced: true}, nil
	}

	contents, err := planDiffFileContents(diff, desired, info, format, opts)
	if err != nil {
		return DiffResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DiffResult{}, err
	}
	if err := opts.Directory.verifySnapshot(migrationSnapshot); err != nil {
		return DiffResult{}, err
	}
	if opts.DryRun {
		return DiffResult{SQL: joinFileContentSQL(contents)}, nil
	}
	preparePublication := func(stagedPaths []string) error {
		if opts.PreparePublication != nil {
			if err := opts.PreparePublication(stagedPaths); err != nil {
				return err
			}
		}
		return opts.Directory.verifySnapshot(migrationSnapshot)
	}
	opts.Directory.publicationStarted.Store(true)
	return writeDiffArtifacts(
		ctx,
		opts.Dir,
		opts.Name,
		contents,
		migrationSnapshot,
		preparePublication,
	)
}

// MayHavePublicationArtifacts reports whether GenerateDiff entered journaled
// publication. Callers must preserve the directory on errors after this point
// so a later run can recover staged artifacts.
func (d *PreparedDiffDirectory) MayHavePublicationArtifacts() bool {
	return d != nil && d.publicationStarted.Load()
}

func (d *PreparedDiffDirectory) currentSnapshot() (fsnapshot.Snapshot, error) {
	if d == nil {
		return fsnapshot.Snapshot{}, errors.New("migrate diff requires prepared migration directory")
	}
	if err := d.verifySnapshot(d.snapshot); err != nil {
		return fsnapshot.Snapshot{}, err
	}
	return d.snapshot.Clone(), nil
}

func (d *PreparedDiffDirectory) verifyIdentity() error {
	if err := d.source.VerifyPath(); err != nil {
		return fmt.Errorf("migration directory changed during migrate diff planning: %w", err)
	}
	return nil
}

func (d *PreparedDiffDirectory) verifySnapshot(expected fsnapshot.Snapshot) error {
	if err := d.verifyIdentity(); err != nil {
		return err
	}
	current, err := migrationsnapshot.CaptureStable(d.source.FS())
	if err != nil {
		return fmt.Errorf("verify migration directory after diff planning: %w", err)
	}
	if !current.Equal(expected) {
		return fmt.Errorf("migration directory changed during migrate diff planning")
	}
	return nil
}

func prepareDiff(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts DiffOptions,
) (preparedDiff, error) {
	if conn == nil {
		return preparedDiff{}, errors.New("migrate diff requires dev database connection")
	}
	if strings.TrimSpace(opts.Dir) == "" {
		return preparedDiff{}, errors.New("migrate diff requires migration directory")
	}
	if len(opts.Desired.Sources) == 0 {
		return preparedDiff{}, errors.New("migrate diff requires desired state")
	}
	opts = normalizeDiffOptions(opts)
	schemas := schemascope.SplitNames(opts.Schemas)
	format := atlasreport.NormalizeMigrateDiffFormat(opts.Format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
		return preparedDiff{}, err
	}
	if err := opts.Qualifier.ValidateScope(conn.Info().Dialect, schemas); err != nil {
		return preparedDiff{}, err
	}
	if err := ctx.Err(); err != nil {
		return preparedDiff{}, err
	}
	if opts.Directory == nil {
		return preparedDiff{}, errors.New("migrate diff requires prepared migration directory")
	}
	return preparedDiff{opts: opts, schemas: schemas, format: format}, nil
}

func resolveDesiredState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts DiffOptions,
) (atlassource.State, error) {
	devURL := conn.Info().URL
	if err := opts.Desired.EnsureDevDatabase(devURL); err != nil {
		return atlassource.State{}, err
	}
	if err := opts.Desired.EnsureDevIsolation(devURL); err != nil {
		return atlassource.State{}, err
	}
	state, err := opts.Desired.Resolve(ctx, atlassource.ResolveOptions{
		Dialect:        conn.Info().Dialect,
		DialectFlag:    "--dev-url",
		DevURL:         devURL,
		ConnectTimeout: opts.SourceConnectTimeout,
		DevLockHeld:    true,
	})
	if err != nil {
		return atlassource.State{}, fmt.Errorf("load --to schema: %w", err)
	}
	return state, nil
}

// planDiffFileContents plans the migration AST, applies the typed qualifier,
// and renders the Atlas migration file contents for one diff run.
func planDiffFileContents(
	diff *difftypes.SchemaDiff,
	desired *goschema.Database,
	info dbschematypes.DBInfo,
	format string,
	opts DiffOptions,
) ([]MigrationFileContent, error) {
	upNodes, err := planner.GenerateSchemaDiffASTWithOptions(diff, desired, info.Dialect, planner.Options{
		Capabilities:      info.Capabilities,
		ConcurrentIndexes: opts.Policy.ConcurrentIndexCreate,
	})
	if err != nil {
		return nil, fmt.Errorf("generate migration SQL: %w", err)
	}
	if err := opts.Qualifier.ApplyToPlan(info.Dialect, desired, upNodes); err != nil {
		return nil, err
	}
	return BuildMigrationFileContents(info.Dialect, info.Capabilities, format, upNodes)
}

func joinFileContentSQL(contents []MigrationFileContent) string {
	sqls := make([]string, 0, len(contents))
	for _, content := range contents {
		sqls = append(sqls, content.SQL)
	}
	return strings.Join(sqls, "\n")
}

func normalizeDiffOptions(opts DiffOptions) DiffOptions {
	if strings.TrimSpace(opts.Name) == "" {
		opts.Name = "migration"
	}
	return opts
}

func readScopedDevSchema(
	conn *dbschema.DatabaseConnection,
	schemas []string,
	defaultSchema string,
) (*dbschematypes.DBSchema, error) {
	current, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return nil, fmt.Errorf("read dev database schema: %w", err)
	}
	return schemascope.FilterDatabaseWithDefaultSchema(
		withoutRevisionTable(current),
		schemas,
		defaultSchema,
	), nil
}

func renderMigrationDiffSQL(statements []string, format string) (string, error) {
	report := atlasreport.NewSchemaDiff(nil, nil, statements)
	var out bytes.Buffer
	if err := atlasreport.WriteSchemaDiff(&out, format, report); err != nil {
		return "", err
	}
	return out.String(), nil
}

func verifyDirSum(fsys fs.FS) error {
	result, err := migratesum.VerifyWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	if errors.Is(err, migratesum.ErrSumFileMissing) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("migration directory checksum verification failed: %w", err)
	}
	if !result.OK() {
		return fmt.Errorf("migration directory checksum verification failed:\n%s", result.Describe())
	}
	return nil
}

func verifyMigrationDirUnchanged(dir string, expected fsnapshot.Snapshot) error {
	current, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	if err != nil {
		return fmt.Errorf("recapture migration directory: %w", err)
	}
	if !expected.Equal(current) {
		return errors.New("migration directory changed during migrate diff planning")
	}
	return nil
}

func withoutRevisionTable(schema *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	if schema == nil {
		return &dbschematypes.DBSchema{}
	}
	out := *schema
	out.Tables = filterByTable(out.Tables, func(table dbschematypes.DBTable) bool {
		return !strings.EqualFold(table.Name, revisionTableName)
	})
	out.Indexes = filterByTable(out.Indexes, func(index dbschematypes.DBIndex) bool {
		return !strings.EqualFold(index.TableName, revisionTableName)
	})
	out.Constraints = filterByTable(out.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return !strings.EqualFold(constraint.TableName, revisionTableName)
	})
	return &out
}

func filterByTable[T any](values []T, keep func(T) bool) []T {
	out := make([]T, 0, len(values))
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}
