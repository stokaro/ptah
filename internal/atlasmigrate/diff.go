package atlasmigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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

	if err := os.MkdirAll(filepath.Dir(filepath.Clean(opts.Dir)), 0755); err != nil {
		return DiffResult{}, fmt.Errorf("create migration directory parent: %w", err)
	}
	dirLock, err := acquireDirLock(ctx, opts.Dir, opts.LockTimeout)
	if err != nil {
		return DiffResult{}, err
	}
	defer func() {
		err = errors.Join(err, dirLock.release())
	}()
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
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return DiffResult{}, fmt.Errorf("create migration directory: %w", err)
	}
	if err := recoverPendingPublication(opts.Dir); err != nil {
		return DiffResult{}, fmt.Errorf("recover migration artifact publication: %w", err)
	}
	migrationSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(opts.Dir))
	if err != nil {
		return DiffResult{}, fmt.Errorf("capture migration directory: %w", err)
	}
	if err := verifyDirSum(migrationSnapshot); err != nil {
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
	if err := verifyMigrationDirUnchanged(opts.Dir, migrationSnapshot); err != nil {
		return DiffResult{}, err
	}
	if opts.DryRun {
		return DiffResult{SQL: joinFileContentSQL(contents)}, nil
	}
	return writeDiffArtifacts(
		ctx,
		opts.Dir,
		opts.Name,
		contents,
		migrationSnapshot,
		opts.PreparePublication,
	)
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
