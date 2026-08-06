package atlasmigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemascope"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

const (
	revisionTableName = "atlas_schema_revisions"
	lockFileName      = ".ptah-migrate-diff.lock"
)

type DiffOptions struct {
	Dir string
	// Root, when set, is the opened project root the migration directory must
	// stay inside. The writer binds Dir through it once and runs every later
	// step -- staging, publication, checksum commit, rollback and recovery --
	// through that handle, so replacing the directory or one of its ancestors
	// after Dir was resolved cannot redirect a write out of the root
	// (stokaro/ptah#1118). The caller retains ownership of the handle.
	//
	// Leave it nil for direct CLI semantics, where an explicit absolute --dir is
	// the operator's own choice of destination and is bound as its own root.
	Root                 *pathguard.OpenedDirectory
	Desired              atlassource.Set
	SourceConnectTimeout time.Duration
	Name                 string
	Format               string
	Schemas              []string
	LockTimeout          time.Duration
	Policy               atlasschema.DiffPolicy
	Qualifier            Qualifier
	DryRun               bool
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// PreparePublication may edit the staged migration files before they are
	// durably published and included in atlas.sum. The callback runs while the
	// migration-directory lock is held.
	PreparePublication func([]string) error
	// VerifyDir re-checks the directory's integrity file against the locked
	// snapshot, after the migration-directory lock is held and before anything
	// is planned. Leave it nil for [verifyDirSum], which accepts a directory
	// carrying no atlas.sum at all.
	//
	// The caller supplies it so that ONE definition of "is this directory
	// verified" answers both the preflight refusal and this recheck. The
	// compatibility surface runs the shared atlas.sum gate before calling in --
	// that is what makes a refusal precede the write (stokaro/ptah#1086) -- and
	// passes the same predicate here so that a directory edited between the two
	// is refused with the same bytes rather than with a second verifier's
	// wording.
	VerifyDir func(fs.FS) error
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

	if err := ensureMigrationDirParent(opts.Root, opts.Dir); err != nil {
		return DiffResult{}, err
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
	// From here on the migration directory is a rooted capability, not a
	// pathname. Everything that reads or writes it -- the capture, the checksum
	// recheck, staging, publication, the atlas.sum commit, rollback and recovery
	// -- goes through this handle, so a directory or ancestor replaced after this
	// point cannot redirect the transaction (stokaro/ptah#1118).
	writer, migrationSnapshot, err := openVerifiedMigrationDir(opts)
	if err != nil {
		return DiffResult{}, err
	}
	defer func() {
		err = errors.Join(err, writer.Close())
	}()

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
	if err := verifyMigrationDirUnchanged(writer, migrationSnapshot); err != nil {
		return DiffResult{}, err
	}
	if opts.DryRun {
		return DiffResult{SQL: joinFileContentSQL(contents)}, nil
	}
	return writeDiffArtifacts(
		ctx,
		writer,
		opts.Name,
		contents,
		migrationSnapshot,
		opts.PreparePublication,
	)
}

// openVerifiedMigrationDir binds the migration directory once and returns it
// together with the snapshot the rest of the run is planned against. Both come
// from the same handle deliberately: verifying one filesystem object and
// planning against another is the defect this ordering closes.
func openVerifiedMigrationDir(
	opts DiffOptions,
) (*migrationWriterDir, fsnapshot.Snapshot, error) {
	writer, err := createMigrationWriterDir(opts.Root, opts.Dir)
	if err != nil {
		return nil, fsnapshot.Snapshot{}, err
	}
	snapshot, err := captureVerifiedMigrationDir(writer, opts)
	if err != nil {
		return nil, fsnapshot.Snapshot{}, errors.Join(err, writer.Close())
	}
	return writer, snapshot, nil
}

func captureVerifiedMigrationDir(
	w *migrationWriterDir,
	opts DiffOptions,
) (fsnapshot.Snapshot, error) {
	if err := recoverPendingPublication(w); err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("recover migration artifact publication: %w", err)
	}
	fsys, err := w.FS()
	if err != nil {
		return fsnapshot.Snapshot{}, err
	}
	snapshot, err := migrationsnapshot.CaptureStable(fsys)
	if err != nil {
		return fsnapshot.Snapshot{}, fmt.Errorf("capture migration directory: %w", err)
	}
	if err := opts.verifyDir(snapshot); err != nil {
		return fsnapshot.Snapshot{}, err
	}
	return snapshot, nil
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
		// `migrate diff` is registered on the Atlas-compatible command tree
		// only, so this surface always reads files written for another tool.
		IgnoreUnknownHCLNames: true,
		Vars:                  opts.Vars,
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
		Capabilities:         info.Capabilities,
		ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
		ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
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

// verifyDir returns the integrity predicate this run re-checks the locked
// snapshot with: the caller's when it supplied one, otherwise [verifyDirSum].
func (o DiffOptions) verifyDir(fsys fs.FS) error {
	if o.VerifyDir != nil {
		return o.VerifyDir(fsys)
	}
	return verifyDirSum(fsys)
}

// verifyDirSum is the default recheck. It deliberately accepts a directory
// carrying no atlas.sum, because diffing into a directory that does not have
// one yet is how the first migration is created; a caller that needs the
// stricter rule -- an unhashed directory that already holds migrations is a
// checksum error -- supplies it through [DiffOptions.VerifyDir].
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

func verifyMigrationDirUnchanged(w *migrationWriterDir, expected fsnapshot.Snapshot) error {
	fsys, err := w.FS()
	if err != nil {
		return err
	}
	current, err := migrationsnapshot.CaptureStable(fsys)
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
