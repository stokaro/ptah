package atlasmigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/internal/schemascope"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
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
	// DirFormat names the directory convention this run reads the existing
	// migrations in and writes the new ones in. The empty value is the native
	// Atlas layout, which is what every caller outside the Atlas-compatible
	// `migrate diff` wants.
	//
	// A foreign layout changes four things and only four: the existing
	// directory is converted before it is replayed, the next version is chosen
	// from that converted view, the files are named and composed in that
	// layout, and atlas.sum is computed over that layout's covered set.
	DirFormat atlasmigrateimport.Format
	// PlanBidirectional plans the forward diff and the rollback that restores
	// its pre-change state through one shared semantic capability.
	//
	// It is a hook rather than a call because of an import edge:
	// [go.5x5.cz/ptah/migration/generator] owns bidirectional planning but
	// imports this package for migration-directory and qualifier primitives, so
	// this package cannot import it back. The compatibility surface imports both
	// and adapts the exported result into BidirectionalPlan.
	//
	// Leaving it nil preserves the native Atlas behavior: this package plans the
	// forward direction directly and no rollback half, because that layout's
	// migration files do not carry one.
	PlanBidirectional func(BidirectionalPlanInput) (BidirectionalPlan, error)
	Schemas           []string
	LockTimeout       time.Duration
	Policy            atlasschema.DiffPolicy
	Qualifier         Qualifier
	DryRun            bool
	// Diagnostics receives non-fatal notices about desired objects whose
	// creation cannot be planned safely from the replayed directory's coverage.
	Diagnostics io.Writer
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames selects the desired-state HCL unknown-name policy;
	// see [go.5x5.cz/ptah/internal/atlassource.ResolveOptions].
	IgnoreUnknownHCLNames bool
	// ValidateDesiredSchema applies a caller-selected policy after the desired
	// source is resolved and before migration-directory planning. Nil accepts
	// every modeled object.
	ValidateDesiredSchema func(*goschema.Database) error
	// ValidateInspectedSchema replaces ValidateDesiredSchema for live database
	// and replayed migration-directory desired states.
	ValidateInspectedSchema func(*goschema.Database) error
	// ValidateLiveObject applies a caller-selected policy to supplemental
	// catalog objects in live desired sources and both replayed database states.
	// Nil performs no supplemental catalog reads.
	ValidateLiveObject func(atlasschema.LiveSchemaObject) error
	// ValidateMigrationSource applies a caller-selected policy to the stable,
	// converted migration-directory snapshot before dev-database replay. The
	// callback also revalidates the locked snapshot before publication planning.
	ValidateMigrationSource func(fs.FS) error
	// ValidateLocalSchemaSource applies a caller-selected policy to each local
	// desired-schema path before parsing or dev-database work.
	ValidateLocalSchemaSource func(string) error
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
	// VerifyName checks Name just before migration files are written, and only
	// then. Leave it nil to accept whatever the file naming produces.
	//
	// The position is the point: the community binary composes its file name
	// from the migration name verbatim and fails at the open, so a name it
	// cannot write refuses a run that HAS changes and passes a run that has
	// none (measured on the pinned v1.3.0: `migrate diff sub/name` exits 0 on a
	// synced directory and 1 on one with changes). Checking earlier would refuse
	// where it accepts, which is the direction compatibility forbids in both
	// senses.
	VerifyName func(string) error
}

// BidirectionalPlanInput is the primitive planning state passed through the
// injected compatibility adapter. It deliberately contains no generator type,
// preserving the dependency direction between the packages.
type BidirectionalPlanInput struct {
	Diff                  *difftypes.SchemaDiff
	DesiredSchema         *goschema.Database
	CurrentSchema         *dbschematypes.DBSchema
	Dialect               string
	Capabilities          capability.Capabilities
	ConcurrentIndexCreate bool
	ConcurrentIndexDrop   bool
}

// BidirectionalPlan is the primitive output internal migrate-diff rendering
// consumes from the injected shared planner.
type BidirectionalPlan struct {
	ForwardNodes                 []ast.Node
	ReverseNodes                 []ast.Node
	ReverseRequiresNoTransaction bool
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

// devSchemaReader reads the replayed dev database at an ALREADY RESOLVED read
// scope. The names come from [schemascope.ReadNames] at the call site, not from
// here, so this verb reads the same schemas `schema diff`, `schema inspect` and
// `schema apply` read (stokaro/ptah#1276).
type devSchemaReader func(
	conn *dbschema.DatabaseConnection,
	readNames []string,
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
	// The directory is replayed as native Atlas in every case, but a foreign
	// layout is CONVERTED into that shape first rather than parsed as if it
	// already were one. The conversion is the same one every reading verb runs
	// (`migrate apply`, `hash`, `validate`, `lint`), so the state this run
	// diffs against is the state those verbs report.
	replaySource, err := validatedDiffReplaySource(migrationSnapshot, opts)
	if err != nil {
		return DiffResult{}, err
	}
	var (
		diff    *difftypes.SchemaDiff
		current *dbschematypes.DBSchema
	)
	if err := runtime.withReplayedSnapshot(
		ctx,
		conn,
		replaySource,
		migrator.MigrationDirFormatAtlas,
		func(replayConn *dbschema.DatabaseConnection) error {
			replayed, compared, err := compareReplayedState(
				ctx, replayConn, runtime, schemas, devDefaultSchema, desired,
				opts.Diagnostics, opts.ValidateLiveObject,
			)
			if err != nil {
				return err
			}
			current, diff = replayed, compared
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

	contents, err := planDiffFileContents(diff, desired, current, info, format, opts)
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
	if opts.VerifyName != nil {
		if err := opts.VerifyName(opts.Name); err != nil {
			return DiffResult{}, err
		}
	}
	return writeDiffArtifacts(
		ctx,
		writer,
		opts.Name,
		contents,
		migrationSnapshot,
		opts.PreparePublication,
		diffWriteLayout{format: opts.dirFormat(), versionFS: replaySource},
	)
}

func validatedDiffReplaySource(snapshot fsnapshot.Snapshot, opts DiffOptions) (fs.FS, error) {
	replaySource, err := diffReplaySource(snapshot, opts)
	if err != nil {
		return nil, err
	}
	if opts.ValidateMigrationSource != nil {
		if err := opts.ValidateMigrationSource(replaySource); err != nil {
			return nil, fmt.Errorf("validate current migration directory: %w", err)
		}
	}
	return replaySource, nil
}

// dirFormat is the layout this run reads and writes, with the zero value
// resolved to the native Atlas one.
func (o DiffOptions) dirFormat() atlasmigrateimport.Format {
	if o.DirFormat == "" {
		return atlasmigrateimport.FormatAtlas
	}
	return o.DirFormat
}

// diffReplaySource returns the Atlas-shaped view of the migration directory
// this run replays and picks its next version from.
//
// A native Atlas directory is already that view. A foreign one is converted
// through the shared reader, which is deliberately the same conversion
// `migrate apply` and `migrate lint` run: a directory whose replayed state
// differed between "what apply would do" and "what diff planned against" would
// generate a migration for changes that are not there.
//
// Integrity is NOT re-checked here. The compatibility surface gates the
// directory over the selected layout's covered set before this package is
// entered, and [DiffOptions.VerifyDir] re-checks the locked snapshot with that
// same predicate; converting first and gating after is the ordering
// stokaro/ptah#973 forbids.
func diffReplaySource(snapshot fsnapshot.Snapshot, opts DiffOptions) (fs.FS, error) {
	format := opts.dirFormat()
	if ReadsNativeAtlasDir(format) {
		return snapshot, nil
	}
	loaded, err := atlasmigrateimport.LoadFS(snapshot, opts.Dir, format)
	if err != nil {
		return nil, fmt.Errorf("read migration directory as %s: %w", format, err)
	}
	return loaded.FS(), nil
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
	if err := sqlitevirtual.ValidateToggle(conn.Info().Dialect); err != nil {
		return preparedDiff{}, err
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
	// `migrate diff` has no target URL, so --dev-url is the only URL that can
	// limit the run to one schema.
	schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(devURL, "", "")
	state, err := opts.Desired.Resolve(ctx, atlassource.ResolveOptions{
		Dialect:                   conn.Info().Dialect,
		DialectFlag:               "--dev-url",
		DevURL:                    devURL,
		SchemaScope:               schemaScope,
		SchemaScopeFlag:           schemaScopeFlag,
		ConnectTimeout:            opts.SourceConnectTimeout,
		DevLockHeld:               true,
		IgnoreUnknownHCLNames:     opts.IgnoreUnknownHCLNames,
		Vars:                      opts.Vars,
		ValidateSchema:            opts.ValidateDesiredSchema,
		ValidateInspectedSchema:   opts.ValidateInspectedSchema,
		ValidateInspectedDatabase: atlasschema.LiveDatabaseValidator(opts.ValidateLiveObject),
		ValidateMigrationSource:   opts.ValidateMigrationSource,
		ValidateLocalSchemaSource: opts.ValidateLocalSchemaSource,
	})
	if err != nil {
		return atlassource.State{}, fmt.Errorf("load --to schema: %w", err)
	}
	return state, nil
}

// planDiffFileContents plans the migration AST, applies the typed qualifier,
// and renders the migration file contents for one diff run — both directions.
func planDiffFileContents(
	diff *difftypes.SchemaDiff,
	desired *goschema.Database,
	current *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	format string,
	opts DiffOptions,
) ([]MigrationFileContent, error) {
	var planned BidirectionalPlan
	if opts.PlanBidirectional == nil {
		upNodes, err := planner.GenerateSchemaDiffASTWithOptions(diff, desired, info.Dialect, planner.Options{
			Capabilities:         info.Capabilities,
			ConcurrentIndexes:    opts.Policy.ConcurrentIndexCreate,
			ConcurrentIndexDrops: opts.Policy.ConcurrentIndexDrop,
		})
		if err != nil {
			return nil, fmt.Errorf("generate migration SQL: %w", err)
		}
		planned.ForwardNodes = upNodes
	} else {
		var err error
		planned, err = opts.PlanBidirectional(BidirectionalPlanInput{
			Diff:                  diff,
			DesiredSchema:         desired,
			CurrentSchema:         current,
			Dialect:               info.Dialect,
			Capabilities:          info.Capabilities,
			ConcurrentIndexCreate: opts.Policy.ConcurrentIndexCreate,
			ConcurrentIndexDrop:   opts.Policy.ConcurrentIndexDrop,
		})
		if err != nil {
			return nil, fmt.Errorf("generate migration plan: %w", err)
		}
	}
	if err := opts.Qualifier.ApplyToPlan(info.Dialect, desired, planned.ForwardNodes); err != nil {
		return nil, err
	}
	contents, err := BuildMigrationFileContents(
		info.Dialect,
		info.Capabilities,
		format,
		planned.ForwardNodes,
	)
	if err != nil {
		return nil, err
	}
	if len(planned.ReverseNodes) == 0 {
		if err := validateMigrationFileContentsTransactionModes(opts.dirFormat(), contents); err != nil {
			return nil, err
		}
		return contents, nil
	}
	priorSchema := dbschematogo.ConvertDBSchemaToGoSchema(current)
	if err := opts.Qualifier.ApplyToPlan(info.Dialect, priorSchema, planned.ReverseNodes); err != nil {
		return nil, err
	}
	reverse, err := renderMigrationStatements(info.Dialect, info.Capabilities, planned.ReverseNodes)
	if err != nil {
		return nil, fmt.Errorf("generate rollback SQL: %w", err)
	}
	contents, err = attachReversePlan(
		contents,
		reverse,
		format,
		planned.ReverseRequiresNoTransaction,
	)
	if err != nil {
		return nil, err
	}
	if err := validateMigrationFileContentsTransactionModes(opts.dirFormat(), contents); err != nil {
		return nil, err
	}
	return contents, nil
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

// compareReplayedState reads the state the migration directory just replayed
// on the dev database and compares the desired state against it.
//
// Which schemas that read covers is resolved here, by the one owner every other
// read consumes. A directory that creates `extra.b`, replayed and then read at
// the dev connection's own schema, reported the replay as having created
// nothing there, and `migrate diff` re-emitted `CREATE TABLE "extra"."b"` into a
// new migration file on every run: two runs, two files, and applying the
// directory failed at SQLSTATE 42P07. Measured on PostgreSQL 17.10
// (stokaro/ptah#1276).
func compareReplayedState(
	ctx context.Context,
	replayConn *dbschema.DatabaseConnection,
	runtime diffRuntime,
	schemas []string,
	defaultSchema string,
	desired *goschema.Database,
	diagnostics io.Writer,
	validateLiveObject func(atlasschema.LiveSchemaObject) error,
) (*dbschematypes.DBSchema, *difftypes.SchemaDiff, error) {
	readNames, err := schemascope.ReadNames(ctx, replayConn.Info(), schemas, replayConn)
	if err != nil {
		return nil, nil, fmt.Errorf("read dev database schema: %w", err)
	}
	replayed, err := runtime.readDevSchema(replayConn, readNames, defaultSchema)
	if err != nil {
		return nil, nil, err
	}
	if err := atlasschema.ValidateLiveObjects(replayConn, readNames, validateLiveObject); err != nil {
		return nil, nil, err
	}
	diff, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(
		ctx, replayConn, desired, replayed, nil,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("compare dev database schema: %w", err)
	}
	atlasschema.ReportUndecidedAdditions(
		diagnostics,
		undecided,
		"the replayed migration directory",
		"--to",
	)
	return replayed, diff, nil
}

// readScopedDevSchema reads the state the migration directory replayed, which
// is the CURRENT side of `migrate diff`.
//
// readNames is the resolved read scope, and it is also what the result is
// filtered by. The two were separate when the filter carried an explicit
// `--schema` selection and the read carried nothing, and that gap is the
// defect: filtering by the names actually read is a no-op for every object the
// read could have returned, and it is exactly the selection when one was made
// (stokaro/ptah#1276).
func readScopedDevSchema(
	conn *dbschema.DatabaseConnection,
	readNames []string,
	defaultSchema string,
) (*dbschematypes.DBSchema, error) {
	current, err := dbschema.ReadSchemaWithSchemas(conn, readNames)
	if err != nil {
		return nil, fmt.Errorf("read dev database schema: %w", err)
	}
	return schemascope.FilterDatabaseWithDefaultSchema(
		withoutRevisionTable(current),
		readNames,
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
