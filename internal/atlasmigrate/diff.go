package atlasmigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/schemafile"
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
	Dir         string
	ToURLs      []string
	Name        string
	Format      string
	Schemas     []string
	LockTimeout time.Duration
	Policy      atlasschema.DiffPolicy
	Qualifier   Qualifier
	DryRun      bool
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

func GenerateDiff(ctx context.Context, conn *dbschema.DatabaseConnection, opts DiffOptions) (result DiffResult, err error) {
	if conn == nil {
		return DiffResult{}, errors.New("migrate diff requires dev database connection")
	}
	if strings.TrimSpace(opts.Dir) == "" {
		return DiffResult{}, errors.New("migrate diff requires migration directory")
	}
	if len(opts.ToURLs) == 0 {
		return DiffResult{}, errors.New("migrate diff requires desired schema URLs")
	}
	opts = normalizeDiffOptions(opts)
	schemas := schemascope.SplitNames(opts.Schemas)
	format := atlasreport.NormalizeMigrateDiffFormat(opts.Format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
		return DiffResult{}, err
	}
	if err := opts.Qualifier.ValidateScope(conn.Info().Dialect, schemas); err != nil {
		return DiffResult{}, err
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return DiffResult{}, fmt.Errorf("create migration directory: %w", err)
	}
	dirLock, err := acquireDirLock(ctx, opts.Dir, opts.LockTimeout)
	if err != nil {
		return DiffResult{}, err
	}
	defer func() {
		if releaseErr := dirLock.release(); releaseErr != nil && err == nil {
			err = releaseErr
		}
	}()
	if err := verifyDirSum(opts.Dir); err != nil {
		return DiffResult{}, err
	}

	if err := replayDir(ctx, conn, opts.Dir); err != nil {
		return DiffResult{}, err
	}
	defaultSchema := conn.Info().Schema
	current, err := readScopedDevSchema(conn, schemas, defaultSchema)
	if err != nil {
		return DiffResult{}, err
	}

	info := conn.Info()
	dialect := info.Dialect
	desired, err := schemafile.LoadAll(opts.ToURLs, schemafile.Options{Dialect: dialect})
	if err != nil {
		return DiffResult{}, fmt.Errorf("load --to schema: %w", err)
	}
	desired = schemascope.FilterGeneratedWithDefaultSchema(desired, schemas, defaultSchema)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, current, nil)
	if err != nil {
		return DiffResult{}, fmt.Errorf("compare dev database schema: %w", err)
	}
	diff = atlasschema.ApplyDiffPolicy(diff, opts.Policy)
	if !diff.HasChanges() {
		return DiffResult{Synced: true}, nil
	}

	contents, err := planDiffFileContents(diff, desired, info, format, opts)
	if err != nil {
		return DiffResult{}, err
	}
	if opts.DryRun {
		return DiffResult{SQL: joinFileContentSQL(contents)}, nil
	}
	return writeDiffArtifacts(opts.Dir, opts.Name, contents)
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

// writeDiffArtifacts writes every planned migration file and then atlas.sum,
// all-or-nothing.
func writeDiffArtifacts(dir, name string, contents []MigrationFileContent) (DiffResult, error) {
	paths, err := writeMigrationFiles(dir, name, contents)
	if err != nil {
		return DiffResult{}, err
	}
	sumPath, err := writeDirSum(dir, paths)
	if err != nil {
		return DiffResult{}, err
	}
	return DiffResult{MigrationPaths: paths, SumPath: sumPath}, nil
}

// writeDirSum refreshes atlas.sum after every migration file was written. A
// failed checksum update rolls the whole generation back: the new migration
// files are removed and the previous atlas.sum content is restored, so the
// directory hash only ever changes after a fully successful generation.
func writeDirSum(dir string, migrationPaths []string) (string, error) {
	sumPath := filepath.Join(dir, migratesum.AtlasFileName)
	previousSum, previousErr := os.ReadFile(sumPath)
	if _, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas); err != nil {
		removeFiles(migrationPaths)
		restoreSumFile(sumPath, previousSum, previousErr)
		return "", fmt.Errorf("write atlas.sum: %w", err)
	}
	return sumPath, nil
}

func removeFiles(paths []string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

// restoreSumFile best-effort restores atlas.sum's previous bytes after a
// failed overwrite, or removes it when it did not exist before. When the
// previous content could not be read for any other reason, the file is left
// alone rather than destroyed.
func restoreSumFile(path string, previous []byte, previousErr error) {
	switch {
	case previousErr == nil:
		_ = os.WriteFile(path, previous, 0644) //nolint:gosec // atlas.sum is a shared checked-in file
	case errors.Is(previousErr, os.ErrNotExist):
		_ = os.Remove(path)
	}
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

type dirLock struct {
	path string
	file *os.File
}

func acquireDirLock(ctx context.Context, migrationsDir string, timeout time.Duration) (*dirLock, error) {
	lockPath := filepath.Join(migrationsDir, lockFileName)
	startedAt := time.Now()
	for {
		lock, err := tryAcquireDirLock(lockPath)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if timeout > 0 && time.Since(startedAt) >= timeout {
			return nil, fmt.Errorf("migration directory lock timeout after %s: %s", timeout, lockPath)
		}
		if err := waitForDirLockRetry(ctx, startedAt, timeout); err != nil {
			return nil, err
		}
	}
}

func tryAcquireDirLock(lockPath string) (*dirLock, error) {
	file, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, err
		}
		return nil, fmt.Errorf("create migration directory lock: %w", err)
	}
	if _, err := fmt.Fprintf(file, "pid=%d\n", os.Getpid()); err != nil {
		_ = file.Close()
		_ = os.Remove(lockPath)
		return nil, fmt.Errorf("write migration directory lock: %w", err)
	}
	return &dirLock{path: lockPath, file: file}, nil
}

func waitForDirLockRetry(ctx context.Context, startedAt time.Time, timeout time.Duration) error {
	wait := 25 * time.Millisecond
	if timeout > 0 {
		remaining := timeout - time.Since(startedAt)
		if remaining <= 0 {
			return nil
		}
		wait = min(wait, remaining)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("acquire migration directory lock: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func (l *dirLock) release() error {
	if l == nil {
		return nil
	}
	closeErr := l.file.Close()
	removeErr := os.Remove(l.path)
	if closeErr != nil && removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("release migration directory lock: %w", errors.Join(closeErr, removeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("close migration directory lock: %w", closeErr)
	}
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("remove migration directory lock: %w", removeErr)
	}
	return nil
}

func verifyDirSum(migrationsDir string) error {
	result, err := migratesum.VerifyDirWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
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

func replayDir(ctx context.Context, conn *dbschema.DatabaseConnection, migrationsDir string) error {
	if err := conn.SchemaWriter().DropAllTables(ctx); err != nil {
		return fmt.Errorf("clean dev database: %w", err)
	}
	provider, err := migrator.NewFSMigrationProvider(
		os.DirFS(migrationsDir),
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	if err != nil {
		return fmt.Errorf("load migration directory: %w", err)
	}
	for _, migration := range provider.Migrations() {
		if err := migration.Up(ctx, conn); err != nil {
			return fmt.Errorf("replay migration %d on --dev-url: %w", migration.Version, err)
		}
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

// writeMigrationFiles writes every planned migration file with consecutive
// versions, all-or-nothing: a failed write removes the files already written
// in this run before returning, so a partial failure leaves no partial state
// behind. Versions are allocated together so a split plan stays adjacent and
// ordered in the directory.
func writeMigrationFiles(dir, name string, contents []MigrationFileContent) ([]string, error) {
	if len(contents) == 0 {
		return nil, fmt.Errorf("migration SQL is empty")
	}
	for _, content := range contents {
		if strings.TrimSpace(content.SQL) == "" {
			return nil, fmt.Errorf("migration SQL is empty")
		}
	}
	version, err := nextMigrationVersion(dir)
	if err != nil {
		return nil, err
	}
	for {
		paths, collidedVersion, err := writeMigrationFilesAt(dir, name, version, contents)
		if err != nil {
			return nil, err
		}
		if collidedVersion == 0 {
			return paths, nil
		}
		// Another writer claimed a version despite the directory lock; retry
		// the whole batch above the collision so the files stay adjacent.
		version = collidedVersion + 1
	}
}

// writeMigrationFilesAt attempts one batch write at base version. On a
// version collision it removes this attempt's files and reports the colliding
// version; on any other failure it removes this attempt's files and returns
// the error.
func writeMigrationFilesAt(dir, name string, version int64, contents []MigrationFileContent) ([]string, int64, error) {
	paths := make([]string, 0, len(contents))
	for i, content := range contents {
		fileVersion := version + int64(i)
		slug := migrationSlug(name + content.NameSuffix)
		path := filepath.Join(dir, fmt.Sprintf("%d_%s.sql", fileVersion, slug))
		err := writeNewMigrationFile(path, content.SQL)
		if errors.Is(err, os.ErrExist) {
			removeFiles(paths)
			return nil, fileVersion, nil
		}
		if err != nil {
			removeFiles(paths)
			return nil, 0, fmt.Errorf("write migration file: %w", err)
		}
		paths = append(paths, path)
	}
	return paths, 0, nil
}

func writeNewMigrationFile(path, sql string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(sql); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write migration SQL: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close migration file: %w", err)
	}
	return nil
}

func nextMigrationVersion(dir string) (int64, error) {
	files, err := migrator.DiscoverMigrationFiles(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	if err != nil {
		return 0, err
	}
	version := migrator.GetNextMigrationVersion()
	for _, file := range files {
		if file.Version >= version {
			version = file.Version + 1
		}
	}
	return version, nil
}

var migrationSlugInvalidChars = regexp.MustCompile(`[^a-z0-9_]+`)

func migrationSlug(name string) string {
	slug := strings.ToLower(strings.TrimSpace(name))
	slug = strings.ReplaceAll(slug, "-", "_")
	slug = strings.ReplaceAll(slug, " ", "_")
	slug = migrationSlugInvalidChars.ReplaceAllString(slug, "")
	slug = strings.Trim(slug, "_")
	if slug == "" {
		return "migration"
	}
	return slug
}
