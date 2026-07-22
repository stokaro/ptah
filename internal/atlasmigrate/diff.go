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
}

type DiffResult struct {
	Synced        bool
	MigrationPath string
	SumPath       string
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
	if strings.TrimSpace(opts.Name) == "" {
		opts.Name = "migration"
	}
	schemas := schemascope.SplitNames(opts.Schemas)
	format := atlasreport.NormalizeMigrateDiffFormat(opts.Format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
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
	current, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	if err != nil {
		return DiffResult{}, fmt.Errorf("read dev database schema: %w", err)
	}
	defaultSchema := conn.Info().Schema
	current = schemascope.FilterDatabaseWithDefaultSchema(withoutRevisionTable(current), schemas, defaultSchema)

	dialect := conn.Info().Dialect
	desired, err := schemafile.LoadAll(opts.ToURLs, schemafile.Options{Dialect: dialect})
	if err != nil {
		return DiffResult{}, fmt.Errorf("load --to schema: %w", err)
	}
	desired = schemascope.FilterGeneratedWithDefaultSchema(desired, schemas, defaultSchema)
	diff := atlasschema.ApplyDiffPolicy(schemadiff.CompareWithDialect(desired, current, dialect), opts.Policy)
	if !diff.HasChanges() {
		return DiffResult{Synced: true}, nil
	}

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, desired, dialect, planner.Options{
		ConcurrentIndexes: opts.Policy.ConcurrentIndexCreate,
	})
	if err != nil {
		return DiffResult{}, fmt.Errorf("generate migration SQL: %w", err)
	}
	sqlText, err := renderMigrationDiffSQL(statements, format)
	if err != nil {
		return DiffResult{}, err
	}
	path, err := writeMigrationFile(opts.Dir, opts.Name, sqlText)
	if err != nil {
		return DiffResult{}, err
	}
	sumPath := filepath.Join(opts.Dir, migratesum.AtlasFileName)
	if _, err := migratesum.WriteWithFormat(opts.Dir, migrator.MigrationDirFormatAtlas); err != nil {
		_ = os.Remove(path)
		return DiffResult{}, fmt.Errorf("write atlas.sum: %w", err)
	}
	return DiffResult{MigrationPath: path, SumPath: sumPath}, nil
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
	if err := conn.SchemaWriter().DropAllTables(); err != nil {
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

func writeMigrationFile(dir, name, sql string) (string, error) {
	if strings.TrimSpace(sql) == "" {
		return "", fmt.Errorf("migration SQL is empty")
	}
	version, err := nextMigrationVersion(dir)
	if err != nil {
		return "", err
	}
	slug := migrationSlug(name)
	for {
		path := filepath.Join(dir, fmt.Sprintf("%d_%s.sql", version, slug))
		err := writeNewMigrationFile(path, sql)
		if err == nil {
			return path, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("write migration file: %w", err)
		}
		version++
	}
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
