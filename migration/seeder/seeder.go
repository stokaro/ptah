// Package seeder applies environment-scoped SQL seed files and records which
// files have already run.
package seeder

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	allEnv      = "all"
	trackerName = "schema_seeds"
)

var seedFilenameRE = regexp.MustCompile(`^([0-9]+)_(.+)\.([A-Za-z0-9_-]+)\.sql$`)

// SeedFile is one discovered seed SQL file.
type SeedFile struct {
	Path        string
	Filename    string
	Version     int
	Description string
	Env         string
	Checksum    string
}

// Options controls seed execution.
type Options struct {
	Env             string
	ProtectedEnvs   []string
	ProtectedTables []string
	Force           bool
	Idempotent      bool
	AllowProd       bool
}

// Result summarizes one seed command run.
type Result struct {
	Env     string
	Total   int
	Applied []SeedFile
	Skipped []SeedFile
}

// Discover scans fsys for seed files matching NNN_description.env.sql.
func Discover(fsys fs.FS) ([]SeedFile, error) {
	var seeds []SeedFile
	err := fs.WalkDir(fsys, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			return nil
		}

		seed, ok, err := parseSeedPath(fsys, path)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("invalid seed filename %q: expected NNN_description.env.sql", path)
		}
		seeds = append(seeds, seed)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan seeds: %w", err)
	}

	slices.SortFunc(seeds, func(a, b SeedFile) int {
		if a.Version != b.Version {
			return a.Version - b.Version
		}
		return strings.Compare(a.Path, b.Path)
	})
	return seeds, nil
}

// Select filters seed files for the requested environment.
func Select(seeds []SeedFile, env string) []SeedFile {
	env = normalizeEnv(env)
	selected := make([]SeedFile, 0, len(seeds))
	for _, seed := range seeds {
		if seed.Env == allEnv || seed.Env == env {
			selected = append(selected, seed)
		}
	}
	return selected
}

func parseSeedPath(fsys fs.FS, path string) (SeedFile, bool, error) {
	filename := filepath.Base(path)
	matches := seedFilenameRE.FindStringSubmatch(filename)
	if matches == nil {
		return SeedFile{}, false, nil
	}

	version, err := strconv.Atoi(matches[1])
	if err != nil {
		return SeedFile{}, false, fmt.Errorf("parse seed version from %q: %w", path, err)
	}

	data, err := fs.ReadFile(fsys, path)
	if err != nil {
		return SeedFile{}, false, fmt.Errorf("read seed file %q: %w", path, err)
	}
	sum := sha256.Sum256(data)
	return SeedFile{
		Path:        path,
		Filename:    filename,
		Version:     version,
		Description: matches[2],
		Env:         normalizeEnv(matches[3]),
		Checksum:    hex.EncodeToString(sum[:]),
	}, true, nil
}

// ValidateOptions returns an error if the command options would be unsafe.
func ValidateOptions(opts Options) error {
	if normalizeEnv(opts.Env) == "" {
		return fmt.Errorf("environment is required")
	}
	if len(opts.ProtectedEnvs) == 0 {
		opts.ProtectedEnvs = DefaultProtectedEnvs()
	}
	if isProtectedEnv(opts.Env, opts.ProtectedEnvs) && !opts.AllowProd {
		return fmt.Errorf("refusing to seed protected environment %q without --allow-prod", opts.Env)
	}
	return nil
}

// Apply applies all matching seed files and records successful runs.
func Apply(ctx context.Context, conn *dbschema.DatabaseConnection, fsys fs.FS, opts Options) (*Result, error) {
	opts.Env = normalizeEnv(opts.Env)
	if len(opts.ProtectedEnvs) == 0 {
		opts.ProtectedEnvs = DefaultProtectedEnvs()
	}
	if err := ValidateOptions(opts); err != nil {
		return nil, err
	}
	if opts.Idempotent && platform.NormalizeDialect(conn.Info().Dialect) == platform.ClickHouse {
		return nil, fmt.Errorf("--idempotent is not supported for clickhouse seeds because transactions and savepoints are unavailable")
	}
	if err := ensureSafeTarget(ctx, conn, opts); err != nil {
		return nil, err
	}

	seeds, err := Discover(fsys)
	if err != nil {
		return nil, err
	}
	selected := Select(seeds, opts.Env)
	result := &Result{Env: opts.Env, Total: len(selected)}
	if len(selected) == 0 {
		return result, nil
	}

	if err := ensureTracker(ctx, conn); err != nil {
		return nil, err
	}
	applied, err := appliedSeeds(ctx, conn)
	if err != nil {
		return nil, err
	}

	for _, seed := range selected {
		if !opts.Force && applied[seed.Path] {
			result.Skipped = append(result.Skipped, seed)
			continue
		}
		if err := applySeed(ctx, conn, fsys, seed, opts); err != nil {
			return result, err
		}
		result.Applied = append(result.Applied, seed)
	}

	return result, nil
}

func applySeed(ctx context.Context, conn *dbschema.DatabaseConnection, fsys fs.FS, seed SeedFile, opts Options) error {
	data, err := fs.ReadFile(fsys, seed.Path)
	if err != nil {
		return fmt.Errorf("read seed file %q: %w", seed.Path, err)
	}
	statements := migrator.SplitSQLStatements(string(data))

	dialect := platform.NormalizeDialect(conn.Info().Dialect)
	if dialect == platform.ClickHouse {
		return applySeedWithoutTransaction(ctx, conn, seed, statements, opts)
	}

	if err := conn.Writer().BeginTransaction(); err != nil {
		return fmt.Errorf("begin seed transaction %s: %w", seed.Filename, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = conn.Writer().RollbackTransaction()
		}
	}()

	if opts.Idempotent {
		if err := conn.Writer().ExecuteSQL(ctx, "SAVEPOINT ptah_seed_file"); err != nil {
			return fmt.Errorf("create seed savepoint %s: %w", seed.Filename, err)
		}
	}

	if err := executeStatements(ctx, conn, statements); err != nil {
		if !opts.Idempotent || !IsConflictError(err) {
			return fmt.Errorf("apply seed %s: %w", seed.Filename, err)
		}
		if rbErr := conn.Writer().ExecuteSQL(ctx, "ROLLBACK TO SAVEPOINT ptah_seed_file"); rbErr != nil {
			return fmt.Errorf("rollback idempotent seed %s: %w", seed.Filename, rbErr)
		}
	} else if opts.Idempotent {
		if err := conn.Writer().ExecuteSQL(ctx, "RELEASE SAVEPOINT ptah_seed_file"); err != nil {
			return fmt.Errorf("release seed savepoint %s: %w", seed.Filename, err)
		}
	}

	if err := recordSeed(ctx, conn, seed, opts); err != nil {
		return fmt.Errorf("record seed %s: %w", seed.Filename, err)
	}
	if err := conn.Writer().CommitTransaction(); err != nil {
		return fmt.Errorf("commit seed %s: %w", seed.Filename, err)
	}
	committed = true
	return nil
}

func applySeedWithoutTransaction(ctx context.Context, conn *dbschema.DatabaseConnection, seed SeedFile, statements []string, opts Options) error {
	if err := executeStatements(ctx, conn, statements); err != nil {
		return fmt.Errorf("apply seed %s: %w", seed.Filename, err)
	}
	if err := recordSeed(ctx, conn, seed, opts); err != nil {
		return fmt.Errorf("record seed %s: %w", seed.Filename, err)
	}
	return nil
}

func executeStatements(ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) error {
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := conn.Writer().ExecuteSQL(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func ensureTracker(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	// Deliberately outside the seed writer: the tracker table is metadata that
	// must exist before either transactional or no-transaction seed execution
	// starts, and this statement is not user-provided SQL.
	_, err := conn.ExecContext(ctx, trackerDDL(conn.Info().Dialect))
	if err != nil {
		return fmt.Errorf("create %s table: %w", trackerName, err)
	}
	return nil
}

func trackerDDL(dialect string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.ClickHouse:
		return `CREATE TABLE IF NOT EXISTS schema_seeds (
    seed_path String,
    env String,
    checksum String,
    applied_at DateTime
) ENGINE = MergeTree ORDER BY seed_path`
	default:
		return `CREATE TABLE IF NOT EXISTS schema_seeds (
    seed_path VARCHAR(512) PRIMARY KEY,
    env VARCHAR(128) NOT NULL,
    checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMP NOT NULL
)`
	}
}

func appliedSeeds(ctx context.Context, conn *dbschema.DatabaseConnection) (map[string]bool, error) {
	rows, err := conn.Query("SELECT seed_path FROM schema_seeds")
	if err != nil {
		return nil, fmt.Errorf("query applied seeds: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			return nil, fmt.Errorf("scan applied seed: %w", err)
		}
		applied[filename] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied seeds: %w", err)
	}
	return applied, nil
}

func recordSeed(ctx context.Context, conn *dbschema.DatabaseConnection, seed SeedFile, opts Options) error {
	deleteSQL := sqlutil.Rebind(conn.Info().Dialect, "DELETE FROM schema_seeds WHERE seed_path = ?")
	if err := conn.Writer().ExecuteSQL(ctx, deleteSQL, seed.Path); err != nil {
		return err
	}

	insertSQL := sqlutil.Rebind(conn.Info().Dialect, "INSERT INTO schema_seeds (seed_path, env, checksum, applied_at) VALUES (?, ?, ?, ?)")
	return conn.Writer().ExecuteSQL(ctx, insertSQL, seed.Path, opts.Env, seed.Checksum, time.Now())
}

func ensureSafeTarget(ctx context.Context, conn *dbschema.DatabaseConnection, opts Options) error {
	if opts.AllowProd || len(opts.ProtectedTables) == 0 {
		return nil
	}

	existing, err := existingTables(ctx, conn)
	if err != nil {
		return err
	}
	protected := make(map[string]string, len(opts.ProtectedTables))
	for _, table := range opts.ProtectedTables {
		table = strings.TrimSpace(table)
		if table != "" {
			protected[strings.ToLower(table)] = table
		}
	}

	var matches []string
	for _, table := range existing {
		if original, ok := protected[strings.ToLower(table)]; ok {
			matches = append(matches, original)
		}
	}
	slices.Sort(matches)
	if len(matches) > 0 {
		return fmt.Errorf("refusing to seed target database because protected tables exist: %s; pass --allow-prod to override", strings.Join(matches, ", "))
	}
	return nil
}

func existingTables(ctx context.Context, conn *dbschema.DatabaseConnection) ([]string, error) {
	var query string
	var args []any
	switch platform.NormalizeDialect(conn.Info().Dialect) {
	case platform.MySQL, platform.MariaDB:
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"
	case platform.ClickHouse:
		query = "SELECT name FROM system.tables WHERE database = currentDatabase() AND is_temporary = 0"
	default:
		query = sqlutil.Rebind(conn.Info().Dialect, "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'")
		args = append(args, conn.Info().Schema)
	}

	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query target tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("scan target table: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate target tables: %w", err)
	}
	return tables, nil
}

// DefaultProtectedEnvs returns environment names that require --allow-prod.
func DefaultProtectedEnvs() []string {
	return []string{"prod", "production"}
}

func isProtectedEnv(env string, protected []string) bool {
	env = normalizeEnv(env)
	for _, value := range protected {
		if normalizeEnv(value) == env {
			return true
		}
	}
	return false
}

func normalizeEnv(env string) string {
	return strings.ToLower(strings.TrimSpace(env))
}

// IsConflictError reports whether err looks like a duplicate-key conflict.
func IsConflictError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}

	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}

	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "duplicate entry") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "unique violation")
}
