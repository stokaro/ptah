// Package seeder applies environment-scoped SQL seed files and records which
// files have already run.
//
// A seed file is named NNN_description.env.sql. The env segment names the
// environment the file belongs to, and env "all" runs in every environment.
// Applied seeds are recorded by path together with a SHA-256 checksum in a
// schema_seeds table the seeder creates in the target database, so a re-run
// skips every recorded file and refuses one whose bytes changed with a
// [ChecksumMismatchError] rather than reporting it skipped. Protected
// environments ("prod" and "production" unless [Options.ProtectedEnvs] says
// otherwise) and protected tables refuse a run unless [Options.AllowProd] is
// set.
//
// [Discover], [Select] and [Apply] form the pipeline; [Apply] runs all three
// against a live [dbschema.DatabaseConnection].
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

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
)

const (
	allEnv      = "all"
	trackerName = "schema_seeds"
)

var seedFilenameRE = regexp.MustCompile(`^([0-9]+)_(.+)\.([A-Za-z0-9_-]+)\.sql$`)

// SeedFile is one discovered seed SQL file.
type SeedFile struct {
	// Path is the file's path relative to the discovered filesystem's root,
	// which is the identity the tracker records.
	Path string
	// Filename is the base name, NNN_description.env.sql.
	Filename string
	// Version is the leading NNN parsed as a number, so 005 sorts before 010.
	Version int
	// Description is the middle segment of the filename.
	Description string
	// Env is the filename's env segment, lowercased.
	Env string
	// Checksum is the hex SHA-256 of the file's bytes.
	Checksum string
}

// Options controls seed execution.
type Options struct {
	// Env is the environment to seed and is required. It is lowercased and
	// trimmed, then matched against each seed file's env segment.
	Env string
	// ProtectedEnvs lists environments that refuse a run unless AllowProd is
	// set. Empty means [DefaultProtectedEnvs].
	ProtectedEnvs []string
	// ProtectedTables marks the target database as off limits by content: when
	// any listed table exists there (matched case-insensitively), [Apply]
	// refuses the run unless AllowProd is set.
	ProtectedTables []string
	// Force re-applies a seed the tracker already records, and is also the
	// answer to a [ChecksumMismatchError]: it re-runs the edited file and
	// records the new checksum.
	Force bool
	// Idempotent tolerates duplicate-key conflicts: each file's statements run
	// under a savepoint, and a conflict (see [IsConflictError]) rolls the file
	// back to it while the seed is still recorded as applied. Any other
	// failure still fails the run. [Apply] refuses the option on ClickHouse,
	// where transactions and savepoints do not exist.
	Idempotent bool
	// AllowProd disables both protections: the protected-environment refusal
	// and the protected-tables probe.
	AllowProd bool
}

// ChecksumMismatchError reports that a seed file changed after it was applied.
//
// A seed is recorded by path, and re-running the command skips every path the
// tracker already holds. Without the checksum being read back, an edited seed
// file is indistinguishable from an unedited one: the run reports it skipped,
// leaves the database holding whatever the old file wrote, and says nothing --
// output identical to the run that genuinely had nothing to do.
//
// It mirrors what the migrator does with an edited migration: the drift is
// refused rather than applied, and re-applying is a decision the caller makes
// explicitly with [Options.Force].
type ChecksumMismatchError struct {
	// Path is the seed's path relative to the seeds directory, which is the
	// identity the tracker stores.
	Path string
	// Stored is the checksum recorded when the seed was applied.
	Stored string
	// Computed is the checksum of the file on disk now.
	Computed string
}

func (e *ChecksumMismatchError) Error() string {
	return fmt.Sprintf(
		"seed %s changed after it was applied: recorded checksum %s, current %s; "+
			"add a new seed file with the change, or pass --force to re-apply this one",
		e.Path, e.Stored, e.Computed,
	)
}

// Result summarizes one seed command run.
type Result struct {
	// Env is the normalized environment the run targeted.
	Env string
	// Total counts the seeds selected for Env, applied and skipped alike.
	Total int
	// Applied holds the seeds this run executed and recorded, in run order.
	Applied []SeedFile
	// Skipped holds the seeds the tracker already recorded unchanged.
	Skipped []SeedFile
}

// Discover scans fsys recursively for seed files matching
// NNN_description.env.sql.
//
// Files without a .sql suffix are ignored, but a .sql file that does not match
// the pattern fails discovery with an error naming it, so a typo cannot
// silently drop a seed. The result is sorted by Version and then by Path, and
// two runs over the same tree produce the same slice. Each [SeedFile] carries
// its fsys-relative Path, its Env lowercased, and the hex SHA-256 Checksum of
// the file's bytes.
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

// Select filters seed files for the requested environment, preserving input
// order. A seed whose Env is "all" is selected for every environment. The env
// argument is lowercased and trimmed before the comparison; the seeds'
// own Env values are compared as they are, which is the lowercased form
// [Discover] produces.
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

// ValidateOptions returns an error if the command options would be unsafe: an
// empty Env, or a protected environment named without AllowProd. An empty
// ProtectedEnvs list is read as [DefaultProtectedEnvs] for the check; the
// caller's opts value is not modified.
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
//
// Apply validates opts (see [ValidateOptions]), refuses a target holding any
// of the [Options.ProtectedTables], creates the schema_seeds tracker table if
// it is absent, then applies each seed selected for opts.Env (see [Select]) in
// [Discover] order, each file in its own transaction -- except on ClickHouse,
// where seeds run without one. A seed already recorded with a matching
// checksum is skipped; a recorded seed whose file changed stops the run with a
// *[ChecksumMismatchError], retrieved with [errors.As]; [Options.Force]
// re-applies both. When no seed matches the environment, Apply returns an
// empty Result without creating the tracker.
//
// An error before the first seed is examined comes with a nil Result; from
// there on the returned Result is non-nil alongside the error and reports what
// was applied and skipped before it.
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
		if !opts.Force {
			stored, recorded := applied[seed.Path]
			if recorded && stored != seed.Checksum {
				return result, &ChecksumMismatchError{
					Path:     seed.Path,
					Stored:   stored,
					Computed: seed.Checksum,
				}
			}
			if recorded {
				result.Skipped = append(result.Skipped, seed)
				continue
			}
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
	// Split with the connection's dialect so a semicolon inside a backslash-
	// escaped string literal (valid on MySQL/MariaDB/ClickHouse) is not
	// mis-split into a separately-executed statement.
	statements := sqlutil.SplitStatementsForDialect(conn.Info().Dialect, string(data))

	dialect := platform.NormalizeDialect(conn.Info().Dialect)
	if dialect == platform.ClickHouse {
		return applySeedWithoutTransaction(ctx, conn, seed, statements, opts)
	}

	tx, err := conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin seed transaction %s: %w", seed.Filename, err)
	}
	txConn := conn.WithExecutor(tx)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if opts.Idempotent {
		if err := txConn.Writer().ExecuteSQL(ctx, "SAVEPOINT ptah_seed_file"); err != nil {
			return fmt.Errorf("create seed savepoint %s: %w", seed.Filename, err)
		}
	}

	if err := executeStatements(ctx, txConn, statements); err != nil {
		if !opts.Idempotent || !IsConflictError(err) {
			return fmt.Errorf("apply seed %s: %w", seed.Filename, err)
		}
		if rbErr := txConn.Writer().ExecuteSQL(ctx, "ROLLBACK TO SAVEPOINT ptah_seed_file"); rbErr != nil {
			return fmt.Errorf("rollback idempotent seed %s: %w", seed.Filename, rbErr)
		}
	} else if opts.Idempotent {
		if err := txConn.Writer().ExecuteSQL(ctx, "RELEASE SAVEPOINT ptah_seed_file"); err != nil {
			return fmt.Errorf("release seed savepoint %s: %w", seed.Filename, err)
		}
	}

	if err := recordSeed(ctx, txConn, seed, opts); err != nil {
		return fmt.Errorf("record seed %s: %w", seed.Filename, err)
	}
	if err := tx.Commit(); err != nil {
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

// trackerDDL is the tracker table every dialect gets.
//
// The checksum column holds the hex SHA-256 of the seed file's bytes as they
// were when the seed ran, and it is read on the next run: a file whose bytes no
// longer hash to the recorded value is refused with a [ChecksumMismatchError]
// rather than reported as already applied.
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

// appliedSeeds returns the checksum recorded for each applied seed path.
//
// The rows are read oldest first so the newest row for a path wins. On every
// dialect but one that ordering is redundant -- seed_path is the tracker's
// primary key, so a path has one row. The ClickHouse tracker is a MergeTree
// whose ORDER BY key constrains nothing, so a second row for a path is a shape
// the table permits, and reading in applied_at order is what decides which of
// them the checksum comparison uses. See [trackerDDL] for both tables.
func appliedSeeds(ctx context.Context, conn *dbschema.DatabaseConnection) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT seed_path, checksum FROM schema_seeds ORDER BY applied_at")
	if err != nil {
		return nil, fmt.Errorf("query applied seeds: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]string)
	for rows.Next() {
		var filename, checksum string
		if err := rows.Scan(&filename, &checksum); err != nil {
			return nil, fmt.Errorf("scan applied seed: %w", err)
		}
		applied[filename] = checksum
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

	rows, err := conn.QueryContext(ctx, query, args...)
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

// DefaultProtectedEnvs returns the environment names protected when
// [Options.ProtectedEnvs] is empty: "prod" and "production". Each call returns
// a fresh slice the caller may modify.
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

// IsConflictError reports whether err looks like a duplicate-key conflict —
// the failure [Options.Idempotent] tolerates. PostgreSQL (SQLSTATE 23505) and
// MySQL/MariaDB (error 1062) are recognized through their driver error types
// with [errors.As]; any other error falls back to a case-insensitive match on
// the usual duplicate-key message wordings.
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
