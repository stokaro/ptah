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

	"ptah.run/core/dataext"
	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/protectedtable"
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
	// Idempotent tolerates duplicate-key conflicts: a file whose statements hit
	// one (see [IsConflictError]) is rolled back as a unit and the run
	// continues, with the seed still recorded as applied. Any other failure
	// still fails the run. [Apply] refuses the option on ClickHouse, which has
	// no transaction to roll back to. YDB has no savepoint, and a conflict ends
	// the transaction it ran in; there the whole transaction is rolled back and
	// the seed is recorded in a transaction of its own.
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
// it is absent, and applies each seed selected for opts.Env (see [Select]) in
// [Discover] order, each file in its own transaction -- except on ClickHouse,
// where seeds run without one, so a failed file there can leave its earlier
// statements applied. On YDB a file runs in one serializable transaction
// together with the row that records it, run again when YDB aborts it for a
// conflicting transaction, and a file holding a statement YDB runs only
// outside a transaction -- a scheme statement, or BATCH UPDATE and BATCH
// DELETE -- is refused before it runs. A seed already recorded with a matching checksum is
// skipped; a recorded seed whose file changed stops the run with a
// *[ChecksumMismatchError], retrieved with [errors.As]; [Options.Force]
// re-applies both. An environment no seed matches is not an error: Apply
// applies nothing and returns an empty Result.
//
// The returned Result may be nil when the run fails before any seed is
// applied, so a caller that inspects it on the error path checks for nil.
// Once seeds are running, a failure returns the Result alongside the error,
// reporting what was applied and skipped before it.
func Apply(ctx context.Context, conn *dbschema.DatabaseConnection, fsys fs.FS, opts Options) (*Result, error) {
	opts.Env = normalizeEnv(opts.Env)
	if len(opts.ProtectedEnvs) == 0 {
		opts.ProtectedEnvs = DefaultProtectedEnvs()
	}
	if err := ValidateOptions(opts); err != nil {
		return nil, err
	}
	described, _ := platform.Lookup(conn.Info().Dialect)
	if described.Seeds.ByOwner && conn.Seeds() == nil {
		return nil, fmt.Errorf("seeds on %s run through the engine's seed service, and the connection has none",
			described.Name)
	}
	if opts.Idempotent && !described.Seeds.ByOwner &&
		(!described.Seeds.Transactional || described.Seeds.Savepoint.Create == "") {
		return nil, fmt.Errorf("--idempotent is not supported for %s seeds because transactions and savepoints are unavailable", described.Name)
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
	described, _ := platform.Lookup(conn.Info().Dialect)
	if described.Seeds.ByOwner {
		return applySeedByOwner(ctx, conn, dataext.Seed{
			Path: seed.Path, Filename: seed.Filename, Checksum: seed.Checksum, Text: string(data), Env: opts.Env,
		}, opts)
	}
	// Split with the connection's dialect so a semicolon inside a backslash-
	// escaped string literal (valid on MySQL/MariaDB/ClickHouse) is not
	// mis-split into a separately-executed statement.
	statements := sqlutil.SplitStatementsForDialect(conn.Info().Dialect, string(data))

	if !described.Seeds.Transactional {
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

	savepoint := savepointStatements(conn.Info().Dialect)
	if opts.Idempotent {
		if err := txConn.Writer().ExecuteSQL(ctx, savepoint.create); err != nil {
			return fmt.Errorf("create seed savepoint %s: %w", seed.Filename, err)
		}
	}

	if err := executeStatements(ctx, txConn, statements); err != nil {
		if !opts.Idempotent || !IsConflictError(err) {
			return fmt.Errorf("apply seed %s: %w", seed.Filename, err)
		}
		if rbErr := txConn.Writer().ExecuteSQL(ctx, savepoint.rollback); rbErr != nil {
			return fmt.Errorf("rollback idempotent seed %s: %w", seed.Filename, rbErr)
		}
	} else if opts.Idempotent && savepoint.release != "" {
		if err := txConn.Writer().ExecuteSQL(ctx, savepoint.release); err != nil {
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

// applySeedByOwner runs a seed file through the connection's seed service,
// for an engine whose seed files do not run as split statements
// (descriptor.Seeds.ByOwner). The service runs the file and its record in one
// transaction, so a file that fails leaves nothing of itself applied. YDB has
// no savepoint, and a conflict ends the transaction it ran in, so with
// [Options.Idempotent] a file that hits a duplicate key is rolled back whole
// and the seed is recorded in a transaction of its own: nothing of the file
// is applied, as on the engines that roll back to a savepoint.
func applySeedByOwner(ctx context.Context, conn *dbschema.DatabaseConnection, seed dataext.Seed, opts Options) error {
	seeds := conn.Seeds()
	err := seeds.Apply(ctx, conn, seed)
	if err == nil {
		return nil
	}
	if !opts.Idempotent || !IsConflictError(err) {
		return fmt.Errorf("apply seed %s: %w", seed.Filename, err)
	}
	if err := seeds.Record(ctx, conn, seed); err != nil {
		return fmt.Errorf("record seed %s: %w", seed.Filename, err)
	}
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

// trackerDDL is the statement that creates the tracker table when it is absent.
// It runs on every Apply, so it has to succeed against a table that exists.
//
// The checksum column holds the hex SHA-256 of the seed file's bytes as they
// were when the seed ran, and it is read on the next run: a file whose bytes no
// longer hash to the recorded value is refused with a [ChecksumMismatchError]
// rather than reported as already applied.
//
// portableTrackerDDL is the spelling most dialects take. A dialect that refuses
// it states its own in its descriptor (descriptor.Seeds.Tracker): ClickHouse
// needs a table engine and its own type names, SQL Server has no IF NOT EXISTS
// on CREATE TABLE and no TIMESTAMP that holds a time, Oracle 21 refuses IF NOT
// EXISTS, Spanner's PostgreSQL interface has neither CHAR nor TIMESTAMP, and
// YDB declares its key in a clause of its own. The internal/builtindialects
// file of each engine records the measurement behind its statement.
//
// Every name is unqualified, so each guard looks in the schema the CREATE
// TABLE and every later tracker statement resolve against.
func trackerDDL(dialect string) string {
	if described, _ := platform.Lookup(dialect); described.Seeds.Tracker != "" {
		return described.Seeds.Tracker
	}
	return portableTrackerDDL
}

const portableTrackerDDL = `CREATE TABLE IF NOT EXISTS schema_seeds (
    seed_path VARCHAR(512) PRIMARY KEY,
    env VARCHAR(128) NOT NULL,
    checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMP NOT NULL
)`

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
	protected := protectedtable.New(opts.ProtectedTables)
	if opts.AllowProd || protected.Empty() {
		return nil
	}

	existing, err := existingTables(ctx, conn)
	if err != nil {
		return err
	}

	var matches []string
	for _, table := range existing {
		// The target's own tables, so the question is whether one of them is
		// fenced rather than whether a change touches it; the entry grammar is
		// shared and the question is not.
		if entry, fenced := protected.Entry("", table); fenced {
			matches = append(matches, entry)
		}
	}
	slices.Sort(matches)
	if len(matches) > 0 {
		return fmt.Errorf("refusing to seed target database because protected tables exist: %s; pass --allow-prod to override", strings.Join(matches, ", "))
	}
	return nil
}

// existingTables lists the base tables of the connection's schema, with the
// query the dialect's descriptor names (descriptor.Seeds.TablesQuery), or the
// portable information_schema query bound to the schema. A dialect whose seeds
// run through the connection's seed service lists them through it.
func existingTables(ctx context.Context, conn *dbschema.DatabaseConnection) ([]string, error) {
	described, _ := platform.Lookup(conn.Info().Dialect)
	if described.Seeds.ByOwner {
		return conn.Seeds().TableNames(ctx, conn, conn.Info().Schema)
	}
	query := described.Seeds.TablesQuery
	var args []any
	if query == "" {
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

// seedSavepoint is how one engine brackets a single seed file, so a duplicate
// row can be rolled back without losing the files already applied in the same
// transaction.
//
// release is empty where the engine has no statement for it, which is not the
// same as not needing one: the savepoint simply lives until the transaction
// ends.
type seedSavepoint struct {
	create   string
	rollback string
	release  string
}

// savepointStatements names those statements for a dialect, from the keywords
// its descriptor states (descriptor.Seeds.Savepoint).
//
// Two engines do not take the portable spelling, and `ptah seed --idempotent`
// failed on the first seed file for both (stokaro/ptah#3330): SQL Server
// spells them SAVE TRANSACTION and ROLLBACK TRANSACTION and has no release,
// and Oracle answers RELEASE SAVEPOINT with ORA-00900. A dialect with no
// savepoint, such as ClickHouse, which has no transactions either, is refused
// an idempotent run before any seed runs.
func savepointStatements(dialect string) seedSavepoint {
	const name = "ptah_seed_file"
	described, _ := platform.Lookup(dialect)
	keywords := described.Seeds.Savepoint
	statement := func(keyword string) string {
		if keyword == "" {
			return ""
		}
		return keyword + " " + name
	}
	return seedSavepoint{
		create:   statement(keywords.Create),
		rollback: statement(keywords.Rollback),
		release:  statement(keywords.Release),
	}
}

// IsConflictError reports whether err looks like a duplicate-key conflict —
// the failure [Options.Idempotent] tolerates. PostgreSQL (SQLSTATE 23505),
// MySQL/MariaDB (error 1062) and YDB (PRECONDITION_FAILED with issue 2012,
// `Conflict with existing key`) are recognized through their driver error
// types with [errors.As]; any other error falls back to a case-insensitive
// match on the usual duplicate-key message wordings.
func IsConflictError(err error) bool {
	if ydbschema.KeyConflict(err) {
		return true
	}
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
