package migrator

import (
	"context"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
)

// MigrationFunc represents a migration function that operates on a database connection
type MigrationFunc func(context.Context, *dbschema.DatabaseConnection) error

// SplitSQLStatements splits a SQL string into individual statements using AST-based parsing.
// This is needed because MySQL doesn't handle multiple statements in a single ExecuteSQL call.
// Unlike simple string splitting, this properly handles semicolons within string literals and comments.
func SplitSQLStatements(sql string) []string {
	normalized := sqlutil.NormalizeClientDelimiters(sql)
	return sqlutil.SplitSQLStatements(sqlutil.StripComments(normalized))
}

// StatementInterceptor lets an external executor take over individual
// migration statements — for example, routing ALTER TABLE statements through
// an online-DDL tool (gh-ost, pt-online-schema-change) instead of executing
// them on the migration connection.
//
// ValidateDirectives is called once per migration, before any statement runs,
// so a bad directive fails the migration cleanly with nothing applied instead
// of surfacing only when the first affected statement is reached.
//
// ExecuteStatement receives one statement (comments stripped) together with
// the file-level directives of the migration it came from (see
// ParseFileDirectives). It returns handled=true when it fully executed the
// statement itself; on handled=false the migrator executes the statement
// normally. A non-nil error aborts the migration.
type StatementInterceptor interface {
	ValidateDirectives(directives map[string]string) error
	ExecuteStatement(ctx context.Context, conn *dbschema.DatabaseConnection, stmt string, directives map[string]string) (handled bool, err error)
}

type migrationExecutionMode int

const (
	migrationExecutionTransactional migrationExecutionMode = iota
	migrationExecutionNoTransaction
)

type sqlMigrationFunc func(context.Context, *dbschema.DatabaseConnection, migrationExecutionMode) error

type sqlMigrationFile struct {
	fn            sqlMigrationFunc
	sql           string
	timeouts      MigrationTimeouts
	noTransaction bool
}

type atlasSQLMigrationFile struct {
	up      sqlMigrationFile
	down    sqlMigrationFile
	hasDown bool
}

type atlasTxtarSQL struct {
	migrationSQL string
	downSQL      string
	hasDown      bool
}

func (f sqlMigrationFile) executionMode() migrationExecutionMode {
	if f.noTransaction {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

// DirectiveNoTransaction opts a SQL migration file out of the per-migration
// transaction. It is intended for database operations that cannot run inside a
// transaction, such as PostgreSQL enum value additions that are used by later
// statements in the same migration.
const DirectiveNoTransaction = "no_transaction"

const (
	atlasTxtarDirective        = "-- atlas:txtar"
	atlasTxtarMigrationSection = "migration.sql"
	atlasTxtarDownSection      = "down.sql"
)

// AtlasDownNotImplementedError reports an Atlas migration that lacks an
// embedded down.sql section. Ptah does not yet synthesize Atlas dynamic down
// plans from the current database state and a dev database.
type AtlasDownNotImplementedError struct {
	Version     int64
	Description string
}

func (e *AtlasDownNotImplementedError) Error() string {
	return fmt.Sprintf(
		"migration %d has no Atlas down migration; dynamic Atlas-style down migrations are not implemented yet; add an atlas txtar down.sql section or migrate down manually",
		e.Version,
	)
}

// MigrationFuncFromSQLFilename returns a migration function that reads SQL from a file
// in the provided filesystem and executes it using the database connection
func MigrationFuncFromSQLFilename(filename string, fsys fs.FS) MigrationFunc {
	return MigrationFuncFromSQLFilenameWithInterceptor(filename, fsys, nil)
}

// MigrationFuncFromSQLFilenameWithInterceptor is MigrationFuncFromSQLFilename
// with an optional StatementInterceptor consulted for every statement; nil
// behaves exactly like MigrationFuncFromSQLFilename.
func MigrationFuncFromSQLFilenameWithInterceptor(filename string, fsys fs.FS, interceptor StatementInterceptor) MigrationFunc {
	return func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		migrationFile, err := migrationFuncFromSQLFilenameWithMetadata(filename, fsys, interceptor, nil)
		if err != nil {
			return err
		}
		return migrationFile.fn(ctx, conn, migrationFile.executionMode())
	}
}

// MigrationFuncFromSQLFilenameWithTimeouts returns a migration function and any
// file-level +ptah timeout directives parsed from the top of the SQL file.
func MigrationFuncFromSQLFilenameWithTimeouts(filename string, fsys fs.FS) (MigrationFunc, MigrationTimeouts, error) {
	return MigrationFuncFromSQLFilenameWithTimeoutsAndInterceptor(filename, fsys, nil)
}

// MigrationFuncFromSQLFilenameWithTimeoutsAndInterceptor returns a migration
// function, file-level timeout directives, and optional statement-interceptor
// support for the SQL file.
func MigrationFuncFromSQLFilenameWithTimeoutsAndInterceptor(
	filename string,
	fsys fs.FS,
	interceptor StatementInterceptor,
) (MigrationFunc, MigrationTimeouts, error) {
	migrationFile, err := migrationFuncFromSQLFilenameWithMetadata(filename, fsys, interceptor, nil)
	if err != nil {
		return nil, MigrationTimeouts{}, err
	}
	return func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return migrationFile.fn(ctx, conn, migrationFile.executionMode())
	}, migrationFile.timeouts, nil
}

func migrationFuncFromSQLFilenameWithMetadata(
	filename string,
	fsys fs.FS,
	interceptor StatementInterceptor,
	atlasTemplateData any,
) (sqlMigrationFile, error) {
	sql, err := readSQLMigrationFile(fsys, filename, atlasTemplateData)
	if err != nil {
		return sqlMigrationFile{}, err
	}

	atlasMigrationFile, ok, err := atlasSQLMigrationFileFromSQL(filename, sql, interceptor)
	if err != nil {
		return sqlMigrationFile{}, err
	}
	if ok {
		return atlasMigrationFile.up, nil
	}

	return migrationFuncFromSQLStringWithMetadata(filename, sql, interceptor)
}

func atlasSQLMigrationFileFromSQLFilenameWithMetadata(
	filename string,
	fsys fs.FS,
	interceptor StatementInterceptor,
	atlasTemplateData any,
) (atlasSQLMigrationFile, error) {
	sql, err := readSQLMigrationFile(fsys, filename, atlasTemplateData)
	if err != nil {
		return atlasSQLMigrationFile{}, err
	}

	atlasMigrationFile, ok, err := atlasSQLMigrationFileFromSQL(filename, sql, interceptor)
	if err != nil {
		return atlasSQLMigrationFile{}, err
	}
	if ok {
		return atlasMigrationFile, nil
	}

	up, err := migrationFuncFromSQLStringWithMetadata(filename, sql, interceptor)
	if err != nil {
		return atlasSQLMigrationFile{}, err
	}
	return atlasSQLMigrationFile{up: up}, nil
}

func readSQLMigrationFile(fsys fs.FS, filename string, atlasTemplateData any) (string, error) {
	sql, _, err := RenderAtlasTemplateSQL(fsys, filename, atlasTemplateData)
	return sql, err
}

func atlasSQLMigrationFileFromSQL(filename, sql string, interceptor StatementInterceptor) (atlasSQLMigrationFile, bool, error) {
	parsed, ok, err := parseAtlasTxtarSQL(filename, sql)
	if err != nil || !ok {
		return atlasSQLMigrationFile{}, ok, err
	}

	up, err := migrationFuncFromSQLStringWithMetadata(filename+"#"+atlasTxtarMigrationSection, parsed.migrationSQL, interceptor)
	if err != nil {
		return atlasSQLMigrationFile{}, true, err
	}
	atlasMigrationFile := atlasSQLMigrationFile{up: up}
	if parsed.hasDown {
		down, err := migrationFuncFromSQLStringWithMetadata(filename+"#"+atlasTxtarDownSection, parsed.downSQL, interceptor)
		if err != nil {
			return atlasSQLMigrationFile{}, true, err
		}
		atlasMigrationFile.down = down
		atlasMigrationFile.hasDown = true
	}
	return atlasMigrationFile, true, nil
}

func migrationFuncFromSQLStringWithMetadata(filename, sql string, interceptor StatementInterceptor) (sqlMigrationFile, error) {
	timeouts, err := parseMigrationTimeoutDirectives(sql)
	if err != nil {
		return sqlMigrationFile{}, err
	}

	noTransaction, err := parseNoTransactionDirective(ParseFileDirectives(sql))
	if err != nil {
		return sqlMigrationFile{}, fmt.Errorf("invalid migration directives in %s: %w", filename, err)
	}
	return sqlMigrationFile{
		fn: func(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
			return executeMigrationFileSQL(ctx, conn, filename, sql, interceptor, mode)
		},
		sql:           sql,
		timeouts:      timeouts,
		noTransaction: noTransaction,
	}, nil
}

func parseAtlasTxtarSQL(filename, sql string) (atlasTxtarSQL, bool, error) {
	if !hasAtlasTxtarDirective(sql) {
		return atlasTxtarSQL{}, false, nil
	}

	sections := make(map[string]*strings.Builder)
	var currentSection string
	sawSection := false
	for _, line := range strings.SplitAfter(sql, "\n") {
		section, isMarker := parseAtlasTxtarSectionMarker(line)
		if isMarker {
			if isAtlasTxtarSQLSection(section) {
				if _, exists := sections[section]; exists {
					return atlasTxtarSQL{}, true, fmt.Errorf("invalid Atlas txtar migration %s: duplicate %s section", filename, section)
				}
				sections[section] = &strings.Builder{}
				currentSection = section
			} else {
				currentSection = ""
			}
			sawSection = true
			continue
		}

		if !sawSection {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			return atlasTxtarSQL{}, true, fmt.Errorf("invalid Atlas txtar migration %s: SQL appears before the first txtar section", filename)
		}

		if builder := sections[currentSection]; builder != nil {
			builder.WriteString(line)
		}
	}

	migrationSection := sections[atlasTxtarMigrationSection]
	if migrationSection == nil {
		return atlasTxtarSQL{}, true, fmt.Errorf("invalid Atlas txtar migration %s: missing migration.sql section", filename)
	}
	downSection := sections[atlasTxtarDownSection]
	if downSection == nil {
		return atlasTxtarSQL{migrationSQL: migrationSection.String()}, true, nil
	}
	return atlasTxtarSQL{
		migrationSQL: migrationSection.String(),
		downSQL:      downSection.String(),
		hasDown:      true,
	}, true, nil
}

func hasAtlasTxtarDirective(sql string) bool {
	for line := range strings.SplitSeq(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		return trimmed == atlasTxtarDirective
	}
	return false
}

func parseAtlasTxtarSectionMarker(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "-- ") || !strings.HasSuffix(trimmed, " --") {
		return "", false
	}
	section := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "-- "), " --"))
	if isAtlasTxtarSQLSection(section) || looksAtlasTxtarFileSection(section) {
		return section, true
	}
	return "", false
}

func isAtlasTxtarSQLSection(section string) bool {
	return section == atlasTxtarMigrationSection || section == atlasTxtarDownSection
}

func looksAtlasTxtarFileSection(section string) bool {
	if len(strings.Fields(section)) != 1 {
		return false
	}
	return strings.ContainsAny(section, `./\`)
}

// NoopMigrationFunc is a no-op migration function
func NoopMigrationFunc(_ctx context.Context, _conn *dbschema.DatabaseConnection) error {
	return nil
}

// Migration represents a database migration
type Migration struct {
	Version         int64
	Description     string
	Checksum        string
	Up              MigrationFunc
	Down            MigrationFunc
	UpSQL           string
	DownSQL         string
	UpTimeouts      MigrationTimeouts
	DownTimeouts    MigrationTimeouts
	downUnavailable bool
	// NoTransaction runs the migration body and metadata update outside the
	// normal per-migration transaction. Use this only for statements that cannot
	// run transactionally; ordinary migrations should leave it false so dry-run,
	// timeout, and rollback behavior all go through the dialect writer.
	NoTransaction bool
}

func (m *Migration) executionMode() migrationExecutionMode {
	if m.NoTransaction {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

// CreateMigrationFromSQL creates a migration from SQL strings
// This is useful for programmatically creating migrations
func CreateMigrationFromSQL(version int64, description, upSQL, downSQL string) *Migration {
	upNoTransaction, upDirectiveErr := parseNoTransactionDirective(ParseFileDirectives(upSQL))
	downNoTransaction, downDirectiveErr := parseNoTransactionDirective(ParseFileDirectives(downSQL))

	migration := &Migration{
		Version:       version,
		Description:   description,
		UpSQL:         upSQL,
		DownSQL:       downSQL,
		NoTransaction: upNoTransaction || downNoTransaction,
	}

	upFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		if upDirectiveErr != nil {
			return fmt.Errorf("invalid up migration directives: %w", upDirectiveErr)
		}
		return executeSQLStatements(ctx, conn, upSQL, migration.executionMode())
	}

	downFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		if downDirectiveErr != nil {
			return fmt.Errorf("invalid down migration directives: %w", downDirectiveErr)
		}
		return executeSQLStatements(ctx, conn, downSQL, migration.executionMode())
	}

	migration.Up = upFunc
	migration.Down = downFunc
	return migration
}

// executeSQLStatements splits SQL into individual statements and executes them
func executeSQLStatements(ctx context.Context, conn *dbschema.DatabaseConnection, sql string, mode migrationExecutionMode) error {
	// Split SQL by semicolons and execute each statement
	statements := SplitSQLStatements(sql)

	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue // Skip empty statements and comments
		}

		if err := executeMigrationStatement(ctx, conn, stmt, mode); err != nil {
			return &MigrationExecutionError{
				Err:            fmt.Errorf("failed to execute SQL statement: %w", err),
				Statement:      stmt,
				StatementIndex: i + 1,
				Total:          len(statements),
			}
		}
	}

	return nil
}

func executeMigrationFileSQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	filename string,
	sql string,
	interceptor StatementInterceptor,
	mode migrationExecutionMode,
) error {
	// Directives live in comments, so they must be read from the raw file
	// before comment stripping, and validated before any statement runs so an
	// invalid directive leaves nothing half-applied.
	var directives map[string]string
	if interceptor != nil {
		directives = ParseFileDirectives(sql)
		if err := interceptor.ValidateDirectives(directives); err != nil {
			return fmt.Errorf("invalid migration directives in %s: %w", filename, err)
		}
	}

	statements := SplitSQLStatements(sql)
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if interceptor != nil {
			handled, err := interceptor.ExecuteStatement(ctx, conn, stmt, directives)
			if err != nil {
				return &MigrationExecutionError{
					Err:            fmt.Errorf("failed to execute migration SQL: %w", err),
					Statement:      stmt,
					StatementIndex: i + 1,
					Total:          len(statements),
				}
			}
			if handled {
				continue
			}
		}

		if err := executeMigrationStatement(ctx, conn, stmt, mode); err != nil {
			return &MigrationExecutionError{
				Err:            fmt.Errorf("failed to execute migration SQL: %w", err),
				Statement:      stmt,
				StatementIndex: i + 1,
				Total:          len(statements),
			}
		}
	}
	return nil
}

// MigrationExecutionError reports the statement that failed while applying a
// SQL migration.
type MigrationExecutionError struct {
	Err            error
	Statement      string
	StatementIndex int
	Total          int
}

func (e *MigrationExecutionError) Error() string {
	return fmt.Sprintf("%v\nSQL: %s", e.Err, e.Statement)
}

func (e *MigrationExecutionError) Unwrap() error {
	return e.Err
}

func executeMigrationStatement(ctx context.Context, conn *dbschema.DatabaseConnection, stmt string, mode migrationExecutionMode) error {
	if mode == migrationExecutionTransactional {
		return conn.Writer().ExecuteSQL(ctx, stmt)
	}
	return executeSQLOutsideTransaction(ctx, conn, stmt)
}

func executeSQLOutsideTransaction(ctx context.Context, conn *dbschema.DatabaseConnection, sql string, args ...any) error {
	if conn.Writer().IsDryRun() {
		return conn.Writer().ExecuteSQL(ctx, sql, args...)
	}

	// Deliberate transaction escape hatch. This is used only for migrations
	// marked no_transaction, where the database rejects transactional execution
	// (for example PostgreSQL ALTER TYPE ADD VALUE followed by using that value).
	_, err := conn.ExecContext(ctx, sql, args...)
	return err
}

func parseNoTransactionDirective(directives map[string]string) (bool, error) {
	value, ok := directives[DirectiveNoTransaction]
	if !ok {
		return false, nil
	}
	noTransaction, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid +ptah %s value %q: expected true or false", DirectiveNoTransaction, value)
	}
	return noTransaction, nil
}
