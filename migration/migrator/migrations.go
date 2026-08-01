package migrator

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/lexer"
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

// SplitSQLStatementsForConnection splits sql into individual statements using
// the connection's dialect, so string-literal boundaries are scanned correctly
// for that engine (a backslash is a C-style escape only for MySQL/MariaDB/
// ClickHouse). Callers that execute the resulting statements one by one against
// a live connection — for example the seeder — should use this rather than the
// dialect-blind [SplitSQLStatements], so a semicolon inside a backslash-escaped
// literal cannot leak out into a separately-executed statement. A nil
// connection falls back to the dialect-blind split.
func SplitSQLStatementsForConnection(conn *dbschema.DatabaseConnection, sql string) []string {
	return splitSQLStatementsForConnection(conn, sql)
}

func splitSQLStatementsForConnection(conn *dbschema.DatabaseConnection, sql string) []string {
	if conn == nil {
		return SplitSQLStatements(sql)
	}
	return splitSQLStatementsForDialect(sql, conn.Info().Dialect)
}

func splitSQLStatementsForDialect(sql, dialect string) []string {
	if strings.TrimSpace(dialect) == "" {
		return SplitSQLStatements(sql)
	}
	statements := sqlutil.SplitSQLStatementsForDialect(sql, dialect)
	filtered := statements[:0]
	for _, stmt := range statements {
		stmt = strings.TrimSpace(sqlutil.StripCommentsForDialect(stmt, dialect))
		if stmt != "" {
			filtered = append(filtered, stmt)
		}
	}
	return filtered
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

// StatementValidator rejects unsafe or unsupported migration statements
// without taking over their execution. Every statement in a migration file is
// validated before the first statement runs.
type StatementValidator interface {
	ValidateStatement(stmt string) error
}

// StatementEvent describes one successfully executed migration statement.
// Directives is an event-local copy; observers may modify it without affecting
// migration execution or later events.
type StatementEvent struct {
	SourcePath string
	Statement  string
	Index      int
	Total      int
	Directives map[string]string
}

// StatementObserver receives successfully executed migration statements. It is
// called after either an interceptor or the normal migrator path executes the
// statement. Returning an error aborts the migration.
type StatementObserver interface {
	ObserveStatement(ctx context.Context, event StatementEvent) error
}

// StatementObserverFunc adapts a function to StatementObserver.
type StatementObserverFunc func(context.Context, StatementEvent) error

// ObserveStatement calls f with the successfully executed statement.
func (f StatementObserverFunc) ObserveStatement(
	ctx context.Context,
	event StatementEvent,
) error {
	return f(ctx, event)
}

type statementExecutionHooks struct {
	interceptor StatementInterceptor
	validator   StatementValidator
	observer    StatementObserver
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
	// checks are Atlas txtar checks.sql assertions that gate the migration
	// body, enforced through the same machinery as `-- +ptah check`.
	checks []Check
}

type atlasSQLMigrationFile struct {
	up      sqlMigrationFile
	down    sqlMigrationFile
	hasDown bool
}

type atlasTxtarSQL struct {
	migrationSQL string
	downSQL      string
	checksSQL    string
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
	atlasTxtarChecksSection    = "checks.sql"
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

// CheckpointRollbackError reports a rollback that targets a version below an
// applied checkpoint. The checkpoint squashed the intermediate history into a
// single snapshot, so that state cannot be reconstructed by rolling back.
type CheckpointRollbackError struct {
	TargetVersion     int64
	CheckpointVersion int64
}

func (e *CheckpointRollbackError) Error() string {
	return fmt.Sprintf(
		"cannot roll back to version %d: it is below checkpoint %d, whose squashed history cannot be reconstructed; roll back to version %d (the checkpoint) or to 0 (drop everything) instead",
		e.TargetVersion, e.CheckpointVersion, e.CheckpointVersion,
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
		hooks := statementExecutionHooks{interceptor: interceptor}
		migrationFile, err := migrationFuncFromSQLFilenameWithMetadata(filename, fsys, hooks, nil)
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
	hooks := statementExecutionHooks{interceptor: interceptor}
	migrationFile, err := migrationFuncFromSQLFilenameWithMetadata(filename, fsys, hooks, nil)
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
	hooks statementExecutionHooks,
	atlasTemplateData any,
) (sqlMigrationFile, error) {
	sql, err := readSQLMigrationFile(fsys, filename, atlasTemplateData)
	if err != nil {
		return sqlMigrationFile{}, err
	}
	return migrationFuncFromSQLContentWithMetadata(filename, sql, hooks)
}

// migrationFuncFromSQLContentWithMetadata builds the up half of a migration
// from already-read (and template-rendered) file content: a txtar archive
// contributes its migration.sql section, any other content is executed as-is.
func migrationFuncFromSQLContentWithMetadata(
	filename, sql string,
	hooks statementExecutionHooks,
) (sqlMigrationFile, error) {
	atlasMigrationFile, ok, err := atlasSQLMigrationFileFromSQL(filename, sql, hooks)
	if err != nil {
		return sqlMigrationFile{}, err
	}
	if ok {
		return atlasMigrationFile.up, nil
	}

	return migrationFuncFromSQLStringWithMetadata(filename, sql, hooks)
}

// atlasSQLMigrationFileFromSQLContentWithMetadata builds both halves of an
// Atlas migration from already-read (and template-rendered) file content: a
// txtar archive contributes its migration.sql and optional down.sql sections,
// any other content becomes an up-only migration.
func atlasSQLMigrationFileFromSQLContentWithMetadata(
	filename, sql string,
	hooks statementExecutionHooks,
) (atlasSQLMigrationFile, error) {
	atlasMigrationFile, ok, err := atlasSQLMigrationFileFromSQL(filename, sql, hooks)
	if err != nil {
		return atlasSQLMigrationFile{}, err
	}
	if ok {
		return atlasMigrationFile, nil
	}

	up, err := migrationFuncFromSQLStringWithMetadata(filename, sql, hooks)
	if err != nil {
		return atlasSQLMigrationFile{}, err
	}
	return atlasSQLMigrationFile{up: up}, nil
}

func readSQLMigrationFile(fsys fs.FS, filename string, atlasTemplateData any) (string, error) {
	sql, _, err := RenderAtlasTemplateSQL(fsys, filename, atlasTemplateData)
	return sql, err
}

func atlasSQLMigrationFileFromSQL(filename, sql string, hooks statementExecutionHooks) (atlasSQLMigrationFile, bool, error) {
	parsed, ok, err := parseAtlasTxtarSQL(filename, sql)
	if err != nil || !ok {
		return atlasSQLMigrationFile{}, ok, err
	}

	up, err := migrationFuncFromSQLStringWithMetadata(filename+"#"+atlasTxtarMigrationSection, parsed.migrationSQL, hooks)
	if err != nil {
		return atlasSQLMigrationFile{}, true, err
	}
	up.checks = parseAtlasTxtarChecks(parsed.checksSQL)
	atlasMigrationFile := atlasSQLMigrationFile{up: up}
	if parsed.hasDown {
		down, err := migrationFuncFromSQLStringWithMetadata(filename+"#"+atlasTxtarDownSection, parsed.downSQL, hooks)
		if err != nil {
			return atlasSQLMigrationFile{}, true, err
		}
		atlasMigrationFile.down = down
		atlasMigrationFile.hasDown = true
	}
	return atlasMigrationFile, true, nil
}

func migrationFuncFromSQLStringWithMetadata(filename, sql string, hooks statementExecutionHooks) (sqlMigrationFile, error) {
	timeouts, err := parseMigrationTimeoutDirectives(sql)
	if err != nil {
		return sqlMigrationFile{}, err
	}

	noTransaction, err := parseNoTransactionDirectiveFromSQL(sql)
	if err != nil {
		return sqlMigrationFile{}, fmt.Errorf("invalid migration directives in %s: %w", filename, err)
	}
	return sqlMigrationFile{
		fn: func(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
			return executeMigrationFileSQL(ctx, conn, filename, sql, hooks, mode)
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
	parsed := atlasTxtarSQL{migrationSQL: migrationSection.String()}
	if checksSection := sections[atlasTxtarChecksSection]; checksSection != nil {
		parsed.checksSQL = checksSection.String()
	}
	if downSection := sections[atlasTxtarDownSection]; downSection != nil {
		parsed.downSQL = downSection.String()
		parsed.hasDown = true
	}
	return parsed, true, nil
}

// parseAtlasTxtarChecks maps a txtar checks.sql section onto Ptah's
// pre-migration check machinery: each statement is an assertion that must
// return a single truthy scalar before the migration body runs, matching
// Atlas's enforcement of the section as a pre-migration gate. Checks are named
// checks.sql#N by position so a failure names the exact assertion.
//
// N counts only non-empty statements, so comment-only spans and stray
// separators (`;;`) do not consume a number: the third assertion a reader can
// see is always checks.sql#3, even when blank statements sit between them.
func parseAtlasTxtarChecks(checksSQL string) []Check {
	statements := SplitSQLStatements(checksSQL)
	checks := make([]Check, 0, len(statements))
	for _, stmt := range statements {
		// Match parseCheckArgs: drop trailing terminators and whitespace so
		// drivers that reject a trailing ';' on a prepared query accept the
		// predicate.
		stmt = strings.TrimRight(strings.TrimSpace(stmt), "; \t")
		if stmt == "" {
			continue
		}
		checks = append(checks, Check{
			Name:   fmt.Sprintf("%s#%d", atlasTxtarChecksSection, len(checks)+1),
			Assert: stmt,
			OnFail: OnFailAbort,
		})
	}
	return checks
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
	return section == atlasTxtarMigrationSection || section == atlasTxtarDownSection || section == atlasTxtarChecksSection
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
	Version                int64
	Description            string
	Checksum               string
	revisionDescription    string
	hasRevisionDescription bool
	Up                     MigrationFunc
	Down                   MigrationFunc
	UpSQL                  string
	DownSQL                string
	UpTimeouts             MigrationTimeouts
	DownTimeouts           MigrationTimeouts
	downUnavailable        bool
	// UpNoTransaction runs the up body and metadata update outside the normal
	// per-migration transaction. Use this only for statements that cannot run
	// transactionally.
	UpNoTransaction bool
	// DownNoTransaction is the down-direction counterpart to UpNoTransaction.
	DownNoTransaction bool
	// NoTransaction reports whether either direction opts out of the normal
	// per-migration transaction. Execution uses the direction-specific fields.
	NoTransaction                bool
	directionalNoTransactionMode bool
	// atlasChecks are pre-migration assertions parsed from an Atlas txtar
	// checks.sql section. They run through the same enforcement machinery as
	// `-- +ptah check` directives, before any checks declared in UpSQL.
	atlasChecks []Check
	// IsCheckpoint marks a checkpoint migration whose up body is the full
	// cumulative schema at its version. On a fresh database the migrator
	// bootstraps from the newest checkpoint and records all lower versions as
	// applied instead of replaying them; an already-migrated database ignores
	// the checkpoint and applies history normally. Ptah-format directories
	// mark checkpoints with the `.checkpoint.` file-name pair; Atlas-format
	// directories mark them with a first-line `-- atlas:checkpoint` file
	// directive.
	IsCheckpoint bool
}

// atlasFilenameDescription preserves Atlas's raw filename description in
// revision metadata while allowing Ptah to retain a human-readable description.
func (m *Migration) atlasFilenameDescription() string {
	if m.hasRevisionDescription {
		return m.revisionDescription
	}
	return m.Description
}

func (m *Migration) upExecutionMode() migrationExecutionMode {
	if m.UpNoTransaction || (!m.directionalNoTransactionMode && m.NoTransaction) {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

func (m *Migration) downExecutionMode() migrationExecutionMode {
	if m.DownNoTransaction || (!m.directionalNoTransactionMode && m.NoTransaction) {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

// CreateMigrationFromSQL creates a migration from SQL strings
// This is useful for programmatically creating migrations
func CreateMigrationFromSQL(version int64, description, upSQL, downSQL string) *Migration {
	upNoTransaction, upDirectiveErr := parseNoTransactionDirectiveFromSQL(upSQL)
	downNoTransaction, downDirectiveErr := parseNoTransactionDirectiveFromSQL(downSQL)

	migration := &Migration{
		Version:                      version,
		Description:                  description,
		UpSQL:                        upSQL,
		DownSQL:                      downSQL,
		UpNoTransaction:              upNoTransaction,
		DownNoTransaction:            downNoTransaction,
		NoTransaction:                upNoTransaction || downNoTransaction,
		directionalNoTransactionMode: true,
	}

	upFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		if upDirectiveErr != nil {
			return fmt.Errorf("invalid up migration directives: %w", upDirectiveErr)
		}
		return executeSQLStatements(ctx, conn, upSQL, migration.upExecutionMode())
	}

	downFunc := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		if downDirectiveErr != nil {
			return fmt.Errorf("invalid down migration directives: %w", downDirectiveErr)
		}
		return executeSQLStatements(ctx, conn, downSQL, migration.downExecutionMode())
	}

	migration.Up = upFunc
	migration.Down = downFunc
	return migration
}

// executeSQLStatements splits SQL into individual statements and executes them
func executeSQLStatements(ctx context.Context, conn *dbschema.DatabaseConnection, sql string, mode migrationExecutionMode) error {
	statements := splitSQLStatementsForConnection(conn, sql)

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
	hooks statementExecutionHooks,
	mode migrationExecutionMode,
) error {
	// Directives live in comments, so they must be read from the raw file
	// before comment stripping, and validated before any statement runs so an
	// invalid directive leaves nothing half-applied.
	var fileDirectives map[string]string
	if hooks.interceptor != nil || hooks.observer != nil {
		fileDirectives = ParseFileDirectives(sql)
	}
	interceptorDirectives := maps.Clone(fileDirectives)
	if hooks.interceptor != nil {
		if err := hooks.interceptor.ValidateDirectives(interceptorDirectives); err != nil {
			return fmt.Errorf("invalid migration directives in %s: %w", filename, err)
		}
	}

	statements := splitSQLStatementsForConnection(conn, sql)
	if err := validateMigrationStatements(filename, statements, hooks.validator); err != nil {
		return err
	}
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		handled := false
		if hooks.interceptor != nil {
			var err error
			handled, err = hooks.interceptor.ExecuteStatement(ctx, conn, stmt, interceptorDirectives)
			if err != nil {
				return &MigrationExecutionError{
					Err:            fmt.Errorf("failed to execute migration SQL: %w", err),
					Statement:      stmt,
					StatementIndex: i + 1,
					Total:          len(statements),
				}
			}
		}

		if !handled {
			if err := executeMigrationStatement(ctx, conn, stmt, mode); err != nil {
				return &MigrationExecutionError{
					Err:            fmt.Errorf("failed to execute migration SQL: %w", err),
					Statement:      stmt,
					StatementIndex: i + 1,
					Total:          len(statements),
				}
			}
		}
		if hooks.observer != nil {
			event := StatementEvent{
				SourcePath: filename,
				Statement:  stmt,
				Index:      i + 1,
				Total:      len(statements),
				Directives: maps.Clone(fileDirectives),
			}
			if err := hooks.observer.ObserveStatement(ctx, event); err != nil {
				return &StatementObservationError{
					Err:   err,
					Event: event,
				}
			}
		}
	}
	return nil
}

func validateMigrationStatements(
	filename string,
	statements []string,
	validator StatementValidator,
) error {
	if validator == nil {
		return nil
	}
	for i, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if err := validator.ValidateStatement(statement); err != nil {
			return &MigrationExecutionError{
				Err:            fmt.Errorf("failed to validate migration SQL in %s: %w", filename, err),
				Statement:      statement,
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

// StatementObservationError reports a post-execution observer failure. The
// statement in Event was applied before the observer returned Err.
type StatementObservationError struct {
	Err   error
	Event StatementEvent
}

func (e *StatementObservationError) Error() string {
	return fmt.Sprintf(
		"failed to observe migration SQL: %v\nSource: %s\nSQL: %s",
		e.Err,
		e.Event.SourcePath,
		e.Event.Statement,
	)
}

func (e *StatementObservationError) Unwrap() error {
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
		return executeSQLOn(ctx, conn, sql, args...)
	}

	// Deliberate transaction escape hatch. This is used only for migrations
	// marked no_transaction, where the database rejects transactional execution
	// (for example PostgreSQL ALTER TYPE ADD VALUE followed by using that value).
	_, err := conn.ExecContext(ctx, sql, args...)
	return err
}

func executeSQLOn(ctx context.Context, conn *dbschema.DatabaseConnection, sql string, args ...any) error {
	return conn.Writer().ExecuteSQL(ctx, sql, args...)
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

func parseNoTransactionDirectiveFromSQL(sql string) (bool, error) {
	noTransaction, err := parseNoTransactionDirective(ParseFileDirectives(sql))
	if err != nil || noTransaction {
		return noTransaction, err
	}
	return hasAtlasTxModeNoneDirective(sql), nil
}

func hasAtlasTxModeNoneDirective(sql string) bool {
	// Match the dialect-blind SplitSQLStatements string handling so a
	// `-- atlas:txmode none` sequence inside a string literal is not mistaken
	// for the directive.
	lexr := lexer.NewLexerWithOptions(sql, lexer.Options{StandardStrings: true})
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		if tok.Type != lexer.TokenComment {
			continue
		}
		comment, ok := strings.CutPrefix(tok.Value, "--")
		if !ok {
			continue
		}
		if !commentStartsLine(sql, tok.Start) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(comment), "atlas:txmode none") {
			return true
		}
	}
	return false
}
