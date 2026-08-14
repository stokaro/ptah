package migrator

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
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

func splitSQLStatementsPreservingCommentsForDialect(sql, dialect string) []string {
	statements := sqlutil.SplitSQLStatementsForDialect(sql, dialect)
	filtered := statements[:0]
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		withoutComments := strings.TrimSpace(sqlutil.StripCommentsForDialect(stmt, dialect))
		if stmt != "" && withoutComments != "" {
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
// statement. When a Migrator executes SQL in no-transaction mode, the observer
// runs after Ptah durably checkpoints that statement's progress. Returning an
// error aborts the migration.
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

type statementProgressRecorder func(context.Context, StatementEvent) error

type statementProgressHooks struct {
	before statementProgressRecorder
	after  statementProgressRecorder
}

type statementProgressRecorderContextKey struct{}

type internalStatementObserver func(context.Context, StatementEvent) error

type internalStatementObserverContextKey struct{}

type migrationResumeContextKey struct{}

// withMigrationResume declares that statements before resumeFrom (1-based) were
// already committed by an earlier attempt at the same migration, so this attempt
// must skip them rather than execute them a second time.
//
// The floor travels in the context rather than as an execution-path parameter
// because the two statement loops that need it, [executeSQLStatements] and
// [executeMigrationFileSQL], are reached through the caller-supplied
// sqlMigrationFunc indirection, which has no room for a new argument.
//
// resumeFrom <= 1 means "run everything", which is the value every first attempt
// installs, so an absent key and a fresh run behave identically.
func withMigrationResume(ctx context.Context, resumeFrom int) context.Context {
	if resumeFrom <= 1 {
		return ctx
	}
	return context.WithValue(ctx, migrationResumeContextKey{}, resumeFrom)
}

// migrationStatementAlreadyApplied reports whether the 1-based statement index
// was committed by an earlier attempt and must not run again.
func migrationStatementAlreadyApplied(ctx context.Context, index int) bool {
	resumeFrom, _ := ctx.Value(migrationResumeContextKey{}).(int)
	return index < resumeFrom
}

func migrationAppliedFloor(ctx context.Context) int {
	return migrationResumeFrom(ctx) - 1
}

func migrationResumeFrom(ctx context.Context) int {
	resumeFrom, _ := ctx.Value(migrationResumeContextKey{}).(int)
	return max(resumeFrom, 1)
}

type statementProgressError struct {
	err     error
	event   StatementEvent
	applied int
	phase   string
}

func (e *statementProgressError) Error() string {
	return fmt.Sprintf("failed to checkpoint migration progress %s statement %d: %v", e.phase, e.event.Index, e.err)
}

func (e *statementProgressError) Unwrap() error {
	return e.err
}

func withStatementProgressRecorder(
	ctx context.Context,
	before statementProgressRecorder,
	after statementProgressRecorder,
) context.Context {
	return context.WithValue(ctx, statementProgressRecorderContextKey{}, statementProgressHooks{
		before: before,
		after:  after,
	})
}

func recordStatementProgressBefore(ctx context.Context, event StatementEvent) error {
	hooks, _ := ctx.Value(statementProgressRecorderContextKey{}).(statementProgressHooks)
	if hooks.before == nil {
		return nil
	}
	if err := hooks.before(ctx, event); err != nil {
		return &statementProgressError{
			err:     err,
			event:   event,
			applied: event.Index - 1,
			phase:   "before",
		}
	}
	return nil
}

func recordStatementProgressAfter(ctx context.Context, event StatementEvent) error {
	hooks, _ := ctx.Value(statementProgressRecorderContextKey{}).(statementProgressHooks)
	if hooks.after == nil {
		return nil
	}
	if err := hooks.after(ctx, event); err != nil {
		return &statementProgressError{
			err:     err,
			event:   event,
			applied: event.Index,
			phase:   "after",
		}
	}
	return nil
}

func withInternalStatementObserver(ctx context.Context, observer internalStatementObserver) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, internalStatementObserverContextKey{}, observer)
}

func observeExecutedStatement(ctx context.Context, event StatementEvent) error {
	observer, _ := ctx.Value(internalStatementObserverContextKey{}).(internalStatementObserver)
	if observer == nil {
		return nil
	}
	if err := observer(ctx, event); err != nil {
		return &StatementObservationError{Err: err, Event: event}
	}
	return nil
}

func recordAndObserveExecutedStatement(ctx context.Context, event StatementEvent) error {
	if err := recordStatementProgressAfter(ctx, event); err != nil {
		return err
	}
	return observeExecutedStatement(ctx, event)
}

type migrationExecutionMode int

const (
	migrationExecutionTransactional migrationExecutionMode = iota
	migrationExecutionNoTransaction
)

type sqlMigrationFunc func(context.Context, *dbschema.DatabaseConnection, migrationExecutionMode) error

type sqlMigrationFile struct {
	fn                   sqlMigrationFunc
	sql                  string
	sourcePath           string
	timeouts             MigrationTimeouts
	txMode               MigrationFileTxMode
	txModeSource         migrationFileTxModeSource
	txModeErr            error
	statementIntercepted bool
	// checkFiles are the raw Atlas txtar checks.sql and checks/*.sql sections.
	// They remain unsplit until execution, when the target dialect is known.
	checkFiles []atlasTxtarCheckFile
}

type atlasSQLMigrationFile struct {
	up      sqlMigrationFile
	down    sqlMigrationFile
	hasDown bool
}

type atlasTxtarSQL struct {
	migrationSQL        string
	migrationLineOffset int
	downSQL             string
	checkFiles          []atlasTxtarCheckFile
	hasDown             bool
}

type atlasTxtarCheckFile struct {
	name string
	sql  string
}

type migrationFileTxModeSource uint8

const (
	migrationFileTxModeSourcePtah migrationFileTxModeSource = iota + 1
	migrationFileTxModeSourceAtlas
)

type parsedMigrationFileTxMode struct {
	mode   MigrationFileTxMode
	source migrationFileTxModeSource
	err    error
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
	Version         int64
	revisionVersion string
	Description     string
}

func (e *AtlasDownNotImplementedError) Error() string {
	version := e.revisionVersion
	if version == "" {
		version = strconv.FormatInt(e.Version, 10)
	}
	return fmt.Sprintf(
		"migration %s has no Atlas down migration; dynamic Atlas-style down migrations are not implemented yet; add an atlas txtar down.sql section or migrate down manually",
		version,
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

// NewMigrationFromSQLFiles reads an up/down SQL pair into a complete Migration.
// Transaction modes, timeouts, source paths, and executable functions remain
// attached to the returned value so callers cannot accidentally discard file
// metadata that affects execution policy.
func NewMigrationFromSQLFiles(
	version int64,
	description, upFilename, downFilename string,
	fsys fs.FS,
) (*Migration, error) {
	return NewMigrationFromSQLFilesWithInterceptor(
		version,
		description,
		upFilename,
		downFilename,
		fsys,
		nil,
	)
}

// NewMigrationFromSQLFilesWithInterceptor is NewMigrationFromSQLFiles with an
// optional StatementInterceptor consulted for every statement in both files.
func NewMigrationFromSQLFilesWithInterceptor(
	version int64,
	description, upFilename, downFilename string,
	fsys fs.FS,
	interceptor StatementInterceptor,
) (*Migration, error) {
	hooks := statementExecutionHooks{interceptor: interceptor}
	up, err := migrationFuncFromSQLFilenameWithMetadata(upFilename, fsys, hooks, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load up migration %s: %w", upFilename, err)
	}
	down, err := migrationFuncFromSQLFilenameWithMetadata(downFilename, fsys, hooks, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load down migration %s: %w", downFilename, err)
	}

	migration := &Migration{Version: version, Description: description}
	setMigrationUp(migration, up)
	setMigrationDown(migration, down)
	return migration, nil
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
	up.checkFiles = parsed.checkFiles
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

	txMode := parseMigrationFileTxMode(filename, sql)
	if txMode.err != nil && txMode.source == migrationFileTxModeSourcePtah {
		return sqlMigrationFile{}, fmt.Errorf("invalid migration directives in %s: %w", filename, txMode.err)
	}
	return sqlMigrationFile{
		fn: func(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
			return executeMigrationFileSQL(ctx, conn, filename, sql, hooks, mode)
		},
		sql:                  sql,
		sourcePath:           filename,
		timeouts:             timeouts,
		txMode:               txMode.mode,
		txModeSource:         txMode.source,
		txModeErr:            txMode.err,
		statementIntercepted: hooks.interceptor != nil,
	}, nil
}

func parseAtlasTxtarSQL(filename, sql string) (atlasTxtarSQL, bool, error) {
	isTxtar, misplacedTxtar := classifyAtlasTxtarDirective(sql)
	if misplacedTxtar {
		return atlasTxtarSQL{}, true, fmt.Errorf(
			"invalid Atlas txtar migration %s: %s must be the first non-empty line",
			filename,
			atlasTxtarDirective,
		)
	}
	if !isTxtar {
		return atlasTxtarSQL{}, false, nil
	}

	sections := make(map[string]*strings.Builder)
	var checkSectionNames []string
	var currentSection string
	var migrationLineOffset int
	sawSection := false
	for lineNumber, line := range strings.SplitAfter(sql, "\n") {
		section, isMarker := parseAtlasTxtarSectionMarker(line)
		if !isMarker {
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
			continue
		}

		sawSection = true
		currentSection = ""
		if !isAtlasTxtarSQLSection(section) && !isAtlasTxtarCheckSection(section) {
			continue
		}
		if _, exists := sections[section]; exists {
			return atlasTxtarSQL{}, true, fmt.Errorf("invalid Atlas txtar migration %s: duplicate %s section", filename, section)
		}
		sections[section] = &strings.Builder{}
		currentSection = section
		if section == atlasTxtarMigrationSection {
			migrationLineOffset = lineNumber + 1
		}
		if isAtlasTxtarCheckSection(section) {
			checkSectionNames = append(checkSectionNames, section)
		}
	}

	migrationSection := sections[atlasTxtarMigrationSection]
	if migrationSection == nil {
		return atlasTxtarSQL{}, true, fmt.Errorf("invalid Atlas txtar migration %s: missing migration.sql section", filename)
	}
	parsed := atlasTxtarSQL{
		migrationSQL:        migrationSection.String(),
		migrationLineOffset: migrationLineOffset,
	}
	for _, section := range checkSectionNames {
		parsed.checkFiles = append(parsed.checkFiles, atlasTxtarCheckFile{
			name: section,
			sql:  sections[section].String(),
		})
	}
	if downSection := sections[atlasTxtarDownSection]; downSection != nil {
		parsed.downSQL = downSection.String()
		parsed.hasDown = true
	}
	return parsed, true, nil
}

// parseAtlasTxtarChecks maps one txtar check file onto Ptah's pre-migration
// check machinery. Each statement is named file#N by position so a failure
// identifies both the embedded file and assertion.
//
// N counts only non-empty statements, so comment-only spans and stray
// separators (`;;`) do not consume a number: the third assertion a reader can
// see is always file#3, even when blank statements sit between them.
func parseAtlasTxtarChecks(filename, checksSQL, dialect string) []Check {
	statements := splitSQLStatementsPreservingCommentsForDialect(checksSQL, dialect)
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
			Name:   fmt.Sprintf("%s#%d", filename, len(checks)+1),
			Assert: stmt,
			OnFail: OnFailAbort,
		})
	}
	return checks
}

func classifyAtlasTxtarDirective(sql string) (isTxtar, misplaced bool) {
	sawContent := false
	sawDirective := false
	sawSection := false
	for line := range strings.SplitSeq(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !sawContent {
			sawContent = true
			if trimmed == atlasTxtarDirective {
				return true, false
			}
		}
		if trimmed == atlasTxtarDirective {
			sawDirective = true
			if sawSection {
				return false, true
			}
			continue
		}
		_, isMarker := parseAtlasTxtarSectionMarker(line)
		if isMarker {
			sawSection = true
		}
		if sawDirective && sawSection {
			return false, true
		}
	}
	return false, false
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

func isAtlasTxtarCheckSection(section string) bool {
	if section == atlasTxtarChecksSection {
		return true
	}
	name, ok := strings.CutPrefix(section, "checks/")
	return ok && name != "" && strings.HasSuffix(name, ".sql")
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
	Version                    int64
	Description                string
	Checksum                   string
	atlasRevisionVersion       string
	hasAtlasRevisionVersion    bool
	atlasRevisionVersionMapped bool
	atlasRevisionType          AtlasRevisionType
	atlasRepeatable            bool
	atlasOrderKey              string
	atlasSumContributions      []atlasSumContribution
	revisionDescription        string
	hasRevisionDescription     bool
	Up                         MigrationFunc
	Down                       MigrationFunc
	UpSQL                      string
	DownSQL                    string
	UpTimeouts                 MigrationTimeouts
	DownTimeouts               MigrationTimeouts
	upParsedTimeouts           MigrationTimeouts
	downParsedTimeouts         MigrationTimeouts
	upTimeoutsFromSQL          bool
	downTimeoutsFromSQL        bool
	downUnavailable            bool
	// UpTxMode is the up file's explicit transaction mode. The zero value uses
	// the migrator's global mode. File and none override global file or none;
	// any explicit file mode conflicts with global all.
	UpTxMode MigrationFileTxMode
	// DownTxMode is the down-direction counterpart to UpTxMode. Rollback has no
	// global transaction-mode flag, so the zero value behaves like file.
	DownTxMode                  MigrationFileTxMode
	upParsedTxMode              MigrationFileTxMode
	downParsedTxMode            MigrationFileTxMode
	upTxModeFromSQL             bool
	downTxModeFromSQL           bool
	upTxModeSource              migrationFileTxModeSource
	downTxModeSource            migrationFileTxModeSource
	upTxModeErr                 error
	downTxModeErr               error
	upSourcePath                string
	downSourcePath              string
	upSQLFunc                   sqlMigrationFunc
	downSQLFunc                 sqlMigrationFunc
	upHasStatementInterceptor   bool
	downHasStatementInterceptor bool
	// atlasCheckFiles are raw Atlas txtar checks.sql and checks/*.sql sections.
	// They are parsed with the live connection dialect before `-- +ptah check`
	// directives in UpSQL, preventing dialect-blind boundary decisions.
	atlasCheckFiles []atlasTxtarCheckFile
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

// atlasSumContribution retains the exact source bytes that fed atlas.sum.
// One logical migration can own more than one contribution when it uses
// directional .up.sql/.down.sql files.
type atlasSumContribution struct {
	name          string
	data          []byte
	includeData   bool
	revisionEntry bool
}

// RevisionVersion returns the version token this migration records in revision
// metadata. Native Ptah migrations and ordinary Atlas migrations use their
// numeric version. Atlas repeatable migrations use Atlas's opaque token, such
// as "R" or "2R".
func (m *Migration) RevisionVersion() string {
	if m.hasAtlasRevisionVersion {
		return m.atlasRevisionVersion
	}
	return strconv.FormatInt(m.Version, 10)
}

func (m *Migration) revisionType() AtlasRevisionType {
	if m.atlasRevisionType != AtlasRevisionTypeUnknown {
		return m.atlasRevisionType
	}
	return AtlasRevisionTypeApplied
}

func (m *Migration) manuallySetRevisionType() AtlasRevisionType {
	if m.atlasRevisionType == AtlasRevisionTypeBaseline|AtlasRevisionTypeApplied {
		return m.atlasRevisionType | AtlasRevisionTypeManuallySet
	}
	return AtlasRevisionTypeApplied | AtlasRevisionTypeManuallySet
}

func (m *Migration) isAtlasRepeatable() bool {
	return m.atlasRepeatable
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
	if m.UpTxMode == MigrationFileTxModeNone {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

func (m *Migration) downExecutionMode() migrationExecutionMode {
	if m.DownTxMode == MigrationFileTxModeNone {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

func migrationExecutionModeForFileTxMode(mode MigrationFileTxMode) migrationExecutionMode {
	if mode == MigrationFileTxModeNone {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

func (m *Migration) parsedUpTxModeForDialect(dialect string) parsedMigrationFileTxMode {
	if m.upTxModeFromSQL && m.UpTxMode == m.upParsedTxMode {
		return parseMigrationFileTxModeForDialect(m.upSourcePath, m.UpSQL, dialect)
	}
	return parsedMigrationFileTxMode{
		mode:   m.UpTxMode,
		source: m.upTxModeSource,
		err:    m.upTxModeErr,
	}
}

func (m *Migration) parsedDownTxModeForDialect(dialect string) parsedMigrationFileTxMode {
	if m.downTxModeFromSQL && m.DownTxMode == m.downParsedTxMode {
		return parseMigrationFileTxModeForDialect(m.downSourcePath, m.DownSQL, dialect)
	}
	return parsedMigrationFileTxMode{
		mode:   m.DownTxMode,
		source: m.downTxModeSource,
		err:    m.downTxModeErr,
	}
}

func (m *Migration) upTimeoutsForDialect(dialect string) (MigrationTimeouts, error) {
	if m.upTimeoutsFromSQL && m.UpTimeouts == m.upParsedTimeouts {
		return parseMigrationTimeoutDirectivesForDialect(m.UpSQL, dialect)
	}
	return m.UpTimeouts, nil
}

func (m *Migration) downTimeoutsForDialect(dialect string) (MigrationTimeouts, error) {
	if m.downTimeoutsFromSQL && m.DownTimeouts == m.downParsedTimeouts {
		return parseMigrationTimeoutDirectivesForDialect(m.DownSQL, dialect)
	}
	return m.DownTimeouts, nil
}

// UpForReplay executes the up direction against a THROWAWAY dev database,
// ignoring an unreadable transaction-mode directive.
//
// Replay exists to reconstruct a schema so something else can be computed from
// it -- a lint analysis, a diff. The transaction mode decides how statements are
// wrapped when a real database is migrated; on a database that is dropped
// immediately afterwards it changes nothing anyone can observe, so a directive
// we cannot read is not a reason to refuse the whole operation.
//
// The apply path keeps refusing, and that matches: measured on the pinned
// community binary, `migrate apply` over a directory carrying
// `-- atlas:txmode unknown` exits 1 with the same complaint, while
// `migrate lint` over the same directory exits 0 and analyzes it.
//
// A migration with no SQL function falls back to [Migration.Up], which still
// refuses. That path is for programmatic migrations, which carry no file
// directive to be unreadable in the first place.
func (m *Migration) UpForReplay(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	if m.upSQLFunc != nil {
		txMode := m.parsedUpTxModeForDialect(databaseConnectionDialect(conn))
		return m.upSQLFunc(ctx, conn, migrationExecutionModeForFileTxMode(txMode.mode))
	}
	return m.Up(ctx, conn)
}

func (m *Migration) executeUp(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
	if m.upTxModeErr != nil {
		return m.upTxModeErr
	}
	if m.upSQLFunc != nil {
		return m.upSQLFunc(ctx, conn, mode)
	}
	return m.Up(ctx, conn)
}

func (m *Migration) executeDown(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
	if m.downTxModeErr != nil {
		return m.downTxModeErr
	}
	if m.downSQLFunc != nil {
		return m.downSQLFunc(ctx, conn, mode)
	}
	return m.Down(ctx, conn)
}

// CreateMigrationFromSQL creates a migration from SQL strings
// This is useful for programmatically creating migrations
func CreateMigrationFromSQL(version int64, description, upSQL, downSQL string) *Migration {
	upTxMode := parseMigrationFileTxMode(description, upSQL)
	downTxMode := parseMigrationFileTxMode(description, downSQL)

	migration := &Migration{
		Version:           version,
		Description:       description,
		UpSQL:             upSQL,
		DownSQL:           downSQL,
		UpTxMode:          upTxMode.mode,
		DownTxMode:        downTxMode.mode,
		upParsedTxMode:    upTxMode.mode,
		downParsedTxMode:  downTxMode.mode,
		upTxModeFromSQL:   true,
		downTxModeFromSQL: true,
		upTxModeSource:    upTxMode.source,
		downTxModeSource:  downTxMode.source,
		upTxModeErr:       upTxMode.err,
		downTxModeErr:     downTxMode.err,
		upSourcePath:      description,
		downSourcePath:    description,
	}

	migration.upSQLFunc = func(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
		return executeSQLStatements(ctx, conn, upSQL, mode)
	}
	migration.downSQLFunc = func(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
		return executeSQLStatements(ctx, conn, downSQL, mode)
	}

	migration.Up = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		txMode := migration.parsedUpTxModeForDialect(databaseConnectionDialect(conn))
		if txMode.err != nil {
			return fmt.Errorf("invalid up migration directives: %w", txMode.err)
		}
		return migration.upSQLFunc(ctx, conn, migrationExecutionModeForFileTxMode(txMode.mode))
	}
	migration.Down = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		txMode := migration.parsedDownTxModeForDialect(databaseConnectionDialect(conn))
		if txMode.err != nil {
			return fmt.Errorf("invalid down migration directives: %w", txMode.err)
		}
		return migration.downSQLFunc(ctx, conn, migrationExecutionModeForFileTxMode(txMode.mode))
	}
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

		event := StatementEvent{
			Statement: stmt,
			Index:     i + 1,
			Total:     len(statements),
		}
		if migrationStatementAlreadyApplied(ctx, event.Index) {
			continue
		}
		if err := recordStatementProgressBefore(ctx, event); err != nil {
			return err
		}
		if err := executeMigrationStatement(ctx, conn, stmt, mode); err != nil {
			return &MigrationExecutionError{
				Err:            fmt.Errorf("failed to execute SQL statement: %w", err),
				Statement:      stmt,
				StatementIndex: i + 1,
				Total:          len(statements),
			}
		}
		if err := recordAndObserveExecutedStatement(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

// observedFileDirectives reads the migration's `-- +ptah` directives for the
// hooks that consume them, in the region where directives are significant.
//
// Nothing reads them when no hook is installed, so the parse is skipped
// entirely rather than computed and thrown away.
func observedFileDirectives(
	conn *dbschema.DatabaseConnection,
	sql string,
	hooks statementExecutionHooks,
) (map[string]string, error) {
	if hooks.interceptor == nil && hooks.observer == nil {
		return nil, nil
	}
	return parseFileDirectivesForDialect(sql, databaseConnectionDialect(conn)), nil
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
	fileDirectives, err := observedFileDirectives(conn, sql, hooks)
	if err != nil {
		return err
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

		event := StatementEvent{
			SourcePath: filename,
			Statement:  stmt,
			Index:      i + 1,
			Total:      len(statements),
			Directives: maps.Clone(fileDirectives),
		}
		if migrationStatementAlreadyApplied(ctx, event.Index) {
			continue
		}
		if err := recordStatementProgressBefore(ctx, event); err != nil {
			return err
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
		if err := recordAndObserveExecutedStatement(ctx, event); err != nil {
			return err
		}
		if hooks.observer != nil {
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

// Error renders the wrapped failure with the statement that caused it.
//
// The statement line is omitted when the wrapped error already carries it. Every
// dialect writer appends its own `SQL: <statement>` line, so adding one here
// unconditionally printed the same statement twice for one failure -- in the
// CLI message, and in the recorded revision `error` column, which is where
// stokaro/ptah#1196 found it.
//
// The comparison is against this error's own Statement rather than a search for
// any SQL line, so a wrapped error mentioning a DIFFERENT statement still gets
// this one appended.
func (e *MigrationExecutionError) Error() string {
	message := fmt.Sprintf("%v", e.Err)
	if strings.Contains(message, "\nSQL: "+e.Statement) {
		return message
	}
	return fmt.Sprintf("%s\nSQL: %s", message, e.Statement)
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

func parseMigrationFileTxMode(filename, sql string) parsedMigrationFileTxMode {
	return parseMigrationFileTxModeForDialect(filename, sql, "")
}

func parseMigrationFileTxModeForDialect(filename, sql, dialect string) parsedMigrationFileTxMode {
	directives := parseFileDirectivesConservatively(sql)
	if dialect != "" {
		directives = parseFileDirectivesForDialect(sql, dialect)
	}
	parsed := parseMigrationFileTxModeWithDirectives(filename, sql, directives)
	if parsed.err != nil {
		return parsed
	}
	// A directive the region does not honor still has to be well formed. The
	// mode above came from the header; this is the separate question of whether
	// a recognized directive elsewhere in the file carries a value nobody can
	// read. See [misplacedDirectiveError].
	if err := misplacedDirectiveError(sql, dialect); err != nil {
		return parsedMigrationFileTxMode{source: migrationFileTxModeSourcePtah, err: err}
	}
	return parsed
}

func parseMigrationFileTxModeWithDirectives(
	filename string,
	sql string,
	directives map[string]string,
) parsedMigrationFileTxMode {
	atlasMode, hasAtlasMode, atlasErr := parseAtlasFileTxMode(filename, sql)
	if atlasErr != nil {
		return parsedMigrationFileTxMode{source: migrationFileTxModeSourceAtlas, err: atlasErr}
	}

	noTransaction, err := parseNoTransactionDirective(directives)
	if err != nil {
		return parsedMigrationFileTxMode{
			source: migrationFileTxModeSourcePtah,
			err:    err,
		}
	}
	if noTransaction {
		return parsedMigrationFileTxMode{
			mode:   MigrationFileTxModeNone,
			source: migrationFileTxModeSourcePtah,
		}
	}
	if hasAtlasMode {
		return parsedMigrationFileTxMode{
			mode:   atlasMode,
			source: migrationFileTxModeSourceAtlas,
		}
	}
	return parsedMigrationFileTxMode{}
}

func databaseConnectionDialect(conn *dbschema.DatabaseConnection) string {
	if conn == nil {
		return ""
	}
	return conn.Info().Dialect
}
