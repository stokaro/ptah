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
	"go.5x5.cz/ptah/migration/migrationfile"
)

// MigrationFunc represents a migration function that operates on a database connection
type MigrationFunc func(context.Context, *dbschema.DatabaseConnection) error

// splitSQLStatementsForConnection splits sql into individual statements using
// the connection's dialect, so string-literal boundaries are scanned correctly
// for that engine (a backslash is a C-style escape only for MySQL/MariaDB/
// ClickHouse) and a semicolon inside a backslash-escaped literal cannot leak
// out into a separately-executed statement. A nil connection falls back to the
// dialect-blind split.
func splitSQLStatementsForConnection(conn *dbschema.DatabaseConnection, sql string) []string {
	if conn == nil {
		return sqlutil.SplitStatements(sql)
	}
	return splitSQLStatementsForDialect(sql, conn.Info().Dialect)
}

func splitSQLStatementsForDialect(sql, dialect string) []string {
	return sqlutil.SplitStatementsForDialect(dialect, sql)
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
// migrationfile.ParseDirectives). It returns handled=true when it fully executed the
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
	before,
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
	timeouts             migrationfile.Timeouts
	txMode               migrationfile.FileTxMode
	txModeSource         migrationfile.FileTxModeSource
	txModeErr            error
	statementIntercepted bool
	// checkFiles are the raw Atlas txtar checks.sql and checks/*.sql sections.
	// They remain unsplit until execution, when the target dialect is known.
	checkFiles []migrationfile.AtlasTxtarCheckFile
}

type atlasSQLMigrationFile struct {
	up      sqlMigrationFile
	down    sqlMigrationFile
	hasDown bool
}

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
	sql, _, err := migrationfile.RenderAtlasTemplateSQL(fsys, filename, atlasTemplateData)
	return sql, err
}

func atlasSQLMigrationFileFromSQL(filename, sql string, hooks statementExecutionHooks) (atlasSQLMigrationFile, bool, error) {
	parsed, ok, err := migrationfile.ParseAtlasTxtar(filename, sql)
	if err != nil || !ok {
		return atlasSQLMigrationFile{}, ok, err
	}

	up, err := migrationFuncFromSQLStringWithMetadata(filename+"#"+migrationfile.AtlasTxtarMigrationSection, parsed.MigrationSQL, hooks)
	if err != nil {
		return atlasSQLMigrationFile{}, true, err
	}
	up.checkFiles = parsed.CheckFiles
	atlasMigrationFile := atlasSQLMigrationFile{up: up}
	if parsed.HasDown {
		down, err := migrationFuncFromSQLStringWithMetadata(filename+"#"+migrationfile.AtlasTxtarDownSection, parsed.DownSQL, hooks)
		if err != nil {
			return atlasSQLMigrationFile{}, true, err
		}
		atlasMigrationFile.down = down
		atlasMigrationFile.hasDown = true
	}
	return atlasMigrationFile, true, nil
}

func migrationFuncFromSQLStringWithMetadata(filename, sql string, hooks statementExecutionHooks) (sqlMigrationFile, error) {
	timeouts, err := migrationfile.ParseTimeouts(sql)
	if err != nil {
		return sqlMigrationFile{}, err
	}

	txMode := migrationfile.ParseFileTxMode(filename, sql)
	if txMode.Err != nil && txMode.Source == migrationfile.FileTxModeSourcePtah {
		return sqlMigrationFile{}, fmt.Errorf("invalid migration directives in %s: %w", filename, txMode.Err)
	}
	return sqlMigrationFile{
		fn: func(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrationExecutionMode) error {
			return executeMigrationFileSQL(ctx, conn, filename, sql, hooks, mode)
		},
		sql:                  sql,
		sourcePath:           filename,
		timeouts:             timeouts,
		txMode:               txMode.Mode,
		txModeSource:         txMode.Source,
		txModeErr:            txMode.Err,
		statementIntercepted: hooks.interceptor != nil,
	}, nil
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

// NoopMigrationFunc is a no-op migration function
func NoopMigrationFunc(_ctx context.Context, _conn *dbschema.DatabaseConnection) error {
	return nil
}

// Migration represents a database migration
type Migration struct {
	Version     int64
	Description string
	Checksum    string
	// sourceRevisionHash is the h1 the SOURCE directory's atlas.sum recorded
	// for the file this migration was converted from, accepted as a valid
	// stored revision checksum so a history the Atlas community binary wrote
	// can be continued (stokaro/ptah#1209).
	//
	// It is deliberately NOT [Migration.Checksum]. That field decides what Ptah
	// WRITES, and an atlas.sum h1 chains over every preceding file, so writing
	// it would make a Ptah history stop verifying whenever an unrelated
	// migration was inserted ahead of it.
	sourceRevisionHash         string
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
	UpTimeouts                 migrationfile.Timeouts
	DownTimeouts               migrationfile.Timeouts
	upParsedTimeouts           migrationfile.Timeouts
	downParsedTimeouts         migrationfile.Timeouts
	upTimeoutsFromSQL          bool
	downTimeoutsFromSQL        bool
	downUnavailable            bool
	// UpTxMode is the up file's explicit transaction mode. The zero value uses
	// the migrator's global mode. File and none override global file or none;
	// any explicit file mode conflicts with global all.
	UpTxMode migrationfile.FileTxMode
	// DownTxMode is the down-direction counterpart to UpTxMode. Rollback has no
	// global transaction-mode flag, so the zero value behaves like file.
	DownTxMode                  migrationfile.FileTxMode
	upParsedTxMode              migrationfile.FileTxMode
	downParsedTxMode            migrationfile.FileTxMode
	upTxModeFromSQL             bool
	downTxModeFromSQL           bool
	upTxModeSource              migrationfile.FileTxModeSource
	downTxModeSource            migrationfile.FileTxModeSource
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
	atlasCheckFiles []migrationfile.AtlasTxtarCheckFile
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
	if m.UpTxMode == migrationfile.FileTxModeNone {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

func (m *Migration) downExecutionMode() migrationExecutionMode {
	if m.DownTxMode == migrationfile.FileTxModeNone {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

func migrationExecutionModeForFileTxMode(mode migrationfile.FileTxMode) migrationExecutionMode {
	if mode == migrationfile.FileTxModeNone {
		return migrationExecutionNoTransaction
	}
	return migrationExecutionTransactional
}

func (m *Migration) parsedUpTxModeForDialect(dialect string) migrationfile.ParsedFileTxMode {
	if m.upTxModeFromSQL && m.UpTxMode == m.upParsedTxMode {
		return migrationfile.ParseFileTxModeForDialect(m.upSourcePath, m.UpSQL, dialect)
	}
	return migrationfile.ParsedFileTxMode{
		Mode:   m.UpTxMode,
		Source: m.upTxModeSource,
		Err:    m.upTxModeErr,
	}
}

func (m *Migration) parsedDownTxModeForDialect(dialect string) migrationfile.ParsedFileTxMode {
	if m.downTxModeFromSQL && m.DownTxMode == m.downParsedTxMode {
		return migrationfile.ParseFileTxModeForDialect(m.downSourcePath, m.DownSQL, dialect)
	}
	return migrationfile.ParsedFileTxMode{
		Mode:   m.DownTxMode,
		Source: m.downTxModeSource,
		Err:    m.downTxModeErr,
	}
}

func (m *Migration) upTimeoutsForDialect(dialect string) (migrationfile.Timeouts, error) {
	if m.upTimeoutsFromSQL && m.UpTimeouts == m.upParsedTimeouts {
		return migrationfile.ParseTimeoutsForDialect(m.UpSQL, dialect)
	}
	return m.UpTimeouts, nil
}

func (m *Migration) downTimeoutsForDialect(dialect string) (migrationfile.Timeouts, error) {
	if m.downTimeoutsFromSQL && m.DownTimeouts == m.downParsedTimeouts {
		return migrationfile.ParseTimeoutsForDialect(m.DownSQL, dialect)
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
		return m.upSQLFunc(ctx, conn, migrationExecutionModeForFileTxMode(txMode.Mode))
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

// CreateMigrationFromSQL creates a programmatic migration from up and down SQL
// strings. Transaction-mode directives in the SQL are honored exactly as they
// are in a file: a `-- +ptah no_transaction` or `-- atlas:txmode` header sets
// UpTxMode and DownTxMode at construction, a malformed directive surfaces as
// an error when the migration runs, and description stands in for the file
// name in those errors. Multi-statement bodies are split with the executing
// connection's dialect rules, one statement at a time.
func CreateMigrationFromSQL(version int64, description, upSQL, downSQL string) *Migration {
	upTxMode := migrationfile.ParseFileTxMode(description, upSQL)
	downTxMode := migrationfile.ParseFileTxMode(description, downSQL)

	migration := &Migration{
		Version:           version,
		Description:       description,
		UpSQL:             upSQL,
		DownSQL:           downSQL,
		UpTxMode:          upTxMode.Mode,
		DownTxMode:        downTxMode.Mode,
		upParsedTxMode:    upTxMode.Mode,
		downParsedTxMode:  downTxMode.Mode,
		upTxModeFromSQL:   true,
		downTxModeFromSQL: true,
		upTxModeSource:    upTxMode.Source,
		downTxModeSource:  downTxMode.Source,
		upTxModeErr:       upTxMode.Err,
		downTxModeErr:     downTxMode.Err,
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
		if txMode.Err != nil {
			return fmt.Errorf("invalid up migration directives: %w", txMode.Err)
		}
		return migration.upSQLFunc(ctx, conn, migrationExecutionModeForFileTxMode(txMode.Mode))
	}
	migration.Down = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		txMode := migration.parsedDownTxModeForDialect(databaseConnectionDialect(conn))
		if txMode.Err != nil {
			return fmt.Errorf("invalid down migration directives: %w", txMode.Err)
		}
		return migration.downSQLFunc(ctx, conn, migrationExecutionModeForFileTxMode(txMode.Mode))
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
	return migrationfile.ParseDirectivesForDialect(sql, databaseConnectionDialect(conn)), nil
}

func executeMigrationFileSQL(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	filename,
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
	if err := preflightTransactionRequirements(conn, filename, sql, statements, mode); err != nil {
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

func databaseConnectionDialect(conn *dbschema.DatabaseConnection) string {
	if conn == nil {
		return ""
	}
	return conn.Info().Dialect
}
