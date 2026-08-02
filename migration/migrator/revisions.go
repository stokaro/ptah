package migrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasretry"
)

const (
	migrationStateApplied        = "applied"
	migrationStatePending        = "pending"
	migrationStateFailed         = "failed"
	ptahOperatorVersion          = "Ptah"
	atlasNullJSON                = "null"
	atlasSetMaxAttempts          = 3
	revisionWriteTimeout         = 10 * time.Second
	unknownStatementOutcomeError = "statement execution outcome is unknown after process interruption"
)

// MigrationRevision records one row from the migration metadata table.
type MigrationRevision struct {
	Version         int64             `json:"version"`
	Description     string            `json:"description"`
	State           string            `json:"state"`
	AtlasType       AtlasRevisionType `json:"atlas_type,omitempty"`
	Applied         int               `json:"applied"`
	Total           int               `json:"total"`
	Error           string            `json:"error,omitempty"`
	ErrorStatement  string            `json:"error_stmt,omitempty"`
	ExecutionTime   time.Duration     `json:"execution_time"`
	Checksum        string            `json:"checksum,omitempty"`
	AppliedAt       time.Time         `json:"applied_at"`
	OperatorVersion string            `json:"operator_version,omitempty"`
	Dirty           bool              `json:"dirty"`
	ChecksumCurrent string            `json:"checksum_current,omitempty"`
}

// DirtyMigrationError reports that a previous migration run left a dirty row.
type DirtyMigrationError struct {
	Revision MigrationRevision
}

func (e *DirtyMigrationError) Error() string {
	return fmt.Sprintf(
		"migration %d is dirty: state=%s applied=%d/%d error=%q error_stmt=%q",
		e.Revision.Version,
		e.Revision.State,
		e.Revision.Applied,
		e.Revision.Total,
		e.Revision.Error,
		e.Revision.ErrorStatement,
	)
}

// IsDirtyMigration reports whether err wraps a dirty migration error.
func IsDirtyMigration(err error) bool {
	var target *DirtyMigrationError
	return errors.As(err, &target)
}

// ChecksumMismatchError reports that an already-applied migration file changed.
type ChecksumMismatchError struct {
	Version  int64
	Stored   string
	Computed string
}

func (e *ChecksumMismatchError) Error() string {
	return fmt.Sprintf("migration %d checksum mismatch: stored %s, current %s", e.Version, e.Stored, e.Computed)
}

// RepairMigrationOptions configures migration metadata repair.
type RepairMigrationOptions struct {
	Version    int64
	Force      bool
	ResumeFrom int
}

// BaselineOptions configures migration metadata baselining.
type BaselineOptions struct {
	Version int64
	Force   bool
}

func migrationChecksum(sqlText string) string {
	sum := sha256.Sum256([]byte(sqlText))
	return hex.EncodeToString(sum[:])
}

// migrationRevisionHash returns the canonical identity stored with a revision
// row: the atlas.sum h1 hash (without its prefix) when the migration directory
// ships one, and the hex SHA-256 of the up SQL otherwise. Every write path and
// the verification path must use this same function so applied migrations keep
// verifying.
func migrationRevisionHash(migration *Migration) string {
	if migration.Checksum == "" {
		return migrationChecksum(migration.UpSQL)
	}
	return normalizeAtlasRevisionHash(migration.Checksum)
}

// revisionChecksumMatches reports whether a stored revision checksum matches
// the migration. Rows written by ptah versions that predate storing the
// atlas.sum hash under the ptah revision table format hold the hex SHA-256 of
// the up SQL instead, so that legacy encoding is accepted too; both encodings
// are content hashes of the current migration, so tampering still changes
// every accepted candidate.
func revisionChecksumMatches(stored string, migration *Migration) bool {
	if stored == migrationRevisionHash(migration) {
		return true
	}
	return migration.Checksum != "" && stored == migrationChecksum(migration.UpSQL)
}

func normalizeAtlasRevisionHash(hash string) string {
	return strings.TrimPrefix(hash, "h1:")
}

func migrationStatementCount(sqlText string) int {
	return len(SplitSQLStatements(sqlText))
}

func migrationStatementCountForDialect(sqlText, dialect string) int {
	return len(splitSQLStatementsForDialect(sqlText, dialect))
}

func (m *Migrator) migrationStatementCount(sqlText string) int {
	if m.conn == nil {
		return migrationStatementCount(sqlText)
	}
	return migrationStatementCountForDialect(sqlText, m.conn.Info().Dialect)
}

func migrationExecutionProgress(err error, dialect string, txMode MigrationTxMode) (applied int, total int, stmt string) {
	var progressErr *statementProgressError
	if errors.As(err, &progressErr) {
		event := progressErr.event
		return progressErr.applied, event.Total, event.Statement
	}

	var observationErr *StatementObservationError
	if errors.As(err, &observationErr) {
		event := observationErr.Event
		applied = event.Index
		total = event.Total
		stmt = event.Statement
		if migrationProgressRolledBack(dialect, txMode) {
			applied = 0
		}
		return applied, total, stmt
	}

	var execErr *MigrationExecutionError
	if !errors.As(err, &execErr) {
		return 0, 0, ""
	}

	total = execErr.Total
	applied = execErr.StatementIndex - 1
	if txMode == MigrationTxModeAll ||
		(txMode == MigrationTxModeFile && (dialect == "postgres" || dialect == "cockroachdb" || dialect == "yugabytedb")) {
		applied = 0
	}
	if applied < 0 {
		applied = 0
	}
	return applied, total, execErr.Statement
}

func migrationProgressRolledBack(dialect string, txMode MigrationTxMode) bool {
	if txMode == MigrationTxModeAll {
		return true
	}
	if txMode != MigrationTxModeFile {
		return false
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.SQLite, platform.SQLServer:
		return true
	default:
		return false
	}
}

func (m *Migrator) getDirtyRevisionSQL() string {
	if m.revisionTableFormat.isAtlas() {
		if m.isSQLServer() {
			return fmt.Sprintf(`SELECT TOP (1) version, description, type, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time, hash, executed_at, COALESCE(operator_version, '')
FROM %s
WHERE (applied <> total OR COALESCE(error, '') <> '') AND %s
ORDER BY %s`, m.qualifiedMigrationsTable(), atlasMetadataRowPredicate, m.atlasVersionNumberExpression())
		}
		return fmt.Sprintf(`SELECT version, description, type, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time, hash, executed_at, COALESCE(operator_version, '')
FROM %s
WHERE (applied <> total OR COALESCE(error, '') <> '') AND %s
ORDER BY %s
LIMIT 1`, m.qualifiedMigrationsTable(), atlasMetadataRowPredicate, m.atlasVersionNumberExpression())
	}
	if m.isSQLServer() {
		return fmt.Sprintf(`SELECT TOP (1) version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at
FROM %s
WHERE state <> ?
ORDER BY version`, m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`SELECT version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at
FROM %s
WHERE state <> ?
ORDER BY version
LIMIT 1`, m.qualifiedMigrationsTable())
}

func (m *Migrator) getRevisionSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(`SELECT version, description, type, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time, hash, executed_at, COALESCE(operator_version, '')
FROM %s
WHERE version = ?`, m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`SELECT %s
FROM %s
WHERE version = ?`, m.ptahRevisionProjection(), m.qualifiedMigrationsTable())
}

func (m *Migrator) getAppliedRevisionsSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(
			`SELECT version, description, type, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time, hash, executed_at, COALESCE(operator_version, '')
FROM %s
WHERE applied = total AND COALESCE(error, '') = '' AND %s
ORDER BY %s`,
			m.qualifiedMigrationsTable(),
			atlasMetadataRowPredicate,
			m.atlasVersionNumberExpression(),
		)
	}
	where := "WHERE state = 'applied'\n"
	if m.legacyRevisionTable {
		where = ""
	}
	return fmt.Sprintf(`SELECT %s
FROM %s
%sORDER BY version`, m.ptahRevisionProjection(), m.qualifiedMigrationsTable(), where)
}

func (m *Migrator) getRevisionsSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(
			`SELECT version, description, type, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time, hash, executed_at, COALESCE(operator_version, '')
FROM %s
WHERE %s
ORDER BY %s`,
			m.qualifiedMigrationsTable(),
			atlasMetadataRowPredicate,
			m.atlasVersionNumberExpression(),
		)
	}
	return fmt.Sprintf(`SELECT %s
FROM %s
ORDER BY version`, m.ptahRevisionProjection(), m.qualifiedMigrationsTable())
}

func (m *Migrator) ptahRevisionProjection() string {
	if m.legacyRevisionTable {
		return "version, description, 'applied', 1, 1, '', '', 0, '', applied_at"
	}
	return "version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at"
}

func (m *Migrator) getRevisionsForUpdateSQL() string {
	switch platform.NormalizeDialect(m.conn.Info().Dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.MySQL, platform.MariaDB:
		return m.getRevisionsSQL() + " FOR UPDATE"
	case platform.SQLServer:
		if m.revisionTableFormat.isAtlas() {
			return fmt.Sprintf(
				`SELECT version, description, type, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time, hash, executed_at, COALESCE(operator_version, '')
FROM %s WITH (UPDLOCK, HOLDLOCK)
WHERE %s
ORDER BY %s`,
				m.qualifiedMigrationsTable(),
				atlasMetadataRowPredicate,
				m.atlasVersionNumberExpression(),
			)
		}
		return fmt.Sprintf(
			`SELECT version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at
FROM %s WITH (UPDLOCK, HOLDLOCK)
ORDER BY version`,
			m.qualifiedMigrationsTable(),
		)
	default:
		return m.getRevisionsSQL()
	}
}

func (m *Migrator) beginMigrationSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(`INSERT INTO %s (version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`INSERT INTO %s (version, description, applied_at, state, applied, total, error, error_stmt, execution_time_ms, checksum)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, m.qualifiedMigrationsTable())
}

func (m *Migrator) completeMigrationSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return revisionUpdateSQL(
			m.connectionDialect(),
			m.qualifiedMigrationsTable(),
			"applied = ?, total = ?, execution_time = ?, error = '', error_stmt = '', partial_hashes = ?, operator_version = ?",
		)
	}
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, applied_at = ?",
	)
}

func (m *Migrator) checkpointMigrationSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return revisionUpdateSQL(
			m.connectionDialect(),
			m.qualifiedMigrationsTable(),
			"applied = ?, total = ?, execution_time = ?, error = '', error_stmt = '', operator_version = ?",
		)
	}
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?",
	)
}

func (m *Migrator) beginRollbackSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return revisionUpdateSQL(
			m.connectionDialect(),
			m.qualifiedMigrationsTable(),
			"applied = ?, total = ?, executed_at = ?, execution_time = ?, error = '', error_stmt = '', partial_hashes = ?, operator_version = ?",
		)
	}
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?",
	)
}

func (m *Migrator) failMigrationSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return revisionUpdateSQL(
			m.connectionDialect(),
			m.qualifiedMigrationsTable(),
			"applied = ?, total = ?, execution_time = ?, error = ?, error_stmt = ?, operator_version = ?",
		)
	}
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"state = ?, applied = ?, total = ?, error = ?, error_stmt = ?, execution_time_ms = ?",
	)
}

func revisionUpdateSQL(dialect, table, assignments string) string {
	if platform.NormalizeDialect(dialect) == platform.ClickHouse {
		return fmt.Sprintf(`ALTER TABLE %s
UPDATE %s
WHERE version = ?
SETTINGS mutations_sync = 1`, table, assignments)
	}
	return fmt.Sprintf(`UPDATE %s
SET %s
WHERE version = ?`, table, assignments)
}

func (m *Migrator) forceAppliedMigrationSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(`INSERT INTO %s (version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?)
%s`, m.qualifiedMigrationsTable(), m.forceAppliedConflictClause())
	}
	return fmt.Sprintf(`INSERT INTO %s (version, description, applied_at, state, applied, total, error, error_stmt, execution_time_ms, checksum)
VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
%s`, m.qualifiedMigrationsTable(), m.forceAppliedConflictClause())
}

func (m *Migrator) insertAtlasRevisionSQL() string {
	return fmt.Sprintf(`INSERT INTO %s (version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?)`, m.qualifiedMigrationsTable())
}

func (m *Migrator) forceAppliedPtahUpdateSQL() string {
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"description = ?, applied_at = ?, state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, checksum = ?",
	)
}

func (m *Migrator) forceAppliedAtlasUpdateSQL() string {
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"description = ?, type = ?, applied = ?, total = ?, executed_at = ?, execution_time = ?, error = '', error_stmt = '', hash = ?, partial_hashes = ?, operator_version = ?",
	)
}

func (m *Migrator) countRevisionsSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, m.qualifiedMigrationsTable(), atlasMetadataRowPredicate)
	}
	return fmt.Sprintf(`SELECT COUNT(*) FROM %s`, m.qualifiedMigrationsTable())
}

func (m *Migrator) countRevisionsAboveSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s > ?`, m.qualifiedMigrationsTable(), m.atlasVersionNumberExpression())
	}
	return fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE version > ?`, m.qualifiedMigrationsTable())
}

func (m *Migrator) deleteRevisionsAboveSQL() string {
	return fmt.Sprintf(
		`DELETE FROM %s WHERE %s > ?`,
		m.qualifiedMigrationsTable(),
		m.atlasVersionNumberExpression(),
	)
}

func (m *Migrator) updateAtlasRevisionTypeSQL() string {
	return fmt.Sprintf(`UPDATE %s SET type = ? WHERE version = ?`, m.qualifiedMigrationsTable())
}

// atlasMetadataRowPredicate excludes Atlas metadata revision rows from a query.
// Atlas writes dot-prefixed pseudo-versions (for example
// `.atlas_cloud_identifier`, inserted by `migrate down` even in local mode)
// into the revision table. Those rows are bookkeeping, not migrations: version
// math, status, and pending calculations skip them, and no write path deletes
// or rewrites them (#957).
const atlasMetadataRowPredicate = "version NOT LIKE '.%'"

// atlasMetadataVersionNullGuard maps a dot-prefixed metadata version to NULL so
// it can be cast to a number safely. See [Migrator.atlasVersionNumberExpression]
// for why this, and not the predicate above, is what protects the statements
// that select over every revision row.
const atlasMetadataVersionNullGuard = `CASE WHEN version LIKE '.%' THEN NULL ELSE version END`

// atlasVersionNumberExpression renders the numeric form of the version column.
//
// The CASE arm turns dot-prefixed metadata rows into NULL before the cast, and
// it is the ONLY protection for the three statements that have no
// [atlasMetadataRowPredicate] filter of their own: getVersionSQL (MAX over
// every row), countRevisionsAboveSQL, and deleteRevisionsAboveSQL. Without it,
// a strict dialect fails those statements outright — PostgreSQL reports
// `invalid input syntax for type bigint: ".atlas_cloud_identifier"` — so
// GetCurrentVersion and SetAtlasRevision would error on any database Atlas
// has run `migrate down` against. SQLite's lenient CAST silently yields 0
// instead, which is why no SQLite-only test can observe the difference and the
// guard is pinned by asserting the generated SQL (#957).
func (m *Migrator) atlasVersionNumberExpression() string {
	// connectionDialect keeps this usable on a zero-value Migrator, so the
	// generated-SQL guard tests can assert every dialect branch without a live
	// database.
	return atlasVersionNumberExpressionFor(m.connectionDialect())
}

// atlasVersionNumberExpressionFor matches the raw dialect string rather than
// routing through platform.NormalizeDialect. That is safe because
// dbschema.ConnectToDatabase normalizes the dialect before the connection
// exists and rejects anything NormalizeDialect does not recognize, so
// conn.Info().Dialect is always one of the platform constants — and
// platform.MySQL and platform.MariaDB are exactly the two strings matched here.
// A non-canonical spelling therefore cannot reach this switch and silently take
// the BIGINT branch. A zero-value Migrator (dialect "") takes the default
// branch by design, which is what the generated-SQL guard tests rely on.
func atlasVersionNumberExpressionFor(dialect string) string {
	switch dialect {
	case "mysql", "mariadb":
		return "CAST(" + atlasMetadataVersionNullGuard + " AS SIGNED)"
	default:
		return "CAST(" + atlasMetadataVersionNullGuard + " AS BIGINT)"
	}
}

func (m *Migrator) createAtlasRevisionsTableSQL() string {
	if m.isSQLServer() {
		return fmt.Sprintf(`IF OBJECT_ID(%s, N'U') IS NULL
BEGIN
    CREATE TABLE %s (
        version NVARCHAR(255) PRIMARY KEY,
        description NVARCHAR(MAX) NOT NULL,
        type BIGINT NOT NULL DEFAULT 2,
        applied BIGINT NOT NULL DEFAULT 0,
        total BIGINT NOT NULL DEFAULT 0,
        executed_at DATETIME2 NOT NULL,
        execution_time BIGINT NOT NULL,
        error NVARCHAR(MAX) NULL,
        error_stmt NVARCHAR(MAX) NULL,
        hash NVARCHAR(255) NOT NULL,
        partial_hashes NVARCHAR(MAX) NULL,
        operator_version NVARCHAR(255) NOT NULL
    )
END`, sqlStringLiteral(m.sqlServerObjectName()), m.qualifiedMigrationsTable())
	}
	switch platform.NormalizeDialect(m.conn.Info().Dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMPTZ NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR NOT NULL,
    partial_hashes JSONB NULL,
    operator_version VARCHAR NOT NULL
)`, m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version VARCHAR(255) PRIMARY KEY,
    description TEXT NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR(255) NOT NULL,
    partial_hashes JSON NULL,
    operator_version VARCHAR(255) NOT NULL
)`, m.qualifiedMigrationsTable())
}

func (m *Migrator) isClickHouse() bool {
	return m.conn != nil && m.conn.Info().Dialect == "clickhouse"
}

func (m *Migrator) forceAppliedConflictClause() string {
	if m.conn == nil {
		return ""
	}
	switch m.conn.Info().Dialect {
	case "postgres", "cockroachdb", "yugabytedb":
		if m.revisionTableFormat.isAtlas() {
			return `ON CONFLICT (version) DO UPDATE SET
description = EXCLUDED.description,
type = EXCLUDED.type,
applied = EXCLUDED.applied,
total = EXCLUDED.total,
executed_at = EXCLUDED.executed_at,
execution_time = EXCLUDED.execution_time,
error = '',
error_stmt = '',
hash = EXCLUDED.hash,
partial_hashes = EXCLUDED.partial_hashes,
operator_version = EXCLUDED.operator_version`
		}
		return `ON CONFLICT (version) DO UPDATE SET
description = EXCLUDED.description,
applied_at = EXCLUDED.applied_at,
state = EXCLUDED.state,
applied = EXCLUDED.applied,
total = EXCLUDED.total,
error = NULL,
error_stmt = NULL,
execution_time_ms = EXCLUDED.execution_time_ms,
checksum = EXCLUDED.checksum`
	case "mysql", "mariadb":
		if m.revisionTableFormat.isAtlas() {
			return `ON DUPLICATE KEY UPDATE
description = VALUES(description),
type = VALUES(type),
applied = VALUES(applied),
total = VALUES(total),
executed_at = VALUES(executed_at),
execution_time = VALUES(execution_time),
error = '',
error_stmt = '',
hash = VALUES(hash),
partial_hashes = VALUES(partial_hashes),
operator_version = VALUES(operator_version)`
		}
		return `ON DUPLICATE KEY UPDATE
description = VALUES(description),
applied_at = VALUES(applied_at),
state = VALUES(state),
applied = VALUES(applied),
total = VALUES(total),
error = NULL,
error_stmt = NULL,
execution_time_ms = VALUES(execution_time_ms),
checksum = VALUES(checksum)`
	default:
		return ""
	}
}

func (m *Migrator) dirtyRevision(ctx context.Context) (*MigrationRevision, error) {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.getDirtyRevisionSQL())
	row := m.conn.QueryRowContext(ctx, query, migrationStateApplied)
	if m.revisionTableFormat.isAtlas() {
		row = m.conn.QueryRowContext(ctx, query)
	}
	revision, err := m.scanRevisionRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	revision.Dirty = true
	return &revision, nil
}

func (m *Migrator) getRevision(ctx context.Context, version int64) (*MigrationRevision, error) {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.getRevisionSQL())
	revision, err := m.scanRevisionRow(m.conn.QueryRowContext(ctx, query, m.revisionVersionArg(version)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	revision.Dirty = revision.State != migrationStateApplied
	return &revision, nil
}

func (m *Migrator) scanRevisionRow(row rowScanner) (MigrationRevision, error) {
	if m.revisionTableFormat.isAtlas() {
		return m.scanAtlasRevisionRow(row)
	}
	var revision MigrationRevision
	var executionTimeMs int64
	var appliedAt any
	if err := row.Scan(
		&revision.Version,
		&revision.Description,
		&revision.State,
		&revision.Applied,
		&revision.Total,
		&revision.Error,
		&revision.ErrorStatement,
		&executionTimeMs,
		&revision.Checksum,
		&appliedAt,
	); err != nil {
		return MigrationRevision{}, err
	}
	parsedAppliedAt, err := parseRevisionAppliedAt(appliedAt)
	if err != nil {
		return MigrationRevision{}, err
	}
	revision.AppliedAt = parsedAppliedAt
	revision.ExecutionTime = time.Duration(executionTimeMs) * time.Millisecond
	revision.Dirty = revision.State != migrationStateApplied
	return revision, nil
}

func (m *Migrator) scanAtlasRevisionRow(row rowScanner) (MigrationRevision, error) {
	var revision MigrationRevision
	var version string
	var executionTime int64
	var executedAt any
	if err := row.Scan(
		&version,
		&revision.Description,
		&revision.AtlasType,
		&revision.Applied,
		&revision.Total,
		&revision.Error,
		&revision.ErrorStatement,
		&executionTime,
		&revision.Checksum,
		&executedAt,
		&revision.OperatorVersion,
	); err != nil {
		return MigrationRevision{}, err
	}
	parsedVersion, err := parseAtlasRevisionVersion(version)
	if err != nil {
		return MigrationRevision{}, err
	}
	parsedExecutedAt, err := parseRevisionAppliedAt(executedAt)
	if err != nil {
		return MigrationRevision{}, err
	}
	revision.Version = parsedVersion
	revision.State = atlasRevisionState(revision)
	revision.AppliedAt = parsedExecutedAt
	revision.ExecutionTime = time.Duration(executionTime)
	revision.Dirty = revision.State != migrationStateApplied
	return revision, nil
}

func atlasRevisionState(revision MigrationRevision) string {
	if revision.Error != "" || revision.Applied != revision.Total {
		return migrationStateFailed
	}
	return migrationStateApplied
}

func parseRevisionAppliedAt(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return parseRevisionAppliedAtString(string(v))
	case string:
		return parseRevisionAppliedAtString(v)
	case nil:
		return time.Time{}, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported applied_at value %T", value)
	}
}

func parseRevisionAppliedAtString(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse applied_at %q", value)
}

func (m *Migrator) failIfDirty(ctx context.Context) error {
	if !m.metadataAvailable || m.legacyRevisionTable {
		return nil
	}
	revision, err := m.dirtyRevision(ctx)
	if err != nil {
		return err
	}
	if revision != nil {
		return &DirtyMigrationError{Revision: *revision}
	}
	return nil
}

func (m *Migrator) beginMigrationRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
) error {
	return m.beginMigrationRevisionOn(ctx, m.conn, migration, startedAt)
}

func (m *Migrator) beginMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	if m.revisionTableFormat.isAtlas() {
		return m.beginAtlasMigrationRevisionOn(ctx, conn, migration, startedAt)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.beginMigrationSQL())
	return executeSQLOn(
		ctx,
		conn,
		query,
		migration.Version,
		migration.Description,
		startedAt,
		migrationStatePending,
		0,
		m.migrationStatementCount(migration.UpSQL),
		nil,
		nil,
		0,
		migrationRevisionHash(migration),
	)
}

func (m *Migrator) beginAtlasMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.beginMigrationSQL())
	return executeSQLOn(
		ctx,
		conn,
		query,
		strconv.FormatInt(migration.Version, 10),
		migration.atlasFilenameDescription(),
		int64(AtlasRevisionTypeApplied),
		0,
		m.migrationStatementCount(migration.UpSQL),
		m.atlasRevisionTimestamp(startedAt),
		int64(0),
		"",
		"",
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		ptahOperatorVersion,
	)
}

func (m *Migrator) completeMigrationRevision(ctx context.Context, migration *Migration, startedAt time.Time) error {
	return m.completeMigrationRevisionOn(ctx, m.conn, migration, startedAt)
}

func (m *Migrator) completeMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	if m.revisionTableFormat.isAtlas() {
		return m.completeAtlasMigrationRevisionOn(ctx, conn, migration, startedAt)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.completeMigrationSQL())
	return executeSQLOn(
		ctx,
		conn,
		query,
		migrationStateApplied,
		m.migrationStatementCount(migration.UpSQL),
		m.migrationStatementCount(migration.UpSQL),
		time.Since(startedAt).Milliseconds(),
		time.Now(),
		migration.Version,
	)
}

func (m *Migrator) completeAtlasMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.completeMigrationSQL())
	total := m.migrationStatementCount(migration.UpSQL)
	return executeSQLOn(
		ctx,
		conn,
		query,
		total,
		total,
		time.Since(startedAt).Nanoseconds(),
		m.atlasNullJSONValue(),
		ptahOperatorVersion,
		strconv.FormatInt(migration.Version, 10),
	)
}

func (m *Migrator) checkpointMigrationRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.checkpointMigrationSQL())
	if m.revisionTableFormat.isAtlas() {
		return executeSQLOutsideTransaction(
			recordCtx,
			m.conn,
			query,
			event.Index,
			event.Total,
			time.Since(startedAt).Nanoseconds(),
			ptahOperatorVersion,
			strconv.FormatInt(migration.Version, 10),
		)
	}
	return executeSQLOutsideTransaction(
		recordCtx,
		m.conn,
		query,
		migrationStatePending,
		event.Index,
		event.Total,
		time.Since(startedAt).Milliseconds(),
		migration.Version,
	)
}

func (m *Migrator) markMigrationStatementInFlight(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.failMigrationSQL())
	if m.revisionTableFormat.isAtlas() {
		return executeSQLOutsideTransaction(
			recordCtx,
			m.conn,
			query,
			event.Index-1,
			event.Total,
			time.Since(startedAt).Nanoseconds(),
			unknownStatementOutcomeError,
			event.Statement,
			ptahOperatorVersion,
			strconv.FormatInt(migration.Version, 10),
		)
	}
	return executeSQLOutsideTransaction(
		recordCtx,
		m.conn,
		query,
		migrationStatePending,
		event.Index-1,
		event.Total,
		unknownStatementOutcomeError,
		event.Statement,
		time.Since(startedAt).Milliseconds(),
		migration.Version,
	)
}

func (m *Migrator) beginRollbackRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	if m.revisionTableFormat.isAtlas() {
		return m.beginAtlasRollbackRevision(ctx, migration, startedAt)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.beginRollbackSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migrationStatePending,
		0,
		m.migrationStatementCount(migration.DownSQL),
		0,
		migration.Version,
	)
}

// beginAtlasRollbackRevision rewrites the revision row in place (applied=0,
// executed_at=now, error cleared) before the down body runs; a failed down then
// records the error into the same row via failAtlasMigrationRevision.
//
// This runs only on the native surface. The Atlas-shaped surface skips it
// entirely — see [Migrator.reproducesAtlasDownBookkeeping] for the measured
// Atlas semantics it reproduces instead, and for why recording the failure is
// the better behavior everywhere Atlas fidelity is not the requirement (#957).
func (m *Migrator) beginAtlasRollbackRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.beginRollbackSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		0,
		m.migrationStatementCount(migration.DownSQL),
		m.atlasRevisionTimestamp(startedAt),
		int64(0),
		m.atlasNullJSONValue(),
		ptahOperatorVersion,
		strconv.FormatInt(migration.Version, 10),
	)
}

func (m *Migrator) failMigrationRevisionWithMode(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	txMode MigrationTxMode,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	applied, total, stmt := migrationExecutionProgress(failure, m.conn.Info().Dialect, txMode)
	if total == 0 {
		total = m.migrationStatementCount(sqlText)
	}
	if m.revisionTableFormat.isAtlas() {
		return m.failAtlasMigrationRevision(recordCtx, migration, startedAt, failure, applied, total, stmt)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.failMigrationSQL())
	return executeSQLOutsideTransaction(
		recordCtx,
		m.conn,
		query,
		migrationStateFailed,
		applied,
		total,
		strings.TrimSpace(failure.Error()),
		stmt,
		time.Since(startedAt).Milliseconds(),
		migration.Version,
	)
}

func durableRevisionWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), revisionWriteTimeout)
}

func (m *Migrator) failAtlasMigrationRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	applied int,
	total int,
	stmt string,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.failMigrationSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		applied,
		total,
		time.Since(startedAt).Nanoseconds(),
		strings.TrimSpace(failure.Error()),
		stmt,
		ptahOperatorVersion,
		strconv.FormatInt(migration.Version, 10),
	)
}

func (m *Migrator) verifyAppliedMigrationChecksums(ctx context.Context, migrations []*Migration) error {
	if !m.metadataAvailable || m.legacyRevisionTable {
		return nil
	}
	for _, migration := range migrations {
		revision, err := m.getRevision(ctx, migration.Version)
		if err != nil {
			return err
		}
		if revision == nil || revision.State != migrationStateApplied || revision.Checksum == "" {
			continue
		}
		stored := normalizeAtlasRevisionHash(revision.Checksum)
		if !revisionChecksumMatches(stored, migration) {
			return &ChecksumMismatchError{
				Version:  migration.Version,
				Stored:   stored,
				Computed: migrationRevisionHash(migration),
			}
		}
	}
	return nil
}

// Baseline records provider migrations as already applied without executing
// their SQL bodies. Ptah metadata records each migration up to version; Atlas
// metadata records the exact baseline revision.
func (m *Migrator) Baseline(ctx context.Context, version int64) error {
	return m.BaselineWithOptions(ctx, BaselineOptions{Version: version})
}

// BaselineWithOptions records provider migrations as already applied without
// executing their SQL bodies.
func (m *Migrator) BaselineWithOptions(ctx context.Context, opts BaselineOptions) error {
	return m.withMigrationLock(ctx, "baseline", func(ctx context.Context) error {
		return m.baselineLocked(ctx, opts)
	})
}

func (m *Migrator) baselineLocked(ctx context.Context, opts BaselineOptions) error {
	if opts.Version <= 0 {
		return fmt.Errorf("baseline version must be greater than zero")
	}
	migrations, err := m.migrationsForBaseline(opts.Version)
	if err != nil {
		return err
	}
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}
	revisionCount, err := m.revisionCount(ctx)
	if err != nil {
		return err
	}
	if revisionCount > 0 && !opts.Force {
		return fmt.Errorf("schema migrations table is not empty; rerun with force to baseline anyway")
	}
	if opts.Force {
		if err := m.failIfRevisionAboveBaseline(ctx, opts.Version); err != nil {
			return err
		}
	}
	return m.baselineMigrations(ctx, migrations)
}

func (m *Migrator) migrationsForBaseline(version int64) ([]*Migration, error) {
	if m.revisionTableFormat.isAtlas() {
		migration := m.migrationByVersion(version)
		if migration == nil {
			return nil, fmt.Errorf("baseline version %q not found", strconv.FormatInt(version, 10))
		}
		return []*Migration{migration}, nil
	}

	migrations := m.migrationsAtOrBelow(version)
	if len(migrations) == 0 {
		return nil, fmt.Errorf("no migrations found at or below baseline version %d", version)
	}
	return migrations, nil
}

func (m *Migrator) migrationsAtOrBelow(version int64) []*Migration {
	migrations := m.migrationProvider.Migrations()
	out := make([]*Migration, 0, len(migrations))
	for _, migration := range migrations {
		if migration.Version <= version {
			out = append(out, migration)
		}
	}
	return out
}

func (m *Migrator) revisionCount(ctx context.Context) (int, error) {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.countRevisionsSQL())
	var count int
	if err := m.conn.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count migration revisions: %w", err)
	}
	return count, nil
}

func (m *Migrator) baselineMigrations(ctx context.Context, migrations []*Migration) error {
	if m.isClickHouse() {
		return m.baselineMigrationsNoTransaction(ctx, migrations)
	}
	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin baseline transaction: %w", err)
	}
	txConn := m.conn.WithExecutor(tx)
	if err := m.writeBaselineMigrations(ctx, txConn, migrations); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit baseline transaction: %w", err)
	}
	return nil
}

func (m *Migrator) baselineMigrationsNoTransaction(ctx context.Context, migrations []*Migration) error {
	return m.writeBaselineMigrations(ctx, m.conn, migrations)
}

func (m *Migrator) writeBaselineMigrations(ctx context.Context, conn *dbschema.DatabaseConnection, migrations []*Migration) error {
	return m.writeBaselineMigrationRows(ctx, conn, migrations)
}

func (m *Migrator) failIfRevisionAboveBaseline(ctx context.Context, version int64) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.countRevisionsAboveSQL())
	var count int
	if err := m.conn.QueryRowContext(ctx, query, version).Scan(&count); err != nil {
		return fmt.Errorf("failed to inspect migration revisions above baseline: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("schema migrations table contains revisions above baseline version %d; refusing to rewrite migration history", version)
	}
	return nil
}

func (m *Migrator) writeBaselineMigrationRows(ctx context.Context, conn *dbschema.DatabaseConnection, migrations []*Migration) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.forceAppliedMigrationSQL())
	for _, migration := range migrations {
		if m.forceAppliedConflictClause() == "" {
			deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())
			if err := conn.Writer().ExecuteSQL(ctx, deleteSQL, m.revisionVersionArg(migration.Version)); err != nil {
				return fmt.Errorf("failed to prepare baseline revision %d: %w", migration.Version, err)
			}
		}
		if m.revisionTableFormat.isAtlas() {
			if err := m.writeAtlasBaselineMigrationRow(ctx, conn, query, migration); err != nil {
				return err
			}
			continue
		}
		if err := conn.Writer().ExecuteSQL(
			ctx,
			query,
			migration.Version,
			migration.Description,
			time.Now(),
			migrationStateApplied,
			m.migrationStatementCount(migration.UpSQL),
			m.migrationStatementCount(migration.UpSQL),
			0,
			migrationRevisionHash(migration),
		); err != nil {
			return fmt.Errorf("failed to record baseline revision %d: %w", migration.Version, err)
		}
	}
	return nil
}

func (m *Migrator) writeAtlasBaselineMigrationRow(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	query string,
	migration *Migration,
) error {
	if err := conn.Writer().ExecuteSQL(
		ctx,
		query,
		strconv.FormatInt(migration.Version, 10),
		migration.atlasFilenameDescription(),
		int64(AtlasRevisionTypeBaseline),
		0,
		0,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		"",
		m.atlasNullJSONValue(),
		ptahOperatorVersion,
	); err != nil {
		return fmt.Errorf("failed to record baseline revision %d: %w", migration.Version, err)
	}
	return nil
}

// SetAtlasRevision moves Atlas revision history to version without executing
// migration SQL. Existing clean rows at or below version are preserved, missing
// rows are recorded as manually set, dirty rows are recorded as applied and
// manually set while retaining their diagnostics, and rows above version are
// removed. It requires the Atlas revision table format; [Migrator.SetRevision]
// is the format-agnostic entry point.
func (m *Migrator) SetAtlasRevision(ctx context.Context, version int64) (AtlasRevisionSetResult, error) {
	if !m.revisionTableFormat.isAtlas() {
		return AtlasRevisionSetResult{}, fmt.Errorf("setting an Atlas revision requires Atlas revision table format")
	}
	return m.SetRevision(ctx, version)
}

// SetRevision moves revision history to version without executing migration
// SQL, in either revision table format. Existing clean rows at or below
// version are preserved, missing rows are recorded as applied (manually set in
// the Atlas layout), dirty rows are marked applied, and rows above version are
// removed.
func (m *Migrator) SetRevision(ctx context.Context, version int64) (AtlasRevisionSetResult, error) {
	// The Atlas-format operation keeps its historical name so lock-timeout
	// diagnostics from the Atlas-compatible surface stay byte-identical.
	operation := "set revision"
	if m.revisionTableFormat.isAtlas() {
		operation = "set Atlas revision"
	}
	var result AtlasRevisionSetResult
	err := m.withMigrationLock(ctx, operation, func(ctx context.Context) error {
		var err error
		result, err = m.setRevisionLocked(ctx, version)
		return err
	})
	return result, err
}

func (m *Migrator) setRevisionLocked(ctx context.Context, version int64) (AtlasRevisionSetResult, error) {
	if version <= 0 {
		return AtlasRevisionSetResult{}, fmt.Errorf("migration version must be greater than zero")
	}
	if m.migrationByVersion(version) == nil {
		return AtlasRevisionSetResult{}, fmt.Errorf("migration with version %q not found", strconv.FormatInt(version, 10))
	}
	if m.isClickHouse() {
		if m.revisionTableFormat.isAtlas() {
			return AtlasRevisionSetResult{}, fmt.Errorf(
				"setting an Atlas revision is not supported for ClickHouse because revision history cannot be updated atomically",
			)
		}
		return AtlasRevisionSetResult{}, fmt.Errorf(
			"setting the migration revision is not supported for ClickHouse because revision history cannot be updated atomically",
		)
	}
	if err := m.Initialize(ctx); err != nil {
		return AtlasRevisionSetResult{}, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if m.conn.Writer().IsDryRun() {
		return AtlasRevisionSetResult{CurrentVersion: version}, nil
	}

	return m.setAtlasRevisionRows(ctx, m.migrationsAtOrBelow(version), version)
}

func (m *Migrator) setAtlasRevisionRows(
	ctx context.Context,
	migrations []*Migration,
	version int64,
) (AtlasRevisionSetResult, error) {
	var result AtlasRevisionSetResult
	var err error
	for attempt := range atlasSetMaxAttempts {
		result, err = m.setAtlasRevisionRowsOnce(ctx, migrations, version)
		if err == nil || !atlasretry.IsRetryable(err) || attempt == atlasSetMaxAttempts-1 {
			return result, err
		}
		if err := waitForAtlasSetRetry(ctx, attempt); err != nil {
			return AtlasRevisionSetResult{}, err
		}
	}
	return result, err
}

func (m *Migrator) setAtlasRevisionRowsOnce(
	ctx context.Context,
	migrations []*Migration,
	version int64,
) (AtlasRevisionSetResult, error) {
	session, err := m.conn.Conn(ctx)
	if err != nil {
		return AtlasRevisionSetResult{}, fmt.Errorf("failed to reserve Atlas revision set connection: %w", err)
	}
	defer func() {
		_ = session.Close()
	}()

	tx, err := session.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return AtlasRevisionSetResult{}, fmt.Errorf("failed to begin Atlas revision set transaction: %w", err)
	}
	existing, err := queryMigrationRowsFrom(
		ctx,
		tx,
		m.getRevisionsForUpdateSQL(),
		m.scanRevisionRow,
		"failed to query migration revisions",
		"failed to scan migration revision",
		"error iterating migration revisions",
	)
	if err != nil {
		_ = tx.Rollback()
		return AtlasRevisionSetResult{}, err
	}
	result := atlasRevisionSetChanges(existing, migrations, version)
	writeRows := m.writePtahSetRevisionRows
	if m.revisionTableFormat.isAtlas() {
		writeRows = m.writeAtlasSetRevisionRows
	}
	if err := writeRows(ctx, tx, existing, migrations, version); err != nil {
		_ = tx.Rollback()
		return AtlasRevisionSetResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AtlasRevisionSetResult{}, fmt.Errorf("failed to commit Atlas revision set transaction: %w", err)
	}
	return result, nil
}

func waitForAtlasSetRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(attempt+1) * 10 * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("Atlas revision set retry canceled: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func atlasRevisionSetChanges(
	existing []MigrationRevision,
	migrations []*Migration,
	version int64,
) AtlasRevisionSetResult {
	result := AtlasRevisionSetResult{CurrentVersion: version}
	revisionsByVersion := make(map[int64]MigrationRevision, len(existing))
	for _, revision := range existing {
		revisionsByVersion[revision.Version] = revision
		if revision.Version > version {
			result.Removed = append(result.Removed, AtlasRevisionChange{
				Version:     revision.Version,
				Description: revision.Description,
			})
		}
	}
	for _, migration := range migrations {
		revision, exists := revisionsByVersion[migration.Version]
		if exists && revision.State == migrationStateApplied {
			continue
		}
		result.Set = append(result.Set, AtlasRevisionChange{
			Version:     migration.Version,
			Description: migration.atlasFilenameDescription(),
		})
	}
	return result
}

func (m *Migrator) writeAtlasSetRevisionRows(
	ctx context.Context,
	tx *sql.Tx,
	existing []MigrationRevision,
	migrations []*Migration,
	version int64,
) error {
	deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteRevisionsAboveSQL())
	if _, err := tx.ExecContext(ctx, deleteSQL, strconv.FormatInt(version, 10)); err != nil {
		return fmt.Errorf("failed to remove Atlas revisions above %d: %w", version, err)
	}

	revisionsByVersion := make(map[int64]MigrationRevision, len(existing))
	for _, revision := range existing {
		revisionsByVersion[revision.Version] = revision
	}

	insertSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.insertAtlasRevisionSQL())
	updateTypeSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.updateAtlasRevisionTypeSQL())
	for _, migration := range migrations {
		revision, exists := revisionsByVersion[migration.Version]
		if exists && revision.State == migrationStateApplied {
			continue
		}
		if exists {
			revisionType := AtlasRevisionTypeApplied | AtlasRevisionTypeManuallySet
			if _, err := tx.ExecContext(
				ctx,
				updateTypeSQL,
				int64(revisionType),
				strconv.FormatInt(revision.Version, 10),
			); err != nil {
				return fmt.Errorf("failed to update dirty Atlas revision %d type: %w", revision.Version, err)
			}
			continue
		}
		if err := m.writeAtlasManuallySetMigrationRow(ctx, tx, insertSQL, migration); err != nil {
			return err
		}
	}
	return nil
}

// writePtahSetRevisionRows is the ptah-layout counterpart of
// writeAtlasSetRevisionRows: rows above version are removed, dirty rows at or
// below version are marked applied, and missing rows are inserted as applied.
func (m *Migrator) writePtahSetRevisionRows(
	ctx context.Context,
	tx *sql.Tx,
	existing []MigrationRevision,
	migrations []*Migration,
	version int64,
) error {
	deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteRevisionsAboveSQL())
	if _, err := tx.ExecContext(ctx, deleteSQL, m.revisionVersionArg(version)); err != nil {
		return fmt.Errorf("failed to remove revisions above %d: %w", version, err)
	}

	revisionsByVersion := make(map[int64]MigrationRevision, len(existing))
	for _, revision := range existing {
		revisionsByVersion[revision.Version] = revision
	}

	insertSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.beginMigrationSQL())
	updateSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.setPtahRevisionAppliedSQL())
	for _, migration := range migrations {
		revision, exists := revisionsByVersion[migration.Version]
		if exists && revision.State == migrationStateApplied {
			continue
		}
		if exists {
			if _, err := tx.ExecContext(
				ctx,
				updateSQL,
				migrationStateApplied,
				revision.Version,
			); err != nil {
				return fmt.Errorf("failed to mark dirty revision %d applied: %w", revision.Version, err)
			}
			continue
		}
		total := m.migrationStatementCount(migration.UpSQL)
		if _, err := tx.ExecContext(
			ctx,
			insertSQL,
			migration.Version,
			migration.Description,
			time.Now(),
			migrationStateApplied,
			total,
			total,
			nil,
			nil,
			0,
			migrationRevisionHash(migration),
		); err != nil {
			return fmt.Errorf("failed to record manually set revision %d: %w", migration.Version, err)
		}
	}
	return nil
}

// setPtahRevisionAppliedSQL marks one ptah-layout revision row applied while
// keeping its recorded error diagnostics, mirroring the Atlas layout's
// applied|manually-set type update.
func (m *Migrator) setPtahRevisionAppliedSQL() string {
	return fmt.Sprintf(`UPDATE %s SET state = ?, applied = total WHERE version = ?`, m.qualifiedMigrationsTable())
}

// RepairMigration clears dirty migration metadata after an operator has fixed
// the database manually, or resumes the up migration from a specific statement.
func (m *Migrator) RepairMigration(ctx context.Context, opts RepairMigrationOptions) error {
	if opts.Version <= 0 {
		return fmt.Errorf("repair version must be greater than zero")
	}
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	migration := m.migrationByVersion(opts.Version)
	if migration == nil {
		return fmt.Errorf("migration %d not found", opts.Version)
	}
	revision, err := m.getRevision(ctx, opts.Version)
	if err != nil {
		return err
	}
	if revision == nil && !opts.Force {
		return fmt.Errorf("migration %d has no revision row; rerun with --force to mark it applied", opts.Version)
	}
	if revision != nil && !revision.Dirty && !opts.Force {
		return fmt.Errorf("migration %d is not dirty; rerun with --force to rewrite it", opts.Version)
	}
	if opts.ResumeFrom > 0 {
		if revision != nil && revision.Error == unknownStatementOutcomeError {
			return fmt.Errorf(
				"migration %d has an unknown statement outcome for %q; inspect the database before repair and omit --resume-from to avoid repeating committed SQL",
				migration.Version,
				revision.ErrorStatement,
			)
		}
		if err := m.resumeMigration(ctx, migration, opts.ResumeFrom); err != nil {
			return err
		}
	}
	return m.forceAppliedMigration(ctx, migration)
}

func (m *Migrator) migrationByVersion(version int64) *Migration {
	for _, migration := range m.migrationProvider.Migrations() {
		if migration.Version == version {
			return migration
		}
	}
	return nil
}

func (m *Migrator) resumeMigration(ctx context.Context, migration *Migration, resumeFrom int) error {
	statements := splitSQLStatementsForConnection(m.conn, migration.UpSQL)
	if resumeFrom < 1 || resumeFrom > len(statements) {
		return fmt.Errorf("resume-from must be between 1 and %d", len(statements))
	}
	for i := resumeFrom - 1; i < len(statements); i++ {
		stmt := strings.TrimSpace(statements[i])
		if stmt == "" {
			continue
		}
		if err := executeSQLOutsideTransaction(ctx, m.conn, stmt); err != nil {
			return fmt.Errorf("failed to resume migration %d at statement %d: %w", migration.Version, i+1, err)
		}
	}
	return nil
}

func (m *Migrator) forceAppliedMigration(ctx context.Context, migration *Migration) error {
	if m.isClickHouse() {
		revision, err := m.getRevision(ctx, migration.Version)
		if err != nil {
			return err
		}
		if revision != nil {
			return m.forceAppliedMigrationClickHouse(ctx, migration)
		}
	}
	if m.forceAppliedConflictClause() == "" {
		deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())
		if err := executeSQLOutsideTransaction(ctx, m.conn, deleteSQL, m.revisionVersionArg(migration.Version)); err != nil {
			return err
		}
	}
	if m.revisionTableFormat.isAtlas() {
		return m.forceAppliedAtlasMigration(ctx, migration)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.forceAppliedMigrationSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migration.Version,
		migration.Description,
		time.Now(),
		migrationStateApplied,
		m.migrationStatementCount(migration.UpSQL),
		m.migrationStatementCount(migration.UpSQL),
		0,
		migrationRevisionHash(migration),
	)
}

func (m *Migrator) forceAppliedAtlasMigration(ctx context.Context, migration *Migration) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.forceAppliedMigrationSQL())
	total := m.migrationStatementCount(migration.UpSQL)
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		strconv.FormatInt(migration.Version, 10),
		migration.atlasFilenameDescription(),
		int64(AtlasRevisionTypeApplied),
		total,
		total,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		ptahOperatorVersion,
	)
}

func (m *Migrator) writeAtlasManuallySetMigrationRow(
	ctx context.Context,
	tx *sql.Tx,
	query string,
	migration *Migration,
) error {
	if _, err := tx.ExecContext(
		ctx,
		query,
		strconv.FormatInt(migration.Version, 10),
		migration.atlasFilenameDescription(),
		int64(AtlasRevisionTypeManuallySet),
		0,
		0,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		ptahOperatorVersion,
	); err != nil {
		return fmt.Errorf("failed to record manually set Atlas revision %d: %w", migration.Version, err)
	}
	return nil
}

func (m *Migrator) atlasRevisionTimestamp(at time.Time) any {
	now := at.UTC().Round(0)
	if m.conn != nil && platform.NormalizeDialect(m.conn.Info().Dialect) == platform.SQLite {
		return now.Format(time.RFC3339Nano)
	}
	return now
}

func (m *Migrator) atlasNullJSONValue() any {
	if m.isSQLServer() || m.isClickHouse() {
		return atlasNullJSON
	}
	return []byte(atlasNullJSON)
}

func (m *Migrator) forceAppliedMigrationClickHouse(ctx context.Context, migration *Migration) error {
	if m.revisionTableFormat.isAtlas() {
		return m.forceAppliedAtlasMigrationClickHouse(ctx, migration)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.forceAppliedPtahUpdateSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migration.Description,
		time.Now(),
		migrationStateApplied,
		m.migrationStatementCount(migration.UpSQL),
		m.migrationStatementCount(migration.UpSQL),
		0,
		migrationRevisionHash(migration),
		migration.Version,
	)
}

func (m *Migrator) forceAppliedAtlasMigrationClickHouse(ctx context.Context, migration *Migration) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.forceAppliedAtlasUpdateSQL())
	total := m.migrationStatementCount(migration.UpSQL)
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migration.atlasFilenameDescription(),
		int64(AtlasRevisionTypeApplied),
		total,
		total,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		ptahOperatorVersion,
		strconv.FormatInt(migration.Version, 10),
	)
}

func (m *Migrator) revisionVersionArg(version int64) any {
	if m.revisionTableFormat.isAtlas() {
		return strconv.FormatInt(version, 10)
	}
	return version
}
