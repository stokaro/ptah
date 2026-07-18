package migrator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/sqlutil"
)

const (
	migrationStateApplied = "applied"
	migrationStatePending = "pending"
	migrationStateFailed  = "failed"
)

// MigrationRevision records one row from the migration metadata table.
type MigrationRevision struct {
	Version         int64         `json:"version"`
	Description     string        `json:"description"`
	State           string        `json:"state"`
	Applied         int           `json:"applied"`
	Total           int           `json:"total"`
	Error           string        `json:"error,omitempty"`
	ErrorStatement  string        `json:"error_stmt,omitempty"`
	ExecutionTime   time.Duration `json:"execution_time"`
	Checksum        string        `json:"checksum,omitempty"`
	AppliedAt       time.Time     `json:"applied_at"`
	Dirty           bool          `json:"dirty"`
	ChecksumCurrent string        `json:"checksum_current,omitempty"`
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

func migrationStatementCount(sqlText string) int {
	return len(SplitSQLStatements(sqlText))
}

func migrationExecutionProgress(err error, dialect string) (applied int, total int, stmt string) {
	var execErr *MigrationExecutionError
	if !errors.As(err, &execErr) {
		return 0, 0, ""
	}

	total = execErr.Total
	applied = execErr.StatementIndex - 1
	if dialect == "postgres" || dialect == "cockroachdb" || dialect == "yugabytedb" {
		applied = 0
	}
	if applied < 0 {
		applied = 0
	}
	return applied, total, execErr.Statement
}

func (m *Migrator) getDirtyRevisionSQL() string {
	return fmt.Sprintf(`SELECT version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at
FROM %s
WHERE state <> ?
ORDER BY version
LIMIT 1`, m.qualifiedMigrationsTable())
}

func (m *Migrator) getRevisionSQL() string {
	return fmt.Sprintf(`SELECT version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at
FROM %s
WHERE version = ?`, m.qualifiedMigrationsTable())
}

func (m *Migrator) beginMigrationSQL() string {
	return fmt.Sprintf(`INSERT INTO %s (version, description, applied_at, state, applied, total, error, error_stmt, execution_time_ms, checksum)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, m.qualifiedMigrationsTable())
}

func (m *Migrator) completeMigrationSQL() string {
	if m.isClickHouse() {
		return fmt.Sprintf(`ALTER TABLE %s
UPDATE state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, applied_at = ?
WHERE version = ?
SETTINGS mutations_sync = 1`, m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`UPDATE %s
SET state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, applied_at = ?
WHERE version = ?`, m.qualifiedMigrationsTable())
}

func (m *Migrator) beginRollbackSQL() string {
	if m.isClickHouse() {
		return fmt.Sprintf(`ALTER TABLE %s
UPDATE state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?
WHERE version = ?
SETTINGS mutations_sync = 1`, m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`UPDATE %s
SET state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?
WHERE version = ?`, m.qualifiedMigrationsTable())
}

func (m *Migrator) failMigrationSQL() string {
	if m.isClickHouse() {
		return fmt.Sprintf(`ALTER TABLE %s
UPDATE state = ?, applied = ?, total = ?, error = ?, error_stmt = ?, execution_time_ms = ?
WHERE version = ?
SETTINGS mutations_sync = 1`, m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`UPDATE %s
SET state = ?, applied = ?, total = ?, error = ?, error_stmt = ?, execution_time_ms = ?
WHERE version = ?`, m.qualifiedMigrationsTable())
}

func (m *Migrator) forceAppliedMigrationSQL() string {
	return fmt.Sprintf(`INSERT INTO %s (version, description, applied_at, state, applied, total, error, error_stmt, execution_time_ms, checksum)
VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
%s`, m.qualifiedMigrationsTable(), m.forceAppliedConflictClause())
}

func (m *Migrator) forceAppliedUpdateSQL() string {
	return fmt.Sprintf(`ALTER TABLE %s
UPDATE description = ?, applied_at = ?, state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, checksum = ?
WHERE version = ?
SETTINGS mutations_sync = 1`, m.qualifiedMigrationsTable())
}

func (m *Migrator) countRevisionsSQL() string {
	return fmt.Sprintf(`SELECT COUNT(*) FROM %s`, m.qualifiedMigrationsTable())
}

func (m *Migrator) countRevisionsAboveSQL() string {
	return fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE version > ?`, m.qualifiedMigrationsTable())
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
	revision, err := m.scanRevisionRow(m.conn.QueryRowContext(ctx, query, migrationStateApplied))
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
	revision, err := m.scanRevisionRow(m.conn.QueryRowContext(ctx, query, version))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	revision.Dirty = revision.State != migrationStateApplied
	return &revision, nil
}

func (m *Migrator) scanRevisionRow(row *sql.Row) (MigrationRevision, error) {
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
	return revision, nil
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
	if m.conn.Writer().IsDryRun() {
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

func (m *Migrator) beginMigrationRevision(ctx context.Context, migration *Migration) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.beginMigrationSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migration.Version,
		migration.Description,
		time.Now(),
		migrationStatePending,
		0,
		migrationStatementCount(migration.UpSQL),
		nil,
		nil,
		0,
		migrationChecksum(migration.UpSQL),
	)
}

func (m *Migrator) completeMigrationRevision(ctx context.Context, migration *Migration, startedAt time.Time) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.completeMigrationSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migrationStateApplied,
		migrationStatementCount(migration.UpSQL),
		migrationStatementCount(migration.UpSQL),
		time.Since(startedAt).Milliseconds(),
		time.Now(),
		migration.Version,
	)
}

func (m *Migrator) beginRollbackRevision(ctx context.Context, migration *Migration) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.beginRollbackSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migrationStatePending,
		0,
		migrationStatementCount(migration.DownSQL),
		0,
		migration.Version,
	)
}

func (m *Migrator) failMigrationRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	applied, total, stmt := migrationExecutionProgress(failure, m.conn.Info().Dialect)
	if total == 0 {
		total = migrationStatementCount(sqlText)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.failMigrationSQL())
	return executeSQLOutsideTransaction(
		ctx,
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

func (m *Migrator) verifyAppliedMigrationChecksums(ctx context.Context, migrations []*Migration) error {
	if m.conn.Writer().IsDryRun() {
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
		checksum := migrationChecksum(migration.UpSQL)
		if revision.Checksum != checksum {
			return &ChecksumMismatchError{
				Version:  migration.Version,
				Stored:   revision.Checksum,
				Computed: checksum,
			}
		}
	}
	return nil
}

// Baseline records provider migrations up to version as already applied without
// executing their SQL bodies.
func (m *Migrator) Baseline(ctx context.Context, version int64) error {
	return m.BaselineWithOptions(ctx, BaselineOptions{Version: version})
}

// BaselineWithOptions records provider migrations up to opts.Version as already
// applied without executing their SQL bodies.
func (m *Migrator) BaselineWithOptions(ctx context.Context, opts BaselineOptions) error {
	return m.withMigrationLock(ctx, "baseline", func(ctx context.Context) error {
		return m.baselineLocked(ctx, opts)
	})
}

func (m *Migrator) baselineLocked(ctx context.Context, opts BaselineOptions) error {
	if opts.Version <= 0 {
		return fmt.Errorf("baseline version must be greater than zero")
	}
	migrations := m.migrationsAtOrBelow(opts.Version)
	if len(migrations) == 0 {
		return fmt.Errorf("no migrations found at or below baseline version %d", opts.Version)
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
	if err := m.conn.Writer().BeginTransaction(); err != nil {
		return fmt.Errorf("failed to begin baseline transaction: %w", err)
	}
	if err := m.writeBaselineMigrations(ctx, migrations); err != nil {
		_ = m.conn.Writer().RollbackTransaction()
		return err
	}
	if err := m.conn.Writer().CommitTransaction(); err != nil {
		return fmt.Errorf("failed to commit baseline transaction: %w", err)
	}
	return nil
}

func (m *Migrator) baselineMigrationsNoTransaction(ctx context.Context, migrations []*Migration) error {
	return m.writeBaselineMigrations(ctx, migrations)
}

func (m *Migrator) writeBaselineMigrations(ctx context.Context, migrations []*Migration) error {
	return m.writeBaselineMigrationRows(ctx, migrations)
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

func (m *Migrator) writeBaselineMigrationRows(ctx context.Context, migrations []*Migration) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.forceAppliedMigrationSQL())
	for _, migration := range migrations {
		if m.forceAppliedConflictClause() == "" {
			deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())
			if err := m.conn.Writer().ExecuteSQL(ctx, deleteSQL, migration.Version); err != nil {
				return fmt.Errorf("failed to prepare baseline revision %d: %w", migration.Version, err)
			}
		}
		if err := m.conn.Writer().ExecuteSQL(
			ctx,
			query,
			migration.Version,
			migration.Description,
			time.Now(),
			migrationStateApplied,
			migrationStatementCount(migration.UpSQL),
			migrationStatementCount(migration.UpSQL),
			0,
			migrationChecksum(migration.UpSQL),
		); err != nil {
			return fmt.Errorf("failed to record baseline revision %d: %w", migration.Version, err)
		}
	}
	return nil
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
	statements := SplitSQLStatements(migration.UpSQL)
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
		if err := executeSQLOutsideTransaction(ctx, m.conn, deleteSQL, migration.Version); err != nil {
			return err
		}
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
		migrationStatementCount(migration.UpSQL),
		migrationStatementCount(migration.UpSQL),
		0,
		migrationChecksum(migration.UpSQL),
	)
}

func (m *Migrator) forceAppliedMigrationClickHouse(ctx context.Context, migration *Migration) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.forceAppliedUpdateSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		migration.Description,
		time.Now(),
		migrationStateApplied,
		migrationStatementCount(migration.UpSQL),
		migrationStatementCount(migration.UpSQL),
		0,
		migrationChecksum(migration.UpSQL),
		migration.Version,
	)
}
