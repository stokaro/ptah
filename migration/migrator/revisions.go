package migrator

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasretry"
	"go.5x5.cz/ptah/internal/revisiontable"
)

const (
	migrationStateApplied        = "applied"
	migrationStatePending        = "pending"
	migrationStateFailed         = "failed"
	ptahOperatorVersion          = revisiontable.PtahOperatorVersion
	ptahDownOperatorVersion      = "Ptah/down"
	atlasNullJSON                = "null"
	atlasSetMaxAttempts          = 3
	revisionWriteTimeout         = 10 * time.Second
	atlasIdentityCollationBlock  = 200
	unknownStatementOutcomeError = "statement execution outcome is unknown after process interruption"
)

// MigrationRevision records one row from the migration metadata table.
type MigrationRevision struct {
	Version      int64  `json:"version"`
	AtlasVersion string `json:"atlas_version,omitempty"`
	Description  string `json:"description"`
	State        string `json:"state"`
	// Direction is the direction that recorded this row. A row left behind by
	// a rollback that stopped halfway reads MigrationDirectionDown, which is
	// what routes `migrations repair --resume-from` to the down body instead of
	// the up body. Rows written before Ptah recorded the direction read
	// MigrationDirectionUp. Atlas-layout rows written by Ptah carry the down
	// marker in operator_version because that schema has no direction column.
	Direction      MigrationDirection `json:"direction,omitempty"`
	AtlasType      AtlasRevisionType  `json:"atlas_type,omitempty"`
	Applied        int                `json:"applied"`
	Total          int                `json:"total"`
	Error          string             `json:"error,omitempty"`
	ErrorStatement string             `json:"error_stmt,omitempty"`
	ExecutionTime  time.Duration      `json:"execution_time"`
	// Checksum is the canonical full migration checksum on a clean native row.
	// A dirty native row with committed progress uses a partial:h1: cumulative
	// source-prefix digest so a resume can prove which statements it may skip.
	// Atlas-format rows keep the Atlas hash here and use partial_hashes internally.
	Checksum        string    `json:"checksum,omitempty"`
	AppliedAt       time.Time `json:"applied_at"`
	OperatorVersion string    `json:"operator_version,omitempty"`
	Dirty           bool      `json:"dirty"`
	ChecksumCurrent string    `json:"checksum_current,omitempty"`
	hasAtlasVersion bool
	partialHashes   []string
}

// MarshalJSON preserves the presence of an exact empty Atlas revision
// identity while keeping the atlas_version member absent for ordinary numeric
// revisions. The standard omitempty behavior cannot distinguish those states.
func (r MigrationRevision) MarshalJSON() ([]byte, error) {
	type revisionJSON MigrationRevision
	if !r.hasAtlasVersion && r.AtlasVersion == "" {
		return json.Marshal(revisionJSON(r))
	}
	return json.Marshal(struct {
		revisionJSON
		AtlasVersion string `json:"atlas_version"`
	}{
		revisionJSON: revisionJSON(r),
		AtlasVersion: r.AtlasVersion,
	})
}

// UnmarshalJSON restores the presence of an Atlas revision identity, including
// the empty identity used by one Flyway repeatable migration.
func (r *MigrationRevision) UnmarshalJSON(data []byte) error {
	type revisionJSON MigrationRevision
	decoded := struct {
		revisionJSON
		AtlasVersion *string `json:"atlas_version"`
	}{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*r = MigrationRevision(decoded.revisionJSON)
	if decoded.AtlasVersion != nil {
		r.AtlasVersion = *decoded.AtlasVersion
		r.hasAtlasVersion = true
	}
	return nil
}

// RevisionVersion returns the exact revision-table version token when the
// revision came from an Atlas-format table, and the decimal numeric version
// otherwise.
func (r MigrationRevision) RevisionVersion() string {
	if r.hasAtlasVersion || r.AtlasVersion != "" {
		return r.AtlasVersion
	}
	return strconv.FormatInt(r.Version, 10)
}

// DirtyMigrationError reports that a previous migration run left a dirty row.
type DirtyMigrationError struct {
	Revision MigrationRevision
}

func (e *DirtyMigrationError) Error() string {
	version := e.Revision.RevisionVersion()
	if e.Revision.hasAtlasVersion && version == "" {
		version = `""`
	}
	return fmt.Sprintf(
		"migration %s is dirty: state=%s applied=%d/%d error=%q error_stmt=%q",
		version,
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
// the up SQL instead, so that legacy encoding is accepted too. The Atlas value
// is a cumulative chain identity rather than a per-file digest, but both
// candidates depend on the current migration bytes and reject an edit.
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

// migrationProgress is what a failed execution reports about how far it got.
//
// FailedIndex is deliberately separate from Applied: Applied is zeroed whenever
// the transaction rolled the body back, and the statement that failed is still
// the one it was. Indexing the migration source by Applied would name the
// file's first statement for every tx-mode-all failure.
type migrationProgress struct {
	Applied     int
	Total       int
	Statement   string
	FailedIndex int
}

func migrationExecutionProgress(
	err error,
	dialect string,
	txMode MigrationTxMode,
) migrationProgress {
	var progressErr *statementProgressError
	if errors.As(err, &progressErr) {
		event := progressErr.event
		return migrationProgress{
			Applied: progressErr.applied, Total: event.Total,
			Statement: event.Statement, FailedIndex: event.Index,
		}
	}

	var observationErr *StatementObservationError
	if errors.As(err, &observationErr) {
		event := observationErr.Event
		// The observed statement executed and only the observation that follows
		// it failed, so no statement of the body is itself the failure: pass 0
		// as the failing index rather than the statement that succeeded.
		applied := rolledBackApplied(dialect, txMode, event.Index)
		return migrationProgress{
			Applied: applied, Total: event.Total,
			Statement: event.Statement, FailedIndex: event.Index,
		}
	}

	var execErr *MigrationExecutionError
	if !errors.As(err, &execErr) {
		return migrationProgress{}
	}

	applied := max(execErr.StatementIndex-1, 0)
	// The recorded counter is not decoration: a retry resumes at applied+1, so
	// overstating it skips a statement that never ran. Whether the earlier
	// statements survived the failure is a property of the transaction mode, the
	// dialect and — on the MySQL family — the statements themselves, which is
	// what rolledBackApplied answers. This branch used to carry its own shorter,
	// unnormalized list that omitted SQLite and SQL Server and so recorded
	// applied=1 for a file whose transaction had rolled the whole body back
	// (#966).
	applied = rolledBackApplied(dialect, txMode, applied)
	return migrationProgress{
		Applied: applied, Total: execErr.Total,
		Statement: execErr.Statement, FailedIndex: execErr.StatementIndex,
	}
}

// rolledBackApplied corrects how many statements a failed migration body left
// committed, given that executed of them ran before it stopped.
//
// A body that did not run inside a transaction keeps its own count. A body that
// did reports zero from this error-only fallback because SQL text cannot prove
// what a stateful database session committed. MySQL and MariaDB replace this
// fallback with the revision-row witness installed by
// [Migrator.withTransactionalProgressRecorder].
func rolledBackApplied(dialect string, txMode MigrationTxMode, executed int) int {
	if !migrationProgressRolledBack(dialect, txMode) {
		return executed
	}
	return 0
}

// migrationProgressRolledBack reports whether the migration body ran inside a
// transaction that the failure rolled back. It says nothing about which
// statements survived that rollback — on the MySQL family, where the server
// commits DDL on its own, some of them do. See [rolledBackApplied].
func migrationProgressRolledBack(dialect string, txMode MigrationTxMode) bool {
	if txMode == MigrationTxModeAll {
		return true
	}
	if txMode != MigrationTxModeFile {
		return false
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.SQLite, platform.SQLServer,
		platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

func (m *Migrator) getDirtyRevisionSQL() string {
	if m.revisionTableFormat.isAtlas() {
		if m.isSQLServer() {
			return fmt.Sprintf(`SELECT TOP (1) %s
FROM %s
				WHERE %s AND %s
				ORDER BY %s, version`, m.atlasRevisionProjection(), m.qualifiedMigrationsTable(), atlasDirtyRevisionPredicate, m.atlasRevisionRowPredicate(), m.atlasVersionNumberExpression())
		}
		return fmt.Sprintf(`SELECT %s
FROM %s
WHERE %s AND %s
ORDER BY %s
LIMIT 1`, m.atlasRevisionProjection(), m.qualifiedMigrationsTable(), atlasDirtyRevisionPredicate, m.atlasRevisionRowPredicate(), m.atlasVersionNumberExpression()+", version")
	}
	if m.isSQLServer() {
		return fmt.Sprintf(`SELECT TOP (1) version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at
FROM %s
WHERE state <> ? OR applied <> total OR %s
ORDER BY version`, m.qualifiedMigrationsTable(), revisionProgressInvalidPredicate)
	}
	return fmt.Sprintf(`SELECT version, description, state, applied, total, COALESCE(error, ''), COALESCE(error_stmt, ''), execution_time_ms, checksum, applied_at
FROM %s
WHERE state <> ? OR applied <> total OR %s
ORDER BY version
LIMIT 1`, m.qualifiedMigrationsTable(), revisionProgressInvalidPredicate)
}

func (m *Migrator) getRevisionSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(`SELECT %s
FROM %s
			WHERE version = ?`, m.atlasRevisionProjection(), m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`SELECT %s
FROM %s
WHERE version = ?`, m.ptahRevisionProjection(), m.qualifiedMigrationsTable())
}

func (m *Migrator) getAppliedRevisionsSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(
			`SELECT %s
FROM %s
WHERE %s AND %s
ORDER BY %s, version`,
			m.atlasRevisionProjection(),
			m.qualifiedMigrationsTable(),
			atlasAppliedRevisionPredicate,
			m.atlasRevisionRowPredicate(),
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
			`SELECT %s
FROM %s
WHERE %s
ORDER BY %s, version`,
			m.atlasRevisionProjection(),
			m.qualifiedMigrationsTable(),
			m.atlasRevisionRowPredicate(),
			m.atlasVersionNumberExpression(),
		)
	}
	return fmt.Sprintf(`SELECT %s
FROM %s
ORDER BY version`, m.ptahRevisionProjection(), m.qualifiedMigrationsTable())
}

func (m *Migrator) atlasRevisionProjection() string {
	return "version, description, type, applied, total, COALESCE(error, ''), " +
		"COALESCE(error_stmt, ''), execution_time, hash, executed_at, partial_hashes, " +
		"COALESCE(operator_version, '')"
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
				`SELECT %s
FROM %s WITH (UPDLOCK, HOLDLOCK)
WHERE %s
ORDER BY %s, version`,
				m.atlasRevisionProjection(),
				m.qualifiedMigrationsTable(),
				m.atlasRevisionRowPredicate(),
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

// restartMigrationSQL rewrites an existing revision row so a retry of the same
// version reuses it instead of inserting a second one.
//
// beginMigrationSQL is a bare INSERT, and the up path used to run it
// unconditionally. Once a failed body had recorded a dirty row, every retry —
// including the one the operator asked for with --allow-dirty after fixing the
// migration — died on `UNIQUE constraint failed` on the version column instead
// of running, with `migrations repair` the only way out (#966). This is the
// up-direction counterpart of beginRollbackSQL, which has always rewritten the
// row in place for the down direction.
func (m *Migrator) restartMigrationSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return revisionUpdateSQL(
			m.connectionDialect(),
			m.qualifiedMigrationsTable(),
			"description = ?, type = ?, applied = ?, total = ?, executed_at = ?, execution_time = ?, error = '', error_stmt = '', hash = ?, partial_hashes = ?, operator_version = ?",
		)
	}
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"description = ?, applied_at = ?, state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, checksum = ?",
	)
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
		"state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, applied_at = ?, checksum = ?",
	)
}

func (m *Migrator) checkpointMigrationSQL() string {
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
		"state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, checksum = ?",
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
		"state = ?, applied = ?, total = ?, error = NULL, error_stmt = NULL, execution_time_ms = ?, checksum = ?",
	)
}

func (m *Migrator) failMigrationSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return revisionUpdateSQL(
			m.connectionDialect(),
			m.qualifiedMigrationsTable(),
			"applied = ?, total = ?, execution_time = ?, error = ?, error_stmt = ?, partial_hashes = ?, operator_version = ?",
		)
	}
	return revisionUpdateSQL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		"state = ?, applied = ?, total = ?, error = ?, error_stmt = ?, execution_time_ms = ?, checksum = ?",
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
		return fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, m.qualifiedMigrationsTable(), m.atlasRevisionRowPredicate())
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

func (m *Migrator) deleteAtlasSetRevisionsAboveSQL(retired, exactRemoved []string) string {
	versionExpression := m.atlasVersionNumberExpression()
	if len(retired) > 0 {
		literals := make([]string, len(retired))
		for index, revision := range retired {
			literals[index] = atlasRevisionStringLiteral(m.connectionDialect(), revision)
		}
		versionExpression = fmt.Sprintf(
			"CASE WHEN version IN (%s) THEN NULL ELSE %s END",
			strings.Join(literals, ", "),
			versionExpression,
		)
	}
	predicate := versionExpression + " > ? AND version <> ?"
	if len(exactRemoved) > 0 {
		literals := make([]string, len(exactRemoved))
		for index, revision := range exactRemoved {
			literals[index] = atlasRevisionStringLiteral(m.connectionDialect(), revision)
		}
		predicate = fmt.Sprintf("(%s) OR version IN (%s)", predicate, strings.Join(literals, ", "))
	}
	return fmt.Sprintf(
		`DELETE FROM %s WHERE %s`,
		m.qualifiedMigrationsTable(),
		predicate,
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

const atlasCloudIdentifierVersion = ".atlas_cloud_identifier"

// atlasRevisionRowPredicate treats persisted dot-prefixed identities as
// migrations while an exact-identity adapter is active, except for Atlas's
// measured cloud-identifier bookkeeping row. The provider map can no longer
// name a migration after its source file rotates out of the directory, but its
// revision row remains complete history. Flyway accepts arbitrary opaque
// tokens, and the pinned CE binary records V.foo as `.foo`; a blanket prefix
// test would make that applied row disappear and re-run its body forever.
func (m *Migrator) atlasRevisionRowPredicate() string {
	if !m.hasAtlasRevisionVersionMap() {
		return atlasMetadataRowPredicate
	}
	return atlasExactIdentityRowPredicateFor(m.connectionDialect())
}

func atlasExactIdentityRowPredicateFor(dialect string) string {
	return "version <> " + atlasRevisionStringLiteral(dialect, atlasCloudIdentifierVersion)
}

const (
	revisionProgressInvalidPredicate = "(applied < 0 OR total < 0 OR applied > total)"
	atlasDownRevisionPredicate       = "COALESCE(operator_version, '') = '" + ptahDownOperatorVersion + "'"
	atlasDirtyRevisionPredicate      = "(" + revisionProgressInvalidPredicate + " OR applied <> total OR COALESCE(error, '') <> '' OR " + atlasDownRevisionPredicate + ")"
	atlasAppliedRevisionPredicate    = "NOT " + revisionProgressInvalidPredicate + " AND applied = total AND COALESCE(error, '') = '' AND NOT (" + atlasDownRevisionPredicate + ")"
)

// atlasMetadataVersionNullGuard maps Atlas metadata and repeatable version
// tokens to NULL so the remaining numeric tokens can be cast safely. See
// [Migrator.atlasVersionNumberExpression] for why this, and not the predicate
// above, is what protects the statements that select over every revision row.
const atlasMetadataVersionNullGuard = `CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END`

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
	fallback := atlasVersionNumberExpressionFor(m.connectionDialect())
	if m.hasAtlasRevisionVersionMap() {
		fallback = atlasRetiredVersionNumberExpressionFor(m.connectionDialect())
	}
	mappings := m.atlasRevisionVersionMappings()
	if len(mappings) == 0 {
		return fallback
	}
	var expression strings.Builder
	expression.WriteString("CASE version")
	for _, mapping := range mappings {
		fmt.Fprintf(&expression, " WHEN %s THEN %d", atlasRevisionStringLiteral(m.connectionDialect(), mapping.revision), mapping.runtime)
	}
	fmt.Fprintf(&expression, " ELSE %s END", fallback)
	return expression.String()
}

func atlasRetiredVersionNumberExpressionFor(dialect string) string {
	// An exact-identity adapter can encounter a recorded token after its source
	// file rotates out of the current directory. Its conversion-time numeric
	// ordering key is no longer reconstructable, so every unmapped row uses the
	// history-only zero runtime. The typed cast keeps the expression valid in
	// SQLite ORDER BY clauses, where a bare 0 is interpreted as a column ordinal.
	return strings.Replace(atlasVersionNumberExpressionFor(dialect), atlasMetadataVersionNullGuard, "0", 1)
}

// validateAtlasRevisionIdentityCollation refuses exact source or recorded
// tokens that the configured revision column would collapse. The zero-row
// SELECT makes the derived version column inherit the real table's type and
// collation, so this also protects existing Atlas tables created under a
// case-insensitive database default. It runs after metadata creation but before
// migration SQL.
func (m *Migrator) validateAtlasRevisionIdentityCollation(ctx context.Context) error {
	mappings := m.atlasRevisionVersionMappings()
	if len(mappings) == 0 {
		return nil
	}
	identities := make(map[string]struct{}, len(mappings))
	for _, mapping := range mappings {
		identities[mapping.revision] = struct{}{}
	}
	rows, err := m.conn.QueryContext(ctx, fmt.Sprintf("SELECT version FROM %s", m.qualifiedMigrationsTable()))
	if err != nil {
		return fmt.Errorf("failed to read recorded Atlas revision identities: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var identity string
		if err := rows.Scan(&identity); err != nil {
			return fmt.Errorf("failed to scan recorded Atlas revision identity: %w", err)
		}
		identities[identity] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate recorded Atlas revision identities: %w", err)
	}
	versions := slices.Sorted(maps.Keys(identities))
	if len(versions) < 2 {
		return nil
	}
	for leftStart := 0; leftStart < len(versions); leftStart += atlasIdentityCollationBlock {
		leftEnd := min(leftStart+atlasIdentityCollationBlock, len(versions))
		for rightStart := leftStart; rightStart < len(versions); rightStart += atlasIdentityCollationBlock {
			rightEnd := min(rightStart+atlasIdentityCollationBlock, len(versions))
			candidates := slices.Clone(versions[leftStart:leftEnd])
			if rightStart != leftStart {
				candidates = append(candidates, versions[rightStart:rightEnd]...)
			}
			distinct, err := m.atlasDistinctRevisionIdentityCount(ctx, candidates)
			if err != nil {
				return err
			}
			if distinct != len(candidates) {
				return fmt.Errorf(
					"revision table cannot distinguish every exact Atlas identity under its configured version collation: %d identities collapse to %d",
					len(candidates),
					distinct,
				)
			}
		}
	}
	return nil
}

// atlasDistinctRevisionIdentityCount keeps each compound SELECT below
// SQLite's default 500-term limit. The caller checks every pair of disjoint
// blocks, so aliases are still detected when their exact spellings fall in
// different blocks rather than only within one independently counted batch.
func (m *Migrator) atlasDistinctRevisionIdentityCount(ctx context.Context, identities []string) (int, error) {
	var query strings.Builder
	fmt.Fprintf(
		&query,
		"SELECT COUNT(DISTINCT version) FROM (SELECT version FROM %s WHERE 1 = 0",
		m.qualifiedMigrationsTable(),
	)
	for _, version := range identities {
		fmt.Fprintf(&query, " UNION ALL SELECT %s", atlasRevisionStringLiteral(m.connectionDialect(), version))
	}
	query.WriteString(") AS ptah_revision_identities")
	var distinct int
	if err := m.conn.QueryRowContext(ctx, query.String()).Scan(&distinct); err != nil {
		return 0, fmt.Errorf("failed to verify exact Atlas revision identities: %w", err)
	}
	return distinct, nil
}

func atlasRevisionStringLiteral(dialect, value string) string {
	return revisiontable.VersionLiteral(dialect, value)
}

type atlasRevisionVersionMapping struct {
	revision string
	runtime  int64
}

func (m *Migrator) atlasRevisionVersionMappings() []atlasRevisionVersionMapping {
	if m.migrationProvider == nil {
		return nil
	}
	if source, ok := m.migrationProvider.(interface {
		atlasRevisionVersionMap() map[int64]string
	}); ok {
		versions := source.atlasRevisionVersionMap()
		// A surviving baseline and a migration it squashed can share one exact
		// source token. One SQL CASE arm must represent that token, and history
		// must retain the higher pre-baseline runtime as its high-water mark.
		runtimeByRevision := make(map[string]int64, len(versions))
		for runtime, revision := range versions {
			if current, exists := runtimeByRevision[revision]; exists && current >= runtime {
				continue
			}
			runtimeByRevision[revision] = runtime
		}
		// The option is intentionally partial: a migration whose order key is
		// absent keeps the revision identity parsed from its Atlas file name.
		// Include those loaded migrations in the CASE so only rows with no
		// current or historical owner take the retired-history fallback.
		for _, migration := range m.migrationProvider.Migrations() {
			revision := migration.RevisionVersion()
			if current, exists := runtimeByRevision[revision]; exists && current >= migration.Version {
				continue
			}
			runtimeByRevision[revision] = migration.Version
		}
		mappings := make([]atlasRevisionVersionMapping, 0, len(runtimeByRevision))
		for revision, runtime := range runtimeByRevision {
			mappings = append(mappings, atlasRevisionVersionMapping{
				revision: revision,
				runtime:  runtime,
			})
		}
		slices.SortFunc(mappings, compareAtlasRevisionVersionMappings)
		return mappings
	}
	mappings := make([]atlasRevisionVersionMapping, 0)
	for _, migration := range m.migrationProvider.Migrations() {
		if migration.hasAtlasRevisionVersion {
			mappings = append(mappings, atlasRevisionVersionMapping{
				revision: migration.RevisionVersion(),
				runtime:  migration.Version,
			})
		}
	}
	return mappings
}

func compareAtlasRevisionVersionMappings(a, b atlasRevisionVersionMapping) int {
	if order := cmp.Compare(a.runtime, b.runtime); order != 0 {
		return order
	}
	return cmp.Compare(a.revision, b.revision)
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
	// connectionDialect keeps this usable on a zero-value Migrator, so the
	// generated-SQL guard tests can assert every dialect branch without a live
	// database. The method used to dereference m.conn.Info() directly, which
	// panicked before any assertion could run.
	return atlasRevisionsTableDDL(
		m.connectionDialect(),
		m.qualifiedMigrationsTable(),
		sqlStringLiteral(m.sqlServerObjectName()),
	)
}

// atlasRevisionsTableDDL renders the Atlas revision table for one dialect.
//
// partial_hashes is the column that forces a per-dialect decision. Atlas stores
// a JSON document there, and Ptah writes the JSON null; the dialects that have a
// real JSON type get one, and the dialects that do not store the same value as
// text. SQL Server has always taken the text side (NVARCHAR(MAX)), and
// atlasNullJSONValue binds a plain string rather than a []byte JSON document for
// both SQL Server and ClickHouse, so the value side of ClickHouse was already
// text before this function had a ClickHouse branch.
//
// ClickHouse must not reach the trailing default. It reads a trailing NULL on a
// column definition as Nullable(T), so `partial_hashes JSON NULL` is asked for as
// Nullable(JSON). ClickHouse 24.x rejects that outright during type analysis --
// code 43, "Nested type JSON cannot be inside Nullable type" -- before the
// IF NOT EXISTS existence check, so no already-provisioned database escapes it.
// Later servers accept Nullable(JSON) and the failure changes shape instead of
// disappearing: the 4-character string `null` is coerced into the JSON type and
// reads back as `{}`, and the column can no longer be scanned into a string by
// any Atlas-compatible consumer. TEXT NULL renders as Nullable(String) on both,
// which stores exactly the value every other dialect stores (#950).
//
// The dialect is normalized here rather than matched raw. dbschema.ConnectToDatabase
// normalizes before the connection exists, so conn.Info().Dialect is already one
// of the platform constants; normalizing is a no-op in production and lets the
// guard tests drive this function with a bare dialect string. A zero-value
// Migrator (dialect "") takes the default branch by design.
func atlasRevisionsTableDDL(dialect, qualifiedTable, sqlServerObjectLiteral string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLServer:
		return fmt.Sprintf(`IF OBJECT_ID(%s, N'U') IS NULL
BEGIN
    CREATE TABLE %s (
        version NVARCHAR(255) COLLATE Latin1_General_100_BIN2 PRIMARY KEY,
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
END`, sqlServerObjectLiteral, qualifiedTable)
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
)`, qualifiedTable)
	case platform.ClickHouse:
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
    partial_hashes TEXT NULL,
    operator_version VARCHAR(255) NOT NULL
)`, qualifiedTable)
	}
	engineClause := ""
	versionDefinition := "VARCHAR(255)"
	if implicitCommitDialect(dialect) {
		engineClause = " ENGINE=InnoDB"
		versionDefinition = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version %s PRIMARY KEY,
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
)%s`, qualifiedTable, versionDefinition, engineClause)
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

// dirtyRevision reads the lowest revision row that is not cleanly applied.
//
// Only the ptah layout's predicate takes an argument; the Atlas layout compares
// applied against total and needs none. This used to issue the ptah query first
// and then overwrite the result for the Atlas layout, which left a *sql.Row that
// nothing ever scanned — and an unscanned Row keeps its connection, and its open
// read cursor, checked out for the life of the process. On SQLite that read lock
// blocks the next write against the same file, so a dirty-guard refusal poisoned
// every later write in the same process with `database is locked (SQLITE_BUSY)`:
// exactly the recovery run this guard exists to send the operator towards
// (#966).
func (m *Migrator) dirtyRevision(ctx context.Context) (*MigrationRevision, error) {
	var revision *MigrationRevision
	err := m.withMigrationMetadataSession(ctx, func(scoped *Migrator) error {
		query := sqlutil.Rebind(scoped.conn.Info().Dialect, scoped.getDirtyRevisionSQL())
		args := []any{migrationStateApplied}
		if scoped.revisionTableFormat.isAtlas() {
			args = nil
		}
		scanned, err := scoped.scanRevisionRow(scoped.conn.QueryRowContext(ctx, query, args...))
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		scanned.Dirty = true
		revision = &scanned
		return nil
	})
	return revision, err
}

func (m *Migrator) getMigrationRevision(ctx context.Context, migration *Migration) (*MigrationRevision, error) {
	return m.getRevisionByVersionArg(ctx, m.migrationRevisionVersionArg(migration))
}

func (m *Migrator) getRevisionByVersionArg(ctx context.Context, version any) (*MigrationRevision, error) {
	var revision *MigrationRevision
	err := m.withMigrationMetadataSession(ctx, func(scoped *Migrator) error {
		query := sqlutil.Rebind(scoped.conn.Info().Dialect, scoped.getRevisionSQL())
		scanned, err := scoped.scanRevisionRow(
			scoped.conn.QueryRowContext(ctx, query, version),
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		scanned.Dirty = scanned.State != migrationStateApplied
		revision = &scanned
		return nil
	})
	return revision, err
}

func (m *Migrator) scanRevisionRow(row rowScanner) (MigrationRevision, error) {
	if m.revisionTableFormat.isAtlas() {
		return m.scanAtlasRevisionRow(row)
	}
	var revision MigrationRevision
	var executionTimeMs int64
	var appliedAt any
	var storedState string
	if err := row.Scan(
		&revision.Version,
		&revision.Description,
		&storedState,
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
	if err := validateRevisionProgress(revision, "read revision metadata"); err != nil {
		return MigrationRevision{}, err
	}
	revision.State, revision.Direction = decodeRevisionState(storedState)
	if err := validateNativeRevisionState(revision, storedState, "read revision metadata"); err != nil {
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
	var partialHashes any
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
		&partialHashes,
		&revision.OperatorVersion,
	); err != nil {
		return MigrationRevision{}, err
	}
	revision.AtlasVersion = version
	revision.hasAtlasVersion = true
	parsedVersion, err := m.atlasRuntimeVersion(version)
	if err != nil {
		return MigrationRevision{}, err
	}
	revision.Version = parsedVersion
	if err := validateRevisionProgress(revision, "read revision metadata"); err != nil {
		return MigrationRevision{}, err
	}
	parsedExecutedAt, err := parseRevisionAppliedAt(executedAt)
	if err != nil {
		return MigrationRevision{}, err
	}
	revision.State = atlasRevisionState(revision)
	revision.Direction = atlasRevisionDirection(revision.OperatorVersion)
	revision.AppliedAt = parsedExecutedAt
	revision.ExecutionTime = time.Duration(executionTime)
	revision.Dirty = revision.State != migrationStateApplied
	// Legacy ClickHouse revisions can contain {} because old Atlas-compatible
	// DDL coerced JSON null into an empty object. Clean rows never resume, so
	// their partial_hashes are irrelevant and must not make status/list fail.
	// Dirty rows with committed progress do resume and are decoded fail-closed.
	if revision.Dirty && revision.Applied > 0 {
		parsedPartialHashes, err := parseAtlasPartialHashes(partialHashes)
		if err != nil {
			return MigrationRevision{}, fmt.Errorf("failed to decode Atlas revision %s partial_hashes: %w", version, err)
		}
		revision.partialHashes = parsedPartialHashes
	}
	return revision, nil
}

func (m *Migrator) atlasRuntimeVersion(revision string) (int64, error) {
	for _, mapping := range m.atlasRevisionVersionMappings() {
		if mapping.revision == revision {
			return mapping.runtime, nil
		}
	}
	if m.hasAtlasRevisionVersionMap() {
		// The exact source token remains complete applied-history identity after
		// its file is removed, but its conversion-time numeric tie slot is not
		// recoverable from that token alone. Keep it at the history-only zero
		// runtime; pending ownership and source linearity use the exact key.
		return 0, nil
	}
	return parseAtlasRevisionVersion(revision)
}

func (m *Migrator) hasAtlasRevisionVersionMap() bool {
	if m.migrationProvider == nil {
		return false
	}
	source, ok := m.migrationProvider.(interface {
		hasAtlasRevisionVersionMap() bool
	})
	return ok && source.hasAtlasRevisionVersionMap()
}

func atlasRevisionDirection(operatorVersion string) MigrationDirection {
	if operatorVersion == ptahDownOperatorVersion {
		return MigrationDirectionDown
	}
	return MigrationDirectionUp
}

func atlasRevisionState(revision MigrationRevision) string {
	if revision.Error != "" || revision.Applied != revision.Total {
		return migrationStateFailed
	}
	// A completed up migration remains represented by its revision row, but a
	// completed down migration does not. Keep Ptah-owned rollback rows dirty
	// until the revision is deleted so a failed metadata write cannot make a
	// reverted migration look applied again.
	if revision.OperatorVersion == ptahDownOperatorVersion {
		return migrationStatePending
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

// failIfUnownedDirtyRevision limits AllowDirty to retrying a migration whose
// source body is still present. Exact Atlas history remains readable after its
// source file is removed, but there is no body or committed-prefix contract to
// resume once that history row is dirty.
func (m *Migrator) failIfUnownedDirtyRevision(
	revisions []MigrationRevision,
	migrations []*Migration,
) error {
	if !m.hasAtlasRevisionVersionMap() {
		return nil
	}
	owned := make(map[string]struct{}, len(migrations))
	for _, migration := range migrations {
		owned[m.migrationDirtyOwnershipKey(migration)] = struct{}{}
	}
	for _, revision := range revisions {
		if !revision.Dirty {
			continue
		}
		if _, ok := owned[m.revisionDirtyOwnershipKey(revision)]; !ok {
			return &DirtyMigrationError{Revision: revision}
		}
	}
	return nil
}

func (m *Migrator) migrationDirtyOwnershipKey(migration *Migration) string {
	if m.revisionTableFormat.isAtlas() {
		return migration.RevisionVersion()
	}
	return strconv.FormatInt(migration.Version, 10)
}

func (m *Migrator) revisionDirtyOwnershipKey(revision MigrationRevision) string {
	if m.revisionTableFormat.isAtlas() {
		return revision.RevisionVersion()
	}
	return strconv.FormatInt(revision.Version, 10)
}

func isZeroProgressUpFailure(revision MigrationRevision) bool {
	return revision.Direction == MigrationDirectionUp &&
		revision.Applied == 0 &&
		revision.Total > 0 &&
		revision.Error != unknownStatementOutcomeError
}

func (m *Migrator) discardRolledBackFailure(ctx context.Context, failure error) error {
	version, confirmed := migrationTransactionRollbackVersion(failure)
	if !m.revisionTableFormat.isAtlas() || !confirmed {
		return nil
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	migration := m.migrationByVersion(version)
	if migration == nil {
		return fmt.Errorf("inspect rolled-back migration failure %d: migration is not registered", version)
	}
	revision, err := m.getMigrationRevision(recordCtx, migration)
	if err != nil {
		return fmt.Errorf("inspect rolled-back migration failure %d: %w", version, err)
	}
	if revision == nil || !revision.Dirty || !isZeroProgressUpFailure(*revision) {
		return nil
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())
	if err := executeSQLOutsideTransaction(
		recordCtx,
		m.conn,
		query,
		m.migrationRevisionVersionArg(migration),
	); err != nil {
		return fmt.Errorf("discard rolled-back migration failure %d: %w", version, err)
	}
	return nil
}

// upRetryPlan says how an up attempt relates to an earlier attempt at the same
// version. The zero value is not usable: build it with [Migrator.planUpRetry],
// which always sets resumeFrom to at least 1.
type upRetryPlan struct {
	// reuseRevision says a dirty revision row for this version already exists,
	// so the attempt must rewrite that row instead of inserting a second one.
	reuseRevision bool
	// resumeFrom is the 1-based index of the first statement this attempt runs.
	// Everything below it was committed by the earlier attempt.
	resumeFrom int
}

// planUpRetry reads the revision row for migration, if any, and decides whether
// this attempt reuses it and where in the body it restarts.
//
// The read has to happen here — before the caller opens the migration's
// transaction — because on a single-connection pool a query issued while the
// transaction holds the only connection deadlocks. See the note in
// [Migrator.applyUpMigrationTransactional].
func (m *Migrator) planUpRetry(ctx context.Context, migration *Migration) (upRetryPlan, error) {
	plan := upRetryPlan{resumeFrom: 1}
	if m.conn.Writer().IsDryRun() || !m.metadataAvailable || m.legacyRevisionTable {
		return plan, nil
	}
	revision, err := m.getMigrationRevision(ctx, migration)
	if err != nil {
		return upRetryPlan{}, err
	}
	// A row that is present and clean means an applied migration is being
	// applied again, which no up path should have selected. Leave it to the
	// INSERT so the contradiction surfaces instead of being overwritten.
	if revision == nil || !revision.Dirty {
		return plan, nil
	}
	if revision.Direction == MigrationDirectionDown {
		return upRetryPlan{}, fmt.Errorf(
			"migration %d is dirty from an interrupted rollback; repair the rollback before migrating up",
			migration.Version,
		)
	}
	resumeFrom, err := resumeStatementFor(*revision, m.migrationStatementCount(migration.UpSQL))
	if err != nil {
		return upRetryPlan{}, err
	}
	if err := m.verifyCommittedPrefix(*revision, migration, MigrationDirectionUp, "resume automatically"); err != nil {
		return upRetryPlan{}, err
	}
	return upRetryPlan{reuseRevision: true, resumeFrom: resumeFrom}, nil
}

// resumeStatementFor returns the 1-based statement index a retry restarts at,
// given the dirty row the previous attempt left and the statement count the
// migration file has now.
//
// It refuses rather than guesses in the two cases where the recorded counter
// cannot be trusted to index into the current file. Refusing costs a retry that
// the community Atlas binary would have run — it resumes at applied+1 by index
// with no such check — but resuming by a stale index executes the wrong
// statements, so this is one place where matching would mean reproducing a
// defect.
func resumeStatementFor(revision MigrationRevision, total int) (int, error) {
	if err := validateRevisionProgress(revision, "resume automatically"); err != nil {
		return 0, err
	}
	if revision.Applied <= 0 {
		return 1, nil
	}
	if revision.Error == unknownStatementOutcomeError {
		return 0, fmt.Errorf(
			"migration %d cannot resume automatically: the outcome of statement %d is unknown after an interrupted run; inspect the database, then use 'ptah migrations repair --version %d'",
			revision.Version,
			revision.Applied+1,
			revision.Version,
		)
	}
	if revision.Total != total {
		return 0, fmt.Errorf(
			"migration %d cannot resume automatically: the previous attempt applied %d of %d statements but the file now has %d; inspect the database, then use 'ptah migrations repair --version %d'",
			revision.Version,
			revision.Applied,
			revision.Total,
			total,
			revision.Version,
		)
	}
	return revision.Applied + 1, nil
}

func validateRevisionProgress(revision MigrationRevision, operation string) error {
	if revision.Applied >= 0 && revision.Total >= 0 && revision.Applied <= revision.Total {
		return nil
	}
	return fmt.Errorf(
		"migration %s cannot %s: revision metadata records invalid progress applied=%d total=%d; inspect the database before choosing a repair point",
		revision.RevisionVersion(),
		operation,
		revision.Applied,
		revision.Total,
	)
}

func validateNativeRevisionState(revision MigrationRevision, storedState, operation string) error {
	switch storedState {
	case migrationStateApplied,
		migrationStatePending,
		migrationStateFailed,
		migrationStatePending + revisionDirectionSeparator + string(MigrationDirectionDown),
		migrationStateFailed + revisionDirectionSeparator + string(MigrationDirectionDown):
	default:
		return fmt.Errorf(
			"migration %d cannot %s: revision metadata records non-canonical state=%q; inspect the database before choosing a repair point",
			revision.Version,
			operation,
			storedState,
		)
	}
	if revision.State != migrationStateApplied {
		return nil
	}
	if revision.Applied == revision.Total {
		return nil
	}
	return fmt.Errorf(
		"migration %d cannot %s: revision metadata records state=%s with applied=%d total=%d; inspect the database before choosing a repair point",
		revision.Version,
		operation,
		revision.State,
		revision.Applied,
		revision.Total,
	)
}

// verifyCommittedPrefix proves that every statement an earlier
// no-transaction attempt recorded as committed still has the same source text.
// A count alone is insufficient: editing statement one while keeping the same
// number of statements would otherwise make a retry skip the edited statement
// and then record the new file as fully applied.
func (m *Migrator) verifyCommittedPrefix(
	revision MigrationRevision,
	migration *Migration,
	direction MigrationDirection,
	operation string,
) error {
	if err := validateRevisionProgress(revision, operation); err != nil {
		return err
	}
	if revision.Applied <= 0 {
		return nil
	}
	sqlText := migrationSQLForDirection(migration, direction)
	hashes, ok := cumulativePartialHashValues(sqlText, m.connectionDialect(), revision.Applied)
	if !ok {
		return committedPrefixVerificationError(revision, operation, "the current file cannot be split into the recorded committed prefix")
	}
	expected := hashes[len(hashes)-1]
	if m.revisionTableFormat.isAtlas() {
		return verifyAtlasCommittedPrefix(revision, migration, direction, operation, expected)
	}
	stored, encoded, err := parseNativeDirtyChecksum(revision.Checksum)
	if err != nil {
		return committedPrefixVerificationError(revision, operation, "the native checksum prefix metadata is malformed: "+err.Error())
	}
	if !encoded {
		if nativeFullChecksumMatches(revision.Checksum, migration, direction) {
			return nil
		}
		return committedPrefixVerificationError(revision, operation, "the legacy native revision has no committed-prefix checksum and its full-file checksum changed")
	}
	if stored != expected {
		return committedPrefixVerificationError(revision, operation, "the already committed statement prefix changed")
	}
	return nil
}

func verifyAtlasCommittedPrefix(
	revision MigrationRevision,
	migration *Migration,
	direction MigrationDirection,
	operation string,
	expected string,
) error {
	if len(revision.partialHashes) == 0 {
		if direction == MigrationDirectionUp && revision.Checksum != "" && revisionChecksumMatches(normalizeAtlasRevisionHash(revision.Checksum), migration) {
			return nil
		}
		return committedPrefixVerificationError(revision, operation, "the legacy Atlas revision has no partial_hashes for this direction")
	}
	if len(revision.partialHashes) != revision.Applied {
		return committedPrefixVerificationError(revision, operation, fmt.Sprintf(
			"Atlas partial_hashes contains %d entries for %d committed statements",
			len(revision.partialHashes),
			revision.Applied,
		))
	}
	if revision.partialHashes[revision.Applied-1] != expected {
		return committedPrefixVerificationError(revision, operation, "the already committed statement prefix changed")
	}
	return nil
}

func nativeFullChecksumMatches(stored string, migration *Migration, direction MigrationDirection) bool {
	if stored == "" {
		return false
	}
	if direction == MigrationDirectionDown {
		return stored == migrationChecksum(migration.DownSQL)
	}
	return revisionChecksumMatches(stored, migration)
}

func committedPrefixVerificationError(revision MigrationRevision, operation, reason string) error {
	return fmt.Errorf(
		"migration %d cannot %s: %s after %d of %d statements committed; inspect the database before choosing a repair point",
		revision.Version,
		operation,
		reason,
		revision.Applied,
		revision.Total,
	)
}

// recordPendingMigrationRevisionOn writes the row that says "this version is
// being applied right now", inserting it or reusing the dirty row a previous
// attempt left, as plan says.
func (m *Migrator) recordPendingMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
	plan upRetryPlan,
) error {
	if plan.reuseRevision {
		return m.restartMigrationRevisionOn(ctx, conn, migration, startedAt, plan.resumeFrom-1)
	}
	return m.beginMigrationRevisionOn(ctx, conn, migration, startedAt)
}

// restartMigrationRevisionOn rewrites an existing revision row back to pending
// for a fresh attempt, recording alreadyApplied as the progress this attempt
// starts from so an interruption before the first executed statement does not
// understate what is committed.
func (m *Migrator) restartMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
	alreadyApplied int,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	if m.revisionTableFormat.isAtlas() {
		return m.restartAtlasMigrationRevisionOn(ctx, conn, migration, startedAt, alreadyApplied)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.restartMigrationSQL())
	return executeSQLOn(
		ctx,
		conn,
		query,
		migration.Description,
		startedAt,
		migrationStatePending,
		alreadyApplied,
		m.migrationStatementCount(migration.UpSQL),
		0,
		m.dirtyRevisionChecksum(migration, MigrationDirectionUp, alreadyApplied),
		migration.Version,
	)
}

func (m *Migrator) restartAtlasMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
	alreadyApplied int,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.restartMigrationSQL())
	return executeSQLOn(
		ctx,
		conn,
		query,
		migration.atlasFilenameDescription(),
		migration.revisionType().sqlArg(),
		alreadyApplied,
		m.migrationStatementCount(migration.UpSQL),
		m.atlasRevisionTimestamp(startedAt),
		int64(0),
		migrationRevisionHash(migration),
		m.atlasPartialHashes(migration.UpSQL, alreadyApplied, m.migrationStatementCount(migration.UpSQL)),
		migration.atlasOperatorVersion(),
		migration.RevisionVersion(),
	)
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
		migration.RevisionVersion(),
		migration.atlasFilenameDescription(),
		migration.revisionType().sqlArg(),
		0,
		m.migrationStatementCount(migration.UpSQL),
		m.atlasRevisionTimestamp(startedAt),
		int64(0),
		"",
		"",
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		migration.atlasOperatorVersion(),
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
		migrationRevisionHash(migration),
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
		migration.atlasOperatorVersion(),
		migration.RevisionVersion(),
	)
}

func (m *Migrator) checkpointMigrationRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
	direction MigrationDirection,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	return m.checkpointMigrationRevisionOn(recordCtx, m.conn, migration, startedAt, event, direction)
}

func (m *Migrator) checkpointMigrationRevisionOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
	direction MigrationDirection,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.checkpointMigrationSQL())
	if m.revisionTableFormat.isAtlas() {
		sqlText := migrationSQLForDirection(migration, direction)
		return executeSQLOn(
			ctx,
			conn,
			query,
			event.Index,
			event.Total,
			time.Since(startedAt).Nanoseconds(),
			m.atlasDirtyPartialHashes(sqlText, direction, event.Index, event.Total),
			atlasOperatorVersionForMigration(migration, direction),
			migration.RevisionVersion(),
		)
	}
	return executeSQLOn(
		ctx,
		conn,
		query,
		encodeRevisionState(migrationStatePending, direction),
		event.Index,
		event.Total,
		time.Since(startedAt).Milliseconds(),
		m.dirtyRevisionChecksum(migration, direction, event.Index),
		migration.Version,
	)
}

func (m *Migrator) markMigrationStatementInFlight(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
	direction MigrationDirection,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	return m.markMigrationStatementInFlightOn(recordCtx, m.conn, migration, startedAt, event, direction)
}

func (m *Migrator) markMigrationStatementInFlightOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
	event StatementEvent,
	direction MigrationDirection,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.failMigrationSQL())
	if m.revisionTableFormat.isAtlas() {
		sqlText := migrationSQLForDirection(migration, direction)
		return executeSQLOn(
			ctx,
			conn,
			query,
			event.Index-1,
			event.Total,
			time.Since(startedAt).Nanoseconds(),
			unknownStatementOutcomeError,
			event.Statement,
			m.atlasDirtyPartialHashes(sqlText, direction, event.Index-1, event.Total),
			atlasOperatorVersionForMigration(migration, direction),
			migration.RevisionVersion(),
		)
	}
	return executeSQLOn(
		ctx,
		conn,
		query,
		encodeRevisionState(migrationStatePending, direction),
		event.Index-1,
		event.Total,
		unknownStatementOutcomeError,
		event.Statement,
		time.Since(startedAt).Milliseconds(),
		m.dirtyRevisionChecksum(migration, direction, event.Index-1),
		migration.Version,
	)
}

// withTransactionalProgressRecorder stores the MySQL-family progress witness
// on the same physical transaction as the migration body. An implicit server
// commit therefore makes the witness durable with the user SQL, while a normal
// rollback removes both. This avoids guessing transaction boundaries from SQL
// keywords, whose effects depend on session state and storage engine details.
func (m *Migrator) withTransactionalProgressRecorder(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
	direction MigrationDirection,
) context.Context {
	if !implicitCommitDialect(m.connectionDialect()) || conn.Writer().IsDryRun() {
		return ctx
	}
	return withStatementProgressRecorder(
		ctx,
		func(ctx context.Context, event StatementEvent) error {
			return m.markMigrationStatementInFlightOn(ctx, conn, migration, startedAt, event, direction)
		},
		func(ctx context.Context, event StatementEvent) error {
			return m.checkpointMigrationRevisionOn(ctx, conn, migration, startedAt, event, direction)
		},
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
		encodeRevisionState(migrationStatePending, MigrationDirectionDown),
		0,
		m.migrationStatementCount(migration.DownSQL),
		0,
		migrationChecksum(migration.DownSQL),
		migration.Version,
	)
}

// beginAtlasRollbackRevision rewrites the revision row in place before the down
// body runs. The Atlas table has no direction column, so operator_version marks
// Ptah-owned rollback state without changing the externally compatible schema.
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
		ptahDownOperatorVersion,
		migration.RevisionVersion(),
	)
}

func (m *Migrator) failMigrationRevisionWithMode(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	txMode MigrationTxMode,
	direction MigrationDirection,
) error {
	if m.conn.Writer().IsDryRun() {
		return nil
	}
	// recordStatementProgressBefore wrote the unknown-outcome marker before the
	// statement entered ExecContext. Cancellation can race a server-side commit,
	// so replacing that marker with an ordinary failure would make the next retry
	// replay SQL whose outcome is unknowable.
	if preservesUnknownStatementOutcome(ctx, failure, txMode) {
		return nil
	}
	recordCtx, cancelRecord := durableRevisionWriteContext(ctx)
	defer cancelRecord()
	if err := m.restoreTransactionalRecoverySession(recordCtx, failure, txMode); err != nil {
		return err
	}
	progress := migrationExecutionProgress(failure, m.conn.Info().Dialect, txMode)
	applied, total, stmt, failedIndex := progress.Applied, progress.Total, progress.Statement, progress.FailedIndex
	if total == 0 {
		total = m.migrationStatementCount(sqlText)
	}
	// The per-statement progress recorder reports its own count, which the
	// rollback correction inside migrationExecutionProgress never sees.
	// Recovery from a rollback reads that number to decide whether the schema
	// was changed at all, so the down direction runs the same correction here.
	// The up direction is left alone deliberately: it is the number the
	// Atlas-shaped surface reports.
	if direction == MigrationDirectionDown {
		applied = rolledBackApplied(m.conn.Info().Dialect, txMode, applied)
	}
	if usesTransactionalProgressWitness(m.conn.Info().Dialect, txMode) {
		revision, err := m.getMigrationRevision(recordCtx, migration)
		if err != nil {
			return fmt.Errorf("failed to read migration %d progress witness: %w", migration.Version, err)
		}
		if revision != nil {
			if preservesProgressWitnessUnknownOutcome(failure, revision, m.connectionDialect()) {
				return nil
			}
			applied = revision.Applied
		}
	}
	if direction == MigrationDirectionUp {
		applied = max(applied, migrationAppliedFloor(ctx))
	}
	if m.revisionTableFormat.isAtlas() {
		return m.failAtlasMigrationRevision(
			recordCtx, migration, startedAt, failure, sqlText, direction, applied, total, stmt, failedIndex,
		)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.failMigrationSQL())
	return executeSQLOutsideTransaction(
		recordCtx,
		m.conn,
		query,
		encodeRevisionState(migrationStateFailed, direction),
		applied,
		total,
		strings.TrimSpace(failure.Error()),
		stmt,
		time.Since(startedAt).Milliseconds(),
		m.dirtyRevisionChecksum(migration, direction, applied),
		migration.Version,
	)
}

func usesTransactionalProgressWitness(dialect string, txMode MigrationTxMode) bool {
	return txMode == MigrationTxModeFile && implicitCommitDialect(dialect)
}

func (m *Migrator) restoreTransactionalRecoverySession(
	ctx context.Context,
	failure error,
	txMode MigrationTxMode,
) error {
	if !usesTransactionalProgressWitness(m.connectionDialect(), txMode) {
		return nil
	}
	if _, rolledBack := migrationTransactionRollbackVersion(failure); !rolledBack {
		return nil
	}
	if _, err := m.conn.ExecContext(ctx, "SET autocommit = 1"); err != nil {
		return fmt.Errorf("failed to restore MySQL-family autocommit before recording migration recovery state: %w", err)
	}
	return nil
}

func preservesProgressWitnessUnknownOutcome(
	failure error,
	revision *MigrationRevision,
	dialect string,
) bool {
	if revision.Error != unknownStatementOutcomeError {
		return false
	}
	var progressErr *statementProgressError
	if errors.As(failure, &progressErr) {
		return true
	}
	var execErr *MigrationExecutionError
	if !errors.As(failure, &execErr) {
		return true
	}
	if errors.Is(execErr.Err, context.Canceled) || errors.Is(execErr.Err, context.DeadlineExceeded) {
		return true
	}
	return !mysqlAtomicFailureStatement(execErr.Statement, dialect)
}

func mysqlAtomicFailureStatement(statement, dialect string) bool {
	tokens := significantSQLTokens(statement, dialect)
	if len(tokens) == 0 {
		return false
	}
	return matchesAnyKeyword(tokens[0], "INSERT", "UPDATE", "DELETE", "REPLACE")
}

func preservesUnknownStatementOutcome(ctx context.Context, failure error, txMode MigrationTxMode) bool {
	if txMode != MigrationTxModeNone {
		return false
	}
	var execErr *MigrationExecutionError
	if !errors.As(failure, &execErr) {
		return false
	}
	return ctx.Err() != nil || errors.Is(execErr.Err, context.Canceled) || errors.Is(execErr.Err, context.DeadlineExceeded)
}

func durableRevisionWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), revisionWriteTimeout)
}

func (m *Migrator) failAtlasMigrationRevision(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	direction MigrationDirection,
	applied int,
	total int,
	stmt string,
	failedIndex int,
) error {
	query := sqlutil.Rebind(m.conn.Info().Dialect, m.failMigrationSQL())
	return executeSQLOutsideTransaction(
		ctx,
		m.conn,
		query,
		applied,
		total,
		time.Since(startedAt).Nanoseconds(),
		atlasFailureError(failure),
		atlasFailureStatement(sqlText, m.connectionDialect(), failedIndex, stmt),
		m.atlasDirtyPartialHashes(sqlText, direction, applied, total),
		atlasOperatorVersionForMigration(migration, direction),
		migration.RevisionVersion(),
	)
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
			if err := conn.Writer().ExecuteSQL(ctx, deleteSQL, m.migrationRevisionVersionArg(migration)); err != nil {
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
	operatorVersion := migration.atlasOperatorVersion()
	if migration.revisionType() == AtlasRevisionTypeBaseline|AtlasRevisionTypeApplied {
		operatorVersion = revisiontable.SourceBaselineOperatorVersion
	}
	if err := conn.Writer().ExecuteSQL(
		ctx,
		query,
		migration.RevisionVersion(),
		migration.atlasFilenameDescription(),
		int64(AtlasRevisionTypeBaseline),
		0,
		0,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		"",
		m.atlasNullJSONValue(),
		operatorVersion,
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
	retired := m.retiredAtlasRevisionVersions()
	exactRemoved, err := m.unownedExactAtlasRevisionsAbove(existing, migrations[len(migrations)-1])
	if err != nil {
		_ = tx.Rollback()
		return AtlasRevisionSetResult{}, err
	}
	result := atlasRevisionSetChanges(existing, migrations, version, retired, exactRemoved)
	if m.revisionTableFormat.isAtlas() {
		err = m.writeAtlasSetRevisionRows(ctx, tx, existing, migrations, version, exactRemoved)
	} else {
		err = m.writePtahSetRevisionRows(ctx, tx, existing, migrations, version)
	}
	if err != nil {
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
	retired []string,
	exactRemoved []string,
) AtlasRevisionSetResult {
	result := AtlasRevisionSetResult{CurrentVersion: version}
	revisions := newSetRevisionIndex(existing)
	target := migrations[len(migrations)-1]
	for _, revision := range existing {
		if revisionRemovedByAtlasSet(revision, target, version, retired, exactRemoved) {
			result.Removed = append(result.Removed, AtlasRevisionChange{
				Version:         revision.Version,
				RevisionVersion: revision.RevisionVersion(),
				Description:     revision.Description,
			})
		}
	}
	for _, migration := range migrations {
		revision, exists := revisions.find(migration)
		if exists && revision.State == migrationStateApplied {
			continue
		}
		result.Set = append(result.Set, AtlasRevisionChange{
			Version:         migration.Version,
			RevisionVersion: migration.RevisionVersion(),
			Description:     migration.atlasFilenameDescription(),
		})
	}
	return result
}

func revisionRemovedByAtlasSet(
	revision MigrationRevision,
	target *Migration,
	version int64,
	retired, exactRemoved []string,
) bool {
	key := revision.RevisionVersion()
	return slices.Contains(exactRemoved, key) ||
		(revision.Version > version && key != target.RevisionVersion() && !slices.Contains(retired, key))
}

func (m *Migrator) unownedExactAtlasRevisionsAbove(
	existing []MigrationRevision,
	target *Migration,
) ([]string, error) {
	if !m.hasExactAtlasRevisionOrder() {
		return nil, nil
	}
	owned := make(map[string]struct{}, len(m.migrationProvider.Migrations()))
	for _, migration := range m.migrationProvider.Migrations() {
		owned[migration.RevisionVersion()] = struct{}{}
	}
	removed := make(map[string]struct{})
	for _, revision := range existing {
		key := revision.RevisionVersion()
		_, isOwned := owned[key]
		if isOwned {
			continue
		}
		if m.atlasRevisionCompare == nil {
			return nil, retiredAtlasRevisionOrderError(key, target.RevisionVersion())
		}
		order, ok := m.atlasRevisionCompare(
			AtlasRevisionOrderIdentity{
				RevisionVersion: key,
				AtlasType:       revision.AtlasType,
				OperatorVersion: revision.OperatorVersion,
			},
			AtlasRevisionOrderIdentity{
				RevisionVersion: target.RevisionVersion(),
				AtlasType:       target.revisionType(),
				OperatorVersion: target.atlasOperatorVersion(),
				Repeatable:      target.isAtlasRepeatable(),
			},
		)
		if !ok {
			return nil, retiredAtlasRevisionOrderError(key, target.RevisionVersion())
		}
		if order > 0 {
			removed[key] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(removed)), nil
}

func retiredAtlasRevisionOrderError(revision, target string) error {
	return fmt.Errorf(
		"cannot set Atlas revision: source order between retired exact identity %q and target %q is ambiguous",
		revision,
		target,
	)
}

func (m *Migrator) retiredAtlasRevisionVersions() []string {
	source, ok := m.migrationProvider.(interface {
		atlasRevisionVersionMap() map[int64]string
	})
	if !ok {
		return nil
	}
	owned := make(map[string]struct{})
	for _, migration := range m.migrationProvider.Migrations() {
		if migration.atlasRevisionVersionMapped {
			owned[migration.RevisionVersion()] = struct{}{}
		}
	}
	retired := make(map[string]struct{})
	for _, revision := range source.atlasRevisionVersionMap() {
		if _, exists := owned[revision]; !exists {
			retired[revision] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(retired))
}

type setRevisionIndex struct {
	byVersion map[int64]MigrationRevision
	byExact   map[string]MigrationRevision
}

func newSetRevisionIndex(revisions []MigrationRevision) setRevisionIndex {
	index := setRevisionIndex{
		byVersion: make(map[int64]MigrationRevision, len(revisions)),
		byExact:   make(map[string]MigrationRevision, len(revisions)),
	}
	for _, revision := range revisions {
		index.byVersion[revision.Version] = revision
		index.byExact[revision.RevisionVersion()] = revision
	}
	return index
}

func (i setRevisionIndex) find(migration *Migration) (MigrationRevision, bool) {
	if migration.atlasRevisionVersionMapped {
		revision, ok := i.byExact[migration.RevisionVersion()]
		return revision, ok
	}
	revision, ok := i.byVersion[migration.Version]
	return revision, ok
}

func (m *Migrator) writeAtlasSetRevisionRows(
	ctx context.Context,
	tx *sql.Tx,
	existing []MigrationRevision,
	migrations []*Migration,
	version int64,
	exactRemoved []string,
) error {
	target := migrations[len(migrations)-1]
	deleteSQL := sqlutil.Rebind(
		m.conn.Info().Dialect,
		m.deleteAtlasSetRevisionsAboveSQL(m.retiredAtlasRevisionVersions(), exactRemoved),
	)
	if _, err := tx.ExecContext(
		ctx,
		deleteSQL,
		version,
		target.RevisionVersion(),
	); err != nil {
		return fmt.Errorf("failed to remove Atlas revisions above %d: %w", version, err)
	}

	revisions := newSetRevisionIndex(existing)

	insertSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.insertAtlasRevisionSQL())
	updateTypeSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.updateAtlasRevisionTypeSQL())
	for _, migration := range migrations {
		revision, exists := revisions.find(migration)
		if exists && revision.State == migrationStateApplied {
			continue
		}
		if exists {
			revisionType := migration.manuallySetRevisionType()
			if _, err := tx.ExecContext(
				ctx,
				updateTypeSQL,
				revisionType.sqlArg(),
				revision.RevisionVersion(),
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
				migrationRevisionHash(migration),
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
	return fmt.Sprintf(
		`UPDATE %s SET state = ?, applied = total, checksum = ? WHERE version = ?`,
		m.qualifiedMigrationsTable(),
	)
}

// RepairMigration clears dirty migration metadata after an operator has fixed
// the database manually, or resumes the migration from a specific statement.
// It holds the configured migration advisory lock across revision inspection,
// resumed SQL, safety checks, and the final metadata write.
//
// The revision's recorded direction decides which body --resume-from runs and
// what a finished resume leaves behind. A row an up migration left dirty
// resumes the up body and is then recorded applied; a row a rollback left
// dirty resumes the down body and the revision is deleted, because a completed
// rollback means the migration is no longer applied. See
// [Migrator.repairRolledBackMigration].
//
// On PostgreSQL the revision is not recorded while an index the migration
// creates is still unusable -- the residue a failed CREATE INDEX CONCURRENTLY
// leaves behind. The repair is refused instead, so an unenforced constraint
// cannot be signed off as applied. See refuseRepairOverUnsafeIndex, including
// why Force does not relax it.
func (m *Migrator) RepairMigration(ctx context.Context, opts RepairMigrationOptions) error {
	if opts.Version <= 0 {
		return fmt.Errorf("repair version must be greater than zero")
	}
	return m.withMigrationLock(ctx, "repair migration", func(ctx context.Context) error {
		return m.repairMigrationLocked(ctx, opts)
	})
}

func (m *Migrator) repairMigrationLocked(ctx context.Context, opts RepairMigrationOptions) error {
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	migration := m.migrationByVersion(opts.Version)
	if migration == nil {
		return fmt.Errorf("migration %d not found", opts.Version)
	}
	revision, err := m.getMigrationRevision(ctx, migration)
	if err != nil {
		return err
	}
	if revision == nil && !opts.Force {
		return fmt.Errorf("migration %d has no revision row; rerun with --force to mark it applied", opts.Version)
	}
	if revision != nil && !revision.Dirty && !opts.Force {
		return fmt.Errorf("migration %d is not dirty; rerun with --force to rewrite it", opts.Version)
	}
	direction := repairMigrationDirection(revision)
	if err := m.validateRepairMigrationSQL(migration, revision, opts, direction); err != nil {
		return err
	}
	if direction == MigrationDirectionDown {
		return m.repairRolledBackMigration(ctx, migration, revision, opts)
	}
	return m.repairUpMigration(ctx, migration, revision, opts)
}

func repairMigrationDirection(revision *MigrationRevision) MigrationDirection {
	if revision != nil && revision.Direction == MigrationDirectionDown {
		return MigrationDirectionDown
	}
	return MigrationDirectionUp
}

func (m *Migrator) validateRepairMigrationSQL(
	migration *Migration,
	revision *MigrationRevision,
	opts RepairMigrationOptions,
	direction MigrationDirection,
) error {
	txMode, err := m.resolveRepairMigrationTxMode(migration, direction)
	if err != nil {
		return err
	}
	committedProgress := revision != nil && revision.Dirty && revision.Applied > 0
	if opts.ResumeFrom <= 0 && txMode != MigrationTxModeNone && !committedProgress {
		return nil
	}
	if err := m.validateNoTransactionSQL(migration, direction); err != nil {
		return fmt.Errorf("migration %d cannot be repaired safely: %w", migration.Version, err)
	}
	return nil
}

func (m *Migrator) repairUpMigration(
	ctx context.Context,
	migration *Migration,
	revision *MigrationRevision,
	opts RepairMigrationOptions,
) error {
	if opts.ResumeFrom > 0 {
		return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
			if err := scoped.resumeUpMigrationFrom(ctx, migration, revision, opts); err != nil {
				return err
			}
			if err := scoped.refuseRepairOverUnsafeIndex(ctx, migration); err != nil {
				return err
			}
			return scoped.forceAppliedMigration(ctx, migration)
		})
	}
	if revision != nil && revision.Dirty && revision.Applied == revision.Total && revision.Total > 0 {
		return m.repairCompletedUpMigration(ctx, migration, revision)
	}
	if err := m.refuseRepairOverUnsafeIndex(ctx, migration); err != nil {
		return err
	}
	return m.forceAppliedMigration(ctx, migration)
}

func (m *Migrator) resolveRepairMigrationTxMode(
	migration *Migration,
	direction MigrationDirection,
) (MigrationTxMode, error) {
	if direction == MigrationDirectionDown {
		return m.resolveDownMigrationTxMode(migration)
	}
	return m.resolveUpMigrationTxMode(migration)
}

func (m *Migrator) repairCompletedUpMigration(
	ctx context.Context,
	migration *Migration,
	revision *MigrationRevision,
) error {
	if err := m.verifyCommittedPrefix(*revision, migration, MigrationDirectionUp, "finalize the migration"); err != nil {
		return err
	}
	if !m.needsPostgresIndexPostcheck(migration, MigrationDirectionUp) {
		return m.forceAppliedMigration(ctx, migration)
	}
	return m.withNoTransactionSession(ctx, func(scoped *Migrator) error {
		if err := scoped.restoreNoTransactionSessionPrefix(
			ctx,
			migration,
			MigrationDirectionUp,
			revision.Total+1,
		); err != nil {
			return err
		}
		if err := scoped.refuseRepairOverUnsafeIndex(ctx, migration); err != nil {
			return err
		}
		return scoped.forceAppliedMigration(ctx, migration)
	})
}

func (m *Migrator) resumeUpMigrationFrom(
	ctx context.Context,
	migration *Migration,
	revision *MigrationRevision,
	opts RepairMigrationOptions,
) error {
	if err := refuseUnknownStatementOutcomeResume(migration, revision); err != nil {
		return err
	}
	if err := m.refuseUpResumeOverRecordedRollback(migration, revision, opts); err != nil {
		return err
	}
	if revision != nil {
		if err := m.verifyCommittedPrefix(*revision, migration, MigrationDirectionUp, "resume the migration"); err != nil {
			return err
		}
	}
	return m.resumeMigration(ctx, migration, opts.ResumeFrom)
}

// refuseUnknownStatementOutcomeResume refuses a --resume-from over a revision
// whose last statement was interrupted before its outcome could be recorded.
// Resuming would either repeat SQL that already committed or skip SQL that
// never ran, and the row cannot say which.
func refuseUnknownStatementOutcomeResume(migration *Migration, revision *MigrationRevision) error {
	if revision == nil || revision.Error != unknownStatementOutcomeError {
		return nil
	}
	return fmt.Errorf(
		"migration %d has an unknown statement outcome for %q; inspect the database before repair and omit --resume-from to avoid repeating committed SQL",
		migration.Version,
		revision.ErrorStatement,
	)
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
	return m.resumeMigrationDirectionOnSession(ctx, migration, resumeFrom, MigrationDirectionUp)
}

func (m *Migrator) resumeMigrationDirectionOnSession(
	ctx context.Context,
	migration *Migration,
	resumeFrom int,
	direction MigrationDirection,
) error {
	sqlText := migrationSQLForDirection(migration, direction)
	executionConn := m.noTransactionConnection()
	statements := splitSQLStatementsForConnection(executionConn, sqlText)
	if resumeFrom < 1 || resumeFrom > len(statements) {
		return fmt.Errorf("resume-from must be between 1 and %d", len(statements))
	}
	if err := m.restoreNoTransactionSessionPrefix(ctx, migration, direction, resumeFrom); err != nil {
		return err
	}
	operation := "migration"
	if direction == MigrationDirectionDown {
		operation = "rollback"
	}
	startedAt := time.Now()
	for i := resumeFrom - 1; i < len(statements); i++ {
		stmt := strings.TrimSpace(statements[i])
		if stmt == "" {
			continue
		}
		event := StatementEvent{Statement: stmt, Index: i + 1, Total: len(statements)}
		if err := m.markMigrationStatementInFlight(ctx, migration, startedAt, event, direction); err != nil {
			return fmt.Errorf("failed to record resumed %s %d at statement %d: %w", operation, migration.Version, event.Index, err)
		}
		if err := executeSQLOutsideTransaction(ctx, executionConn, stmt); err != nil {
			return m.failResumedMigrationDirection(ctx, migration, startedAt, err, event, direction)
		}
		if err := m.checkpointMigrationRevision(ctx, migration, startedAt, event, direction); err != nil {
			return fmt.Errorf("failed to record resumed %s %d at statement %d: %w", operation, migration.Version, event.Index, err)
		}
		if err := m.observePostgresIndexStatement(ctx, executionConn, stmt); err != nil {
			return fmt.Errorf("failed to observe resumed %s %d at statement %d: %w", operation, migration.Version, event.Index, err)
		}
	}
	return nil
}

func (m *Migrator) failResumedMigrationDirection(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	event StatementEvent,
	direction MigrationDirection,
) error {
	execErr := &MigrationExecutionError{
		Err:            fmt.Errorf("failed to execute SQL statement: %w", failure),
		Statement:      event.Statement,
		StatementIndex: event.Index,
		Total:          event.Total,
	}
	if direction == MigrationDirectionDown {
		return m.failRollbackWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			execErr,
			migration.DownSQL,
			fmt.Sprintf("failed to resume rollback of migration %d", migration.Version),
			MigrationTxModeNone,
		)
	}
	return m.failMigrationWithDirtyStateWithMode(
		ctx,
		migration,
		startedAt,
		execErr,
		migration.UpSQL,
		fmt.Sprintf("failed to resume migration %d", migration.Version),
		MigrationTxModeNone,
		MigrationDirectionUp,
	)
}

func migrationSQLForDirection(migration *Migration, direction MigrationDirection) string {
	if direction == MigrationDirectionDown {
		return migration.DownSQL
	}
	return migration.UpSQL
}

func (m *Migrator) dirtyRevisionChecksum(
	migration *Migration,
	direction MigrationDirection,
	applied int,
) string {
	sqlText := migrationSQLForDirection(migration, direction)
	fallback := migrationRevisionHash(migration)
	if direction == MigrationDirectionDown {
		fallback = migrationChecksum(sqlText)
	}
	return nativeDirtyChecksum(sqlText, m.connectionDialect(), applied, fallback)
}

func atlasOperatorVersionForMigration(migration *Migration, direction MigrationDirection) string {
	if direction == MigrationDirectionDown {
		return ptahDownOperatorVersion
	}
	return migration.atlasOperatorVersion()
}

func (m *Migration) atlasOperatorVersion() string {
	if m.atlasRevisionVersionMapped {
		return revisiontable.SourceIdentityOperatorVersion
	}
	return ptahOperatorVersion
}

func (m *Migrator) forceAppliedMigration(ctx context.Context, migration *Migration) error {
	if m.isClickHouse() {
		revision, err := m.getMigrationRevision(ctx, migration)
		if err != nil {
			return err
		}
		if revision != nil {
			return m.forceAppliedMigrationClickHouse(ctx, migration)
		}
	}
	if m.forceAppliedConflictClause() == "" {
		deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())
		if err := executeSQLOutsideTransaction(ctx, m.conn, deleteSQL, m.migrationRevisionVersionArg(migration)); err != nil {
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
		migration.RevisionVersion(),
		migration.atlasFilenameDescription(),
		migration.revisionType().sqlArg(),
		total,
		total,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		migration.atlasOperatorVersion(),
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
		migration.RevisionVersion(),
		migration.atlasFilenameDescription(),
		migration.manuallySetRevisionType().sqlArg(),
		0,
		0,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		migration.atlasOperatorVersion(),
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
	return m.atlasJSONValue(atlasNullJSON)
}

// atlasJSONValue binds one JSON document the way the column that holds it was
// declared: a plain string where the column is text (SQL Server, ClickHouse),
// and a []byte JSON document everywhere the dialect has a real JSON type.
func (m *Migrator) atlasJSONValue(document string) any {
	if m.isSQLServer() || m.isClickHouse() {
		return document
	}
	return []byte(document)
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
		migration.revisionType().sqlArg(),
		total,
		total,
		m.atlasRevisionTimestamp(time.Now()),
		int64(0),
		migrationRevisionHash(migration),
		m.atlasNullJSONValue(),
		migration.atlasOperatorVersion(),
		migration.RevisionVersion(),
	)
}

func (m *Migrator) revisionVersionArg(version int64) any {
	if m.revisionTableFormat.isAtlas() {
		return strconv.FormatInt(version, 10)
	}
	return version
}

func (m *Migrator) migrationRevisionVersionArg(migration *Migration) any {
	if m.revisionTableFormat.isAtlas() {
		return migration.RevisionVersion()
	}
	return migration.Version
}
