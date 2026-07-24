package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
)

// MigrationStatus represents the current state of migrations
type MigrationStatus struct {
	CurrentVersion       int64              `json:"current_version"`
	AppliedMigrations    []int64            `json:"applied_migrations"`
	PendingMigrations    []int64            `json:"pending_migrations"`
	OutOfOrderMigrations []int64            `json:"out_of_order_migrations"`
	TotalMigrations      int                `json:"total_migrations"`
	HasPendingChanges    bool               `json:"has_pending_changes"`
	DirtyRevision        *MigrationRevision `json:"dirty_revision,omitempty"`
}

// MigrationDirection identifies the migration direction in a selected plan.
type MigrationDirection string

const (
	// MigrationDirectionUp applies pending migrations.
	MigrationDirectionUp MigrationDirection = "up"
	// MigrationDirectionDown rolls applied migrations back.
	MigrationDirectionDown MigrationDirection = "down"
)

// MigrationPlan describes the migration work selected while holding the
// migration lock.
type MigrationPlan struct {
	Direction      MigrationDirection
	CurrentVersion int64
	TargetVersion  int64
	Versions       []int64
}

// PreMigrationHook runs after the migrator has acquired its migration lock and
// selected the final migration plan, but before it changes schema or revision
// state.
type PreMigrationHook func(ctx context.Context, plan MigrationPlan) error

// MigrateUpOptions selects the pending up migration plan.
type MigrateUpOptions struct {
	// TargetVersion limits the run to pending migrations at or below this
	// version. Zero means latest.
	TargetVersion int64
	// Amount limits the run to the first N pending migrations after exec-order
	// and target-version filtering. Zero means all selected migrations.
	Amount uint64
	// AllowDirty skips the default dirty revision guard. Callers should expose
	// this only as an explicit recovery escape hatch.
	AllowDirty bool
	// AssumedAppliedVersions are treated as applied for plan selection without
	// reading or writing revision metadata. This is intended for dry-run paths
	// that need to model metadata-only operations such as baseline.
	AssumedAppliedVersions []int64
	// Preflight runs after the migration lock is acquired and the final plan is
	// selected, but before any schema or revision changes.
	Preflight PreMigrationHook
}

// Migrator handles database migrations for ptah
type Migrator struct {
	conn                 *dbschema.DatabaseConnection
	migrationProvider    MigrationProvider
	defaultTimeouts      MigrationTimeouts
	migrationsTable      string
	migrationsSchema     string
	revisionTableFormat  RevisionTableFormat
	execOrder            ExecOrder
	txMode               MigrationTxMode
	migrationLockName    string
	migrationLockTimeout time.Duration
	initialized          bool
	logger               *slog.Logger
	observer             Observer
	skipChecks           bool
}

// NewFSMigrator creates a new migrator that loads migrations from a filesystem.
// It scans the provided filesystem for migration files following the naming convention
// NNNNNNNNNN_description.up.sql and NNNNNNNNNN_description.down.sql and automatically
// registers them with the migrator. Returns an error if the filesystem cannot be scanned
// or if any migrations are incomplete (missing up or down files).
func NewFSMigrator(conn *dbschema.DatabaseConnection, fsys fs.FS, opts ...FSProviderOption) (*Migrator, error) {
	provider, err := NewFSMigrationProvider(fsys, opts...)
	if err != nil {
		return nil, err
	}
	return NewMigrator(conn, provider), nil
}

// NewMigrator creates a new migrator with the given database connection
func NewMigrator(conn *dbschema.DatabaseConnection, provider MigrationProvider) *Migrator {
	return &Migrator{
		conn:                conn,
		migrationProvider:   provider,
		migrationsTable:     defaultPtahMigrationsTable,
		revisionTableFormat: RevisionTableFormatPtah,
		execOrder:           ExecOrderLinear,
		txMode:              MigrationTxModeFile,
		migrationLockName:   migrationAdvisoryLockName,
		logger:              slog.Default(),
		observer:            NoopObserver{},
	}
}

// WithLogger sets the logger for the migrator
func (m *Migrator) WithLogger(l *slog.Logger) *Migrator {
	tmp := *m
	if l == nil {
		l = slog.Default()
	}
	tmp.logger = l
	return &tmp
}

// WithObserver sets the migration observer used for tracing and metrics.
func (m *Migrator) WithObserver(observer Observer) *Migrator {
	tmp := *m
	if observer == nil {
		observer = NoopObserver{}
	}
	tmp.observer = observer
	return &tmp
}

// WithSkipChecks controls whether pre-migration `-- +ptah check` assertions are
// evaluated before applying up migrations. The default (false) enforces checks;
// pass true as an explicit emergency bypass, mirroring --allow-destructive.
func (m *Migrator) WithSkipChecks(skip bool) *Migrator {
	tmp := *m
	tmp.skipChecks = skip
	return &tmp
}

// runMigrationChecks evaluates the pre-migration assertion checks embedded in a
// migration's up SQL against conn, before any body statement runs. It is a no-op
// when checks are skipped. A malformed check directive or an unsatisfied
// assertion returns an error so the caller aborts with nothing applied.
func (m *Migrator) runMigrationChecks(ctx context.Context, conn *dbschema.DatabaseConnection, migration *Migration) error {
	if m.skipChecks {
		return nil
	}
	checks, err := ParseChecks(migration.UpSQL)
	if err != nil {
		return fmt.Errorf("migration %d has invalid pre-migration check directives: %w", migration.Version, err)
	}
	return runChecks(ctx, conn, migration.Version, checks)
}

// rejectChecksUnderTxModeAll refuses a migration that declares pre-migration
// checks when running with tx-mode all. Under a single shared transaction a
// check reads committed pre-batch state on the pool connection and cannot see
// earlier batched migrations' uncommitted changes, so it would silently
// evaluate a precondition against stale state. Bypassing checks lifts the
// restriction.
func (m *Migrator) rejectChecksUnderTxModeAll(migration *Migration) error {
	if m.skipChecks {
		return nil
	}
	checks, err := ParseChecks(migration.UpSQL)
	if err != nil {
		return fmt.Errorf("migration %d has invalid pre-migration check directives: %w", migration.Version, err)
	}
	if len(checks) > 0 {
		return fmt.Errorf("migration %d declares pre-migration checks, which cannot run with tx-mode all; use the default per-file transaction mode or --skip-checks", migration.Version)
	}
	return nil
}

// WithExecOrder sets how this migrator handles pending migrations whose
// version is below the current high-water mark.
func (m *Migrator) WithExecOrder(execOrder ExecOrder) *Migrator {
	tmp := *m
	tmp.execOrder = normalizeExecOrder(execOrder)
	return &tmp
}

// WithTransactionMode sets how pending up migrations are wrapped in
// transactions.
func (m *Migrator) WithTransactionMode(mode MigrationTxMode) *Migrator {
	tmp := *m
	tmp.txMode = normalizeMigrationTxMode(mode)
	return &tmp
}

// WithMigrationsTable sets the table used to record applied migrations.
func (m *Migrator) WithMigrationsTable(schema, table string) *Migrator {
	tmp := *m
	tmp.migrationsSchema = strings.TrimSpace(schema)
	tmp.migrationsTable = strings.TrimSpace(table)
	if tmp.migrationsTable == "" {
		tmp.migrationsTable = tmp.defaultMigrationsTable()
	}
	tmp.initialized = false
	return &tmp
}

// WithRevisionTableFormat sets the database table layout used for migration
// revision metadata.
func (m *Migrator) WithRevisionTableFormat(format RevisionTableFormat) *Migrator {
	tmp := *m
	tmp.revisionTableFormat = format
	if tmp.migrationsTable == "" || tmp.migrationsTable == defaultPtahMigrationsTable {
		tmp.migrationsTable = tmp.defaultMigrationsTable()
	}
	tmp.initialized = false
	return &tmp
}

func (m *Migrator) defaultMigrationsTable() string {
	if m.revisionTableFormat.isAtlas() {
		return defaultAtlasRevisionsTable
	}
	return defaultPtahMigrationsTable
}

func (m *Migrator) qualifiedMigrationsTable() string {
	table := m.migrationsTableName()
	schema := m.metadataTableSchemaName()
	if schema == "" {
		return m.quoteIdentifier(table)
	}
	return m.quoteIdentifier(schema) + "." + m.quoteIdentifier(table)
}

// MigrationsTableIdentifier returns the dialect-quoted metadata table name.
func (m *Migrator) MigrationsTableIdentifier() string {
	return m.qualifiedMigrationsTable()
}

func (m *Migrator) migrationsSchemaStatement() string {
	schema := m.migrationsSchema
	if m.isSQLServer() {
		schema = m.metadataTableSchemaName()
		if strings.EqualFold(schema, "dbo") {
			return ""
		}
	}
	if schema == "" {
		return ""
	}
	if platform.NormalizeDialect(m.connectionDialect()) == platform.SQLite {
		return ""
	}
	if m.isSQLServer() {
		return fmt.Sprintf(
			"IF SCHEMA_ID(%s) IS NULL EXEC(%s)",
			sqlStringLiteral(schema),
			sqlStringLiteral("CREATE SCHEMA "+m.quoteIdentifier(schema)),
		)
	}
	return "CREATE SCHEMA IF NOT EXISTS " + m.quoteIdentifier(schema)
}

func (m *Migrator) quoteIdentifier(identifier string) string {
	if m.conn == nil {
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
	switch m.conn.Info().Dialect {
	case "mysql", "mariadb", "clickhouse":
		return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
	case platform.SQLServer:
		return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
	default:
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
}

func (m *Migrator) isSQLServer() bool {
	return m.conn != nil && m.conn.Info().Dialect == platform.SQLServer
}

func (m *Migrator) connectionDialect() string {
	if m.conn == nil {
		return ""
	}
	return m.conn.Info().Dialect
}

func (m *Migrator) connectionSchemaName() string {
	if m.conn == nil {
		return ""
	}
	return m.conn.Info().Schema
}

func sqlStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func (m *Migrator) createMigrationsTableSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return m.createAtlasRevisionsTableSQL()
	}
	if m.isSQLServer() {
		return fmt.Sprintf(`IF OBJECT_ID(%s, N'U') IS NULL
BEGIN
    CREATE TABLE %s (
        version BIGINT PRIMARY KEY,
        description NVARCHAR(MAX) NOT NULL,
        applied_at DATETIME2 NOT NULL,
        state NVARCHAR(32) NOT NULL DEFAULT 'applied',
        applied INT NOT NULL DEFAULT 1,
        total INT NOT NULL DEFAULT 1,
        error NVARCHAR(MAX) NULL,
        error_stmt NVARCHAR(MAX) NULL,
        execution_time_ms BIGINT NOT NULL DEFAULT 0,
        checksum NVARCHAR(64) NOT NULL DEFAULT ''
    )
END`, sqlStringLiteral(m.sqlServerObjectName()), m.qualifiedMigrationsTable())
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
)`, m.qualifiedMigrationsTable())
}

func (m *Migrator) getVersionSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(
			"SELECT COALESCE(MAX(%s), 0) FROM %s WHERE applied = total AND COALESCE(error, '') = ''",
			m.atlasVersionNumberExpression(),
			m.qualifiedMigrationsTable(),
		)
	}
	return fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s WHERE state = 'applied'", m.qualifiedMigrationsTable())
}

func (m *Migrator) getAppliedMigrationsSQL() string {
	if m.revisionTableFormat.isAtlas() {
		return fmt.Sprintf(
			"SELECT version FROM %s WHERE applied = total AND COALESCE(error, '') = '' ORDER BY %s",
			m.qualifiedMigrationsTable(),
			m.atlasVersionNumberExpression(),
		)
	}
	return fmt.Sprintf("SELECT version FROM %s WHERE state = 'applied' ORDER BY version", m.qualifiedMigrationsTable())
}

func (m *Migrator) deleteMigrationSQL() string {
	return fmt.Sprintf("DELETE FROM %s WHERE version = ?", m.qualifiedMigrationsTable())
}

// Initialize creates the migrations table if it doesn't exist
func (m *Migrator) Initialize(ctx context.Context) error {
	// Skip if already initialized
	if m.initialized {
		return nil
	}

	if m.conn.Writer().IsDryRun() {
		m.logger.Info("[DRY RUN] Would initialize migrations metadata", "table", m.qualifiedMigrationsTable())
		return nil
	}

	if schemaSQL := m.migrationsSchemaStatement(); schemaSQL != "" {
		// Deliberately outside the migration writer: Initialize runs before any
		// per-migration transaction exists. Dry-run returns above so metadata DDL
		// is never written when callers asked for a simulation.
		if _, err := m.conn.ExecContext(ctx, schemaSQL); err != nil {
			return fmt.Errorf("failed to create migrations schema: %w", err)
		}
	}

	// Deliberately outside the migration writer for the same reason as schema
	// creation: there is no active migration transaction yet.
	if _, err := m.conn.ExecContext(ctx, m.createMigrationsTableSQL()); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	if !m.revisionTableFormat.isAtlas() {
		if err := m.ensureMigrationsVersionColumn(ctx); err != nil {
			return fmt.Errorf("failed to prepare migrations version column: %w", err)
		}
		if err := m.ensureMigrationsRevisionColumns(ctx); err != nil {
			return fmt.Errorf("failed to prepare migrations revision columns: %w", err)
		}
	}

	// Mark as initialized
	m.initialized = true
	return nil
}

func (m *Migrator) ensureMigrationsRevisionColumns(ctx context.Context) error {
	columns := []struct {
		name       string
		definition string
	}{
		{name: "state", definition: "VARCHAR(32) NOT NULL DEFAULT 'applied'"},
		{name: "applied", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "total", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "error", definition: "TEXT NULL"},
		{name: "error_stmt", definition: "TEXT NULL"},
		{name: "execution_time_ms", definition: "BIGINT NOT NULL DEFAULT 0"},
		{name: "checksum", definition: "VARCHAR(64) NOT NULL DEFAULT ''"},
	}
	for _, column := range columns {
		if err := m.ensureMigrationsRevisionColumn(ctx, column.name, column.definition); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) ensureMigrationsRevisionColumn(ctx context.Context, name, definition string) error {
	exists, err := m.migrationsColumnExists(ctx, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD %s %s",
		m.qualifiedMigrationsTable(),
		m.quoteIdentifier(name),
		m.migrationsRevisionColumnDefinition(name, definition),
	)
	if _, err := m.conn.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to add migrations metadata column %s: %w", name, err)
	}
	return nil
}

func (m *Migrator) migrationsRevisionColumnDefinition(name, fallback string) string {
	if !m.isSQLServer() {
		return fallback
	}
	switch name {
	case "state":
		return "NVARCHAR(32) NOT NULL DEFAULT 'applied'"
	case "error", "error_stmt":
		return "NVARCHAR(MAX) NULL"
	case "checksum":
		return "NVARCHAR(64) NOT NULL DEFAULT ''"
	default:
		return fallback
	}
}

func (m *Migrator) migrationsColumnExists(ctx context.Context, name string) (bool, error) {
	switch m.conn.Info().Dialect {
	case platform.ClickHouse:
		return m.clickHouseMigrationsColumnExists(ctx, name)
	case platform.SQLite:
		return m.sqliteMigrationsColumnExists(ctx, name)
	}
	query := sqlutil.Rebind(m.conn.Info().Dialect, `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = ?`)
	var count int
	if err := m.conn.QueryRowContext(ctx, query, m.metadataSchemaName(), m.migrationsTableName(), name).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	return count > 0, nil
}

func (m *Migrator) sqliteMigrationsColumnExists(ctx context.Context, name string) (bool, error) {
	conn, err := m.conn.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "PRAGMA table_info("+m.quoteIdentifier(m.migrationsTableName())+")")
	if err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid          int
			columnName   string
			dataType     string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &columnName, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("failed to scan migrations metadata column %s: %w", name, err)
		}
		if columnName == name {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	return false, nil
}

func (m *Migrator) clickHouseMigrationsColumnExists(ctx context.Context, name string) (bool, error) {
	var count int
	if err := m.conn.QueryRowContext(
		ctx,
		`SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = ?`,
		m.migrationsTableName(),
		name,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", name, err)
	}
	return count > 0, nil
}

func (m *Migrator) ensureMigrationsVersionColumn(ctx context.Context) error {
	switch m.conn.Info().Dialect {
	case "postgres", "cockroachdb", "yugabytedb":
		return m.ensurePostgresMigrationsVersionColumn(ctx)
	case "mysql", "mariadb":
		return m.ensureMySQLMigrationsVersionColumn(ctx)
	default:
		return nil
	}
}

func (m *Migrator) ensurePostgresMigrationsVersionColumn(ctx context.Context) error {
	dataType, err := m.migrationsVersionColumnType(
		ctx,
		sqlutil.Rebind(m.conn.Info().Dialect, `
SELECT data_type
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = 'version'`),
	)
	if err != nil {
		return err
	}
	if dataType == "bigint" {
		return nil
	}
	_, err = m.conn.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s TYPE BIGINT",
		m.qualifiedMigrationsTable(),
		m.quoteIdentifier("version"),
	))
	if err != nil {
		return fmt.Errorf("failed to widen version column from %s to BIGINT: %w", dataType, err)
	}
	return nil
}

func (m *Migrator) ensureMySQLMigrationsVersionColumn(ctx context.Context) error {
	dataType, err := m.migrationsVersionColumnType(
		ctx,
		`SELECT data_type
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = 'version'`,
	)
	if err != nil {
		return err
	}
	if dataType == "bigint" {
		return nil
	}
	_, err = m.conn.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE %s MODIFY COLUMN %s BIGINT NOT NULL",
		m.qualifiedMigrationsTable(),
		m.quoteIdentifier("version"),
	))
	if err != nil {
		return fmt.Errorf("failed to widen version column from %s to BIGINT: %w", dataType, err)
	}
	return nil
}

func (m *Migrator) migrationsVersionColumnType(ctx context.Context, query string) (string, error) {
	var dataType string
	err := m.conn.QueryRowContext(ctx, query, m.metadataSchemaName(), m.migrationsTableName()).Scan(&dataType)
	if err != nil {
		return "", fmt.Errorf("failed to inspect migrations version column: %w", err)
	}
	return strings.ToLower(dataType), nil
}

func (m *Migrator) metadataSchemaName() string {
	return metadataInformationSchemaName(m.connectionDialect(), m.connectionSchemaName(), m.migrationsSchema)
}

func metadataInformationSchemaName(dialect, connectionSchema, configuredSchema string) string {
	if schema := metadataTableSchemaName(dialect, connectionSchema, configuredSchema); schema != "" {
		return schema
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres:
		return "public"
	case platform.MySQL, platform.MariaDB:
		return strings.TrimSpace(connectionSchema)
	}
	return ""
}

func (m *Migrator) metadataTableSchemaName() string {
	return metadataTableSchemaName(m.connectionDialect(), m.connectionSchemaName(), m.migrationsSchema)
}

func metadataTableSchemaName(dialect, connectionSchema, configuredSchema string) string {
	if schema := strings.TrimSpace(configuredSchema); schema != "" {
		return schema
	}
	if platform.NormalizeDialect(dialect) != platform.SQLServer {
		return ""
	}
	if schema := strings.TrimSpace(connectionSchema); schema != "" {
		return schema
	}
	return "dbo"
}

func (m *Migrator) migrationsTableName() string {
	if m.migrationsTable == "" {
		return m.defaultMigrationsTable()
	}
	return m.migrationsTable
}

func (m *Migrator) sqlServerObjectName() string {
	if schema := m.metadataTableSchemaName(); schema != "" {
		return m.quoteIdentifier(schema) + "." + m.quoteIdentifier(m.migrationsTableName())
	}
	return m.quoteIdentifier(m.migrationsTableName())
}

// GetCurrentVersion returns the current migration version from the database
func (m *Migrator) GetCurrentVersion(ctx context.Context) (int64, error) {
	// First ensure the migrations table exists
	if err := m.Initialize(ctx); err != nil {
		return 0, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if m.conn.Writer().IsDryRun() {
		return 0, nil
	}

	// Query the current version
	return m.scanCurrentVersion(ctx)
}

// GetAppliedMigrations returns a list of applied migration versions
func (m *Migrator) GetAppliedMigrations(ctx context.Context) ([]int64, error) {
	return queryMigrationRows(
		ctx,
		m,
		m.getAppliedMigrationsSQL(),
		m.scanAppliedVersion,
		"failed to query applied migrations",
		"failed to scan migration version",
		"error iterating migration rows",
	)
}

// GetAppliedRevisions returns full metadata rows for applied migrations.
func (m *Migrator) GetAppliedRevisions(ctx context.Context) ([]MigrationRevision, error) {
	return queryMigrationRows(
		ctx,
		m,
		m.getAppliedRevisionsSQL(),
		m.scanRevisionRow,
		"failed to query applied migration revisions",
		"failed to scan migration revision",
		"error iterating migration revision rows",
	)
}

func queryMigrationRows[T any](
	ctx context.Context,
	m *Migrator,
	query string,
	scan func(rowScanner) (T, error),
	queryErr string,
	scanErr string,
	iterErr string,
) ([]T, error) {
	if err := m.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if m.conn.Writer().IsDryRun() {
		return []T{}, nil
	}

	rows, err := m.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", queryErr, err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]T, 0)
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", scanErr, err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", iterErr, err)
	}
	return items, nil
}

func (m *Migrator) scanCurrentVersion(ctx context.Context) (int64, error) {
	row := m.conn.QueryRowContext(ctx, m.getVersionSQL())
	var version int64
	if err := row.Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}
	return version, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (m *Migrator) scanAppliedVersion(row rowScanner) (int64, error) {
	if m.revisionTableFormat.isAtlas() {
		var version string
		if err := row.Scan(&version); err != nil {
			return 0, err
		}
		return parseAtlasRevisionVersion(version)
	}
	var version int64
	if err := row.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func parseAtlasRevisionVersion(version string) (int64, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(version), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Atlas revision version %q is not a numeric Ptah migration version: %w", version, err)
	}
	return parsed, nil
}

// GetPendingMigrations returns a list of pending migration versions
func (m *Migrator) GetPendingMigrations(ctx context.Context) ([]int64, error) {
	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	return pendingMigrationVersions(m.migrationProvider.Migrations(), applied), nil
}

// GetPreviousMigrationVersion finds the previous migration version compared to the current one.
// Returns an error and -1 if no previous migrations exist.
func (m *Migrator) GetPreviousMigrationVersion(ctx context.Context) (int64, error) {
	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to get applied migrations: %w", err)
	}
	if len(applied) == 0 {
		return -1, fmt.Errorf("no previous migrations exist")
	}
	if len(applied) == 1 {
		return 0, nil
	}

	return applied[len(applied)-2], nil
}

// GetMigrationStatus returns information about the current migration status using the provided filesystem
func (m *Migrator) GetMigrationStatus(ctx context.Context) (status *MigrationStatus, err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.status", m.operationAttributes("")...)
	defer func() { span.End(err) }()

	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)
	pendingMigrations := pendingMigrationVersions(m.MigrationProvider().Migrations(), appliedMigrations)
	outOfOrderMigrations := outOfOrderMigrationVersions(pendingMigrations, currentVersion)
	var dirtyRevision *MigrationRevision
	if !m.conn.Writer().IsDryRun() {
		var err error
		dirtyRevision, err = m.dirtyRevision(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get dirty migration revision: %w", err)
		}
	}

	status = &MigrationStatus{
		CurrentVersion:       currentVersion,
		AppliedMigrations:    appliedMigrations,
		PendingMigrations:    pendingMigrations,
		OutOfOrderMigrations: outOfOrderMigrations,
		TotalMigrations:      len(m.MigrationProvider().Migrations()),
		HasPendingChanges:    len(pendingMigrations) > 0 || dirtyRevision != nil,
		DirtyRevision:        dirtyRevision,
	}
	span.SetAttributes(
		attr("migration.current_version", status.CurrentVersion),
		attr("migration.pending_count", len(status.PendingMigrations)),
		attr("migration.out_of_order_count", len(status.OutOfOrderMigrations)),
		attr("migration.total_count", status.TotalMigrations),
	)
	return status, nil
}

// MigrateUp migrates the database up to the latest version
func (m *Migrator) MigrateUp(ctx context.Context) error {
	return m.MigrateUpWithPreflight(ctx, nil)
}

// MigrateUpWithPreflight migrates up after running hook inside the migration
// advisory lock. A nil hook is equivalent to [Migrator.MigrateUp].
func (m *Migrator) MigrateUpWithPreflight(ctx context.Context, hook PreMigrationHook) (err error) {
	return m.MigrateUpWithOptions(ctx, MigrateUpOptions{Preflight: hook})
}

// MigrateUpWithOptions migrates up using an explicitly selected apply plan.
func (m *Migrator) MigrateUpWithOptions(ctx context.Context, opts MigrateUpOptions) (err error) {
	if err := validateMigrateUpOptions(opts); err != nil {
		return err
	}
	observer := m.migrationObserver()
	attrs := m.operationAttributes(MigrationDirectionUp)
	if opts.TargetVersion > 0 {
		attrs = append(attrs, attr("migration.requested_target_version", opts.TargetVersion))
	}
	if opts.Amount > 0 {
		attrs = append(attrs, attr("migration.requested_amount", opts.Amount))
	}
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.up", attrs...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate up", func(ctx context.Context) error {
		return m.migrateUpLocked(ctx, opts)
	})
}

func validateMigrateUpOptions(opts MigrateUpOptions) error {
	if opts.TargetVersion < 0 {
		return fmt.Errorf("target version must be greater than or equal to zero")
	}
	if opts.TargetVersion > 0 && opts.Amount > 0 {
		return fmt.Errorf("target version and amount cannot both be set")
	}
	return nil
}

func (m *Migrator) migrateUpLocked(ctx context.Context, opts MigrateUpOptions) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if !opts.AllowDirty {
		if err := m.failIfDirty(ctx); err != nil {
			return err
		}
	}

	migrations := m.migrationProvider.Migrations()
	if opts.TargetVersion > 0 && !hasMigrationVersion(migrations, opts.TargetVersion) {
		return fmt.Errorf("target version %d was not found in the migration provider", opts.TargetVersion)
	}

	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	appliedMigrations = mergeAppliedVersions(appliedMigrations, opts.AssumedAppliedVersions)
	currentVersion := maxAppliedVersion(appliedMigrations)

	if err := m.verifyAppliedMigrationChecksums(ctx, migrations); err != nil {
		return err
	}

	migrationsToApply, err := m.migrationsToApply(migrations, appliedMigrations, opts.TargetVersion)
	if err != nil {
		return err
	}
	migrationsToApply = limitMigrationsToApply(migrationsToApply, opts.Amount)
	if err := m.validateUpTransactionMode(migrationsToApply); err != nil {
		return err
	}
	if err := runPreMigrationHook(ctx, opts.Preflight, MigrationPlan{
		Direction:      MigrationDirectionUp,
		CurrentVersion: currentVersion,
		TargetVersion:  upTargetVersion(currentVersion, migrationsToApply),
		Versions:       migrationVersions(migrationsToApply),
	}); err != nil {
		return err
	}
	if span := rootSpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attr("migration.current_version", currentVersion),
			attr("migration.target_version", upTargetVersion(currentVersion, migrationsToApply)),
			attr("migration.pending_count", len(migrationsToApply)),
		)
	}

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "totalMigrations", len(migrations))
	if err := m.applyUpMigrations(ctx, migrationsToApply); err != nil {
		return err
	}

	m.logger.Info("All migrations applied successfully")
	return nil
}

func hasMigrationVersion(migrations []*Migration, version int64) bool {
	for _, migration := range migrations {
		if migration.Version == version {
			return true
		}
	}
	return false
}

func limitMigrationsToApply(migrations []*Migration, amount uint64) []*Migration {
	if amount == 0 || amount >= uint64(len(migrations)) {
		return migrations
	}
	return migrations[:amount]
}

func mergeAppliedVersions(applied []int64, assumed []int64) []int64 {
	if len(assumed) == 0 {
		return applied
	}
	merged := make([]int64, 0, len(applied)+len(assumed))
	seen := make(map[int64]struct{}, len(applied)+len(assumed))
	for _, version := range applied {
		if _, ok := seen[version]; ok {
			continue
		}
		seen[version] = struct{}{}
		merged = append(merged, version)
	}
	for _, version := range assumed {
		if _, ok := seen[version]; ok {
			continue
		}
		seen[version] = struct{}{}
		merged = append(merged, version)
	}
	slices.Sort(merged)
	return merged
}

// MigrateDown migrates the database down to the previous version
func (m *Migrator) MigrateDown(ctx context.Context) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.down", m.operationAttributes(MigrationDirectionDown)...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate down", func(ctx context.Context) error {
		return m.migrateDownLocked(ctx)
	})
}

func (m *Migrator) migrateDownLocked(ctx context.Context) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}

	targetVersion, err := m.GetPreviousMigrationVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get previous version: %w", err)
	}

	return m.migrateDownToLocked(ctx, targetVersion, nil)
}

// MigrateDownTo migrates the database down to the specified target version
func (m *Migrator) MigrateDownTo(ctx context.Context, targetVersion int64) error {
	return m.MigrateDownToWithPreflight(ctx, targetVersion, nil)
}

// MigrateDownToWithPreflight migrates down after running hook inside the
// migration advisory lock. A nil hook is equivalent to [Migrator.MigrateDownTo].
func (m *Migrator) MigrateDownToWithPreflight(ctx context.Context, targetVersion int64, hook PreMigrationHook) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.down", append(m.operationAttributes(MigrationDirectionDown), attr("migration.requested_target_version", targetVersion))...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate down", func(ctx context.Context) error {
		return m.migrateDownToLocked(ctx, targetVersion, hook)
	})
}

func (m *Migrator) migrateDownToLocked(ctx context.Context, targetVersion int64, hook PreMigrationHook) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}

	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)

	// Skip if already at or below target version (shouldn't happen)
	if targetVersion >= currentVersion {
		m.logger.Info("Already at or below target version", "targetVersion", targetVersion, "currentVersion", currentVersion)
		return nil
	}

	migrationMap := migrationsByVersion(m.migrationProvider.Migrations())
	if err := m.verifyAppliedMigrationChecksums(ctx, m.migrationProvider.Migrations()); err != nil {
		return err
	}
	migrationsToRollback, err := migrationsToRollback(migrationMap, appliedMigrations, targetVersion)
	if err != nil {
		return err
	}
	if err := runPreMigrationHook(ctx, hook, MigrationPlan{
		Direction:      MigrationDirectionDown,
		CurrentVersion: currentVersion,
		TargetVersion:  downTargetVersion(appliedMigrations, targetVersion),
		Versions:       migrationVersions(migrationsToRollback),
	}); err != nil {
		return err
	}
	if span := rootSpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attr("migration.current_version", currentVersion),
			attr("migration.target_version", downTargetVersion(appliedMigrations, targetVersion)),
			attr("migration.pending_count", len(migrationsToRollback)),
		)
	}

	m.logger.Info("Migrating down", "targetVersion", targetVersion, "currentVersion", currentVersion, "totalMigrations", len(m.migrationProvider.Migrations()))

	// Rebind once: template + dialect are loop-invariant. Migration version
	// is bound as a parameter via the dialect-native placeholder.
	deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())

	for _, migration := range migrationsToRollback {
		if err := m.rollbackMigration(ctx, migration, deleteSQL); err != nil {
			return err
		}
	}

	m.logger.Info("All migrations rolled back successfully")
	return nil
}

// MigrateTo migrates the database to a specific version (up or down)
func (m *Migrator) MigrateTo(ctx context.Context, targetVersion int64) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.to", append(m.operationAttributes(""), attr("migration.requested_target_version", targetVersion))...)
	defer func() { span.End(err) }()
	ctx = contextWithRootSpan(ctx, span)
	return m.withMigrationLock(ctx, "migrate to", func(ctx context.Context) error {
		return m.migrateToLocked(ctx, targetVersion)
	})
}

func (m *Migrator) migrateToLocked(ctx context.Context, targetVersion int64) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if err := m.failIfDirty(ctx); err != nil {
		return err
	}

	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)

	if targetVersion == currentVersion {
		m.logger.Info("Already at target version", "version", targetVersion)
		return nil
	}

	if targetVersion > currentVersion {
		// Migrate up to target version
		return m.migrateUpTo(ctx, targetVersion)
	}

	if targetVersion > 0 && !slices.Contains(appliedMigrations, targetVersion) {
		return fmt.Errorf("target version %d is below current version %d but is not applied", targetVersion, currentVersion)
	}

	// Migrate down to target version
	return m.migrateDownToLocked(ctx, targetVersion, nil)
}

// MigrationProvider returns the migration provider
func (m *Migrator) MigrationProvider() MigrationProvider {
	return m.migrationProvider
}

// migrateUpTo migrates the database up to a specific version
func (m *Migrator) migrateUpTo(ctx context.Context, targetVersion int64) error {
	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)

	migrations := m.migrationProvider.Migrations()
	if err := m.verifyAppliedMigrationChecksums(ctx, migrations); err != nil {
		return err
	}
	migrationsToApply, err := m.migrationsToApply(migrations, appliedMigrations, targetVersion)
	if err != nil {
		return err
	}
	if err := m.validateUpTransactionMode(migrationsToApply); err != nil {
		return err
	}
	if span := rootSpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attr("migration.current_version", currentVersion),
			attr("migration.target_version", upTargetVersion(currentVersion, migrationsToApply)),
			attr("migration.pending_count", len(migrationsToApply)),
		)
	}

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "targetVersion", targetVersion, "totalMigrations", len(migrations))
	if err := m.applyUpMigrations(ctx, migrationsToApply); err != nil {
		return err
	}

	m.logger.Info("Migrated successfully", "targetVersion", targetVersion)
	return nil
}

func runPreMigrationHook(ctx context.Context, hook PreMigrationHook, plan MigrationPlan) error {
	if hook == nil || len(plan.Versions) == 0 {
		return nil
	}
	plan.Versions = slices.Clone(plan.Versions)
	return hook(ctx, plan)
}

func migrationVersions(migrations []*Migration) []int64 {
	versions := make([]int64, 0, len(migrations))
	for _, migration := range migrations {
		versions = append(versions, migration.Version)
	}
	return versions
}

func maxMigrationVersion(migrations []*Migration) int64 {
	var maxVersion int64
	for _, migration := range migrations {
		if migration.Version > maxVersion {
			maxVersion = migration.Version
		}
	}
	return maxVersion
}

func upTargetVersion(currentVersion int64, migrations []*Migration) int64 {
	return max(currentVersion, maxMigrationVersion(migrations))
}

func downTargetVersion(applied []int64, targetVersion int64) int64 {
	var finalVersion int64
	for _, version := range applied {
		if version <= targetVersion && version > finalVersion {
			finalVersion = version
		}
	}
	return finalVersion
}

func (m *Migrator) applyUpMigrations(ctx context.Context, migrations []*Migration) error {
	switch m.txMode {
	case MigrationTxModeNone:
		return m.applyUpMigrationsNoTransaction(ctx, migrations)
	case MigrationTxModeAll:
		return m.applyUpMigrationsInSingleTransaction(ctx, migrations)
	default:
		return m.applyUpMigrationsPerFile(ctx, migrations)
	}
}

func (m *Migrator) applyUpMigrationsPerFile(ctx context.Context, migrations []*Migration) error {
	for _, migration := range migrations {
		if err := m.applyUpMigrationObserved(ctx, migration); err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) applyUpMigrationsNoTransaction(ctx context.Context, migrations []*Migration) error {
	if err := m.validateUpTransactionMode(migrations); err != nil {
		return err
	}
	for _, migration := range migrations {
		if err := m.applyUpMigrationForcedNoTransactionObserved(ctx, migration); err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) applyUpMigrationsInSingleTransaction(ctx context.Context, migrations []*Migration) error {
	if len(migrations) == 0 {
		return nil
	}
	if err := m.validateUpTransactionMode(migrations); err != nil {
		return err
	}

	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx-mode all transaction: %w", err)
	}
	txConn := m.conn.WithExecutor(tx)
	startedAt := make(map[int64]time.Time, len(migrations))
	for _, migration := range migrations {
		startedAt[migration.Version] = time.Now()
		if err := m.applyUpMigrationInExistingTransaction(ctx, txConn, migration, startedAt[migration.Version]); err != nil {
			_ = tx.Rollback()
			return m.recordRolledBackBatchFailure(ctx, migration, startedAt[migration.Version], err)
		}
		if err := m.recordAppliedMigrationOn(ctx, txConn, migration, startedAt[migration.Version]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to record migration %d in tx-mode all transaction: %w", migration.Version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit tx-mode all transaction: %w", err)
	}
	m.logger.Info("Applied migrations in one transaction", "count", len(migrations))
	return nil
}

func (m *Migrator) validateUpTransactionMode(migrations []*Migration) error {
	switch m.txMode {
	case MigrationTxModeAll:
		if err := m.validateTxModeAllDialect(); err != nil {
			return err
		}
		for _, migration := range migrations {
			if migration.upExecutionMode() == migrationExecutionNoTransaction {
				return fmt.Errorf("migration %d is marked no_transaction and cannot run with tx-mode all", migration.Version)
			}
			if !mergeMigrationTimeouts(m.defaultTimeouts, migration.UpTimeouts).IsZero() {
				return fmt.Errorf("migration %d has timeouts and cannot run with tx-mode all", migration.Version)
			}
			if err := m.rejectChecksUnderTxModeAll(migration); err != nil {
				return err
			}
		}
	case MigrationTxModeNone:
		for _, migration := range migrations {
			if !mergeMigrationTimeouts(m.defaultTimeouts, migration.UpTimeouts).IsZero() {
				return fmt.Errorf("migration %d has timeouts and cannot run with tx-mode none", migration.Version)
			}
		}
	}
	return nil
}

func (m *Migrator) validateTxModeAllDialect() error {
	dialect := platform.NormalizeDialect(m.conn.Info().Dialect)
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.SQLite:
		return nil
	default:
		return fmt.Errorf("tx-mode all is not supported for dialect %q", m.conn.Info().Dialect)
	}
}

func (m *Migrator) applyUpMigrationObserved(ctx context.Context, migration *Migration) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.apply", m.migrationAttributes(MigrationDirectionUp, migration)...)
	startedAt := time.Now()
	defer func() {
		duration := time.Since(startedAt)
		span.End(err)
		metricAttrs := m.migrationMetricAttributes(MigrationDirectionUp, migration)
		observer.RecordDuration(ctx, "ptah_migration_duration_seconds", duration, metricAttrs...)
		if err != nil {
			observer.AddCounter(ctx, "ptah_migrations_failed_total", 1, metricAttrs...)
			return
		}
		observer.AddCounter(ctx, "ptah_migrations_applied_total", 1, metricAttrs...)
	}()

	m.logger.Info("Applying migration", "version", migration.Version, "description", migration.Description)
	if err := m.beginMigrationRevision(ctx, migration); err != nil {
		return fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
	}
	if migration.upExecutionMode() == migrationExecutionNoTransaction {
		return m.applyUpMigrationNoTransaction(ctx, migration, startedAt)
	}
	return m.applyUpMigrationTransactional(ctx, migration, startedAt)
}

func (m *Migrator) applyUpMigrationForcedNoTransactionObserved(ctx context.Context, migration *Migration) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.apply", m.migrationAttributes(MigrationDirectionUp, migration)...)
	startedAt := time.Now()
	defer func() {
		duration := time.Since(startedAt)
		span.End(err)
		metricAttrs := m.migrationMetricAttributes(MigrationDirectionUp, migration)
		observer.RecordDuration(ctx, "ptah_migration_duration_seconds", duration, metricAttrs...)
		if err != nil {
			observer.AddCounter(ctx, "ptah_migrations_failed_total", 1, metricAttrs...)
			return
		}
		observer.AddCounter(ctx, "ptah_migrations_applied_total", 1, metricAttrs...)
	}()

	return m.applyUpMigrationForcedNoTransactionAt(ctx, migration, startedAt)
}

func (m *Migrator) applyUpMigrationForcedNoTransactionAt(ctx context.Context, migration *Migration, startedAt time.Time) error {
	if err := m.beginMigrationRevision(ctx, migration); err != nil {
		return fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
	}
	return m.applyUpMigrationNoTransaction(ctx, migration, startedAt)
}

func (m *Migrator) applyUpMigrationInExistingTransaction(
	ctx context.Context,
	txConn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
) error {
	m.logger.Info("Applying migration in tx-mode all", "version", migration.Version, "description", migration.Description)
	// Pre-migration checks are rejected under tx-mode all by
	// validateUpTransactionMode, because a check on the pool connection cannot
	// observe earlier batched migrations' uncommitted changes and would evaluate
	// against stale state. Nothing to run here.
	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, mergeMigrationTimeouts(m.defaultTimeouts, migration.UpTimeouts))
	if err != nil {
		return fmt.Errorf("failed to apply timeouts for migration %d: %w", migration.Version, err)
	}
	if err := migration.Up(ctx, txConn); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
	}
	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		return err
	}
	m.logger.Info("Applied migration in tx-mode all", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) recordRolledBackBatchFailure(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
) error {
	if beginErr := m.beginMigrationRevision(ctx, migration); beginErr != nil {
		return fmt.Errorf("%w; additionally failed to record pending migration %d after tx-mode all rollback: %v", failure, migration.Version, beginErr)
	}
	return m.failMigrationWithDirtyStateWithMode(
		ctx,
		migration,
		startedAt,
		failure,
		migration.UpSQL,
		"",
		MigrationTxModeAll,
	)
}

func (m *Migrator) recordAppliedMigrationOn(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migration *Migration,
	startedAt time.Time,
) error {
	if err := m.beginMigrationRevisionOn(ctx, conn, migration); err != nil {
		return fmt.Errorf("failed to record pending migration %d: %w", migration.Version, err)
	}
	if err := m.completeMigrationRevisionOn(ctx, conn, migration, startedAt); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
	}
	return nil
}

func (m *Migrator) applyUpMigrationTransactional(ctx context.Context, migration *Migration, startedAt time.Time) error {
	// Pre-migration checks read committed state and run before the transaction
	// opens. A check cannot execute inside the migration transaction (the schema
	// executor exposes no query path), and running it on the pool while the tx
	// already holds a connection would deadlock a single-connection pool. Running
	// it first, on the pool, reads the correct pre-migration state and aborts with
	// nothing applied before any statement or transaction runs.
	if err := m.runMigrationChecks(ctx, m.conn, migration); err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("pre-migration check failed for migration %d", migration.Version),
		)
	}

	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to begin transaction for migration %d", migration.Version),
		)
	}
	txConn := m.conn.WithExecutor(tx)

	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, mergeMigrationTimeouts(m.defaultTimeouts, migration.UpTimeouts))
	if err != nil {
		_ = tx.Rollback()
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to apply timeouts for migration %d", migration.Version),
		)
	}

	if err := migration.Up(ctx, txConn); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		_ = tx.Rollback()
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to apply migration %d", migration.Version),
		)
	}

	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		_ = tx.Rollback()
		return m.failMigrationWithDirtyState(ctx, migration, startedAt, err, migration.UpSQL, "")
	}

	if err := tx.Commit(); err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to commit transaction for migration %d", migration.Version),
		)
	}
	if err := m.completeMigrationRevision(ctx, migration, startedAt); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
	}

	m.logger.Info("Applied migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) applyUpMigrationNoTransaction(ctx context.Context, migration *Migration, startedAt time.Time) error {
	if err := ensureNoTransactionHasNoTimeouts(migration.Version, mergeMigrationTimeouts(m.defaultTimeouts, migration.UpTimeouts)); err != nil {
		return m.failMigrationWithDirtyStateWithMode(ctx, migration, startedAt, err, migration.UpSQL, "", MigrationTxModeNone)
	}
	if err := m.runMigrationChecks(ctx, m.conn, migration); err != nil {
		return m.failMigrationWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("pre-migration check failed for migration %d", migration.Version),
			MigrationTxModeNone,
		)
	}
	if err := migration.Up(ctx, m.conn); err != nil {
		return m.failMigrationWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.UpSQL,
			fmt.Sprintf("failed to apply migration %d", migration.Version),
			MigrationTxModeNone,
		)
	}
	if err := m.completeMigrationRevision(ctx, migration, startedAt); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
	}
	m.logger.Info("Applied non-transactional migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) rollbackMigration(ctx context.Context, migration *Migration, deleteSQL string) error {
	return m.rollbackMigrationObserved(ctx, migration, deleteSQL)
}

func (m *Migrator) rollbackMigrationObserved(ctx context.Context, migration *Migration, deleteSQL string) (err error) {
	observer := m.migrationObserver()
	ctx, span := observer.StartSpan(ctx, "ptah.migrate.rollback", m.migrationAttributes(MigrationDirectionDown, migration)...)
	startedAt := time.Now()
	defer func() {
		duration := time.Since(startedAt)
		span.End(err)
		metricAttrs := m.migrationMetricAttributes(MigrationDirectionDown, migration)
		observer.RecordDuration(ctx, "ptah_migration_duration_seconds", duration, metricAttrs...)
		if err != nil {
			observer.AddCounter(ctx, "ptah_migrations_failed_total", 1, metricAttrs...)
			return
		}
		observer.AddCounter(ctx, "ptah_migrations_rolled_back_total", 1, metricAttrs...)
	}()

	m.logger.Info("Rolling back migration", "version", migration.Version, "description", migration.Description)
	if err := m.beginRollbackRevision(ctx, migration); err != nil {
		return fmt.Errorf("failed to record pending rollback %d: %w", migration.Version, err)
	}
	if migration.downUnavailable {
		return m.rollbackMigrationWithoutRegisteredDown(ctx, migration, startedAt, deleteSQL)
	}
	if migration.downExecutionMode() == migrationExecutionNoTransaction {
		return m.rollbackMigrationNoTransaction(ctx, migration, startedAt, deleteSQL)
	}
	return m.rollbackMigrationTransactional(ctx, migration, startedAt, deleteSQL)
}

func (m *Migrator) operationAttributes(direction MigrationDirection) []ObservationAttribute {
	attrs := []ObservationAttribute{
		attr("db.system", m.connectionDialect()),
	}
	if direction != "" {
		attrs = append(attrs, attr("migration.direction", string(direction)))
	}
	return attrs
}

func (m *Migrator) migrationAttributes(direction MigrationDirection, migration *Migration) []ObservationAttribute {
	return []ObservationAttribute{
		attr("db.system", m.connectionDialect()),
		attr("migration.direction", string(direction)),
		attr("migration.version", migration.Version),
		attr("migration.description", migration.Description),
	}
}

func (m *Migrator) migrationMetricAttributes(direction MigrationDirection, migration *Migration) []ObservationAttribute {
	return []ObservationAttribute{
		attr("db.system", m.connectionDialect()),
		attr("migration.direction", string(direction)),
		attr("migration.version", migration.Version),
	}
}

func (m *Migrator) rollbackMigrationWithoutRegisteredDown(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
) error {
	if err := migration.Down(ctx, m.conn); err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to revert migration %d", migration.Version),
		)
	}
	if err := executeSQLOutsideTransaction(ctx, m.conn, deleteSQL, m.revisionVersionArg(migration.Version)); err != nil {
		return fmt.Errorf("failed to record migration reversion %d: %w", migration.Version, err)
	}
	m.logger.Info("Rolled back migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) rollbackMigrationTransactional(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
) error {
	tx, err := m.conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to begin transaction for migration %d", migration.Version),
		)
	}
	txConn := m.conn.WithExecutor(tx)

	restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, txConn, mergeMigrationTimeouts(m.defaultTimeouts, migration.DownTimeouts))
	if err != nil {
		_ = tx.Rollback()
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to apply timeouts for migration %d", migration.Version),
		)
	}

	if err := migration.Down(ctx, txConn); err != nil {
		err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
		_ = tx.Rollback()
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to revert migration %d", migration.Version),
		)
	}

	if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
		_ = tx.Rollback()
		return m.failMigrationWithDirtyState(ctx, migration, startedAt, err, migration.DownSQL, "")
	}

	if err := txConn.Writer().ExecuteSQL(ctx, deleteSQL, m.revisionVersionArg(migration.Version)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to record migration reversion %d: %w", migration.Version, err)
	}

	if err := tx.Commit(); err != nil {
		return m.failMigrationWithDirtyState(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to commit transaction for migration %d", migration.Version),
		)
	}

	m.logger.Info("Rolled back migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) rollbackMigrationNoTransaction(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	deleteSQL string,
) error {
	if err := ensureNoTransactionHasNoTimeouts(migration.Version, mergeMigrationTimeouts(m.defaultTimeouts, migration.DownTimeouts)); err != nil {
		return m.failMigrationWithDirtyStateWithMode(ctx, migration, startedAt, err, migration.DownSQL, "", MigrationTxModeNone)
	}
	if err := migration.Down(ctx, m.conn); err != nil {
		return m.failMigrationWithDirtyStateWithMode(
			ctx,
			migration,
			startedAt,
			err,
			migration.DownSQL,
			fmt.Sprintf("failed to revert migration %d", migration.Version),
			MigrationTxModeNone,
		)
	}
	if err := executeSQLOutsideTransaction(ctx, m.conn, deleteSQL, m.revisionVersionArg(migration.Version)); err != nil {
		return fmt.Errorf("failed to record migration reversion %d: %w", migration.Version, err)
	}
	m.logger.Info("Rolled back non-transactional migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) failMigrationWithDirtyState(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	prefix string,
) error {
	return m.failMigrationWithDirtyStateWithMode(ctx, migration, startedAt, failure, sqlText, prefix, MigrationTxModeFile)
}

func (m *Migrator) failMigrationWithDirtyStateWithMode(
	ctx context.Context,
	migration *Migration,
	startedAt time.Time,
	failure error,
	sqlText string,
	prefix string,
	txMode MigrationTxMode,
) error {
	if revisionErr := m.failMigrationRevisionWithMode(ctx, migration, startedAt, failure, sqlText, txMode); revisionErr != nil {
		if prefix == "" {
			return fmt.Errorf("%w; additionally failed to record dirty migration state: %v", failure, revisionErr)
		}
		return fmt.Errorf("%s: %w; additionally failed to record dirty migration state: %v", prefix, failure, revisionErr)
	}
	if prefix == "" {
		return failure
	}
	return fmt.Errorf("%s: %w", prefix, failure)
}

func ensureNoTransactionHasNoTimeouts(version int64, timeouts MigrationTimeouts) error {
	if timeouts.IsZero() {
		return nil
	}
	return fmt.Errorf("migration %d is marked no_transaction, so migration timeouts cannot be applied safely", version)
}

func (m *Migrator) migrationsToApply(migrations []*Migration, applied []int64, targetVersion int64) ([]*Migration, error) {
	currentVersion := maxAppliedVersion(applied)
	pendingVersions := pendingMigrationVersions(migrations, applied)
	outOfOrderVersions := outOfOrderMigrationVersions(pendingVersions, currentVersion)
	execOrder := normalizeExecOrder(m.execOrder)

	if execOrder == ExecOrderLinear && len(outOfOrderVersions) > 0 {
		return nil, NewOutOfOrderError(currentVersion, outOfOrderVersions)
	}

	appliedSet := versionSet(applied)
	migrationsToApply := make([]*Migration, 0, len(pendingVersions))
	for _, migration := range migrations {
		if _, ok := appliedSet[migration.Version]; ok {
			continue
		}
		if targetVersion > 0 && migration.Version > targetVersion {
			continue
		}
		if execOrder == ExecOrderLinearSkip && migration.Version < currentVersion {
			m.logger.Warn("Skipping out-of-order migration", "version", migration.Version, "currentVersion", currentVersion)
			continue
		}
		migrationsToApply = append(migrationsToApply, migration)
	}

	return migrationsToApply, nil
}

func pendingMigrationVersions(migrations []*Migration, applied []int64) []int64 {
	appliedSet := versionSet(applied)
	pending := make([]int64, 0, len(migrations))
	for _, migration := range migrations {
		if _, ok := appliedSet[migration.Version]; ok {
			continue
		}
		pending = append(pending, migration.Version)
	}
	slices.Sort(pending)
	return pending
}

func outOfOrderMigrationVersions(pending []int64, currentVersion int64) []int64 {
	outOfOrder := make([]int64, 0)
	for _, version := range pending {
		if version < currentVersion {
			outOfOrder = append(outOfOrder, version)
		}
	}
	return outOfOrder
}

func maxAppliedVersion(applied []int64) int64 {
	if len(applied) == 0 {
		return 0
	}
	return slices.Max(applied)
}

func versionSet(versions []int64) map[int64]struct{} {
	set := make(map[int64]struct{}, len(versions))
	for _, version := range versions {
		set[version] = struct{}{}
	}
	return set
}

func migrationsByVersion(migrations []*Migration) map[int64]*Migration {
	result := make(map[int64]*Migration, len(migrations))
	for _, migration := range migrations {
		result[migration.Version] = migration
	}
	return result
}

func migrationsToRollback(migrationsByVersion map[int64]*Migration, applied []int64, targetVersion int64) ([]*Migration, error) {
	rollbackVersions := make([]int64, 0, len(applied))
	for _, version := range applied {
		if version > targetVersion {
			rollbackVersions = append(rollbackVersions, version)
		}
	}
	sort.Slice(rollbackVersions, func(i, j int) bool { return rollbackVersions[i] > rollbackVersions[j] })

	rollbackMigrations := make([]*Migration, 0, len(rollbackVersions))
	for _, version := range rollbackVersions {
		migration, ok := migrationsByVersion[version]
		if !ok {
			return nil, fmt.Errorf("applied migration %d is above target version %d but is missing from the migration provider", version, targetVersion)
		}
		rollbackMigrations = append(rollbackMigrations, migration)
	}
	return rollbackMigrations, nil
}
