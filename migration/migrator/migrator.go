package migrator

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
)

// MigrationStatus represents the current state of migrations
type MigrationStatus struct {
	CurrentVersion       int64   `json:"current_version"`
	AppliedMigrations    []int64 `json:"applied_migrations"`
	PendingMigrations    []int64 `json:"pending_migrations"`
	OutOfOrderMigrations []int64 `json:"out_of_order_migrations"`
	TotalMigrations      int     `json:"total_migrations"`
	HasPendingChanges    bool    `json:"has_pending_changes"`
}

// Migrator handles database migrations for ptah
type Migrator struct {
	conn                 *dbschema.DatabaseConnection
	migrationProvider    MigrationProvider
	defaultTimeouts      MigrationTimeouts
	migrationsTable      string
	migrationsSchema     string
	execOrder            ExecOrder
	migrationLockTimeout time.Duration
	initialized          bool
	logger               *slog.Logger
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
		conn:              conn,
		migrationProvider: provider,
		migrationsTable:   "schema_migrations",
		execOrder:         ExecOrderLinear,
		logger:            slog.Default(),
	}
}

// WithLogger sets the logger for the migrator
func (m *Migrator) WithLogger(l *slog.Logger) *Migrator {
	tmp := *m
	tmp.logger = l
	return &tmp
}

// WithExecOrder sets how this migrator handles pending migrations whose
// version is below the current high-water mark.
func (m *Migrator) WithExecOrder(execOrder ExecOrder) *Migrator {
	tmp := *m
	tmp.execOrder = normalizeExecOrder(execOrder)
	return &tmp
}

// WithMigrationsTable sets the table used to record applied migrations.
func (m *Migrator) WithMigrationsTable(schema, table string) *Migrator {
	tmp := *m
	tmp.migrationsSchema = strings.TrimSpace(schema)
	tmp.migrationsTable = strings.TrimSpace(table)
	if tmp.migrationsTable == "" {
		tmp.migrationsTable = "schema_migrations"
	}
	tmp.initialized = false
	return &tmp
}

func (m *Migrator) qualifiedMigrationsTable() string {
	table := m.migrationsTable
	if table == "" {
		table = "schema_migrations"
	}
	if m.migrationsSchema == "" {
		return m.quoteIdentifier(table)
	}
	return m.quoteIdentifier(m.migrationsSchema) + "." + m.quoteIdentifier(table)
}

func (m *Migrator) migrationsSchemaStatement() string {
	if m.migrationsSchema == "" {
		return ""
	}
	return "CREATE SCHEMA IF NOT EXISTS " + m.quoteIdentifier(m.migrationsSchema)
}

func (m *Migrator) quoteIdentifier(identifier string) string {
	if m.conn == nil {
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
	switch m.conn.Info().Dialect {
	case "mysql", "mariadb", "clickhouse":
		return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
	}
}

func (m *Migrator) createMigrationsTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL
)`, m.qualifiedMigrationsTable())
}

func (m *Migrator) getVersionSQL() string {
	return fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s", m.qualifiedMigrationsTable())
}

func (m *Migrator) getAppliedMigrationsSQL() string {
	return fmt.Sprintf("SELECT version FROM %s ORDER BY version", m.qualifiedMigrationsTable())
}

func (m *Migrator) recordMigrationSQL() string {
	return fmt.Sprintf("INSERT INTO %s (version, description, applied_at) VALUES (?, ?, ?)", m.qualifiedMigrationsTable())
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
	_, err := m.conn.ExecContext(ctx, m.createMigrationsTableSQL())
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	if err := m.ensureMigrationsVersionColumn(ctx); err != nil {
		return fmt.Errorf("failed to prepare migrations version column: %w", err)
	}

	// Mark as initialized
	m.initialized = true
	return nil
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
	if m.migrationsSchema != "" {
		return m.migrationsSchema
	}
	if m.conn != nil {
		if schema := strings.TrimSpace(m.conn.Info().Schema); schema != "" {
			return schema
		}
	}
	if m.conn != nil && m.conn.Info().Dialect == "postgres" {
		return "public"
	}
	return ""
}

func (m *Migrator) migrationsTableName() string {
	if m.migrationsTable == "" {
		return "schema_migrations"
	}
	return m.migrationsTable
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
	var version int64
	row := m.conn.QueryRowContext(ctx, m.getVersionSQL())
	if err := row.Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}

	return version, nil
}

// GetAppliedMigrations returns a list of applied migration versions
func (m *Migrator) GetAppliedMigrations(ctx context.Context) ([]int64, error) {
	// First ensure the migrations table exists
	if err := m.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize migrations table: %w", err)
	}
	if m.conn.Writer().IsDryRun() {
		return []int64{}, nil
	}

	// Query all applied migration versions
	rows, err := m.conn.Query(m.getAppliedMigrationsSQL())
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make([]int64, 0)
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("failed to scan migration version: %w", err)
		}
		applied = append(applied, version)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration rows: %w", err)
	}

	return applied, nil
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
func (m *Migrator) GetMigrationStatus(ctx context.Context) (*MigrationStatus, error) {
	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)
	pendingMigrations := pendingMigrationVersions(m.MigrationProvider().Migrations(), appliedMigrations)
	outOfOrderMigrations := outOfOrderMigrationVersions(pendingMigrations, currentVersion)

	return &MigrationStatus{
		CurrentVersion:       currentVersion,
		AppliedMigrations:    appliedMigrations,
		PendingMigrations:    pendingMigrations,
		OutOfOrderMigrations: outOfOrderMigrations,
		TotalMigrations:      len(m.MigrationProvider().Migrations()),
		HasPendingChanges:    len(pendingMigrations) > 0,
	}, nil
}

// MigrateUp migrates the database up to the latest version
func (m *Migrator) MigrateUp(ctx context.Context) error {
	return m.withMigrationLock(ctx, "migrate up", m.migrateUpLocked)
}

func (m *Migrator) migrateUpLocked(ctx context.Context) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	appliedMigrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}
	currentVersion := maxAppliedVersion(appliedMigrations)

	migrations := m.migrationProvider.Migrations()
	migrationsToApply, err := m.migrationsToApply(migrations, appliedMigrations, 0)
	if err != nil {
		return err
	}

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "totalMigrations", len(migrations))
	if err := m.applyUpMigrations(ctx, migrationsToApply); err != nil {
		return err
	}

	m.logger.Info("All migrations applied successfully")
	return nil
}

// MigrateDown migrates the database down to the previous version
func (m *Migrator) MigrateDown(ctx context.Context) error {
	return m.withMigrationLock(ctx, "migrate down", func(ctx context.Context) error {
		return m.migrateDownLocked(ctx)
	})
}

func (m *Migrator) migrateDownLocked(ctx context.Context) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	targetVersion, err := m.GetPreviousMigrationVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get previous version: %w", err)
	}

	return m.migrateDownToLocked(ctx, targetVersion)
}

// MigrateDownTo migrates the database down to the specified target version
func (m *Migrator) MigrateDownTo(ctx context.Context, targetVersion int64) error {
	return m.withMigrationLock(ctx, "migrate down", func(ctx context.Context) error {
		return m.migrateDownToLocked(ctx, targetVersion)
	})
}

func (m *Migrator) migrateDownToLocked(ctx context.Context, targetVersion int64) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
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
	migrationsToRollback, err := migrationsToRollback(migrationMap, appliedMigrations, targetVersion)
	if err != nil {
		return err
	}

	m.logger.Info("Migrating down", "targetVersion", targetVersion, "currentVersion", currentVersion, "totalMigrations", len(m.migrationProvider.Migrations()))

	// Rebind once: template + dialect are loop-invariant. Migration version
	// is bound as a parameter via the dialect-native placeholder.
	deleteSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.deleteMigrationSQL())

	for _, migration := range migrationsToRollback {
		m.logger.Info("Rolling back migration", "version", migration.Version, "description", migration.Description)
		if migration.downUnavailable {
			if err := migration.Down(ctx, m.conn); err != nil {
				return fmt.Errorf("failed to revert migration %d: %w", migration.Version, err)
			}
			continue
		}
		if migration.NoTransaction {
			if err := m.rollbackMigrationNoTransaction(ctx, migration, deleteSQL); err != nil {
				return err
			}
			continue
		}

		// Begin transaction for this migration rollback
		if err := m.conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, mergeMigrationTimeouts(m.defaultTimeouts, migration.DownTimeouts))
		if err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to apply timeouts for migration %d: %w", migration.Version, err)
		}

		// Apply down migration
		if err := migration.Down(ctx, m.conn); err != nil {
			err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to revert migration %d: %w", migration.Version, err)
		}

		if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return err
		}

		// Remove migration record.
		if err := m.conn.Writer().ExecuteSQL(ctx, deleteSQL, migration.Version); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to record migration reversion %d: %w", migration.Version, err)
		}

		// Commit transaction
		if err := m.conn.Writer().CommitTransaction(); err != nil {
			return fmt.Errorf("failed to commit transaction for migration %d: %w", migration.Version, err)
		}

		m.logger.Info("Rolled back migration", "version", migration.Version, "description", migration.Description)
	}

	m.logger.Info("All migrations rolled back successfully")
	return nil
}

// MigrateTo migrates the database to a specific version (up or down)
func (m *Migrator) MigrateTo(ctx context.Context, targetVersion int64) error {
	return m.withMigrationLock(ctx, "migrate to", func(ctx context.Context) error {
		return m.migrateToLocked(ctx, targetVersion)
	})
}

func (m *Migrator) migrateToLocked(ctx context.Context, targetVersion int64) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
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
	return m.migrateDownToLocked(ctx, targetVersion)
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
	migrationsToApply, err := m.migrationsToApply(migrations, appliedMigrations, targetVersion)
	if err != nil {
		return err
	}

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "targetVersion", targetVersion, "totalMigrations", len(migrations))
	if err := m.applyUpMigrations(ctx, migrationsToApply); err != nil {
		return err
	}

	m.logger.Info("Migrated successfully", "targetVersion", targetVersion)
	return nil
}

func (m *Migrator) applyUpMigrations(ctx context.Context, migrations []*Migration) error {
	// Rebind once: the template and dialect are loop-invariant. Values are
	// still bound as native driver parameters via these placeholders so we
	// never interpolate user-controlled strings into the SQL text.
	recordSQL := sqlutil.Rebind(m.conn.Info().Dialect, m.recordMigrationSQL())

	for _, migration := range migrations {
		m.logger.Info("Applying migration", "version", migration.Version, "description", migration.Description)
		if migration.NoTransaction {
			if err := m.applyUpMigrationNoTransaction(ctx, migration, recordSQL); err != nil {
				return err
			}
			continue
		}

		// Begin transaction for this migration
		if err := m.conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		restoreTimeouts, err := m.applyTimeoutsWithRestore(ctx, mergeMigrationTimeouts(m.defaultTimeouts, migration.UpTimeouts))
		if err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to apply timeouts for migration %d: %w", migration.Version, err)
		}

		// Apply migration
		if err := migration.Up(ctx, m.conn); err != nil {
			err = m.restoreTimeoutsAfterFailure(ctx, migration.Version, restoreTimeouts, err)
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
		}

		if err := m.restoreTimeouts(ctx, migration.Version, restoreTimeouts); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return err
		}

		// Record migration via parameter binding.
		if err := m.conn.Writer().ExecuteSQL(ctx, recordSQL, migration.Version, migration.Description, time.Now()); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
		}

		// Commit transaction
		if err := m.conn.Writer().CommitTransaction(); err != nil {
			return fmt.Errorf("failed to commit transaction for migration %d: %w", migration.Version, err)
		}

		m.logger.Info("Applied migration", "version", migration.Version, "description", migration.Description)
	}

	return nil
}

func (m *Migrator) applyUpMigrationNoTransaction(ctx context.Context, migration *Migration, recordSQL string) error {
	if err := ensureNoTransactionHasNoTimeouts(migration.Version, mergeMigrationTimeouts(m.defaultTimeouts, migration.UpTimeouts)); err != nil {
		return err
	}
	if err := migration.Up(ctx, m.conn); err != nil {
		return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
	}
	if err := executeSQLOutsideTransaction(ctx, m.conn, recordSQL, migration.Version, migration.Description, time.Now()); err != nil {
		return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
	}
	m.logger.Info("Applied non-transactional migration", "version", migration.Version, "description", migration.Description)
	return nil
}

func (m *Migrator) rollbackMigrationNoTransaction(ctx context.Context, migration *Migration, deleteSQL string) error {
	if err := ensureNoTransactionHasNoTimeouts(migration.Version, mergeMigrationTimeouts(m.defaultTimeouts, migration.DownTimeouts)); err != nil {
		return err
	}
	if err := migration.Down(ctx, m.conn); err != nil {
		return fmt.Errorf("failed to revert migration %d: %w", migration.Version, err)
	}
	if err := executeSQLOutsideTransaction(ctx, m.conn, deleteSQL, migration.Version); err != nil {
		return fmt.Errorf("failed to record migration reversion %d: %w", migration.Version, err)
	}
	m.logger.Info("Rolled back non-transactional migration", "version", migration.Version, "description", migration.Description)
	return nil
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
