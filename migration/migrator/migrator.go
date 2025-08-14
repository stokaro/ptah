package migrator

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"

	"github.com/stokaro/ptah/dbschema"
)

// MigrationStatus represents the current state of migrations
type MigrationStatus struct {
	CurrentVersion    int   `json:"current_version"`
	PendingMigrations []int `json:"pending_migrations"`
	TotalMigrations   int   `json:"total_migrations"`
	HasPendingChanges bool  `json:"has_pending_changes"`
}

// Migrator handles database migrations for ptah
type Migrator struct {
	conn              *dbschema.DatabaseConnection
	migrationProvider MigrationProvider
	initialized       bool
	logger            *slog.Logger
}

// NewFSMigrator creates a new migrator that loads migrations from a filesystem.
// It scans the provided filesystem for migration files following the naming convention
// NNNNNNNNNN_description.up.sql and NNNNNNNNNN_description.down.sql and automatically
// registers them with the migrator. Returns an error if the filesystem cannot be scanned
// or if any migrations are incomplete (missing up or down files).
func NewFSMigrator(conn *dbschema.DatabaseConnection, fsys fs.FS) (*Migrator, error) {
	provider, err := NewFSMigrationProvider(fsys)
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
		logger:            slog.Default(),
	}
}

// WithLogger sets the logger for the migrator
func (m *Migrator) WithLogger(l *slog.Logger) *Migrator {
	tmp := *m
	tmp.logger = l
	return &tmp
}

// Initialize creates the migrations table if it doesn't exist
func (m *Migrator) Initialize(ctx context.Context) error {
	// Skip if already initialized
	if m.initialized {
		return nil
	}

	// Execute the schema creation SQL directly on the database connection
	// This avoids transaction conflicts with the PostgreSQL writer
	_, err := m.conn.ExecContext(ctx, migrationsSchemaSQL)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Mark as initialized
	m.initialized = true
	return nil
}

// GetCurrentVersion returns the current migration version from the database
func (m *Migrator) GetCurrentVersion(ctx context.Context) (int, error) {
	// First ensure the migrations table exists
	if err := m.Initialize(ctx); err != nil {
		return 0, fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	// Query the current version
	var version int
	row := m.conn.QueryRowContext(ctx, getVersionSQL)
	if err := row.Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}

	return version, nil
}

// GetAppliedMigrations returns a list of applied migration versions
func (m *Migrator) GetAppliedMigrations(ctx context.Context) ([]int, error) {
	// First ensure the migrations table exists
	if err := m.Initialize(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	// Query all applied migration versions
	rows, err := m.conn.Query("SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var applied []int
	for rows.Next() {
		var version int
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
func (m *Migrator) GetPendingMigrations(ctx context.Context) ([]int, error) {
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return nil, err
	}

	migrations := m.migrationProvider.Migrations()

	var pending []int
	for _, migration := range migrations {
		if migration.Version > currentVersion {
			pending = append(pending, migration.Version)
		}
	}

	sort.Ints(pending)
	return pending, nil
}

// GetPreviousMigrationVersion finds the previous migration version compared to the current one.
// Returns an error and -1 if no previous migrations exist.
func (m *Migrator) GetPreviousMigrationVersion(ctx context.Context) (int, error) {
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return -1, fmt.Errorf("failed to get current version: %w", err)
	}

	// If current version is 0, there are no previous migrations
	if currentVersion == 0 {
		return -1, fmt.Errorf("no previous migrations exist")
	}

	migrations := m.migrationProvider.Migrations()

	previousVersion := -1
	for _, migration := range migrations {
		if migration.Version >= currentVersion {
			break
		}
		previousVersion = migration.Version
	}

	return previousVersion, nil
}

// GetMigrationStatus returns information about the current migration status using the provided filesystem
func (m *Migrator) GetMigrationStatus(ctx context.Context) (*MigrationStatus, error) {
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current version: %w", err)
	}

	pendingMigrations, err := m.GetPendingMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending migrations: %w", err)
	}

	return &MigrationStatus{
		CurrentVersion:    currentVersion,
		PendingMigrations: pendingMigrations,
		TotalMigrations:   len(m.MigrationProvider().Migrations()),
		HasPendingChanges: len(pendingMigrations) > 0,
	}, nil
}

// MigrateUp migrates the database up to the latest version
func (m *Migrator) MigrateUp(ctx context.Context) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	// Get the current version
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	migrations := m.migrationProvider.Migrations()

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "totalMigrations", len(migrations))

	// Apply migrations that are newer than current version
	for _, migration := range migrations {
		if migration.Version <= currentVersion {
			m.logger.Info("Skipping migration", "version", migration.Version, "description", migration.Description)
			continue
		}

		m.logger.Info("Applying migration", "version", migration.Version, "description", migration.Description)

		// Begin transaction for this migration
		if err := m.conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		// Apply migration
		if err := migration.Up(ctx, m.conn); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
		}

		// Record migration
		timestamp := FormatTimestampForDatabase(m.conn.Info().Dialect)
		recordSQL := fmt.Sprintf(recordMigrationSQL, migration.Version, migration.Description, timestamp)
		if err := m.conn.Writer().ExecuteSQL(recordSQL); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
		}

		// Commit transaction
		if err := m.conn.Writer().CommitTransaction(); err != nil {
			return fmt.Errorf("failed to commit transaction for migration %d: %w", migration.Version, err)
		}

		m.logger.Info("Applied migration", "version", migration.Version, "description", migration.Description)
	}

	m.logger.Info("All migrations applied successfully")
	return nil
}

// MigrateDown migrates the database down to the previous version
func (m *Migrator) MigrateDown(ctx context.Context) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	targetVersion, err := m.GetPreviousMigrationVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get previous version: %w", err)
	}

	return m.MigrateDownTo(ctx, targetVersion)
}

// MigrateDownTo migrates the database down to the specified target version
func (m *Migrator) MigrateDownTo(ctx context.Context, targetVersion int) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	// Get the current version
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	// Skip if already at or below target version (shouldn't happen)
	if targetVersion >= currentVersion {
		m.logger.Info("Already at or below target version", "targetVersion", targetVersion, "currentVersion", currentVersion)
		return nil
	}

	migrations := m.migrationProvider.Migrations()

	m.logger.Info("Migrating down", "targetVersion", targetVersion, "currentVersion", currentVersion, "totalMigrations", len(migrations))

	// Sort migrations by version in descending order for rollback
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version > migrations[j].Version
	})

	// Apply down migrations for versions greater than target
	for _, migration := range migrations {
		if migration.Version <= targetVersion || migration.Version > currentVersion {
			continue
		}

		m.logger.Info("Rolling back migration", "version", migration.Version, "description", migration.Description)

		// Begin transaction for this migration rollback
		if err := m.conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		// Apply down migration
		if err := migration.Down(ctx, m.conn); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to revert migration %d: %w", migration.Version, err)
		}

		// Remove migration record
		deleteSQL := fmt.Sprintf(deleteMigrationSQL, migration.Version)
		if err := m.conn.Writer().ExecuteSQL(deleteSQL); err != nil {
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
func (m *Migrator) MigrateTo(ctx context.Context, targetVersion int) error {
	// Initialize the migrations table
	if err := m.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize migrations table: %w", err)
	}

	// Get the current version
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	if targetVersion == currentVersion {
		m.logger.Info("Already at target version", "version", targetVersion)
		return nil
	}

	if targetVersion > currentVersion {
		// Migrate up to target version
		return m.migrateUpTo(ctx, targetVersion)
	}

	// Migrate down to target version
	return m.MigrateDownTo(ctx, targetVersion)
}

// MigrationProvider returns the migration provider
func (m *Migrator) MigrationProvider() MigrationProvider {
	return m.migrationProvider
}

// migrateUpTo migrates the database up to a specific version
func (m *Migrator) migrateUpTo(ctx context.Context, targetVersion int) error {
	// Get the current version
	currentVersion, err := m.GetCurrentVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}

	migrations := m.migrationProvider.Migrations()

	m.logger.Info("Migrating up", "currentVersion", currentVersion, "targetVersion", targetVersion, "totalMigrations", len(migrations))

	// Apply migrations up to target version
	for _, migration := range migrations {
		if migration.Version <= currentVersion || migration.Version > targetVersion {
			continue
		}

		m.logger.Info("Applying migration", "version", migration.Version, "description", migration.Description)

		// Begin transaction for this migration
		if err := m.conn.Writer().BeginTransaction(); err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		// Apply migration
		if err := migration.Up(ctx, m.conn); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
		}

		// Record migration
		timestamp := FormatTimestampForDatabase(m.conn.Info().Dialect)
		recordSQL := fmt.Sprintf(recordMigrationSQL, migration.Version, migration.Description, timestamp)
		if err := m.conn.Writer().ExecuteSQL(recordSQL); err != nil {
			_ = m.conn.Writer().RollbackTransaction()
			return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
		}

		// Commit transaction
		if err := m.conn.Writer().CommitTransaction(); err != nil {
			return fmt.Errorf("failed to commit transaction for migration %d: %w", migration.Version, err)
		}

		m.logger.Info("Applied migration", "version", migration.Version, "description", migration.Description)
	}

	m.logger.Info("Migrated successfully", "targetVersion", targetVersion)
	return nil
}
